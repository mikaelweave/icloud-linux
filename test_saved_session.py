from contextlib import ExitStack
import base64
import io
import json
import multiprocessing
from pathlib import Path
import tempfile
import threading
import time
import unittest
from unittest.mock import patch

from pyicloud.cookie_jar import PyiCloudCookieJar
from pyicloud.exceptions import PyiCloudAPIResponseException, PyiCloudFailedLoginException
from requests import ConnectionError, Response
from requests.cookies import create_cookie

from saved_session import (
    BoundedTransport, Publication, ResumeService, SavedSession, SessionStore,
    SessionUnavailable, State, failure_kind,
)


USER = "synthetic@example.test"


def trusted():
    return {
        "dsInfo": {"dsid": "123", "hsaVersion": 2, "appleId": USER},
        "hsaTrustedBrowser": True,
        "hsaChallengeRequired": False,
        "webservices": {
            "drivews": {"url": "https://drive.example.test"},
            "docws": {"url": "https://docs.example.test"},
        },
    }


def response(payload, status=200, headers=None):
    result = Response()
    result.status_code = status
    result._content = json.dumps(payload).encode()
    result.headers.update({"Content-Type": "application/json"})
    result.headers.update(headers or {})
    result.url = "https://setup.icloud.com/setup/ws/1/validate"
    return result


def publish_in_process(directory, results, purpose=Publication.STARTUP):
    try:
        store = SessionStore(directory, USER)
        candidate = store.begin(purpose=purpose)
        results.put(store.publish(candidate))
        candidate.close()
    except Exception as error:
        results.put(type(error).__name__)


def hold_startup_in_process(directory, ready, release):
    with SessionStore(directory, USER)._lock("resume.lock", nonblocking=True):
        ready.set()
        release.wait(timeout=10)


class RedirectTests(unittest.TestCase):
    def test_streaming_get_follows_redirect(self):
        with tempfile.TemporaryDirectory() as root:
            owner = SavedSession(USER, root)
            client = owner._client(SessionStore(root, USER))
            self.addCleanup(owner.candidate.close)
            self.addCleanup(client.session.close)
            calls = []

            def send(adapter, request, **kwargs):
                calls.append((request.method, request.url))
                reply = Response()
                reply.request = request
                reply.url = request.url
                if len(calls) == 1:
                    reply.status_code = 302
                    reply.headers["Location"] = "https://download.example.test/file"
                    reply._content = b""
                else:
                    reply.status_code = 200
                    reply.raw = io.BytesIO(b"synthetic file")
                    reply._content_consumed = False
                return reply

            with patch("requests.adapters.HTTPAdapter.send", autospec=True, side_effect=send):
                result = client.session.get("https://drive.example.test/file", stream=True)
                self.assertEqual(result.raw.read(), b"synthetic file")
            self.assertEqual(calls, [
                ("GET", "https://drive.example.test/file"),
                ("GET", "https://download.example.test/file"),
            ])

    def test_remote_mutation_redirect_is_not_replayed(self):
        for status in (307, 308):
            for method in ("POST", "PUT", "DELETE"):
                with self.subTest(status=status, method=method), tempfile.TemporaryDirectory() as root:
                    owner = SavedSession(USER, root)
                    client = owner._client(SessionStore(root, USER))
                    self.addCleanup(owner.candidate.close)
                    self.addCleanup(client.session.close)
                    reply = response({}, status, {"Location": "https://other.example.test/upload"})
                    reply.url = "https://drive.example.test/upload"
                    with patch("requests.adapters.HTTPAdapter.send", return_value=reply) as send:
                        with self.assertRaises(PyiCloudAPIResponseException):
                            client.session.request(method, reply.url, json={"data": "synthetic"})
                    self.assertEqual(send.call_count, 1)


class SavedSessionTests(unittest.TestCase):
    def setUp(self):
        self.stack = ExitStack()
        self.addCleanup(self.stack.close)
        self.root = Path(self.stack.enter_context(tempfile.TemporaryDirectory()))
        self.store = SessionStore(self.root, USER)
        self.calls = []
        self.replies = []
        self.stack.enter_context(patch(
            "requests.sessions.Session.request", autospec=True, side_effect=self.transport,
        ))
        for name in (
            "authenticate", "_authenticate", "_srp_authentication",
            "_get_mfa_auth_options", "_request_sms_2fa_code", "request_2fa_code",
            "validate_2fa_code", "trust_session",
            "send_verification_code", "validate_verification_code",
            "_authenticate_with_credentials_service",
        ):
            self.stack.enter_context(patch.object(
                ResumeService, name, side_effect=AssertionError("forbidden " + name),
            ))
        self.stack.enter_context(patch(
            "pyicloud.base.get_password_from_keyring",
            side_effect=AssertionError("keyring access forbidden"),
        ))
        self.stack.enter_context(patch(
            "builtins.input", side_effect=AssertionError("interactive input forbidden"),
        ))

    def seed(self, cookie=True, token=True, upload=True, expired=False):
        session, cookiejar = self.store.names
        data = {"client_id": "synthetic-client", "trust_token": "saved-trust"}
        if token:
            data["session_token"] = "saved-token"
        (self.root / session).write_text(json.dumps(data))
        jar = PyiCloudCookieJar(filename=str(self.root / cookiejar))
        if cookie:
            jar.set_cookie(create_cookie(
                "X-APPLE-WEBAUTH-TOKEN", "saved-cookie", domain=".icloud.com",
                expires=int(time.time()) + (-100 if expired else 3600),
            ))
        if upload:
            jar.set_cookie(create_cookie(
                "X-APPLE-WEBAUTH-VALIDATE", "v=1:t=upload-token", domain=".icloud.com",
                expires=int(time.time()) + 3600,
            ))
        jar.save(ignore_expires=expired)
        candidate = self.store.begin()
        self.store.publish(candidate)
        candidate.close()
        self.before = (self.store.root / "accepted.json").read_bytes()

    def transport(self, session, method, url, **kwargs):
        self.calls.append((method, url, kwargs))
        if not self.replies:
            raise AssertionError("unexpected network operation " + url)
        expected, reply = self.replies.pop(0)
        self.assertTrue(url.endswith("/" + expected), url)
        if isinstance(reply, BaseException):
            raise reply
        if callable(reply):
            return reply(session)
        return reply

    def resume(self):
        owner = SavedSession(USER, self.root)
        self.addCleanup(lambda: owner.candidate and owner.candidate.close())
        self.addCleanup(lambda: owner.client and owner.client.session.close())
        return owner.resume()

    def ready_replies(self, first="validate"):
        self.replies.extend([
            (first, response(trusted())),
            ("requestWebAccessState", response({"isICDRSDisabled": False})),
        ])

    def assert_preserved(self):
        self.assertEqual((self.store.root / "accepted.json").read_bytes(), self.before)

    def test_cookie_only_reuse_installs_complete_state_without_token_login(self):
        self.seed(token=False)
        self.ready_replies()
        owner = self.resume()
        self.assertEqual(owner.state, State.AUTHENTICATED)
        self.assertEqual(owner.client.params["dsid"], "123")
        self.assertEqual(owner.client.get_webservice_url("drivews"), "https://drive.example.test")
        self.assertEqual(len(self.calls), 2)
        self.assertFalse(self.replies)
        self.assertNotEqual(owner.candidate.directory, str(self.root))
        self.assertIsNone(owner.client._password_raw)
        self.assertEqual(owner.client.session.data["client_id"], "synthetic-client")
        self.assertIs(owner.resume(), owner)
        self.assertEqual(len(self.calls), 2)

    def test_token_reuse_without_cookie(self):
        self.seed(cookie=False)
        self.ready_replies("accountLogin")
        owner = self.resume()
        self.assertEqual(owner.state, State.AUTHENTICATED)
        self.assertEqual(self.calls[0][2]["json"]["trustToken"], "saved-trust")
        self.assertEqual(self.calls[0][2]["json"]["dsWebAuthToken"], "saved-token")

    def test_expired_cookie_is_not_revived(self):
        self.seed(expired=True)
        self.ready_replies("accountLogin")
        owner = self.resume()
        self.assertEqual(owner.state, State.AUTHENTICATED)
        self.assertEqual(len(self.calls), 2)

    def test_untrusted_successful_http_response_preserves_accepted_pair(self):
        self.seed(cookie=False)
        data = trusted()
        data["hsaTrustedBrowser"] = False

        def rejected(session):
            session.data["session_token"] = "rejected-replacement"
            session.cookies.set_cookie(create_cookie("replacement", "secret"))
            return response(data)

        self.replies.append(("accountLogin", rejected))
        owner = self.resume()
        self.assertEqual(owner.state, State.NEEDS_USER)
        self.assertIsNone(owner.client)
        self.assert_preserved()
        self.assertEqual(len(self.calls), 1)

    def test_validation_transient_errors_never_escalate_to_token(self):
        for reply in (response({}, 429), response({}, 500), ConnectionError("secret")):
            with self.subTest(reply=type(reply).__name__):
                self.seed()
                self.calls.clear()
                self.replies = [("validate", reply)]
                self.assertEqual(self.resume().state, State.RETRY_LATER)
                self.assertEqual(len(self.calls), 1)
                self.assert_preserved()

    def test_transient_startup_stays_parked_until_new_explicit_attempt(self):
        self.seed()
        self.replies = [("validate", response({}, 500))]
        first = self.resume()
        self.assertEqual(first.state, State.RETRY_LATER)
        first.resume()
        self.assertEqual(len(self.calls), 1)
        self.assert_preserved()

        self.ready_replies()
        restarted = self.resume()
        self.assertEqual(restarted.state, State.AUTHENTICATED)
        self.assertEqual(len(self.calls), 3)
        self.assertNotEqual((self.store.root / "accepted.json").read_bytes(), self.before)

    def test_wrapped_token_errors_are_transient(self):
        for reply in (response({}, 429), response({}, 500), ConnectionError("secret")):
            with self.subTest(reply=type(reply).__name__):
                self.seed(cookie=False)
                self.calls.clear()
                self.replies = [("accountLogin", reply)]
                self.assertEqual(self.resume().state, State.RETRY_LATER)
                self.assertEqual(len(self.calls), 1)
                self.assert_preserved()

    def test_cookie_rejection_gets_one_fresh_token_candidate(self):
        self.seed()

        def rejected(session):
            session.data["session_token"] = "failed-probe-token"
            return response({}, 401)

        self.replies.append(("validate", rejected))
        self.ready_replies("accountLogin")
        self.assertEqual(self.resume().state, State.AUTHENTICATED)
        self.assertEqual(self.calls[1][2]["json"]["dsWebAuthToken"], "saved-token")

    def test_token_rejection_is_terminal_and_not_retried(self):
        self.seed(cookie=False)
        self.replies.append(("accountLogin", response({}, 401)))
        owner = self.resume()
        self.assertEqual(owner.state, State.NEEDS_USER)
        owner.resume()
        self.assertEqual(len(self.calls), 1)
        self.assert_preserved()

    def test_missing_saved_state_does_not_contact_network(self):
        self.assertEqual(self.resume().state, State.NEEDS_USER)
        self.assertFalse(self.calls)

    def test_positive_untrusted_cookie_response_does_not_try_token(self):
        self.seed()
        data = trusted()
        data["hsaTrustedBrowser"] = False
        self.replies.append(("validate", response(data)))
        self.assertEqual(self.resume().state, State.NEEDS_USER)
        self.assertEqual(len(self.calls), 1)
        self.assert_preserved()

    def test_readiness_rejects_missing_state_terms_and_account_mismatch(self):
        cases = [
            ({}, State.RETRY_LATER),
            (dict(trusted(), webservices={}), State.RETRY_LATER),
            (dict(trusted(), termsUpdateNeeded=True), State.NEEDS_USER),
            (dict(trusted(), hsaChallengeRequired=True), State.NEEDS_USER),
            (dict(trusted(), dsInfo={"dsid": "123", "appleId": "other@example.test"}), State.NEEDS_USER),
        ]
        for data, expected in cases:
            with self.subTest(data=data):
                self.seed()
                self.replies = [("validate", response(data))]
                self.assertEqual(self.resume().state, expected)
                self.assert_preserved()

    def test_missing_upload_cookie_is_not_declared_ready(self):
        self.seed(upload=False)
        self.replies.append(("validate", response(trusted())))
        owner = self.resume()
        self.assertEqual(owner.state, State.NEEDS_USER)
        self.assertEqual(owner.reason, "upload-cookie-missing")
        self.assert_preserved()

    def test_pcs_consent_is_never_requested_or_skipped(self):
        cases = [
            ({}, "pcs-state-ambiguous"),
            ({"isICDRSDisabled": True}, "pcs-state-ambiguous"),
            ({"isICDRSDisabled": True, "isDeviceConsentedForPCS": False}, "pcs-consent-required"),
        ]
        for data, reason in cases:
            with self.subTest(data=data):
                self.seed()
                self.replies = [
                    ("validate", response(trusted())),
                    ("requestWebAccessState", response(data)),
                ]
                owner = self.resume()
                self.assertEqual(owner.state, State.NEEDS_USER)
                self.assertEqual(owner.reason, reason)
                self.assert_preserved()

    def test_already_consented_pcs_access_can_resume_without_notification(self):
        self.seed()
        self.replies = [
            ("validate", response(trusted())),
            ("requestWebAccessState", response({
                "isICDRSDisabled": True, "isDeviceConsentedForPCS": True,
            })),
            ("requestPCS", response({"status": "success"})),
        ]
        owner = self.resume()
        self.assertEqual(owner.state, State.AUTHENTICATED)
        self.assertEqual(self.calls[-1][2]["json"]["derivedFromUserAction"], False)
        owner.client.drive
        owner.client.drive
        self.assertEqual(len(self.calls), 3)
        self.assertFalse(any(url.endswith("enableDeviceConsentForPCS") for _, url, _ in self.calls))

    def test_pcs_not_ready_never_retries_or_replaces_session(self):
        for message in ("Cookies not available yet on server.",
                        "Requested the device to upload cookies."):
            with self.subTest(message=message):
                self.seed()
                self.calls.clear()
                self.replies = [
                    ("validate", response(trusted())),
                    ("requestWebAccessState", response({
                        "isICDRSDisabled": True, "isDeviceConsentedForPCS": True,
                    })),
                    ("requestPCS", response({"status": "pending", "message": message})),
                ]
                owner = self.resume()
                self.assertEqual(owner.state, State.RETRY_LATER)
                self.assertEqual(owner.reason, "pcs-pending")
                self.assert_preserved()
                self.assertEqual(len(self.calls), 3)
                self.assertFalse(any(url.endswith("enableDeviceConsentForPCS") for _, url, _ in self.calls))

    def test_unknown_pcs_result_is_not_assumed_pending_or_consented(self):
        for payload in ({}, {"message": "Cookies not available yet on server."},
                        {"status": "failure", "message": "unknown"}):
            with self.subTest(payload=payload):
                self.seed()
                self.replies = [
                    ("validate", response(trusted())),
                    ("requestWebAccessState", response({
                        "isICDRSDisabled": True, "isDeviceConsentedForPCS": True,
                    })),
                    ("requestPCS", response(payload)),
                ]
                owner = self.resume()
                self.assertEqual(owner.state, State.NEEDS_USER)
                self.assertEqual(owner.reason, "pcs-response-ambiguous")
                self.assert_preserved()

    def test_pcs_success_cannot_publish_missing_or_expired_upload_cookie(self):
        for action in ("remove", "expire"):
            with self.subTest(action=action):
                self.seed()
                self.calls.clear()
                def pcs_response(session):
                    for cookie in list(session.cookies):
                        if cookie.name == "X-APPLE-WEBAUTH-VALIDATE":
                            if action == "remove":
                                session.cookies.clear(cookie.domain, cookie.path, cookie.name)
                            else:
                                cookie.expires = int(time.time()) - 1
                    return response({"status": "success"})
                self.replies = [
                    ("validate", response(trusted())),
                    ("requestWebAccessState", response({
                        "isICDRSDisabled": True, "isDeviceConsentedForPCS": True,
                    })),
                    ("requestPCS", pcs_response),
                ]
                owner = self.resume()
                self.assertEqual(owner.state, State.NEEDS_USER)
                self.assertEqual(owner.reason, "upload-cookie-missing")
                self.assertIsNone(owner.client)
                self.assert_preserved()

    def test_cookie_expiry_during_publication_cannot_replace_accepted_bundle(self):
        import saved_session

        self.seed()
        original_write = saved_session._write_file
        active = []

        def pcs_response(session):
            active.append(session)
            return response({"status": "success"})

        def write(path, data):
            original_write(path, data)
            if Path(path).name.startswith("pending-"):
                for cookie in active[0].cookies:
                    if cookie.name == "X-APPLE-WEBAUTH-VALIDATE":
                        cookie.expires = int(time.time()) - 1

        self.replies = [
            ("validate", response(trusted())),
            ("requestWebAccessState", response({
                "isICDRSDisabled": True, "isDeviceConsentedForPCS": True,
            })),
            ("requestPCS", pcs_response),
        ]
        with patch("saved_session._write_file", side_effect=write):
            owner = self.resume()
        self.assertEqual(owner.state, State.NEEDS_USER)
        self.assertEqual(owner.reason, "upload-cookie-missing")
        self.assert_preserved()

    def test_directory_fsync_failure_restores_prior_bundle(self):
        import saved_session

        self.seed()
        self.ready_replies()
        original_sync = saved_session._sync_directory
        calls = []

        def fail_once(path):
            calls.append(path)
            if len(calls) == 1:
                raise OSError("synthetic directory fsync failure")
            return original_sync(path)

        with patch("saved_session._sync_directory", side_effect=fail_once):
            owner = self.resume()
        self.assertEqual(owner.state, State.RETRY_LATER)
        self.assert_preserved()
        self.assertGreaterEqual(len(calls), 2)

    def test_replace_that_takes_effect_then_raises_restores_prior_bundle(self):
        import saved_session

        self.seed()
        self.ready_replies()
        original_replace = saved_session.os.replace

        def replace(src, dst):
            original_replace(src, dst)
            if Path(src).name.startswith("pending-"):
                raise OSError("synthetic post-replace failure")

        with patch("saved_session.os.replace", side_effect=replace):
            owner = self.resume()
        self.assertEqual(owner.state, State.RETRY_LATER)
        self.assertEqual(owner.reason, "publication-failed")
        self.assert_preserved()

    def test_failed_rollback_is_reported_as_indeterminate(self):
        self.seed()
        self.ready_replies()
        with patch("saved_session._sync_directory", side_effect=OSError("synthetic fsync failure")):
            owner = self.resume()
        self.assertEqual(owner.state, State.INDETERMINATE)
        self.assertEqual(owner.reason, "publication-indeterminate")
        self.assertIsNone(owner.client)
        call_count = len(self.calls)
        owner.resume()
        self.assertEqual(len(self.calls), call_count)

    def test_rollback_replace_failure_is_indeterminate(self):
        import saved_session

        self.seed()
        self.ready_replies()
        original_replace = saved_session.os.replace

        def replace(src, dst):
            if Path(src).name.startswith("rollback-"):
                raise OSError("synthetic rollback failure")
            return original_replace(src, dst)

        with patch("saved_session._sync_directory", side_effect=OSError("synthetic fsync failure")), \
             patch("saved_session.os.replace", side_effect=replace):
            owner = self.resume()
        self.assertEqual(owner.state, State.INDETERMINATE)
        self.assertEqual(owner.reason, "publication-indeterminate")
        self.assertNotEqual((self.store.root / "accepted.json").read_bytes(), self.before)

    def test_changed_checkpoint_is_never_overwritten_during_rollback(self):
        import saved_session

        self.seed()
        self.ready_replies()
        newer = json.loads(self.before)
        newer["generation"] = "external-generation"
        newer_bytes = json.dumps(newer).encode()

        def interfere(path):
            external = self.store.root / "external-test"
            saved_session._write_file(external, newer_bytes)
            saved_session.os.replace(external, self.store.root / "accepted.json")
            raise OSError("synthetic fsync failure")

        with patch("saved_session._sync_directory", side_effect=interfere):
            owner = self.resume()
        self.assertEqual(owner.state, State.INDETERMINATE)
        self.assertEqual(owner.reason, "publication-indeterminate")
        self.assertEqual((self.store.root / "accepted.json").read_bytes(), newer_bytes)

    def test_failed_first_publication_restores_absence(self):
        import saved_session

        self.seed()
        checkpoint = self.store.root / "accepted.json"
        checkpoint.unlink()
        self.ready_replies()
        original_sync = saved_session._sync_directory
        calls = []

        def fail_once(path):
            calls.append(path)
            if len(calls) == 1:
                raise OSError("synthetic directory fsync failure")
            return original_sync(path)

        with patch("saved_session._sync_directory", side_effect=fail_once):
            owner = self.resume()
        self.assertEqual(owner.state, State.RETRY_LATER)
        self.assertEqual(owner.reason, "publication-failed")
        self.assertFalse(checkpoint.exists())

    def test_daemon_rollback_precedes_competing_interactive_publication(self):
        import saved_session

        self.seed()
        competitor = self.store.begin(purpose=Publication.INTERACTIVE)
        self.addCleanup(competitor.close)
        self.ready_replies()
        owner = SavedSession(USER, self.root)
        self.addCleanup(lambda: owner.candidate and owner.candidate.close())
        entered = threading.Event()
        release = threading.Event()
        started = threading.Event()
        finished = threading.Event()
        restored = []
        results = []
        original_sync = saved_session._sync_directory
        original_flock = saved_session.fcntl.flock

        def lock(fd, operation):
            if threading.current_thread().name == "interactive-publisher":
                started.set()
            return original_flock(fd, operation)

        def sync(path):
            if threading.current_thread().name == "daemon-publisher":
                if not entered.is_set():
                    entered.set()
                    if not release.wait(5):
                        raise TimeoutError("test timed out")
                    raise OSError("synthetic directory fsync failure")
                restored.append((self.store.root / "accepted.json").read_bytes())
            return original_sync(path)

        def publish_competitor():
            try:
                results.append(self.store.publish(competitor))
            finally:
                finished.set()

        first = threading.Thread(target=owner.resume, name="daemon-publisher")
        second = threading.Thread(target=publish_competitor, name="interactive-publisher")
        with patch("saved_session._sync_directory", side_effect=sync), \
             patch("saved_session.fcntl.flock", side_effect=lock):
            first.start()
            self.assertTrue(entered.wait(5))
            second.start()
            self.assertTrue(started.wait(5))
            try:
                self.assertFalse(finished.wait(0.2))
            finally:
                release.set()
            first.join(5)
            second.join(5)
        self.assertFalse(first.is_alive())
        self.assertFalse(second.is_alive())
        self.assertEqual(owner.reason, "publication-failed")
        self.assertEqual(restored, [self.before])
        self.assertEqual(len(results), 1)
        final = json.loads((self.store.root / "accepted.json").read_bytes())
        self.assertEqual(final["generation"], results[0])

    def test_expiry_after_atomic_replacement_is_not_reported_as_failed_publication(self):
        import saved_session

        self.seed()
        active = []
        original_replace = saved_session.os.replace

        def state(session):
            active.append(session)
            return response({"isICDRSDisabled": False})

        def replace(src, dst):
            original_replace(src, dst)
            if Path(src).name.startswith("pending-"):
                for cookie in active[0].cookies:
                    if cookie.name == "X-APPLE-WEBAUTH-VALIDATE":
                        cookie.expires = int(time.time()) - 1

        self.replies = [("validate", response(trusted())), ("requestWebAccessState", state)]
        with patch("saved_session.os.replace", side_effect=replace):
            owner = self.resume()
        self.assertEqual(owner.state, State.AUTHENTICATED)
        self.assertNotEqual((self.store.root / "accepted.json").read_bytes(), self.before)

    def test_421_allows_one_numeric_partition_correction(self):
        self.seed()
        self.replies.append(("validate", response({}, 421, {"x-apple-user-partition": "42"})))
        self.ready_replies()
        self.assertEqual(self.resume().state, State.AUTHENTICATED)
        self.assertIn("p42-setup.icloud.com", self.calls[1][1])

    def test_partition_correction_cannot_redirect_to_arbitrary_host(self):
        self.seed(cookie=False)
        self.replies.append(("accountLogin", response(
            {}, 421, {"x-apple-user-partition": "evil.example/"},
        )))
        self.assertEqual(self.resume().state, State.NEEDS_USER)
        self.assertEqual(len(self.calls), 1)
        self.assert_preserved()

    def test_routing_retry_is_bounded_even_if_server_repeats_421(self):
        self.seed(cookie=False)
        self.replies = [
            ("accountLogin", response({}, 421, {"x-apple-user-partition": "42"})),
            ("accountLogin", response({}, 421, {"x-apple-user-partition": "43"})),
        ]
        self.assertEqual(self.resume().state, State.NEEDS_USER)
        self.assertEqual(len(self.calls), 2)
        self.assert_preserved()

    def test_unsupported_dependency_is_rejected_before_io_or_network(self):
        with patch("saved_session.version", return_value="2.7.0"):
            owner = self.resume()
        self.assertEqual(owner.state, State.NEEDS_USER)
        self.assertEqual(owner.reason, "unsupported-pyicloud")
        self.assertFalse(self.calls)

    def test_ambiguous_cookie_does_not_choose_a_credential(self):
        self.seed()
        candidate = self.store.begin()
        jar = PyiCloudCookieJar(filename=str(Path(candidate.directory) / self.store.names[1]))
        jar.load()
        jar.set_cookie(create_cookie(
            "X-APPLE-WEBAUTH-TOKEN", "second-cookie", domain="setup.icloud.com",
            expires=int(time.time()) + 3600,
        ))
        jar.save()
        self.store.publish(candidate)
        candidate.close()
        owner = self.resume()
        self.assertEqual(owner.reason, "ambiguous-cookie")
        self.assertFalse(self.calls)

    def test_concurrent_calls_share_one_startup_attempt(self):
        self.seed()
        self.ready_replies()
        owner = SavedSession(USER, self.root)
        workers = [threading.Thread(target=owner.resume) for _ in range(3)]
        for worker in workers:
            worker.start()
        for worker in workers:
            worker.join(timeout=5)
            self.assertFalse(worker.is_alive())
        self.addCleanup(owner.candidate.close)
        self.assertEqual(owner.state, State.AUTHENTICATED)
        self.assertEqual(len(self.calls), 2)

    def test_failed_runtime_response_only_changes_private_copy(self):
        self.seed()
        self.ready_replies()
        owner = self.resume()
        checkpoint = (self.store.root / "accepted.json").read_bytes()
        self.replies.append(("read", response({}, 401)))
        with self.assertRaises(PyiCloudAPIResponseException):
            owner.client.session.post("https://drive.example.test/read")
        self.assertEqual((self.store.root / "accepted.json").read_bytes(), checkpoint)

    def test_new_interactive_checkpoint_wins_during_startup_probe(self):
        self.seed()

        def concurrent_auth(session):
            candidate = self.store.begin(purpose=Publication.INTERACTIVE)
            self.store.publish(candidate)
            candidate.close()
            self.newer = (self.store.root / "accepted.json").read_bytes()
            return response({"isICDRSDisabled": False})

        self.replies = [
            ("validate", response(trusted())),
            ("requestWebAccessState", concurrent_auth),
            ("validate", response(trusted())),
            ("requestWebAccessState", response({"isICDRSDisabled": False})),
        ]
        owner = self.resume()
        self.assertEqual(owner.state, State.AUTHENTICATED)
        self.assertEqual(owner.generation, json.loads(self.newer)["generation"])
        self.assertEqual(len(self.calls), 4)
        self.assertEqual((self.store.root / "accepted.json").read_bytes(), self.newer)

    def test_second_adoption_race_stops_without_another_exchange_or_publication(self):
        self.seed()

        def concurrent_auth(session):
            candidate = self.store.begin(purpose=Publication.INTERACTIVE)
            self.store.publish(candidate)
            candidate.close()
            self.newer = (self.store.root / "accepted.json").read_bytes()
            return response({"isICDRSDisabled": False})

        self.replies = [
            ("validate", response(trusted())), ("requestWebAccessState", concurrent_auth),
            ("validate", response(trusted())), ("requestWebAccessState", concurrent_auth),
        ]
        owner = self.resume()
        self.assertEqual(owner.state, State.RETRY_LATER)
        self.assertEqual(owner.reason, "newer-session-exists")
        self.assertEqual(len(self.calls), 4)
        self.assertEqual((self.store.root / "accepted.json").read_bytes(), self.newer)

    def test_adopted_session_is_rechecked_after_pcs(self):
        self.seed()

        def concurrent_auth(session):
            candidate = self.store.begin(purpose=Publication.INTERACTIVE)
            self.store.publish(candidate)
            candidate.close()
            self.newer = (self.store.root / "accepted.json").read_bytes()
            return response({"isICDRSDisabled": False})

        def pcs_response(session):
            for cookie in list(session.cookies):
                if cookie.name == "X-APPLE-WEBAUTH-VALIDATE":
                    session.cookies.clear(cookie.domain, cookie.path, cookie.name)
            return response({"status": "success"})

        self.replies = [
            ("validate", response(trusted())), ("requestWebAccessState", concurrent_auth),
            ("validate", response(trusted())),
            ("requestWebAccessState", response({
                "isICDRSDisabled": True, "isDeviceConsentedForPCS": True,
            })),
            ("requestPCS", pcs_response),
        ]
        owner = self.resume()
        self.assertEqual(owner.state, State.NEEDS_USER)
        self.assertEqual(owner.reason, "upload-cookie-missing")
        self.assertIsNone(owner.client)
        self.assertEqual((self.store.root / "accepted.json").read_bytes(), self.newer)

    def test_refused_adoption_never_exchanges_the_new_saved_token(self):
        self.seed()

        def concurrent_auth(session):
            candidate = self.store.begin(purpose=Publication.INTERACTIVE)
            self.store.publish(candidate)
            candidate.close()
            self.newer = (self.store.root / "accepted.json").read_bytes()
            return response({"isICDRSDisabled": False})

        self.replies = [
            ("validate", response(trusted())), ("requestWebAccessState", concurrent_auth),
            ("validate", response({}, 401)),
        ]
        owner = self.resume()
        self.assertEqual(owner.state, State.NEEDS_USER)
        self.assertEqual(len(self.calls), 3)
        self.assertEqual((self.store.root / "accepted.json").read_bytes(), self.newer)

    def test_cross_process_startup_attempt_is_single_flight_without_polling(self):
        self.seed()
        context = multiprocessing.get_context("fork")
        ready, release = context.Event(), context.Event()
        process = context.Process(
            target=hold_startup_in_process, args=(str(self.root), ready, release),
        )
        process.start()
        try:
            self.assertTrue(ready.wait(timeout=5))
            owner = self.resume()
            self.assertEqual(owner.state, State.RETRY_LATER)
            self.assertEqual(owner.reason, "resume-in-progress")
            self.assertFalse(self.calls)
            owner.resume()
            self.assertFalse(self.calls)
            self.assert_preserved()
        finally:
            release.set()
            process.join(timeout=5)
        self.assertFalse(process.is_alive())
        self.assertEqual(process.exitcode, 0)
        self.ready_replies()
        self.assertEqual(self.resume().state, State.AUTHENTICATED)

    def test_sticky_mfa_cannot_be_hidden_by_missing_hsa_version(self):
        self.seed()

        def challenge(session):
            session._service._requires_mfa = True
            data = trusted()
            data["dsInfo"].pop("hsaVersion")
            return response(data)

        self.replies = [("validate", challenge)]
        self.assertEqual(self.resume().state, State.NEEDS_USER)
        self.assert_preserved()

    def test_driver_startup_has_no_interactive_fallback_and_sanitizes_errors(self):
        from driver import ICloudFS
        from unittest.mock import Mock

        self.seed()
        self.replies = [("validate", ConnectionError("secret-sentinel"))]
        fs = ICloudFS.__new__(ICloudFS)
        fs.logger = Mock()
        fs.init_icloud(USER, str(self.root / "cache"),
                       str(self.root), require_session=False)
        self.assertIsNone(fs.api)
        self.assertEqual(fs.saved_session.state, State.RETRY_LATER)
        self.assertNotIn("secret-sentinel", str(fs.logger.mock_calls))
        self.assert_preserved()

    def test_driver_pcs_guidance_distinguishes_ambiguous_and_pending(self):
        from driver import ICloudFS
        from unittest.mock import Mock

        for state, result, expected in (
            ({}, None, "incomplete"),
            ({"isICDRSDisabled": True, "isDeviceConsentedForPCS": True},
             {"status": "pending", "message": "Cookies not available yet on server."}, "retry later"),
        ):
            with self.subTest(state=state):
                self.seed()
                self.replies = [("validate", response(trusted())),
                                ("requestWebAccessState", response(state))]
                if result is not None:
                    self.replies.append(("requestPCS", response(result)))
                fs = ICloudFS.__new__(ICloudFS)
                fs.logger = Mock()
                fs.init_icloud(USER, str(self.root / "cache"), str(self.root), require_session=False)
                logged = str(fs.logger.mock_calls).lower()
                self.assertIn(expected, logged)
                self.assertNotIn("--enable-pcs", logged)
                self.assert_preserved()

    def test_driver_startup_uses_accepted_generation(self):
        from driver import ICloudFS
        from unittest.mock import Mock

        self.seed()
        self.ready_replies()
        fs = ICloudFS.__new__(ICloudFS)
        fs.logger = Mock()
        fs.init_icloud(USER, str(self.root / "cache"),
                       str(self.root), require_session=True)
        self.addCleanup(fs.saved_session.candidate.close)
        self.assertIs(fs.api, fs.saved_session.client)
        self.assertTrue(fs._is_authenticated())
        self.assertEqual(fs.saved_session.state, State.AUTHENTICATED)


class SessionStoreTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.store = SessionStore(self.root, USER)
        self.first = self.store.begin()
        self.addCleanup(self.first.close)
        for name in self.store.names:
            (Path(self.first.directory) / name).write_bytes(
                b'{"session_token": "synthetic"}' if name.endswith(".session") else b"#LWP-Cookies-2.0\n"
            )
        self.store.publish(self.first)
        self.before = (self.store.root / "accepted.json").read_bytes()

    def test_snapshot_preserves_pair_and_uses_private_modes(self):
        candidate = self.store.begin()
        self.addCleanup(candidate.close)
        self.assertEqual(Path(candidate.directory).stat().st_mode & 0o777, 0o700)
        for name in self.store.names:
            path = Path(candidate.directory) / name
            self.assertEqual(path.stat().st_mode & 0o777, 0o600)
            self.assertEqual(path.read_bytes(), (Path(self.first.directory) / name).read_bytes())
        self.assertEqual((self.store.root / "accepted.json").stat().st_mode & 0o777, 0o600)

    def test_interruption_before_atomic_replace_preserves_old_pair(self):
        with patch("saved_session.os.replace", side_effect=OSError("interrupted")):
            with self.assertRaisesRegex(SessionUnavailable, "publication-failed"):
                self.store.publish(self.first)
        self.assertEqual((self.store.root / "accepted.json").read_bytes(), self.before)
        self.assertFalse(list(self.store.root.glob("pending-*")))

    def test_newer_cross_process_publication_beats_stale_candidate(self):
        stale = self.store.begin()
        self.addCleanup(stale.close)
        context = multiprocessing.get_context("fork")
        results = context.Queue()
        process = context.Process(target=publish_in_process, args=(str(self.root), results))
        process.start()
        process.join(timeout=5)
        self.assertFalse(process.is_alive())
        self.assertEqual(process.exitcode, 0)
        generation = results.get(timeout=1)
        with self.assertRaisesRegex(SessionUnavailable, "newer-session-exists"):
            self.store.publish(stale)
        self.assertEqual(json.loads((self.store.root / "accepted.json").read_bytes())["generation"], generation)
        results.close()

    def test_interactive_rebase_cannot_overwrite_a_newer_interactive_result(self):
        older = self.store.begin(purpose=Publication.INTERACTIVE)
        newer = self.store.begin(purpose=Publication.INTERACTIVE)
        self.addCleanup(older.close)
        self.addCleanup(newer.close)
        self.store.publish(newer)
        startup = self.store.begin()
        self.addCleanup(startup.close)
        self.store.publish(startup)
        before = (self.store.root / "accepted.json").read_bytes()
        with self.assertRaisesRegex(SessionUnavailable, "newer-session-exists"):
            self.store.publish(older)
        self.assertEqual((self.store.root / "accepted.json").read_bytes(), before)

    def test_interactive_priority_survives_multiple_startup_publications(self):
        interactive = self.store.begin(purpose=Publication.INTERACTIVE)
        self.addCleanup(interactive.close)
        for _ in range(3):
            startup = self.store.begin()
            self.store.publish(startup)
            startup.close()
        generation = self.store.publish(interactive)
        accepted = json.loads((self.store.root / "accepted.json").read_bytes())
        self.assertEqual(accepted["interactive_generation"], generation)

    def test_failed_candidate_never_changes_legacy_pair(self):
        for name in self.store.names:
            (self.root / name).write_bytes(b"legacy-untouched")
        candidate = self.store.begin()
        for name in self.store.names:
            (Path(candidate.directory) / name).write_bytes(b"failed-response")
        candidate.close()
        for name in self.store.names:
            self.assertEqual((self.root / name).read_bytes(), b"legacy-untouched")
        self.assertEqual((self.store.root / "accepted.json").read_bytes(), self.before)

    def test_damaged_checkpoint_does_not_fall_back_to_legacy_credentials(self):
        (self.store.root / "accepted.json").write_text("broken")
        with self.assertRaisesRegex(SessionUnavailable, "invalid-session-bundle"):
            self.store.begin()

    def test_only_explicit_interactive_candidates_get_empty_storage_for_corruption(self):
        original = json.loads(self.before)
        variants = [b"", b"truncated", b"[]", json.dumps(dict(original, format=99)).encode()]
        for name, value in (
            (self.store.names[0], b"not-json"),
            (self.store.names[0], b"[]"),
            (self.store.names[1], b"not-a-cookiejar"),
        ):
            damaged = dict(original, files=dict(original["files"]))
            damaged["files"][name] = base64.b64encode(value).decode()
            variants.append(json.dumps(damaged).encode())
        damaged = dict(original, files={name: "invalid-base64!" for name in self.store.names})
        variants.append(json.dumps(damaged).encode())
        checkpoint = self.store.root / "accepted.json"
        for raw in variants:
            with self.subTest(raw=raw):
                checkpoint.write_bytes(raw)
                with self.assertRaisesRegex(SessionUnavailable, "invalid-session-bundle"):
                    self.store.begin()
                fresh = self.store.begin(purpose=Publication.INTERACTIVE)
                try:
                    self.assertIsNone(fresh.expected)
                    self.assertIsNone(fresh.interactive_base)
                    self.assertIsNotNone(fresh.repair_revision)
                    self.assertEqual(
                        json.loads((Path(fresh.directory) / self.store.names[0]).read_bytes()), {},
                    )
                    jar = PyiCloudCookieJar(filename=str(Path(fresh.directory) / self.store.names[1]))
                    jar.load()
                    self.assertFalse(list(jar))
                    self.assertEqual(checkpoint.read_bytes(), raw)
                finally:
                    fresh.close()

    def test_newer_cross_process_interactive_repair_beats_stale_repair_candidate(self):
        checkpoint = self.store.root / "accepted.json"
        broken = b"corrupt-checkpoint"
        checkpoint.write_bytes(broken)
        stale = self.store.begin(purpose=Publication.INTERACTIVE)
        self.addCleanup(stale.close)
        context = multiprocessing.get_context("fork")
        results = context.Queue()
        process = context.Process(
            target=publish_in_process, args=(str(self.root), results, Publication.INTERACTIVE),
        )
        process.start()
        process.join(timeout=5)
        self.assertFalse(process.is_alive())
        self.assertEqual(process.exitcode, 0)
        generation = results.get(timeout=1)
        results.close()
        before = checkpoint.read_bytes()
        with self.assertRaisesRegex(SessionUnavailable, "newer-session-exists"):
            self.store.publish(stale)
        self.assertEqual(checkpoint.read_bytes(), before)
        self.assertEqual(json.loads(before)["interactive_generation"], generation)
        self.assertEqual(next(self.store.root.glob("corrupt-*")).read_bytes(), broken)

    def test_corrupt_repair_cannot_rebase_over_changed_corrupt_bytes(self):
        checkpoint = self.store.root / "accepted.json"
        checkpoint.write_bytes(b"corrupt-A")
        repair = self.store.begin(purpose=Publication.INTERACTIVE)
        self.addCleanup(repair.close)
        checkpoint.write_bytes(b"corrupt-B")
        with self.assertRaisesRegex(SessionUnavailable, "newer-session-exists"):
            self.store.publish(repair)
        self.assertEqual(checkpoint.read_bytes(), b"corrupt-B")
        self.assertFalse(list(self.store.root.glob("corrupt-*")))

    def test_existing_managed_directory_permissions_are_not_changed(self):
        self.store.root.chmod(0o755)
        with self.assertRaisesRegex(SessionUnavailable, "unsafe-session-directory"):
            SessionStore(self.root, USER)
        self.assertEqual(self.store.root.stat().st_mode & 0o777, 0o755)

    def test_legacy_symlinks_are_not_followed(self):
        other = self.root / "other"
        other.mkdir()
        store = SessionStore(other, USER)
        (other / store.names[0]).symlink_to(self.root / "sensitive")
        with self.assertRaises(OSError):
            store.begin()


class ClassificationTests(unittest.TestCase):
    def test_resume_client_cannot_invoke_general_authentication(self):
        client = ResumeService.__new__(ResumeService)
        with self.assertRaisesRegex(SessionUnavailable, "interactive-authentication-required"):
            client.authenticate()

    def test_numeric_500_remains_transient_despite_authentication_wording(self):
        error = PyiCloudAPIResponseException("Authentication required for Account.", 500)
        wrapped = PyiCloudFailedLoginException("Invalid authentication token.")
        wrapped.__cause__ = error
        self.assertEqual(failure_kind(wrapped), "transient")

    def test_string_codes_are_not_assumed_to_be_http_status(self):
        self.assertEqual(failure_kind(PyiCloudAPIResponseException("secret", "401")), "transient")

    def test_transport_bounds_requests_without_automatic_retries(self):
        adapter = BoundedTransport(max_retries=0)
        with patch("requests.adapters.HTTPAdapter.send") as send:
            adapter.send("synthetic-request", timeout=None)
            self.assertEqual(send.call_args.kwargs["timeout"], (10, 30))
            adapter.send("synthetic-request", timeout=7)
            self.assertEqual(send.call_args.kwargs["timeout"], 7)
        self.assertEqual(adapter.max_retries.total, 0)


if __name__ == "__main__":
    unittest.main()
