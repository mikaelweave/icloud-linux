import io
import json
import logging
import os
import runpy
import socket
import sqlite3
import sys
import tempfile
import threading
import time
from pathlib import Path
import unittest
from contextlib import closing
from http.cookiejar import LWPCookieJar
from types import SimpleNamespace
from unittest.mock import Mock, patch
from urllib.parse import urlparse

import requests
from pyicloud import PyiCloudService
from pyicloud.exceptions import PyiCloudAcceptTermsException
from requests.cookies import create_cookie

import auth
from driver import ICloudFS
from auth import do_2fa, do_sms_forced, main
from saved_session import Publication, SessionStore


class OfflineAuthTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.enterContext(patch.dict(os.environ, {"HOME": self.tmp.name, "XDG_CONFIG_HOME": self.tmp.name}))
        self.enterContext(patch.object(socket.socket, "connect", side_effect=AssertionError("network blocked")))
        self.enterContext(patch.object(requests.Session, "send", self.send))
        self.enterContext(patch("builtins.input", return_value="123456"))
        self.enterContext(patch("sys.stdout", new_callable=io.StringIO))
        self.enterContext(patch("sys.stderr", new_callable=io.StringIO))
        user = Mock()
        user.start_authentication.return_value = ("synthetic@example.test", b"a")
        user.process_challenge.return_value = b"m1"
        user.H_AMK = b"m2"
        self.enterContext(patch("pyicloud.base.srp.User", return_value=user))
        self.enterContext(patch("pyicloud.base.SrpPassword"))
        self.requests = []
        self.valid = True
        self.trust = True
        self.mode = "sms"
        self.sms_error = False
        self.bridge = False
        self.terms = False
        self.pcs_state = {"isICDRSDisabled": False}
        self.pcs_result = {"status": "success"}
        self.pcs_requests = []
        self.pcs_cookie_action = None
        self.command_api = None
        self.api = PyiCloudService(
            "synthetic@example.test", "synthetic", cookie_directory=self.tmp.name,
            authenticate=False,
        )

    def send(self, request, **kwargs):
        path = urlparse(request.url).path
        self.requests.append((request.method, path))
        status = 200
        body = {}
        headers = {}
        if path.endswith("/signin/init"):
            body = {"salt": "YQ==", "b": "Yg==", "c": "c", "iteration": 1, "protocol": "s2k"}
        elif path.endswith("/signin/complete"):
            status, body = 409, {"authType": "hsa2"}
            headers["X-Apple-Session-Token"] = "synthetic-token"
        elif path.endswith("/appleauth/auth"):
            body = {"direct": {
                "authInitialRoute": "auth/bridge/step" if self.bridge else "",
                "hasTrustedDevices": self.bridge,
                "twoSV": {
                    "bridgeInitiateData": {"synthetic": True} if self.bridge else {},
                    "phoneNumberVerification": {
                        "trustedPhoneNumbers": [{"id": 1, "pushMode": self.mode}]
                    },
                },
            }}
        elif path.endswith("/verify/phone"):
            if self.sms_error:
                status, body = 400, {"errorMessage": "synthetic-secret-response"}
        elif path.endswith(("/verify/phone/securitycode", "/verify/trusteddevice/securitycode")):
            status = 409
            body = {"authenticationType": "hsa2", "securityCode": {"valid": self.valid}}
        elif path.endswith("/2sv/trust"):
            status = 200 if self.trust else 400
        elif path.endswith("/accountLogin"):
            body = {"hsaTrustedBrowser": True,
                    "dsInfo": {"hsaVersion": 2, "dsid": "123", "appleId": "synthetic@example.test"},
                    "webservices": {"drivews": {"url": "https://drive.example.test"},
                                    "docws": {"url": "https://docs.example.test"}},
                    "termsUpdateNeeded": self.terms}
        elif path.endswith("/requestWebAccessState"):
            body = self.pcs_state
        elif path.endswith("/enableDeviceConsentForPCS"):
            body = {"isDeviceConsentNotificationSent": True}
        elif path.endswith("/requestPCS"):
            self.pcs_requests.append(json.loads(request.body))
            if self.pcs_cookie_action:
                for cookie in list(self.command_api.session.cookies):
                    if cookie.name == "X-APPLE-WEBAUTH-VALIDATE":
                        if self.pcs_cookie_action == "remove":
                            self.command_api.session.cookies.clear(cookie.domain, cookie.path, cookie.name)
                        else:
                            cookie.expires = int(time.time()) - 1
            body = self.pcs_result
        elif path.endswith("/getTerms"):
            body = {"iCloudTerms": {"version": 1}}
        elif path.endswith("/repairDone"):
            self.terms = False
        elif path.endswith("/setup/ws/1/validate"):
            pass
        elif not path.endswith("/authorize/signin"):
            raise AssertionError(f"Unexpected offline request {request.method} {path}")
        response = requests.Response()
        response.status_code = status
        response.url = request.url
        response.request = request
        response.headers = requests.structures.CaseInsensitiveDict(
            {"Content-Type": "application/json", **headers}
        )
        response._content = json.dumps(body).encode()
        response.reason = "synthetic"
        return response

    def count(self, suffix):
        return sum(path.endswith(suffix) for _, path in self.requests)

    def run_pcs_command(self, *args):
        config = os.path.join(self.tmp.name, "config.yaml")
        with open(config, "w", encoding="utf-8") as file:
            file.write(f"username: synthetic@example.test\npassword: synthetic\ncookie_dir: {self.tmp.name}\n")

        def service(*service_args, **kwargs):
            self.command_api = PyiCloudService(*service_args, **kwargs)
            self.command_api.session.cookies.set_cookie(create_cookie(
                "X-APPLE-WEBAUTH-VALIDATE", "v=1:t=synthetic", domain=".icloud.com",
                expires=4102444800,
            ))
            return self.command_api

        with patch("sys.argv", ["auth.py", config, *args]), patch("auth.PyiCloudService", side_effect=service):
            auth.main()

    def seed_prior_bundle(self):
        store = SessionStore(self.tmp.name, "synthetic@example.test")
        candidate = store.begin()
        try:
            store.publish(candidate)
        finally:
            candidate.close()
        return store.root / "accepted.json"

    def test_pcs_cookie_loss_cannot_publish_interactive_candidate(self):
        checkpoint = self.seed_prior_bundle()
        before = checkpoint.read_bytes()
        self.pcs_state = {"isICDRSDisabled": True, "isDeviceConsentedForPCS": True}
        for action in ("remove", "expire"):
            with self.subTest(action=action):
                self.pcs_cookie_action = action
                with self.assertRaises(SystemExit):
                    self.run_pcs_command("--enable-pcs")
                self.assertEqual(checkpoint.read_bytes(), before)
                self.assertNotIn("AUTH_OK", sys.stdout.getvalue())
                self.assertIn("upload-cookie-missing", sys.stderr.getvalue())

    def test_cookie_expiry_during_interactive_publication_preserves_bundle(self):
        import saved_session

        checkpoint = self.seed_prior_bundle()
        before = checkpoint.read_bytes()
        original_write = saved_session._write_file
        self.pcs_state = {"isICDRSDisabled": True, "isDeviceConsentedForPCS": True}

        def write(path, data):
            original_write(path, data)
            if Path(path).name.startswith("pending-"):
                for cookie in self.command_api.session.cookies:
                    if cookie.name == "X-APPLE-WEBAUTH-VALIDATE":
                        cookie.expires = int(time.time()) - 1

        with patch("saved_session._write_file", side_effect=write), self.assertRaises(SystemExit):
            self.run_pcs_command("--enable-pcs")
        self.assertEqual(checkpoint.read_bytes(), before)
        self.assertNotIn("AUTH_OK", sys.stdout.getvalue())
        self.assertIn("upload-cookie-missing", sys.stderr.getvalue())

    def test_interactive_directory_fsync_failure_restores_prior_bundle(self):
        import saved_session

        checkpoint = self.seed_prior_bundle()
        before = checkpoint.read_bytes()
        original_sync = saved_session._sync_directory
        calls = []

        def fail_once(path):
            calls.append(path)
            if len(calls) == 1:
                raise OSError("synthetic directory fsync failure")
            return original_sync(path)

        with patch("saved_session._sync_directory", side_effect=fail_once), self.assertRaises(SystemExit):
            self.run_pcs_command()
        self.assertEqual(checkpoint.read_bytes(), before)
        self.assertNotIn("AUTH_OK", sys.stdout.getvalue())
        self.assertGreaterEqual(len(calls), 2)

    def test_interactive_failed_first_publication_restores_absence(self):
        import saved_session

        store = SessionStore(self.tmp.name, "synthetic@example.test")
        original_sync = saved_session._sync_directory
        calls = []

        def fail_once(path):
            calls.append(path)
            if len(calls) == 1:
                raise OSError("synthetic fsync failure")
            return original_sync(path)

        with patch("saved_session._sync_directory", side_effect=fail_once), self.assertRaises(SystemExit):
            self.run_pcs_command()
        self.assertFalse((store.root / "accepted.json").exists())
        self.assertNotIn("AUTH_OK", sys.stdout.getvalue())

    def test_interactive_replace_then_error_restores_prior_bundle(self):
        import saved_session

        checkpoint = self.seed_prior_bundle()
        before = checkpoint.read_bytes()
        original_replace = saved_session.os.replace

        def replace(src, dst):
            original_replace(src, dst)
            if Path(src).name.startswith("pending-"):
                raise OSError("synthetic post-replace failure")

        with patch("saved_session.os.replace", side_effect=replace), self.assertRaises(SystemExit):
            self.run_pcs_command()
        self.assertEqual(checkpoint.read_bytes(), before)
        self.assertIn("publication-failed", sys.stderr.getvalue())
        self.assertNotIn("AUTH_OK", sys.stdout.getvalue())

    def test_interactive_rollback_failure_reports_indeterminate(self):
        checkpoint = self.seed_prior_bundle()
        with patch("saved_session._sync_directory", side_effect=OSError("synthetic fsync failure")):
            with self.assertRaises(SystemExit):
                self.run_pcs_command()
        self.assertIn("publication-indeterminate", sys.stderr.getvalue())
        self.assertNotIn("Session not saved", sys.stderr.getvalue())
        self.assertNotIn("AUTH_OK", sys.stdout.getvalue())

    def test_interactive_rollback_replace_failure_is_indeterminate(self):
        import saved_session

        checkpoint = self.seed_prior_bundle()
        before = checkpoint.read_bytes()
        original_replace = saved_session.os.replace

        def replace(src, dst):
            if Path(src).name.startswith("rollback-"):
                raise OSError("synthetic rollback failure")
            return original_replace(src, dst)

        with patch("saved_session._sync_directory", side_effect=OSError("synthetic fsync failure")), \
             patch("saved_session.os.replace", side_effect=replace), self.assertRaises(SystemExit):
            self.run_pcs_command()
        self.assertIn("publication-indeterminate", sys.stderr.getvalue())
        self.assertNotIn("Session not saved", sys.stderr.getvalue())
        self.assertNotIn("AUTH_OK", sys.stdout.getvalue())
        self.assertNotEqual(checkpoint.read_bytes(), before)

    def test_interactive_rollback_precedes_competing_publication(self):
        import saved_session

        checkpoint = self.seed_prior_bundle()
        before = checkpoint.read_bytes()
        store = SessionStore(self.tmp.name, "synthetic@example.test")
        competitor = store.begin(purpose=Publication.INTERACTIVE)
        self.addCleanup(competitor.close)
        entered = threading.Event()
        release = threading.Event()
        started = threading.Event()
        finished = threading.Event()
        restored = []
        results = []
        exits = []
        original_sync = saved_session._sync_directory
        original_flock = saved_session.fcntl.flock

        def lock(fd, operation):
            if threading.current_thread().name == "other-publisher":
                started.set()
            return original_flock(fd, operation)

        def sync(path):
            if threading.current_thread().name == "auth-publisher":
                if not entered.is_set():
                    entered.set()
                    if not release.wait(5):
                        raise TimeoutError("test timed out")
                    raise OSError("synthetic directory fsync failure")
                restored.append(checkpoint.read_bytes())
            return original_sync(path)

        def authenticate():
            try:
                self.run_pcs_command()
            except SystemExit as exc:
                exits.append(exc.code)

        def publish_competitor():
            try:
                results.append(store.publish(competitor))
            finally:
                finished.set()

        first = threading.Thread(target=authenticate, name="auth-publisher")
        second = threading.Thread(target=publish_competitor, name="other-publisher")
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
        self.assertEqual(exits, [1])
        self.assertEqual(restored, [before])
        self.assertEqual(len(results), 1)
        self.assertEqual(json.loads(checkpoint.read_bytes())["generation"], results[0])
        self.assertNotIn("AUTH_OK", sys.stdout.getvalue())

    def test_interactive_expiry_after_replacement_does_not_report_failure(self):
        import saved_session

        checkpoint = self.seed_prior_bundle()
        before = checkpoint.read_bytes()
        original_replace = saved_session.os.replace

        def replace(src, dst):
            original_replace(src, dst)
            if Path(src).name.startswith("pending-"):
                for cookie in self.command_api.session.cookies:
                    if cookie.name == "X-APPLE-WEBAUTH-VALIDATE":
                        cookie.expires = int(time.time()) - 1

        with patch("saved_session.os.replace", side_effect=replace):
            self.run_pcs_command()
        self.assertIn("AUTH_OK", sys.stdout.getvalue())
        self.assertNotEqual(checkpoint.read_bytes(), before)

    def test_ambiguous_pcs_state_does_not_suggest_consent_loop(self):
        checkpoint = self.seed_prior_bundle()
        before = checkpoint.read_bytes()
        for state in ({}, {"isICDRSDisabled": True}):
            with self.subTest(state=state):
                sys.stderr.seek(0)
                sys.stderr.truncate()
                self.pcs_state = state
                with self.assertRaises(SystemExit):
                    self.run_pcs_command("--enable-pcs")
                self.assertIn("incomplete", sys.stderr.getvalue().lower())
                self.assertNotIn("--enable-pcs", sys.stderr.getvalue())
                self.assertEqual(checkpoint.read_bytes(), before)
                self.assertEqual(self.count("/enableDeviceConsentForPCS"), 0)
                self.assertEqual(self.count("/requestPCS"), 0)

    def test_pending_pcs_guides_wait_not_reauthentication(self):
        checkpoint = self.seed_prior_bundle()
        before = checkpoint.read_bytes()
        self.pcs_state = {"isICDRSDisabled": True, "isDeviceConsentedForPCS": True}
        self.pcs_result = {"status": "pending", "message": "Cookies not available yet on server."}
        with self.assertRaises(SystemExit):
            self.run_pcs_command("--enable-pcs")
        self.assertIn("retry", sys.stderr.getvalue().lower())
        self.assertNotIn("--enable-pcs", sys.stderr.getvalue())
        self.assertNotIn("AUTH_OK", sys.stdout.getvalue())
        self.assertEqual(checkpoint.read_bytes(), before)
        self.assertEqual(self.count("/requestPCS"), 1)

    def test_explicit_consent_then_pending_pcs_keeps_previous_bundle(self):
        checkpoint = self.seed_prior_bundle()
        before = checkpoint.read_bytes()
        self.pcs_state = {"isICDRSDisabled": True, "isDeviceConsentedForPCS": False}
        self.pcs_result = {"status": "pending", "message": "Cookies not available yet on server."}

        def approve(prompt):
            if "PCS" in prompt:
                self.pcs_state["isDeviceConsentedForPCS"] = True
            return "123456"

        with patch("builtins.input", side_effect=approve), self.assertRaises(SystemExit):
            self.run_pcs_command("--enable-pcs")
        self.assertIn("retry later", sys.stderr.getvalue().lower())
        self.assertNotIn("--enable-pcs", sys.stderr.getvalue())
        self.assertNotIn("AUTH_OK", sys.stdout.getvalue())
        self.assertEqual(checkpoint.read_bytes(), before)
        self.assertEqual(self.count("/enableDeviceConsentForPCS"), 1)
        self.assertEqual(self.count("/requestPCS"), 1)

    def test_normal_flow_sends_once_and_verifies_before_trust(self):
        self.api.authenticate(force_refresh=True)
        do_2fa(self.api)
        self.assertEqual(self.count("/verify/phone"), 1)
        self.assertEqual(self.count("/verify/phone/securitycode"), 1)
        self.assertEqual(self.count("/2sv/trust"), 1)
        self.assertTrue(self.api.is_trusted_session)
        self.assertLess(
            self.requests.index(("POST", "/appleauth/auth/verify/phone/securitycode")),
            self.requests.index(("GET", "/appleauth/auth/2sv/trust")),
        )

    def test_forced_sms_reuses_automatic_sms_delivery(self):
        self.api.authenticate(force_refresh=True)
        do_sms_forced(self.api)
        self.assertEqual(self.count("/verify/phone"), 1)
        self.assertEqual(self.count("/verify/phone/securitycode"), 1)
        self.assertTrue(self.api.is_trusted_session)

    def test_trusted_device_prompt_is_not_requested_twice(self):
        self.bridge = True
        state = SimpleNamespace(uses_legacy_trusted_device_verifier=True)
        with patch.object(self.api._trusted_device_bridge, "start", return_value=state) as start:
            with patch.object(self.api._trusted_device_bridge, "close"):
                self.api.authenticate(force_refresh=True)
                do_2fa(self.api)
        self.assertEqual(start.call_count, 1)
        self.assertEqual(self.count("/verify/trusteddevice/securitycode"), 1)
        self.assertEqual(self.count("/2sv/trust"), 1)
        self.assertTrue(self.api.is_trusted_session)

    def test_forced_sms_requests_when_not_automatically_sent(self):
        self.mode = "push"
        self.api.authenticate(force_refresh=True)
        do_sms_forced(self.api)
        self.assertEqual(self.count("/verify/phone"), 1)
        self.assertEqual(self.count("/verify/phone/securitycode"), 1)
        self.assertTrue(self.api.is_trusted_session)

    def test_forced_sms_error_does_not_print_response_data(self):
        self.mode = "push"
        self.sms_error = True
        self.api.authenticate(force_refresh=True)
        with patch("builtins.input") as prompt:
            with self.assertRaises(SystemExit) as result:
                do_sms_forced(self.api)
        self.assertEqual(result.exception.code, 1)
        self.assertNotIn("synthetic-secret-response", sys.stdout.getvalue())
        self.assertNotIn("synthetic-secret-response", sys.stderr.getvalue())
        self.assertIn("ERROR triggering SMS", sys.stderr.getvalue())
        self.assertEqual(self.count("/verify/phone"), 1)
        self.assertEqual(self.count("/verify/phone/securitycode"), 0)
        self.assertEqual(self.count("/2sv/trust"), 0)
        prompt.assert_not_called()

    def test_invalid_code_never_trusts(self):
        self.valid = False
        self.api.authenticate(force_refresh=True)
        with self.assertRaises(SystemExit):
            do_sms_forced(self.api)
        self.assertEqual(self.count("/2sv/trust"), 0)
        self.assertFalse(self.api.is_trusted_session)

    def test_normal_flow_invalid_code_never_trusts(self):
        self.valid = False
        self.api.authenticate(force_refresh=True)
        with self.assertRaises(SystemExit):
            do_2fa(self.api)
        self.assertEqual(self.count("/2sv/trust"), 0)
        self.assertFalse(self.api.is_trusted_session)

    def test_failed_trust_never_reports_success(self):
        self.trust = False
        self.api.authenticate(force_refresh=True)
        with self.assertRaises(SystemExit):
            do_2fa(self.api)
        self.assertEqual(self.count("/verify/phone/securitycode"), 1)
        self.assertEqual(self.count("/2sv/trust"), 1)
        self.assertFalse(self.api.is_trusted_session)

    def test_command_authenticates_with_synthetic_config(self):
        config = os.path.join(self.tmp.name, "config.yaml")
        with open(config, "w", encoding="utf-8") as file:
            file.write(f"username: synthetic@example.test\npassword: synthetic\ncookie_dir: {self.tmp.name}\n")
        def service(*args, **kwargs):
            api = PyiCloudService(*args, **kwargs)
            api.session.cookies.set_cookie(create_cookie(
                "X-APPLE-WEBAUTH-VALIDATE", "v=1:t=synthetic", domain=".icloud.com",
                expires=4102444800,
            ))
            return api

        with patch("sys.argv", ["auth.py", config]), patch("auth.PyiCloudService", side_effect=service):
            auth.main()
        self.assertIn("AUTH_OK", auth.sys.stdout.getvalue())
        self.assertEqual(self.count("/verify/phone"), 1)
        self.assertEqual(self.count("/verify/phone/securitycode"), 1)
        self.assertEqual(self.count("/2sv/trust"), 1)

    def test_command_does_not_report_success_when_trust_fails(self):
        config = os.path.join(self.tmp.name, "config.yaml")
        with open(config, "w", encoding="utf-8") as file:
            file.write(f"username: synthetic@example.test\npassword: synthetic\ncookie_dir: {self.tmp.name}\n")
        self.trust = False
        with patch("sys.argv", ["auth.py", config]):
            with self.assertRaises(SystemExit):
                auth.main()
        self.assertNotIn("AUTH_OK", auth.sys.stdout.getvalue())
        self.assertEqual(self.count("/verify/phone/securitycode"), 1)
        self.assertEqual(self.count("/2sv/trust"), 1)

    def test_pcs_consent_requires_explicit_opt_in_and_verified_access(self):
        config = os.path.join(self.tmp.name, "config.yaml")
        with open(config, "w", encoding="utf-8") as file:
            file.write(f"username: synthetic@example.test\npassword: synthetic\ncookie_dir: {self.tmp.name}\n")
        def service(*args, **kwargs):
            api = PyiCloudService(*args, **kwargs)
            api.session.cookies.set_cookie(create_cookie(
                "X-APPLE-WEBAUTH-VALIDATE", "v=1:t=synthetic", domain=".icloud.com",
                expires=4102444800,
            ))
            return api

        self.pcs_state = {"isICDRSDisabled": True, "isDeviceConsentedForPCS": False}
        with patch("sys.argv", ["auth.py", config]), patch("auth.PyiCloudService", side_effect=service):
            with self.assertRaises(SystemExit):
                auth.main()
        self.assertNotIn("AUTH_OK", sys.stdout.getvalue())
        self.assertEqual(self.count("/enableDeviceConsentForPCS"), 0)
        self.assertEqual(self.count("/requestPCS"), 0)

        def approve(prompt):
            if "PCS" in prompt:
                self.pcs_state["isDeviceConsentedForPCS"] = True
            return "123456"

        with patch("sys.argv", ["auth.py", config, "--enable-pcs"]), \
             patch("auth.PyiCloudService", side_effect=service), \
             patch("builtins.input", side_effect=approve):
            auth.main()
        self.assertIn("AUTH_OK", sys.stdout.getvalue())
        self.assertEqual(self.count("/enableDeviceConsentForPCS"), 1)
        self.assertEqual(self.count("/requestPCS"), 1)
        self.assertTrue(self.pcs_requests[0]["derivedFromUserAction"])

    def test_pcs_failure_does_not_publish_or_report_success(self):
        config = os.path.join(self.tmp.name, "config.yaml")
        with open(config, "w", encoding="utf-8") as file:
            file.write(f"username: synthetic@example.test\npassword: synthetic\ncookie_dir: {self.tmp.name}\n")
        def service(*args, **kwargs):
            api = PyiCloudService(*args, **kwargs)
            api.session.cookies.set_cookie(create_cookie(
                "X-APPLE-WEBAUTH-VALIDATE", "v=1:t=synthetic", domain=".icloud.com",
                expires=4102444800,
            ))
            return api
        self.pcs_state = {"isICDRSDisabled": True, "isDeviceConsentedForPCS": True}
        self.pcs_result = {"status": "pending", "message": "Cookies not available yet on server."}
        with patch("sys.argv", ["auth.py", config, "--enable-pcs"]), patch("auth.PyiCloudService", side_effect=service):
            with self.assertRaises(SystemExit):
                auth.main()
        self.assertNotIn("AUTH_OK", sys.stdout.getvalue())
        self.assertEqual(self.count("/enableDeviceConsentForPCS"), 0)
        self.assertEqual(self.count("/requestPCS"), 1)

    def test_already_consented_pcs_succeeds_without_notification(self):
        config = os.path.join(self.tmp.name, "config.yaml")
        with open(config, "w", encoding="utf-8") as file:
            file.write(f"username: synthetic@example.test\npassword: synthetic\ncookie_dir: {self.tmp.name}\n")
        def service(*args, **kwargs):
            api = PyiCloudService(*args, **kwargs)
            api.session.cookies.set_cookie(create_cookie(
                "X-APPLE-WEBAUTH-VALIDATE", "v=1:t=synthetic", domain=".icloud.com",
                expires=4102444800,
            ))
            return api
        self.pcs_state = {"isICDRSDisabled": True, "isDeviceConsentedForPCS": True}
        with patch("sys.argv", ["auth.py", config]), patch("auth.PyiCloudService", side_effect=service):
            auth.main()
        self.assertIn("AUTH_OK", sys.stdout.getvalue())
        self.assertEqual(self.count("/enableDeviceConsentForPCS"), 0)
        self.assertEqual(self.count("/requestPCS"), 1)
        self.assertFalse(self.pcs_requests[0]["derivedFromUserAction"])

    def test_pcs_consent_not_granted_cannot_request_access_or_publish(self):
        config = os.path.join(self.tmp.name, "config.yaml")
        with open(config, "w", encoding="utf-8") as file:
            file.write(f"username: synthetic@example.test\npassword: synthetic\ncookie_dir: {self.tmp.name}\n")
        def service(*args, **kwargs):
            api = PyiCloudService(*args, **kwargs)
            api.session.cookies.set_cookie(create_cookie(
                "X-APPLE-WEBAUTH-VALIDATE", "v=1:t=synthetic", domain=".icloud.com",
                expires=4102444800,
            ))
            return api
        self.pcs_state = {"isICDRSDisabled": True, "isDeviceConsentedForPCS": False}
        with patch("sys.argv", ["auth.py", config, "--enable-pcs"]), patch("auth.PyiCloudService", side_effect=service):
            with self.assertRaises(SystemExit):
                auth.main()
        self.assertNotIn("AUTH_OK", sys.stdout.getvalue())
        self.assertEqual(self.count("/enableDeviceConsentForPCS"), 1)
        self.assertEqual(self.count("/requestPCS"), 0)
        store = SessionStore(self.tmp.name, "synthetic@example.test")
        self.assertFalse((store.root / "accepted.json").exists())

    def test_terms_are_not_accepted_without_explicit_opt_in(self):
        self.terms = True
        self.api.session.data["session_token"] = "synthetic-token"
        with self.assertRaises(PyiCloudAcceptTermsException):
            self.api.authenticate(force_refresh=True)
        self.assertEqual(self.count("/repairDone"), 0)

        opted_in = PyiCloudService(
            "synthetic@example.test", "synthetic", cookie_directory=self.tmp.name,
            authenticate=False, accept_terms=True,
        )
        opted_in.session.data["session_token"] = "synthetic-token"
        opted_in.authenticate(force_refresh=True)
        self.assertEqual(self.count("/getTerms"), 1)
        self.assertEqual(self.count("/repairDone"), 1)
        self.assertTrue(opted_in.is_trusted_session)

    def test_loads_legacy_lwp_cookies_and_session(self):
        legacy = LWPCookieJar(self.api.session.cookiejar_path)
        legacy.set_cookie(create_cookie("X-APPLE-WEBAUTH-TOKEN", "synthetic-cookie"))
        legacy.save(ignore_discard=True)
        with open(self.api.session.session_path, "w", encoding="utf-8") as file:
            json.dump({"session_token": "synthetic-token", "client_id": "synthetic-client"}, file)
        fresh = PyiCloudService(
            "synthetic@example.test", "synthetic", cookie_directory=self.tmp.name,
            authenticate=False,
        )
        self.assertEqual(fresh.session.cookies.get("X-APPLE-WEBAUTH-TOKEN"), "synthetic-cookie")
        self.assertEqual(fresh.session.data["session_token"], "synthetic-token")
        self.assertEqual(fresh.session.data["client_id"], "synthetic-client")

    def test_driver_never_attempts_interactive_authentication(self):
        fs = SimpleNamespace(logger=logging.getLogger("offline-test"))
        with self.assertRaisesRegex(RuntimeError, "no-saved-session"):
            ICloudFS.init_icloud(fs, "synthetic@example.test", self.tmp.name, self.tmp.name)
        self.assertEqual(self.count("/verify/phone"), 0)
        self.assertEqual(self.count("/verify/phone/securitycode"), 0)
        self.assertEqual(self.count("/2sv/trust"), 0)

    def test_hydration_uses_synthetic_saved_session(self):
        config_dir = os.path.join(self.tmp.name, ".config", "icloud-linux")
        os.makedirs(config_dir)
        with open(os.path.join(config_dir, "config.yaml"), "w", encoding="utf-8") as file:
            file.write(
                f"username: synthetic@example.test\npassword: synthetic\n"
                f"cache_dir: {self.tmp.name}\ncookie_dir: {self.tmp.name}\n"
            )
        with open(self.api.session.session_path, "w", encoding="utf-8") as file:
            json.dump({"session_token": "synthetic-token"}, file)
        self.api.session.cookies.set_cookie(create_cookie(
            "X-APPLE-WEBAUTH-VALIDATE", "v=1:t=synthetic", domain=".icloud.com",
            expires=4102444800,
        ))
        self.api.session.cookies.save()
        with closing(sqlite3.connect(os.path.join(self.tmp.name, "state.sqlite3"))) as db:
            db.execute(
                "CREATE TABLE entries (path TEXT, remote_docwsid TEXT, remote_zone TEXT, "
                "size INTEGER, mtime INTEGER, type TEXT, hydrated INTEGER, tombstone INTEGER)"
            )
            db.commit()
            with patch.object(sqlite3, "connect", return_value=db):
                with patch.object(sys, "argv", ["hydrate_dir.py", "/synthetic"]):
                    with self.assertRaises(SystemExit) as result:
                        runpy.run_path(os.path.join(os.path.dirname(__file__), "hydrate_dir.py"), run_name="__main__")
        self.assertEqual(result.exception.code, 0)
        self.assertEqual(self.count("/accountLogin"), 1)
        self.assertEqual(self.count("/verify/phone"), 0)


class AuthPublicationTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.username = "synthetic@example.test"
        self.store = SessionStore(self.root, self.username)
        candidate = self.store.begin()
        self.addCleanup(candidate.close)
        for name in self.store.names:
            (Path(candidate.directory) / name).write_bytes(
                b"{}" if name.endswith(".session") else b"#LWP-Cookies-2.0\n"
            )
        self.store.publish(candidate)
        self.before = (self.store.root / "accepted.json").read_bytes()
        self.config = {
            "username": self.username,
            "password": "synthetic-password",
            "cookie_dir": str(self.root),
        }

    def run_auth(self, authenticate, extra=()):
        with patch("auth.load_config", return_value=self.config), patch(
            "auth.get_partition", return_value=None
        ), patch("sys.argv", ["auth.py", *extra]), patch(
            "auth.PyiCloudService.authenticate", autospec=True, side_effect=authenticate
        ), patch.object(
            PyiCloudService, "_check_pcs_consent", return_value={"isICDRSDisabled": False}
        ), patch("sys.stdout", new_callable=io.StringIO) as stdout:
            main()
            return stdout.getvalue()

    def accept(self, api):
        api.data = {
            "dsInfo": {"dsid": "123", "hsaVersion": 2, "appleId": self.username},
            "hsaTrustedBrowser": True,
            "webservices": {
                "drivews": {"url": "https://drive.example.test"},
                "docws": {"url": "https://docs.example.test"},
            },
        }
        api.session.data["session_token"] = "new-interactive-token"
        api.session.cookies.set_cookie(create_cookie(
            "X-APPLE-WEBAUTH-VALIDATE", "v=1:t=upload-token", domain=".icloud.com",
            expires=int(time.time()) + 3600,
        ))

    def test_interactive_success_publishes_only_after_final_trust_check(self):
        def authenticate(api, force_refresh):
            self.assertTrue(force_refresh)
            self.assertNotEqual(api.session._cookie_directory, str(self.root))
            self.assertEqual(api._password_raw, "synthetic-password")
            api.data = {
                "dsInfo": {"dsid": "123", "hsaVersion": 2, "appleId": self.username},
                "hsaTrustedBrowser": True,
                "webservices": {
                    "drivews": {"url": "https://drive.example.test"},
                    "docws": {"url": "https://docs.example.test"},
                },
            }
            api.session.data["session_token"] = "new-interactive-token"
            api.session.cookies.set_cookie(create_cookie(
                "X-APPLE-WEBAUTH-VALIDATE", "v=1:t=upload-token", domain=".icloud.com",
                expires=int(time.time()) + 3600,
            ))

        output = self.run_auth(authenticate)
        self.assertIn("AUTH_OK", output)
        self.assertNotEqual((self.store.root / "accepted.json").read_bytes(), self.before)
        candidate = self.store.begin()
        self.addCleanup(candidate.close)
        session = json.loads((Path(candidate.directory) / self.store.names[0]).read_bytes())
        self.assertEqual(session["session_token"], "new-interactive-token")

    def test_failed_interactive_attempt_preserves_previous_credentials(self):
        def authenticate(api, force_refresh):
            api.session.data["session_token"] = "failed-interactive-token"
            api.session._save_session_data()
            raise RuntimeError("synthetic authentication failure")

        with self.assertRaises(RuntimeError):
            self.run_auth(authenticate)
        self.assertEqual((self.store.root / "accepted.json").read_bytes(), self.before)

    def test_interactive_auth_rejects_missing_expired_malformed_or_ambiguous_upload_cookie(self):
        for kind in ("missing", "expired", "malformed", "ambiguous"):
            with self.subTest(kind=kind):
                def authenticate(api, force_refresh):
                    api.data = {
                        "dsInfo": {"dsid": "123", "hsaVersion": 2, "appleId": self.username},
                        "hsaTrustedBrowser": True,
                        "webservices": {
                            "drivews": {"url": "https://drive.example.test"},
                            "docws": {"url": "https://docs.example.test"},
                        },
                    }
                    if kind != "missing":
                        api.session.cookies.set_cookie(create_cookie(
                            "X-APPLE-WEBAUTH-VALIDATE",
                            "malformed" if kind == "malformed" else "v=1:t=upload-token",
                            domain=".icloud.com",
                            expires=int(time.time()) + (-100 if kind == "expired" else 3600),
                        ))
                    if kind == "ambiguous":
                        api.session.cookies.set_cookie(create_cookie(
                            "X-APPLE-WEBAUTH-VALIDATE", "t=other-token",
                            domain="setup.icloud.com", expires=int(time.time()) + 3600,
                        ))
                with patch("sys.stderr", new_callable=io.StringIO), self.assertRaises(SystemExit):
                    self.run_auth(authenticate)
                self.assertEqual((self.store.root / "accepted.json").read_bytes(), self.before)

    def test_incomplete_interactive_mfa_cannot_publish(self):
        def authenticate(api, force_refresh):
            api.data = {
                "dsInfo": {"dsid": "123", "hsaVersion": 2},
                "hsaTrustedBrowser": False,
            }

        with patch("auth.do_2fa") as do_2fa, patch("sys.stderr", new_callable=io.StringIO), self.assertRaises(SystemExit):
            self.run_auth(authenticate)
        do_2fa.assert_called_once()
        self.assertEqual((self.store.root / "accepted.json").read_bytes(), self.before)

    def test_successful_mfa_can_publish_over_intervening_startup_checkpoint(self):
        def authenticate(api, force_refresh):
            self.accept(api)
            api.data["hsaTrustedBrowser"] = False

        def complete_mfa(api):
            startup = self.store.begin()
            self.addCleanup(startup.close)
            self.store.publish(startup)
            api.data["hsaTrustedBrowser"] = True

        with patch("auth.do_2fa", side_effect=complete_mfa) as do_2fa:
            output = self.run_auth(authenticate)
        do_2fa.assert_called_once()
        self.assertIn("AUTH_OK", output)
        candidate = self.store.begin()
        self.addCleanup(candidate.close)
        saved = json.loads((Path(candidate.directory) / self.store.names[0]).read_bytes())
        self.assertEqual(saved["session_token"], "new-interactive-token")

    def test_explicit_interactive_auth_can_replace_corrupt_checkpoint_only_after_acceptance(self):
        broken = b'{"format": 1, "truncated":'
        checkpoint = self.store.root / "accepted.json"
        checkpoint.write_bytes(broken)
        for name in self.store.names:
            (self.root / name).write_bytes(b"must-not-be-imported-from-legacy")

        def authenticate(api, force_refresh):
            self.assertEqual(checkpoint.read_bytes(), broken)
            self.assertNotIn("session_token", api.session.data)
            self.assertFalse(list(api.session.cookies))
            self.accept(api)

        output = self.run_auth(authenticate)
        self.assertIn("AUTH_OK", output)
        self.assertNotEqual(checkpoint.read_bytes(), broken)
        archives = list(self.store.root.glob("corrupt-*"))
        self.assertEqual(len(archives), 1)
        self.assertEqual(archives[0].read_bytes(), broken)
        candidate = self.store.begin()
        self.addCleanup(candidate.close)
        saved = json.loads((Path(candidate.directory) / self.store.names[0]).read_bytes())
        self.assertEqual(saved["session_token"], "new-interactive-token")

    def test_failed_fresh_interactive_auth_leaves_corrupt_checkpoint_untouched(self):
        broken = b"corrupt-checkpoint"
        checkpoint = self.store.root / "accepted.json"
        checkpoint.write_bytes(broken)
        called = []

        def authenticate(api, force_refresh):
            called.append(True)
            self.accept(api)
            api.session._save_session_data()
            raise RuntimeError("fresh-auth-refused")

        with self.assertRaisesRegex(RuntimeError, "fresh-auth-refused"):
            self.run_auth(authenticate)
        self.assertTrue(called)
        self.assertEqual(checkpoint.read_bytes(), broken)
        self.assertFalse(list(self.store.root.glob("corrupt-*")))


if __name__ == "__main__":
    unittest.main()
