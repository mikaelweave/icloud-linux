import base64
from contextlib import contextmanager, ExitStack
from dataclasses import dataclass
from enum import Enum
import fcntl
import hashlib
from http.cookiejar import LoadError
from importlib.metadata import version
import json
import os
from pathlib import Path
import re
import stat
import tempfile
import threading
import uuid
import warnings

from pyicloud import PyiCloudService
from pyicloud.cookie_jar import PyiCloudCookieJar
from pyicloud.exceptions import (
    PyiCloud2FARequiredException,
    PyiCloud2SARequiredException,
    PyiCloudAcceptTermsException,
    PyiCloudAuthRequiredException,
)
from requests.adapters import HTTPAdapter
from requests.exceptions import TooManyRedirects


class State(str, Enum):
    RECOVERING = "recovering"
    AUTHENTICATED = "authenticated"
    RETRY_LATER = "retry-later"
    NEEDS_USER = "needs-user"
    INDETERMINATE = "publication-indeterminate"


class Publication(str, Enum):
    STARTUP = "startup"
    INTERACTIVE = "interactive"


class SessionUnavailable(RuntimeError):
    pass


def _private_directory(path):
    path.mkdir(mode=0o700, exist_ok=True)
    info = path.lstat()
    if not stat.S_ISDIR(info.st_mode) or info.st_uid != os.getuid() or info.st_mode & 0o077:
        raise SessionUnavailable("unsafe-session-directory")


def _read_file(path):
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    with os.fdopen(fd, "rb") as source:
        if not stat.S_ISREG(os.fstat(source.fileno()).st_mode):
            raise SessionUnavailable("unsafe-session-file")
        data = source.read(8 * 1024 * 1024 + 1)
        if len(data) > 8 * 1024 * 1024:
            raise SessionUnavailable("oversized-session-file")
        return data


def _write_file(path, data):
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, "wb") as target:
        target.write(data)
        target.flush()
        os.fsync(target.fileno())


def _sync_directory(path):
    fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


@dataclass
class Candidate:
    temporary: tempfile.TemporaryDirectory
    expected: str | None
    interactive_base: str | None = None
    purpose: Publication = Publication.STARTUP
    repair_revision: str | None = None

    @property
    def directory(self):
        return self.temporary.name

    def close(self):
        self.temporary.cleanup()


class SessionStore:
    """One atomic bundle owns the accepted pair. Clients write private copies."""

    def __init__(self, cookie_directory, username):
        self.legacy = Path(cookie_directory)
        self.legacy.mkdir(parents=True, exist_ok=True)
        managed = self.legacy / ".saved-sessions"
        _private_directory(managed)
        self.root = managed / hashlib.sha256(username.encode()).hexdigest()
        _private_directory(self.root)
        stem = "".join(c for c in username if re.match(r"\w", c))
        self.names = (stem + ".session", stem + ".cookiejar")
        self.username = username

    @contextmanager
    def _lock(self, name="lock", *, nonblocking=False):
        fd = os.open(self.root / name, os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
        try:
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | (fcntl.LOCK_NB if nonblocking else 0))
            except BlockingIOError:
                raise SessionUnavailable("resume-in-progress") from None
            yield
        finally:
            os.close(fd)

    def _current_bytes(self):
        try:
            return _read_file(self.root / "accepted.json")
        except FileNotFoundError:
            return None

    def _decode_bundle(self, raw):
        if raw is None:
            return None
        try:
            bundle = json.loads(raw)
        except ValueError:
            raise SessionUnavailable("invalid-session-bundle") from None
        if (
            not isinstance(bundle, dict)
            or bundle.get("format") != 1
            or bundle.get("account") != self.username
            or not isinstance(bundle.get("generation"), str) or not bundle["generation"]
            or not isinstance(bundle.get("files"), dict)
            or set(bundle["files"]) != set(self.names)
            or ("interactive_generation" in bundle
                and bundle["interactive_generation"] is not None
                and not isinstance(bundle["interactive_generation"], str))
        ):
            raise SessionUnavailable("invalid-session-bundle")
        try:
            files = {name: base64.b64decode(bundle["files"][name], validate=True) for name in self.names}
            if not isinstance(json.loads(files[self.names[0]]), dict):
                raise ValueError()
            if not files[self.names[1]].startswith(b"#LWP-Cookies-2.0"):
                raise ValueError()
        except (ValueError, TypeError):
            raise SessionUnavailable("invalid-session-bundle") from None
        return bundle

    def _accepted(self):
        return self._decode_bundle(self._current_bytes())

    def _validate_files(self, directory):
        if not isinstance(json.loads(_read_file(Path(directory) / self.names[0])), dict):
            raise ValueError("Invalid saved session shape")
        # Cookie parsers can include persisted secrets in diagnostic warnings.
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            PyiCloudCookieJar(filename=str(Path(directory) / self.names[1])).load()

    @staticmethod
    def _interactive_generation(bundle):
        return bundle.get("interactive_generation", bundle["generation"]) if bundle else None

    def begin(self, *, purpose=Publication.STARTUP):
        temporary = tempfile.TemporaryDirectory(prefix="work-", dir=self.root)
        try:
            with self._lock():
                raw = self._current_bytes()
                repair_revision = None
                defaults = dict(zip(self.names, (b"{}", b"#LWP-Cookies-2.0\n")))
                try:
                    bundle = self._decode_bundle(raw)
                    for name in self.names:
                        if bundle is not None:
                            data = base64.b64decode(bundle["files"][name], validate=True)
                        else:
                            try:
                                data = _read_file(self.legacy / name)
                            except FileNotFoundError:
                                data = defaults[name]
                        # Precreate both private files so pyicloud's open("w")
                        # cannot create a secret using the caller's umask.
                        _write_file(Path(temporary.name) / name, data)
                    self._validate_files(temporary.name)
                except (ValueError, LoadError, SessionUnavailable) as error:
                    if isinstance(error, SessionUnavailable) and error.args != ("invalid-session-bundle",):
                        raise
                    if purpose is not Publication.INTERACTIVE:
                        raise SessionUnavailable("invalid-session-bundle") from None
                    bundle = None
                    repair_revision = hashlib.sha256(raw).hexdigest() if raw is not None else None
                    for name, data in defaults.items():
                        (Path(temporary.name) / name).unlink(missing_ok=True)
                        _write_file(Path(temporary.name) / name, data)
            return Candidate(
                temporary, bundle["generation"] if bundle else None,
                self._interactive_generation(bundle), purpose, repair_revision,
            )
        except BaseException:
            temporary.cleanup()
            raise

    def publish(self, candidate, validate=None):
        self._validate_files(candidate.directory)
        files = {name: _read_file(Path(candidate.directory) / name) for name in self.names}
        json.loads(files[self.names[0]])
        generation = uuid.uuid4().hex
        bundle = {
            "format": 1,
            "account": self.username,
            "generation": generation,
            "interactive_generation": (
                generation if candidate.purpose is Publication.INTERACTIVE else candidate.interactive_base
            ),
            "files": {name: base64.b64encode(data).decode("ascii") for name, data in files.items()},
        }
        encoded = json.dumps(bundle).encode()
        if len(encoded) > 8 * 1024 * 1024:
            raise SessionUnavailable("oversized-session-bundle")
        committed = False
        try:
            with self._lock():
                raw = self._current_bytes()
                if candidate.repair_revision is not None:
                    # Unknown corrupt ancestry cannot be rebased across any writer.
                    conflict = (
                        candidate.purpose is not Publication.INTERACTIVE or raw is None
                        or hashlib.sha256(raw).hexdigest() != candidate.repair_revision
                    )
                elif candidate.purpose is Publication.INTERACTIVE:
                    current = self._decode_bundle(raw)
                    conflict = self._interactive_generation(current) != candidate.interactive_base
                else:
                    current = self._decode_bundle(raw)
                    conflict = (current["generation"] if current else None) != candidate.expected
                if conflict:
                    raise SessionUnavailable("newer-session-exists")
                if candidate.repair_revision is not None:
                    archive = self.root / ("corrupt-" + candidate.repair_revision)
                    try:
                        _write_file(archive, raw)
                    except FileExistsError:
                        if _read_file(archive) != raw:
                            raise SessionUnavailable("corrupt-archive-conflict") from None
                    _sync_directory(self.root)
                pending = self.root / ("pending-" + generation)
                try:
                    _write_file(pending, encoded)
                    if validate is not None:
                        validate()
                    replaced = False
                    try:
                        os.replace(pending, self.root / "accepted.json")
                        replaced = True
                        _sync_directory(self.root)
                        committed = True
                    except BaseException as error:
                        committed = False
                        try:
                            self._restore_failed_publication(raw, encoded, replaced)
                        except BaseException as rollback_error:
                            raise SessionUnavailable("publication-indeterminate") from rollback_error
                        if not isinstance(error, Exception):
                            raise
                        raise SessionUnavailable("publication-failed") from error
                finally:
                    if not committed:
                        try:
                            pending.unlink(missing_ok=True)
                        except OSError:
                            pass
        except BaseException as error:
            if committed:
                raise SessionUnavailable("publication-indeterminate") from error
            raise
        candidate.expected = generation
        candidate.interactive_base = bundle["interactive_generation"]
        candidate.repair_revision = None
        return generation

    def _restore_failed_publication(self, previous, encoded, replaced):
        current = self._current_bytes()
        if not replaced and current == previous:
            return
        if current != encoded:
            raise SessionUnavailable("publication-indeterminate")
        if previous is None:
            (self.root / "accepted.json").unlink()
        else:
            rollback = self.root / ("rollback-" + uuid.uuid4().hex)
            try:
                _write_file(rollback, previous)
                os.replace(rollback, self.root / "accepted.json")
                rollback = None
            finally:
                if rollback is not None:
                    try:
                        rollback.unlink(missing_ok=True)
                    except OSError:
                        pass
        _sync_directory(self.root)
        if self._current_bytes() != previous:
            raise SessionUnavailable("publication-indeterminate")

    def confirm_current(self, candidate):
        with self._lock():
            bundle = self._accepted()
            if bundle is None or bundle["generation"] != candidate.expected:
                raise SessionUnavailable("newer-session-exists")


def check_dependency():
    if version("pyicloud") != "2026.10.1":
        raise SessionUnavailable("unsupported-pyicloud")
    for method in ("_validate_token", "_authenticate_with_token", "_update_state",
                   "_check_pcs_consent", "_send_pcs_request", "_is_mfa_required"):
        if not callable(getattr(PyiCloudService, method, None)):
            raise SessionUnavailable("unsupported-pyicloud")


class BoundedTransport(HTTPAdapter):
    def send(self, request, **kwargs):
        if kwargs.get("timeout") is None:
            kwargs["timeout"] = (10, 30)
        return super().send(request, **kwargs)


def restrict_redirects(session):
    resolve = session.resolve_redirects

    def safe_redirects(response, request, **kwargs):
        if response.is_redirect and request.method not in ("GET", "HEAD"):
            raise TooManyRedirects("Redirect refused for a non-read request", response=response)
        yield from resolve(response, request, **kwargs)

    session.resolve_redirects = safe_redirects


class ResumeService(PyiCloudService):
    def authenticate(self, *args, **kwargs):
        raise SessionUnavailable("interactive-authentication-required")

    def _request_pcs_for_service(self, app_name):
        if app_name in getattr(self, "_startup_pcs_ready", set()):
            return
        verify_pcs_access(self, app_name, self._check_pcs_consent())
        self._startup_pcs_ready = {*getattr(self, "_startup_pcs_ready", set()), app_name}


def verify_pcs_access(api, app_name, state, user_action=False):
    if not isinstance(state, dict):
        raise SessionUnavailable("pcs-state-ambiguous")
    if state.get("isICDRSDisabled") is False:
        return
    if state.get("isICDRSDisabled") is not True:
        raise SessionUnavailable("pcs-state-ambiguous")
    consent = state.get("isDeviceConsentedForPCS")
    if consent is False:
        raise SessionUnavailable("pcs-consent-required")
    if consent is not True:
        raise SessionUnavailable("pcs-state-ambiguous")
    result = api._send_pcs_request(app_name, derived_from_user_action=user_action)
    if isinstance(result, dict) and result.get("status") == "success":
        return
    if isinstance(result, dict) and isinstance(result.get("status"), str) and result.get("message") in (
        "Requested the device to upload cookies.",
        "Cookies not available yet on server.",
    ):
        raise SessionUnavailable("pcs-pending")
    raise SessionUnavailable("pcs-response-ambiguous")


def pcs_guidance(reason):
    return {
        "pcs-consent-required": (
            "Apple reports that device consent is required. Run './icloudctl auth --enable-pcs' "
            "and approve the request on your Apple device."
        ),
        "pcs-notification-failed": (
            "Apple did not confirm a PCS consent notification. Check your device's web access "
            "settings and try again later."
        ),
        "pcs-state-ambiguous": (
            "Apple returned incomplete PCS state, so consent cannot be determined. "
            "Check your account's iCloud web access settings and contact Apple Support if it persists."
        ),
        "pcs-response-ambiguous": (
            "Apple returned an unsupported or incomplete PCS access response. "
            "Check your account's iCloud web access settings and contact Apple Support if it persists."
        ),
        "pcs-pending": (
            "Apple reports that PCS cookies are not ready. Complete any device request, "
            "wait, and retry later."
        ),
    }.get(reason)


def readiness(api, username):
    data = api.data
    if not isinstance(data, dict):
        return "invalid-response"
    if data.get("termsUpdateNeeded"):
        return "terms-required"
    info = data.get("dsInfo")
    if not isinstance(info, dict) or not isinstance(info.get("dsid"), (str, int)) or not info["dsid"]:
        return "invalid-response"
    if info.get("appleId") and info["appleId"].casefold() != username.casefold():
        return "account-mismatch"
    if (
        data.get("hsaTrustedBrowser") is not True
        or data.get("hsaChallengeRequired")
        or api._is_mfa_required()
        or api.requires_2fa
        or api.requires_2sa
    ):
        return "verification-required"
    services = data.get("webservices")
    if not isinstance(services, dict):
        return "drive-unavailable"
    for name in ("drivews", "docws"):
        service = services.get(name)
        if not isinstance(service, dict) or not isinstance(service.get("url"), str):
            return "drive-unavailable"
        if not service["url"].startswith("https://"):
            return "drive-unavailable"
    upload_cookies = [c for c in api.session.cookies
                      if c.name == "X-APPLE-WEBAUTH-VALIDATE"
                      and c.value and not c.is_expired()]
    if len(upload_cookies) != 1 or not re.search(r"\bt=([^:]+)", upload_cookies[0].value):
        return "upload-cookie-missing"
    return None


def require_readiness(api, username):
    reason = readiness(api, username)
    if reason:
        raise SessionUnavailable(reason)


def _exception_chain(error):
    seen = set()
    while error is not None and id(error) not in seen:
        seen.add(id(error))
        yield error
        error = error.__cause__ or error.__context__


def failure_kind(error):
    chain = list(_exception_chain(error))
    for exc in chain:
        response = getattr(exc, "response", None)
        status = getattr(response, "status_code", None)
        code = getattr(exc, "code", None)
        if status == 429 or (isinstance(status, int) and status >= 500):
            return "transient"
        if code == 429 or (isinstance(code, int) and code >= 500):
            return "transient"
    if any(isinstance(exc, (PyiCloud2FARequiredException, PyiCloud2SARequiredException,
                            PyiCloudAcceptTermsException)) for exc in chain):
        return "needs-user"
    for exc in chain:
        response = getattr(exc, "response", None)
        if isinstance(exc, PyiCloudAuthRequiredException):
            return "rejected"
        if getattr(exc, "code", None) in (401, 421, 450) or getattr(response, "status_code", None) in (401, 421, 450):
            return "rejected"
    return "transient"


class SavedSession:
    """One attempt owns one client generation. There is no automatic retry."""

    def __init__(self, username, cookie_directory):
        self.username = username
        self.cookie_directory = cookie_directory
        self.state = State.RECOVERING
        self.reason = None
        self.client = None
        self.generation = None
        self.candidate = None
        self._routing_used = False
        self._attempt_lock = threading.Lock()

    def _client(self, store):
        self.candidate = store.begin()
        api = ResumeService(
            self.username, password=None,
            cookie_directory=self.candidate.directory,
            authenticate=False, accept_terms=False,
        )
        restrict_redirects(api.session)
        api.session.mount("https://", BoundedTransport(max_retries=0))
        api.session.mount("http://", BoundedTransport(max_retries=0))
        return api

    def _call(self, api, operation):
        try:
            return operation()
        except Exception as error:
            for exc in _exception_chain(error):
                response = getattr(exc, "response", None)
                if getattr(response, "status_code", None) != 421 or self._routing_used:
                    continue
                partition = response.headers.get("x-apple-user-partition", "")
                if re.fullmatch(r"[0-9]{1,3}", partition):
                    self._routing_used = True
                    api._setup_endpoint = f"https://p{partition}-setup.icloud.com/setup/ws/1"
                    return operation()
            raise

    def _finish(self, state, reason):
        self.state = state
        self.reason = reason
        return self

    def _prepare(self, api):
        require_readiness(api, self.username)
        api._update_state()
        api._request_pcs_for_service("iclouddrive")
        require_readiness(api, self.username)

    def resume(self):
        with self._attempt_lock:
            return self._resume()

    def _resume(self):
        if self.state is not State.RECOVERING or self.candidate is not None:
            return self
        api = None
        locks = ExitStack()
        try:
            check_dependency()
            store = SessionStore(self.cookie_directory, self.username)
            locks.enter_context(store._lock("resume.lock", nonblocking=True))
            api = self._client(store)
            cookies = [c for c in api.session.cookies
                       if c.name == "X-APPLE-WEBAUTH-TOKEN" and c.value and not c.is_expired()]
            if len(cookies) > 1:
                return self._finish(State.NEEDS_USER, "ambiguous-cookie")
            validated = False
            if cookies:
                try:
                    api.data = self._call(api, api._validate_token)
                    validated = True
                except Exception as error:
                    kind = failure_kind(error)
                    if kind != "rejected":
                        return self._finish(
                            State.NEEDS_USER if kind == "needs-user" else State.RETRY_LATER,
                            "verification-required" if kind == "needs-user" else "validation-unavailable",
                        )
                    endpoint = api._setup_endpoint
                    api.session.close()
                    self.candidate.close()
                    api = self._client(store)
                    api._setup_endpoint = endpoint
            if not validated:
                if not api.session.data.get("session_token"):
                    return self._finish(State.NEEDS_USER, "no-saved-session")
                self._call(api, api._authenticate_with_token)
            self._prepare(api)
            api.session._save_session_data()
            api.session.cookies.save()
            require_readiness(api, self.username)
            try:
                self.generation = store.publish(
                    self.candidate, validate=lambda: require_readiness(api, self.username),
                )
            except SessionUnavailable as error:
                if error.args != ("newer-session-exists",):
                    raise
                api.session.close()
                self.candidate.close()
                api = self._client(store)
                # Adopt at most once, validate against Apple again, and never
                # exchange tokens or republish after losing a generation race.
                api.data = self._call(api, api._validate_token)
                self._prepare(api)
                store.confirm_current(self.candidate)
                require_readiness(api, self.username)
                self.generation = self.candidate.expected
            self.client = api
            return self._finish(State.AUTHENTICATED, "saved-session-accepted")
        except SessionUnavailable as error:
            reason = error.args[0]
            needs_user = reason in (
                "unsupported-pyicloud", "pcs-consent-required", "pcs-state-ambiguous",
                "pcs-response-ambiguous", "terms-required",
                "account-mismatch", "verification-required", "upload-cookie-missing",
                "invalid-session-bundle",
            )
            if reason == "publication-indeterminate":
                state = State.INDETERMINATE
            else:
                state = State.NEEDS_USER if needs_user else State.RETRY_LATER
            return self._finish(state, reason)
        except Exception as error:
            kind = failure_kind(error)
            return self._finish(
                State.NEEDS_USER if kind in ("needs-user", "rejected") else State.RETRY_LATER,
                "verification-required" if kind == "needs-user" else "resume-refused" if kind == "rejected" else "resume-unavailable",
            )
        finally:
            locks.close()
            if self.client is None:
                if api is not None:
                    api.session.close()
                if self.candidate is not None:
                    self.candidate.close()
