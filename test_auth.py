import io
import json
import logging
import os
import runpy
import socket
import sqlite3
import sys
import tempfile
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
from auth import do_2fa, do_sms_forced


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
            body = {"hsaTrustedBrowser": True, "dsInfo": {"hsaVersion": 2},
                    "termsUpdateNeeded": self.terms}
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
        with patch("sys.argv", ["auth.py", config]):
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

    def test_driver_rejects_failed_trust(self):
        self.trust = False
        fs = SimpleNamespace(logger=logging.getLogger("offline-test"))
        with patch.object(sys.stdin, "isatty", return_value=True):
            with self.assertRaisesRegex(RuntimeError, "session trust failed"):
                ICloudFS.init_icloud(
                    fs, "synthetic@example.test", "synthetic", self.tmp.name,
                    self.tmp.name,
                )
        self.assertEqual(self.count("/verify/phone"), 1)
        self.assertEqual(self.count("/verify/phone/securitycode"), 1)
        self.assertEqual(self.count("/2sv/trust"), 1)

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


if __name__ == "__main__":
    unittest.main()
