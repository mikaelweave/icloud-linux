import contextlib
import io
import os
import shutil
import tempfile
import unittest
from unittest.mock import Mock, patch

import auth


class AuthBootstrapTests(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.mkdtemp(prefix="icloud-linux-auth-test-")
        self.config_path = os.path.join(self.root, "config.yaml")
        self.cookie_dir = os.path.join(self.root, "cookies")
        with open(self.config_path, "w", encoding="utf-8") as handle:
            handle.write(
                "username: user@example.com\n"
                "password: secret\n"
                f"cookie_dir: {self.cookie_dir}\n"
            )

    def tearDown(self):
        shutil.rmtree(self.root)

    def run_auth(self, api, inputs=()):
        input_iter = iter(inputs)
        stdout = io.StringIO()
        stderr = io.StringIO()
        with (
            patch.object(auth, "PyiCloudService", return_value=api),
            patch("sys.argv", ["auth.py", self.config_path]),
            patch("builtins.input", side_effect=lambda prompt="": next(input_iter)),
            contextlib.redirect_stdout(stdout),
            contextlib.redirect_stderr(stderr),
        ):
            auth.main()
        return stdout.getvalue(), stderr.getvalue()

    def test_2fa_push_flow_trusts_successful_session(self):
        api = Mock()
        api.requires_2fa = True
        api.requires_2sa = False
        api.is_trusted_session = False

        def validate_2fa_code(code):
            api.requires_2fa = False
            return True

        api.validate_2fa_code.side_effect = validate_2fa_code

        stdout, stderr = self.run_auth(api, inputs=["123456"])

        api.request_2fa_code.assert_called_once_with()
        api.validate_2fa_code.assert_called_once_with("123456")
        api.trust_session.assert_called_once_with()
        self.assertIn("AUTH_OK", stdout)
        self.assertNotIn("DEBUG", stderr)

    def test_2fa_invalid_code_exits_cleanly(self):
        api = Mock()
        api.requires_2fa = True
        api.requires_2sa = False
        api.is_trusted_session = False
        api.validate_2fa_code.return_value = False

        with self.assertRaises(SystemExit) as raised:
            self.run_auth(api, inputs=["000000"])

        self.assertEqual(raised.exception.code, 1)
        api.request_2fa_code.assert_called_once_with()
        api.trust_session.assert_not_called()

    def test_2sa_device_flow(self):
        api = Mock()
        api.requires_2fa = False
        api.requires_2sa = True
        api.trusted_devices = [{"deviceName": "Phone"}]
        api.send_verification_code.return_value = True

        def validate_verification_code(device, code):
            api.requires_2sa = False
            return True

        api.validate_verification_code.side_effect = validate_verification_code

        stdout, stderr = self.run_auth(api, inputs=["0", "654321"])

        api.send_verification_code.assert_called_once_with(api.trusted_devices[0])
        api.validate_verification_code.assert_called_once_with(api.trusted_devices[0], "654321")
        self.assertIn("AUTH_OK", stdout)
        self.assertNotIn("DEBUG", stderr)


if __name__ == "__main__":
    unittest.main()
