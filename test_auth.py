import io
import unittest
from unittest.mock import Mock, patch

from pyicloud.exceptions import PyiCloudAPIResponseException

from auth import do_sms_forced, get_partition, sms_code_was_accepted


def api_error(status, payload, text="sensitive response"):
    response = Mock()
    response.status_code = status
    response.json.return_value = payload
    response.text = text
    return PyiCloudAPIResponseException(
        "Authentication required for Account.",
        status,
        response,
    )


class SmsValidationTests(unittest.TestCase):
    def test_partition_request_has_connect_and_read_timeouts(self):
        session = Mock()
        session.post.return_value.headers = {"x-apple-user-partition": "1"}

        with patch("auth.requests.Session", return_value=session):
            self.assertEqual(get_partition(), "1")

        session.post.assert_called_once_with(
            "https://setup.icloud.com/setup/ws/1/validate",
            json={},
            timeout=(10, 60),
        )

    def test_recognizes_accepted_409_response(self):
        error = api_error(409, {"securityCode": {"valid": True, "code": "secret"}})

        self.assertTrue(sms_code_was_accepted(error))

    def test_rejects_invalid_or_different_error_response(self):
        self.assertFalse(
            sms_code_was_accepted(
                api_error(409, {"securityCode": {"valid": False}})
            )
        )
        self.assertFalse(
            sms_code_was_accepted(
                api_error(401, {"securityCode": {"valid": True}})
            )
        )
        self.assertFalse(sms_code_was_accepted(api_error(409, [])))

    def test_accepted_409_continues_to_trust_session(self):
        api = Mock()
        api._request_sms_2fa_code.return_value = True
        api._validate_sms_code.side_effect = api_error(
            409,
            {"securityCode": {"valid": True, "code": "secret"}},
        )
        api.is_trusted_session = False
        api.trust_session.return_value = True

        with patch("builtins.input", return_value="123456"):
            do_sms_forced(api)

        api.trust_session.assert_called_once_with()

    def test_invalid_response_does_not_print_sensitive_payload(self):
        api = Mock()
        api._request_sms_2fa_code.return_value = True
        api._validate_sms_code.side_effect = api_error(
            409,
            {"securityCode": {"valid": False, "code": "secret"}},
        )
        stderr = io.StringIO()

        with patch("builtins.input", return_value="123456"), patch(
            "sys.stderr", stderr
        ), self.assertRaises(SystemExit):
            do_sms_forced(api)

        self.assertNotIn("secret", stderr.getvalue())
        self.assertEqual(stderr.getvalue().strip(), "Code validation failed.")


if __name__ == "__main__":
    unittest.main()
