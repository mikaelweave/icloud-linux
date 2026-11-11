import os
from pathlib import Path
import socket
import sys
import tempfile
import unittest
from unittest.mock import patch


def main():
    root = Path(__file__).resolve().parent
    with tempfile.TemporaryDirectory(prefix=".offline-tests-", dir=root) as directory:
        fixture = Path(directory)
        home, temporary = fixture / "home", fixture / "tmp"
        home.mkdir()
        temporary.mkdir()
        environment = {"HOME": str(home), "TMPDIR": str(temporary), "ICLOUD_CHINA": "0"}
        for kind in ("CONFIG", "CACHE", "DATA", "STATE"):
            environment[f"XDG_{kind}_HOME"] = str(home / kind.lower())
        old_tempdir = tempfile.tempdir
        try:
            tempfile.tempdir = str(temporary)
            with patch.dict(os.environ, environment), patch.object(
                socket.socket, "connect", side_effect=AssertionError("network forbidden")
            ), patch.object(
                socket.socket, "connect_ex", side_effect=AssertionError("network forbidden")
            ), patch.object(
                socket.socket, "sendto", side_effect=AssertionError("network forbidden")
            ), patch.object(
                socket.socket, "sendmsg", side_effect=AssertionError("network forbidden")
            ), patch.object(
                socket, "create_connection", side_effect=AssertionError("network forbidden")
            ), patch.object(
                socket, "getaddrinfo", side_effect=AssertionError("DNS forbidden")
            ), patch.object(
                socket, "gethostbyname", side_effect=AssertionError("DNS forbidden")
            ), patch.object(
                socket, "gethostbyname_ex", side_effect=AssertionError("DNS forbidden")
            ), patch.object(
                socket, "gethostbyaddr", side_effect=AssertionError("DNS forbidden")
            ), patch.object(
                socket, "getnameinfo", side_effect=AssertionError("DNS forbidden")
            ):
                suite = (
                    unittest.defaultTestLoader.loadTestsFromNames(sys.argv[1:])
                    if sys.argv[1:] else unittest.defaultTestLoader.discover(str(root))
                )
                result = unittest.TextTestRunner(verbosity=1).run(suite)
        finally:
            tempfile.tempdir = old_tempdir
    return 0 if result.wasSuccessful() else 1


if __name__ == "__main__":
    raise SystemExit(main())
