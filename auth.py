#!/usr/bin/env python3
import os
import sys
import yaml
from pyicloud import PyiCloudService
from pyicloud.exceptions import PyiCloudTrustedDeviceVerificationException


def load_config(path):
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def main():
    config_path = os.path.expanduser(
        sys.argv[1] if len(sys.argv) > 1 else "~/.config/icloud-linux/config.yaml"
    )
    cfg = load_config(config_path)

    username = cfg.get("username")
    password = cfg.get("password")
    cookie_dir = os.path.expanduser(
        cfg.get("cookie_dir", "~/.config/icloud-linux/cookies")
    )
    os.makedirs(cookie_dir, exist_ok=True)

    if not username or not password:
        print("Missing username/password in config", file=sys.stderr)
        sys.exit(1)

    api = PyiCloudService(username, password, cookie_directory=cookie_dir)

    print(f"DEBUG: requires_2fa={api.requires_2fa}, requires_2sa={api.requires_2sa}, is_trusted={api.is_trusted_session}", file=sys.stderr)

    if api.requires_2fa:
        print("Sending push to your Apple devices — tap Allow when prompted...", flush=True)
        api.request_2fa_code()
        print("Push approved. Enter the 6-digit code shown on your device.")
        code = input("2FA code: ").strip()
        try:
            result = api.validate_2fa_code(code)
        except PyiCloudTrustedDeviceVerificationException as e:
            print(f"DEBUG: bridge verification failed ({e}), retrying via legacy path", file=sys.stderr)
            api._clear_trusted_device_bridge_state()
            result = api.validate_2fa_code(code)
        print(f"DEBUG: validate_2fa_code={result}, requires_2sa={api.requires_2sa}, is_trusted={api.is_trusted_session}", file=sys.stderr)
        if not result:
            print("Invalid 2FA code", file=sys.stderr)
            sys.exit(1)
        if not api.is_trusted_session:
            api.trust_session()
            print(f"DEBUG: after trust_session: requires_2fa={api.requires_2fa}, requires_2sa={api.requires_2sa}, is_trusted={api.is_trusted_session}", file=sys.stderr)

    if api.requires_2sa:
        print("2SA required.")
        devices = api.trusted_devices
        for i, device in enumerate(devices):
            label = device.get("deviceName") or f"SMS to {device.get('phoneNumber', 'unknown')}"
            print(f"{i}: {label}")
        idx = int(input("Select device index [0]: ").strip() or "0")
        device = devices[idx]
        if not api.send_verification_code(device):
            print("Failed to send verification code", file=sys.stderr)
            sys.exit(1)
        code = input("Verification code: ").strip()
        if not api.validate_verification_code(device, code):
            print("Invalid verification code", file=sys.stderr)
            sys.exit(1)

    if api.requires_2fa or api.requires_2sa:
        print("Authentication incomplete", file=sys.stderr)
        sys.exit(1)

    print("AUTH_OK")
    print(f"Cookie/session data stored under: {cookie_dir}")


if __name__ == "__main__":
    main()
