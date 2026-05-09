# Authentication and 2FA

## How Apple HSA2 authentication works

iCloud uses HSA2 (Hardware Security Account v2) for two-factor authentication. When
pyicloud signs in with your Apple ID credentials, Apple detects the new login attempt
and routes the 2FA challenge through a **WebSocket bridge**:

1. pyicloud connects to `websocket.push.apple.com` and registers a push token
2. pyicloud posts a "step 0" payload to Apple's auth endpoint, which triggers Apple to
   send a push notification to your trusted devices ("Are you trying to sign in?")
3. You tap **Allow** on your iPhone or Mac
4. Apple sends the approval back over the WebSocket
5. A 6-digit code appears on your device — you enter it to complete sign-in

The bridge step is why you must run `./icloudctl auth` interactively rather than
letting the background service handle 2FA — the service has no terminal and cannot
orchestrate the WebSocket handshake.

## Re-authenticating

Sessions expire roughly every 30 days. When the service starts failing with
"2FA required, but no interactive terminal is available", re-authenticate:

```bash
systemctl --user stop icloud.service
./icloudctl auth          # follow the prompts; tap Allow on your Apple device
systemctl --user start icloud.service
```

The service is configured with `RestartPreventExitStatus=2` so it will enter a
permanent `failed` state on auth errors rather than crash-looping. This prevents
hammering Apple's auth endpoint and triggering rate-limits or account lockouts.

## pyicloud 2.5.0 bridge patch (PR #233)

**Background:** pyicloud 2.5.0 introduced the WebSocket bridge described above, but
Apple subsequently changed their bridge push payload format — the `sessionUUID` field
was replaced by `flowid`. This caused pyicloud to raise a Pydantic validation error
and fall back to a legacy path that does not trigger the device push, so users never
received the "Are you trying to sign in?" notification.

**Symptoms:**
- `./icloudctl auth` prompts for a code but no push arrives on Apple devices
- Signing in via a browser (appleid.apple.com) works fine and does send a push
- The background service crash-loops with "Invalid email/password combination" or
  "2FA required, no interactive terminal" at high restart counts

**Fix:** `pyicloud/hsa2_bridge.py` is patched (in the venv) to accept `flowid` as a
fallback when `sessionUUID` is absent, based on the approach in
[timlaing/pyicloud PR #233](https://github.com/timlaing/pyicloud/pull/233).

The patch makes `_BridgePushPayloadModel` accept both fields:

```python
session_uuid: Optional[StrictStr] = Field(default=None, alias="sessionUUID")
flow_id: Optional[StrictStr] = Field(default=None, alias="flowid")
```

And resolves whichever is present:

```python
resolved_session_uuid = validated.session_uuid or validated.flow_id
```

The mismatch check between the locally generated session UUID and the payload UUID is
also removed, because with `flowid` Apple generates the identifier rather than echoing
back the client's value.

**When PR #233 merges into a pyicloud release**, update `requirements.txt` to the new
version, remove the comment, and re-run `pip install -r requirements.txt` inside the
venv. The venv patch will then be superseded by the official release.

## Service reliability

The systemd service is configured to stop permanently on auth failures rather than
retrying forever. Two parts make this work:

**`driver.py`** — catches auth exceptions at startup and exits with code 2:

```python
try:
    fs.init_icloud(username, password, cache_dir, cookie_dir)
except (PyiCloudFailedLoginException, PyiCloud2FARequiredException, ...):
    logger.error("Service stopping due to auth failure. Run './icloudctl auth' then './icloudctl start'.")
    sys.exit(2)
```

**`icloudctl`** — the generated service file includes:

```ini
RestartPreventExitStatus=2
```

Network errors and crashes still use exit code 1 and will be restarted automatically.
Only auth failures (which require user interaction to resolve) stop the service.
