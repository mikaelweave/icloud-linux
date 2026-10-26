# Authentication and 2FA

Run `./icloudctl auth` interactively to establish a trusted iCloud session.
The background service reuses the saved cookies and cannot prompt for a code.
Use your Apple account password, not an app-specific password.

## Authentication methods

The bootstrap detects Apple's account partition before signing in. It requests
Apple's preferred 2FA delivery method and asks for the numeric code. Accounts
using older two-step authentication can still select a trusted device and enter
its verification code.

If your device receives a push notification without a numeric code, request SMS:

```bash
./icloudctl auth --force-sms
```

If Apple requires updated terms of service, accept them explicitly:

```bash
./icloudctl auth --accept-terms
```

`./icloudctl auth --debug` prints the reported authentication mode and delivery
options. A browser trust token can also be supplied with `--trust-token`.
Treat that token as a credential.

## Re-authenticating

When the session expires, run:

```bash
./icloudctl auth
./icloudctl restart
```

For a system-service installation, use `./icloudctl restart-system` instead.
`./icloudctl auth-status` reports the age of the saved session files. This is
an estimate, not a live check that Apple still accepts the session.

## Service behavior without a session

When authentication fails during non-interactive startup, the driver keeps
running without a sync engine. Already hydrated cached files remain readable.
Writes return `EACCES`, and reads requiring a download return `EIO`.
Authenticate interactively and restart the service to restore synchronization.

This replaces the older startup behavior that exited with code 2 on
authentication failures. Generated units retain `RestartPreventExitStatus=2`
for compatibility, but non-interactive authentication failures no longer use
that exit path.

## Dependency compatibility

The project now requires `pyicloud>=2.6.4` for deferred authentication.
Setup no longer runs the compatibility patch for pyicloud 2.5.0.
`scripts/patch_pyicloud_hsa2_bridge.py` remains as a legacy utility and is not
part of installation or authentication.
