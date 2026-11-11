# icloud-linux

Mount iCloud Drive on Linux as a fast local-first FUSE filesystem with persistent caching, selective hydration, and on-demand sync.

## What This Is

`icloud-linux` makes your iCloud Drive show up like a normal folder on Linux.

It is designed to feel much more local than a naive network mount:

- folders and filenames are cached on disk
- file contents are downloaded into a local mirror on demand or in the background
- reads usually come from local storage, not from iCloud on every access
- local changes are written immediately and synced back on the next sync pass
- remote changes are pulled in by the sync engine on demand or on a timer

In practice, that means `find`, editors, shells, and normal file browsing work against a persistent local cache instead of blocking on iCloud for every operation.

## How It Works

There are three main pieces:

- Metadata crawl: the first run scans your iCloud Drive and builds a local index.
- Hydration: file contents are downloaded into the local cache — either on demand when a file is opened (lazy mode) or proactively in the background (background mode).
- Sync engine: local edits upload and remote changes are refreshed either automatically on a timer or on demand via `icloudctl sync`.

Important behavior:

- The mount is local-first.
- Restarts reuse the existing cache instead of starting from zero.
- If you open a file before it has finished hydrating, that file is downloaded first and then served locally.
- Failed warmup downloads are retried automatically with backoff.
- If a path changed both locally and remotely, the local version is preserved as a conflict copy instead of being silently overwritten.
- With `auto_sync: false`, the driver starts and mounts immediately without spawning background polling threads. Run `icloudctl sync` when you want a fresh pull from iCloud.

## Who This Is For

This project is for people who want:

- a normal folder they can browse on Linux
- Apple ID + 2FA support
- a persistent local cache
- control over which folders are hydrated vs stub-only
- on-demand syncing instead of constant background polling

If you want a quick setup and do not care about the internal details, use `./icloudctl quickstart`.

## Requirements

You need:

- Linux
- Python 3.10 or later with `venv`
- FUSE
- `systemctl --user`

### Debian / Ubuntu

```bash
sudo apt-get update
sudo apt-get install -y fuse libfuse-dev pkg-config python3-venv
```

### Fedora

```bash
sudo dnf install python3-devel fuse fuse-libs fuse-devel gcc make
```

## Fast Setup

```bash
git clone https://github.com/IsmaeelAkram/icloud-linux.git
cd icloud-linux
./icloudctl quickstart ~/iCloud
```

This will:

1. create the Python virtual environment
2. install dependencies
3. create the config and user service
4. ask for your Apple ID email and password
5. run the one-time interactive authentication flow
6. start the background service

After setup, your files will be mounted at `~/iCloud` unless you chose another path.

## Simple Setup, Step By Step

If you prefer to do setup one step at a time:

```bash
./icloudctl init ~/iCloud
./icloudctl configure
./icloudctl auth
./icloudctl start
```

## Why Authentication Is Split Into Two Steps

Systemd user services are non-interactive. They cannot pause and wait for a 2FA code.

So this project uses:

- `./icloudctl auth` for the interactive one-time Apple login and 2FA flow
- a generated user service that reuses the saved session cookies in the background

At startup, the daemon validates an unexpired saved web-auth cookie. If the
cookie is missing or Apple rejects it, the daemon attempts one saved-token
exchange. The daemon does not use a password, request MFA, grant trust, accept
terms, or request device consent. Apple may still send a notification or reject
the saved token. `hydrate_dir.py` uses the same resume-only path.

The startup log reports `authenticated`, `needs-user`, `retry-later`, or
`publication-indeterminate`. `authenticated` means Apple reported a trusted
session and the required Drive metadata, PCS access, and upload cookie were
present at the last readiness check before publication. A cookie can expire
after that check, including before startup returns. `authenticated` does not
guarantee that later requests will succeed. `retry-later` includes transport errors,
throttling, server errors, storage conflicts, and `pcs-pending`. Retry startup
later. The daemon does not schedule a login retry.

For `needs-user`, check the reason in the startup log. If Apple explicitly
requires device consent, run:

```bash
./icloudctl auth --enable-pcs
./icloudctl restart
```

For other authentication or verification requirements, run:

```bash
./icloudctl auth
./icloudctl restart
```

For `pcs-state-ambiguous` or `pcs-response-ambiguous`, the adapter cannot
determine access from Apple's response. Check your account's iCloud web access
settings and contact Apple Support if the response remains incomplete. Repeated
consent requests cannot resolve a missing consent field.

For `pcs-pending`, complete any Apple device request and wait for the cookies
to become available. Restart the daemon later. If an interactive authentication
attempt reports this state, it does not save the candidate; wait before retrying
authentication. The daemon never sends a device-consent notification. When
Apple already reports device consent, the daemon makes one PCS request without
claiming a user action. Apple may request that a device upload cookies in
response.

When `--enable-pcs` sends a request, approve it on your Apple device, then
press Enter. The command checks Apple's consent state and PCS response and
rechecks session readiness before saving.

There is no automatic runtime session recovery or reload. If a session fails
after startup, the client is not replaced. Runtime cookie changes remain in the
client's private working directory and are not continuously checkpointed.
Existing hydrated cache reads remain available when startup cannot resume.

The adapter requires `pyicloud==2026.10.1`. It honors cookie expiry and Apple's
reported trust. An HTTP 421 with a numeric partition hint permits one retry
against the corresponding Apple setup host. Requests have connect and read
timeouts and no automatic transport retries. GET and HEAD requests can follow
HTTP redirects. Other methods do not follow redirects automatically.

### Saved session storage

Before upgrading, stop old-version processes that use the same cookie
directory, including authentication and standalone hydration commands. Old
versions do not honor the new session store or its process lock.

The store lives under `<cookie_dir>/.saved-sessions/<account-hash>/`. Each
client writes a private working copy. New directories use mode `0700` and
secret files use mode `0600`. The first attempt reads the legacy account
`.session` and `.cookiejar` files without changing them. After Apple accepts
the candidate, one atomic `accepted.json` bundle holds the pair. A lock and
generation check prevent stale candidates from replacing a newer interactive
session when all writers use the lock. The store checks readiness immediately
before replacing the bundle and synchronizes the directory after replacement.
If publication fails after replacement, the store attempts to restore the
previous bundle while holding the lock. If it cannot confirm restoration is
durable, it reports `publication-indeterminate`. The old bundle may no longer
be present. Stop all session users and inspect the private store before
retrying; its files contain credentials.

A process crash or power loss during publication or rollback leaves the
checkpoint outcome dependent on the filesystem and storage guarantees. The
process cannot report an indeterminate outcome after it crashes. The lock does
not protect against old versions or other writers that ignore it.

Once a bundle exists, the legacy files are not imported again. Interactive
authentication also uses a private candidate and publishes only after the
trust and Drive checks pass. A corrupt bundle requires explicit authentication.
The old bundle is retained until a validated replacement is published.
An abrupt exit or cleanup failure can leave `work-*`, `pending-*`, or
`rollback-*` entries containing credentials. Remove them only after all
processes using the store have stopped.

An incomplete or malformed metadata crawl is discarded rather than applied as
remote deletions. A failed on-demand crawl does not write `sync_done`. The
marker does not distinguish concurrent sync requests.

This change does not add runtime authentication recovery or uncertain-write
reconciliation. The sync engine's write and retry logic is unchanged from the
merged version. The redirect policy above is the exception. A failed or
interrupted remote mutation may have succeeded on Apple even when the client
did not receive confirmation. Existing retries are not proof that repeating
such a mutation is safe.

### Offline verification

Run the suite with an interpreter that already has the pinned dependencies:

```bash
python -B offline_tests.py
```

The runner creates a synthetic home and temporary directory in the checkout
and blocks socket connections and DNS. Authentication tests use synthetic
sessions and mocked responses through the installed pyicloud implementation.
They do not contact Apple or use your account configuration.

Existing installations can update the Python dependencies without recreating
their configuration or service:

```bash
.venv/bin/python -m pip install -r requirements.txt
```

The pinned pyicloud release avoids repeated delivery requests within one
authentication challenge. Apple can still require a new code when a session
expires or a different delivery method is selected.

If Apple reports that updated terms of service must be accepted, run:

```bash
./icloudctl auth --accept-terms
```

### iOS 26 Beta 2FA Workaround

iOS 26 beta may deliver a push notification popup instead of a numeric 2FA code. If this happens, use `--force-sms` to request an SMS code:

```bash
./icloudctl auth --force-sms
```

Apple may send a device prompt during sign-in before the SMS request. If
sign-in already requested an SMS, the command does not request another one.

Use `--debug` to see Apple's reported auth mode and diagnose delivery issues:

```bash
./icloudctl auth --debug
```

## Everyday Commands

```bash
./icloudctl start
./icloudctl stop
./icloudctl restart
./icloudctl refresh
./icloudctl status
./icloudctl logs
./icloudctl doctor
./icloudctl hydrate [--dry-run] [--verbose]
./icloudctl sync [--timeout SECONDS] [--quiet]
./icloudctl clear-cache
./icloudctl uninstall
```

What they do:

- `start`: starts the background user service
- `stop`: stops the service and unmounts the folder
- `restart`: restarts the service cleanly
- `refresh`: asks the running service to crawl remote iCloud Drive metadata now
- `status`: shows whether the service is running
- `logs`: tails the service logs
- `doctor`: checks common setup issues
- `hydrate`: blocks until all eligible files (per `sync_paths` / `exclude_paths`) are fully downloaded locally. Use this before copying files to ensure nothing triggers a mid-copy download. `--dry-run` shows what would be downloaded without actually downloading.
- `sync`: triggers an on-demand remote metadata sync in the running driver (sends SIGUSR1, waits for completion). Useful when `auto_sync: false` is set.
- `clear-cache`: deletes the local mirror and sync database, then rebuilds them on next start
- `uninstall`: removes the generated user service

## What Happens After You Start It

On the first run:

- the service crawls your iCloud Drive metadata
- it mounts the folder
- if `warmup_mode: background`, it starts downloading file contents into the local cache
- if `warmup_mode: lazy`, file contents download only when each file is first opened

On later runs:

- it reuses the cache stored on disk
- if `auto_sync: true`, it refreshes remote metadata and uploads local changes on a timer
- if `auto_sync: false`, it mounts immediately with no background polling; run `icloudctl sync` on demand

## Controlling the Synchronization Boundary

By default all of iCloud Drive is indexed (stubs created everywhere), and the
mount permits mutations throughout the drive. Configure `sync_paths` to make
selective sync a hard boundary: only allowlisted paths can be hydrated or
mutated through the mount.

**`sync_paths`** — allow-list. Only paths in this list have file contents
downloaded or may be created, written, truncated, renamed, moved, or deleted
through the mount. Operations outside the allowlist fail with `EACCES` and are
never queued for upload. An empty or omitted value retains unrestricted
behavior.

**`exclude_paths`** — deny-list. Paths matching these prefixes are never
hydrated or mutated through the mount, even if they fall under a `sync_path`.
The deny-list is evaluated first.

Example config:

```yaml
# Only hydrate /Downloads
sync_paths:
  - /Downloads

# But skip this large subfolder for now
exclude_paths:
  - /Downloads/Large Archive
```

With this config:

- `/Downloads/*` → file contents downloaded and mutations permitted
- `/Downloads/Large Archive/*` → stubs only; mutations rejected
- Everything else on iCloud → stubs only; mutations rejected

For a rename or move, both the source and destination must be permitted.
When you're ready to synchronize an excluded folder, remove it from
`exclude_paths` and restart the service.

## Auto-Sync vs On-Demand Sync

**`auto_sync: true`** (default) — background threads poll iCloud on the configured `upload_interval_seconds` and `remote_refresh_interval_seconds` schedules. Good for setups where files change frequently.

**`auto_sync: false`** — no polling threads start. The driver mounts and waits. Use `icloudctl sync` to pull the latest remote changes when you need them. Good for low-churn libraries where constant polling is wasteful and you want predictable resource usage.

When `auto_sync: false`, the recommended workflow is:

```bash
icloudctl sync          # pull latest metadata from iCloud
icloudctl hydrate       # download file contents for all eligible paths
# now copy from mirror: ~/.cache/icloud-linux/mirror/Downloads/
```

## Shared Folders And Presented Permissions

This fork adds two capabilities on top of upstream, aimed at containerized access
(for example an Incus / OpenClaw container that reads the mount or the on-disk
mirror directly):

**iCloud shared folders are read/write.** Folders shared with you in iCloud Drive
are mounted like any other path, and local edits (create, write, rename, move,
delete) are pushed back through iCloud's share-aware Drive endpoints. The share
identity is persisted in the sync-state database so shared files survive restarts
and continue to upload/download correctly.

**Presented ownership and permissions are configurable.** Add a `permissions:`
block to your config to control the default uid/gid and modes for cloud files.
New files and directories use the caller's UID/GID and requested creation mode
(with the caller's umask applied by FUSE). `chmod`, including `fchmod`, changes
the mounted permissions; `getattr` reports those modes and ownership.

```yaml
permissions:
  file_mode: "0644"
  dir_mode: "0755"
  # uid: 1000   # defaults to the process uid
  # gid: 1000   # defaults to the process gid
```

To let another user or container access the FUSE mount itself, set
`fuse_options.allow_other: true`. On most hosts this also requires
`user_allow_other` in `/etc/fuse.conf`.

The mount always uses `default_permissions` so the kernel enforces POSIX access
checks even with `allow_other`. A `0600` file is private to its creator, including
when that creator is an unprivileged container user with a mapped host UID.
Existing cloud files retain the configured defaults until explicitly changed.

Mode and creator ownership are persisted locally in the sync-state database,
survive hydration, cache normalization, renames and service restarts, and are
not uploaded as iCloud metadata. Atomic replacement carries the staging file's
permissions to the destination. The backing mirror continues to use configured
cache modes and process ownership so the daemon can synchronize private or
read-only mounted files. Share the FUSE mount, not the backing mirror: direct
cache access bypasses both these permissions and sync tracking. `fsync` flushes
local cached content; it does not confirm a cloud upload.

## Copying Files to Another Location

Once files are hydrated, copy from the local mirror rather than from the FUSE mount:

```bash
cp -r ~/.cache/icloud-linux/mirror/Downloads/ ~/Desktop/intake/
```

The mirror is a plain directory on disk — reads are instant, no network involved. Copying from the FUSE mount works too but may trigger downloads for any files not yet hydrated.

## Local Cache And Sync State

The project keeps its local state here:

- Config: `~/.config/icloud-linux/config.yaml`
- Session cookies: `~/.config/icloud-linux/cookies`
- Service env: `~/.config/icloud-linux/icloud.env`
- User service: `~/.config/systemd/user/icloud.service`
- Local cache root: `~/.cache/icloud-linux`
- Local mirror: `~/.cache/icloud-linux/mirror`
- Sync state database: `~/.cache/icloud-linux/state.sqlite3`
- Logs: `~/.local/state/icloud-linux/icloud.log`
- On-demand sync marker: `~/.local/state/icloud-linux/sync_done`

## What "Sync Engine" Means Here

This repo is not just a read-only mount and it is not only a foreground downloader.

The sync engine:

- tracks local dirty files and directories
- uploads local changes (when `auto_sync: true` or on `icloudctl sync`)
- refreshes remote metadata (when `auto_sync: true` or on `icloudctl sync`)
- hydrates missing file contents on demand or in the background
- preserves local conflict copies when local and remote diverge

That makes it closer to a real cached sync client than a simple network filesystem wrapper.

## Troubleshooting

### The service will not start

Run:

```bash
./icloudctl status
./icloudctl doctor
./icloudctl logs
```

### Authentication expired

Run:

```bash
./icloudctl auth
./icloudctl restart
```

### Service starts but auth fails silently (unauthenticated mode)

When running under systemd (no TTY), a failed auth parks the service in unauthenticated mode instead of crashing — this prevents the service from crash-looping and triggering Apple's account lockout. You will see `UNAUTHENTICATED mode` in the logs. Fix by running:

```bash
./icloudctl auth
./icloudctl restart
```

### I want to rebuild everything locally

Run:

```bash
./icloudctl clear-cache
```

That removes the local mirror and sync database. The next start will rebuild them from iCloud.

### I want to confirm it is using the local cache

After the service has had time to hydrate files:

```bash
find ~/iCloud -type f | head
```

You can watch logs in another terminal:

```bash
./icloudctl logs
```

Normal activity with `auto_sync: false` will show the reconcile pass at startup and then go quiet until you run `icloudctl sync`.

### I want to check hydration progress

```bash
./icloudctl hydrate --dry-run
```

This reports how many files are already local vs need downloading, without downloading anything.

## Notes

- Warmup downloads are intentionally conservative because iCloud file downloads are sensitive to aggressive parallelism.
- The generated systemd unit is created by `./icloudctl`; the repo does not rely on checked-in service files anymore.
- `./icloudctl` installs and manages a user-level systemd service. It does not install or manage a system-level unit.
- `icloudctl sync` checks the user service first, then a manually configured system service named `icloud.service`. A system service can run as a non-root account with `User=`.
- Run `icloudctl sync` as the driver's account and with the same home directory. The caller needs permission to signal the driver. Both processes use `~/.local/state/icloud-linux/sync_done`. Running the helper with `sudo` against a non-root driver can make it watch the wrong completion file and time out.
- When `auto_sync: false`, the SIGUSR1 signal triggers a one-shot sync in the background; `icloudctl sync` handles sending that signal and waiting for completion.
