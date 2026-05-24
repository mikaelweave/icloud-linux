# icloud-linux

Mount iCloud Drive on Linux as a fast local-first FUSE filesystem with persistent caching, background hydration, and bidirectional sync.

## What This Is

`icloud-linux` makes your iCloud Drive show up like a normal folder on Linux.

It is designed to feel much more local than a naive network mount:

- folders and filenames are cached on disk
- file contents are downloaded into a local mirror
- reads usually come from local storage, not from iCloud on every access
- local changes are written immediately and synced back in the background
- remote changes are pulled in by a real sync engine

In practice, that means `find`, editors, shells, and normal file browsing work against a persistent local cache instead of blocking on iCloud for every operation.

## How It Works

There are three main pieces:

- Metadata crawl: the first run scans your iCloud Drive and builds a local index.
- Background hydration: after metadata is known, file contents are downloaded into the local cache.
- Sync engine: local edits upload in the background and remote changes are refreshed on a timer.

Important behavior:

- The mount is local-first.
- Restarts reuse the existing cache instead of starting from zero.
- If you open a file before it has finished hydrating, that file is downloaded first and then served locally.
- Failed warmup downloads are retried automatically with backoff.
- If a path changed both locally and remotely, the local version is preserved as a conflict copy instead of being silently overwritten.

## Who This Is For

This project is for people who want:

- a normal folder they can browse on Linux
- Apple ID + 2FA support
- a persistent local cache
- real background syncing instead of only on-demand reads

If you want a quick setup and do not care about the internal details, use `./icloudctl quickstart`.

## Requirements

You need:

- Linux
- Python 3 with `venv`
- FUSE
- `systemctl --user` for workstation installs, or `systemctl` for root/container installs

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
git clone https://github.com/ismaeelakram/icloud-linux.git
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

For a root-owned container install that should run under the system service
manager instead of `systemctl --user`:

```bash
./icloudctl init-system /root/iCloud
./icloudctl configure your_apple_id@example.com
./icloudctl auth
./icloudctl start-system
```

## Why Authentication Is Split Into Two Steps

Systemd user services are non-interactive. They cannot pause and wait for a 2FA code.

So this project uses:

- `./icloudctl auth` for the interactive one-time Apple login and 2FA flow
- a generated user service that reuses the saved session cookies in the background

During 2FA, `./icloudctl auth` sends the trusted-device push first. Tap
**Allow** on your Apple device, then enter the 6-digit code shown on that
device.

If Apple expires your session, run:

```bash
./icloudctl auth
./icloudctl restart
```

To see how old the saved session is and roughly how many days remain before
reauthentication is likely needed, run:

```bash
./icloudctl auth-status
```

## Everyday Commands

```bash
./icloudctl start
./icloudctl stop
./icloudctl restart
./icloudctl status
./icloudctl auth-status
./icloudctl logs
./icloudctl doctor
./icloudctl clear-cache
./icloudctl uninstall
```

What they do:

- `start`: starts the background user service
- `stop`: stops the service and unmounts the folder
- `restart`: restarts the service cleanly
- `status`: shows whether the service is running
- `auth-status`: reports cookie/session age and approximate days left
- `logs`: tails the service logs
- `doctor`: checks common setup issues
- `clear-cache`: deletes the local mirror and sync database, then rebuilds them on next start
- `uninstall`: removes the generated user service

## What Happens After You Start It

On the first run:

- the service crawls your iCloud Drive metadata
- it mounts the folder
- it starts downloading file contents into the local cache in the background

On later runs:

- it reuses the cache stored on disk
- it refreshes remote metadata in the background
- it continues hydrating anything still missing

Local file writes are committed to the mirror immediately and uploaded by the sync engine in the background.

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

## What “Sync Engine” Means Here

This repo is not just a read-only mount and it is not only a foreground downloader.

The sync engine:

- tracks local dirty files and directories
- uploads local changes on a timer
- refreshes remote metadata on a timer
- hydrates missing file contents in the background
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
rg --files ~/iCloud | head
```

You can watch logs in another terminal:

```bash
./icloudctl logs
```

Normal activity should mostly look like background crawl, hydration, and sync logs rather than a separate remote fetch for every file operation.

### OpenClaw, Incus, or a VM user cannot write into the mounted vault

If you are sharing the mounted vault into another user or container, enable FUSE mount sharing in `~/.config/icloud-linux/config.yaml`:

```yaml
fuse_options:
  allow_other: true
```

Then make sure `/etc/fuse.conf` on the host contains:

```text
user_allow_other
```

If the other environment only needs read access, the default permission layout is fine.
If the host does **not** need the files and only the guest will use them, it is usually simpler to run `icloud-linux` inside that guest instead of exporting a host-managed mount.
If you want another VM or container user to create folders and files through the mount, either:

1. map the presented owner to that user's numeric IDs, or
2. relax the shared file and directory modes.

Example for a VM user with UID/GID `1000`:

```yaml
permissions:
  uid: 1000
  gid: 1000
  file_mode: "0644"
  dir_mode: "0755"
```

Example for broader shared write access when IDs do not line up cleanly:

```yaml
permissions:
  file_mode: "0666"
  dir_mode: "0777"
```

Restart the service after changing the config:

```bash
./icloudctl restart
```

Newly created and hydrated files are normalized to the configured `permissions.file_mode` and `permissions.dir_mode` values so a restrictive service umask does not leave newer vault files stuck at `0600`.
Do **not** treat `~/.cache/icloud-linux/mirror` as a writable multi-user share: direct writes to the mirror bypass the normal FUSE dirty-tracking path, so the supported writable path is the mounted filesystem itself.

## Notes

- Warmup downloads are intentionally conservative because iCloud file downloads are sensitive to aggressive parallelism.
- The generated systemd unit is created by `./icloudctl`; the repo does not rely on checked-in service files anymore.
- Workstation installs use a user-level systemd service. Root/container installs can use the documented `*-system` commands.
