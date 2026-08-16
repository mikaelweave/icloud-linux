#!/usr/bin/env python3

import atexit
import datetime
import errno
import hashlib
import json
import logging
import os
import signal
import shutil
import sqlite3
import stat
import sys
import tempfile
import threading
import time
import uuid
from contextlib import closing
from collections import deque
from concurrent.futures import ThreadPoolExecutor

import fuse
import yaml
from fuse import Fuse
from pyicloud import PyiCloudService
from pyicloud.exceptions import (
    PyiCloud2FARequiredException,
    PyiCloud2SARequiredException,
    PyiCloudAPIResponseException,
    PyiCloudAuthRequiredException,
    PyiCloudFailedLoginException,
)
from pyicloud.services.drive import DriveNode

from icloud_session import (
    CONTROL_PLANE_TIMEOUT,
    DOWNLOAD_TIMEOUT,
    install_pyi_cloud_session_timeouts,
)

if not hasattr(fuse, "__version__"):
    fuse.__version__ = "0.2"

fuse.fuse_python_api = (0, 2)


ROOT_DRIVEWSID = "FOLDER::com.apple.CloudDocs::root"
DIRECTORY_NODE_TYPES = {"folder", "app_library"}
IO_CHUNK_SIZE = 1024 * 1024
DEFAULT_FILE_MODE = 0o644
DEFAULT_DIR_MODE = 0o755
SYNC_FAILURE_AUTH = "auth"
SYNC_FAILURE_TERMINAL = "terminal"
SYNC_FAILURE_TRANSIENT = "transient"
MAX_SYNC_ATTEMPTS = 8
AUTH_SYNC_COOLDOWN_SECONDS = 300
SQLITE_BUSY_TIMEOUT_SECONDS = 30
CRAWL_FOLDER_TIMEOUT = 60
AUTH_ERROR_TYPES = (
    PyiCloud2FARequiredException,
    PyiCloud2SARequiredException,
    PyiCloudAuthRequiredException,
    PyiCloudFailedLoginException,
)


def _retry_delay_for_attempt(attempt):
    return min(300, 5 * (2 ** max(0, attempt - 1)))


class SyncBlocked(Exception):
    pass


class SyncAuthenticationBlocked(Exception):
    pass


class MissingMirrorFile(Exception):
    pass


class HydrationFailed(Exception):
    pass


class RemoteSnapshot(dict):
    def __init__(self):
        super().__init__()
        self.complete = True
        self.failed_folders = []


class SyncPassContext:
    def __init__(self):
        self.failed_remote_parents = set()
        self.auth_failure_logged = False


def _http_status_from_exception(exc):
    try:
        response = getattr(exc, "response", None)
    except Exception:
        response = None
    if response is not None:
        try:
            status = getattr(response, "status_code", None)
        except Exception:
            status = None
        if isinstance(status, int) and not isinstance(status, bool):
            return status
    if isinstance(exc, PyiCloudAPIResponseException):
        for attribute in ("code", "status"):
            try:
                status = getattr(exc, attribute, None)
            except Exception:
                continue
            if isinstance(status, int) and not isinstance(status, bool):
                return status
    return None


def remote_item_is_absent(exc, operation):
    """Return whether a delete received a definitive remote not-found response."""
    return operation == "delete" and _http_status_from_exception(exc) == 404


def classify_sync_failure(exc, operation):
    """Classify a failed sync operation for the durable sync queue."""
    if isinstance(exc, AUTH_ERROR_TYPES):
        return SYNC_FAILURE_AUTH
    if isinstance(exc, MissingMirrorFile):
        return SYNC_FAILURE_TERMINAL

    # Conservative allow-list: only conditions we can prove are permanent are
    # terminal. pyicloud raises a typed auth exception only for HTTP 450, so a
    # bare 403 may be an expired session rather than a real permission denial;
    # retrying it costs bounded latency, while quarantining it needs a human.
    if remote_item_is_absent(exc, operation):
        return SYNC_FAILURE_TERMINAL
    return SYNC_FAILURE_TRANSIENT


def normalize_icloud_path(path):
    """Return an absolute, normalized iCloud Drive path."""
    normalized = os.path.normpath("/" + path.lstrip("/"))
    return "/" if normalized == "." else normalized


def normalize_icloud_paths(paths):
    """Normalize configured path prefixes, preserving an empty allow-list."""
    if not paths:
        return []
    return [normalize_icloud_path(path) for path in paths]


def path_allowed(path, sync_paths, exclude_paths):
    """Return whether a path is within the configured synchronization boundary."""
    path = normalize_icloud_path(path)

    for prefix in exclude_paths:
        if prefix == "/" or path == prefix or path.startswith(prefix + "/"):
            return False

    if sync_paths is None:
        return True
    return any(
        prefix == "/" or path == prefix or path.startswith(prefix + "/")
        for prefix in sync_paths
    )


class Stat(fuse.Stat):
    def __init__(self):
        self.st_mode = 0
        self.st_ino = 0
        self.st_dev = 0
        self.st_nlink = 0
        self.st_uid = 0
        self.st_gid = 0
        self.st_size = 0
        self.st_atime = 0
        self.st_mtime = 0
        self.st_ctime = 0


class IgnoreIcdrsWarning(logging.Filter):
    def filter(self, record):
        return "ICDRS is not disabled; requestWebAccessState=" not in record.getMessage()


def parse_remote_time(value):
    if not value:
        return int(time.time())
    try:
        parsed = datetime.datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ")
    except ValueError:
        return int(time.time())
    return int(calendar_timegm(parsed.timetuple()))


def calendar_timegm(timetuple):
    return int(datetime.datetime(*timetuple[:6], tzinfo=datetime.timezone.utc).timestamp())


def parse_permission_mode(value, default, label):
    if value is None:
        return default
    if isinstance(value, bool):
        raise ValueError(f"{label} must be an octal mode, not a boolean")
    if isinstance(value, int):
        mode = value
    elif isinstance(value, str):
        text = value.strip()
        if not text:
            raise ValueError(f"{label} must not be empty")
        if text.lower().startswith("0o"):
            mode = int(text, 8)
        elif all(char in "01234567" for char in text):
            mode = int(text, 8)
        else:
            raise ValueError(f"{label} must be an octal mode like 0755")
    else:
        raise ValueError(f"{label} must be an int or octal string")
    if mode < 0 or mode > 0o777:
        raise ValueError(f"{label} must be between 0000 and 0777")
    return mode


def parse_optional_int(value, default, label):
    if value is None:
        return default
    if isinstance(value, bool):
        raise ValueError(f"{label} must be an integer, not a boolean")
    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{label} must be an integer") from exc


def resolve_permissions_config(permissions):
    if permissions is None:
        permissions = {}
    if not isinstance(permissions, dict):
        raise ValueError("permissions must be a mapping")
    return {
        "uid": parse_optional_int(permissions.get("uid"), os.getuid(), "permissions.uid"),
        "gid": parse_optional_int(permissions.get("gid"), os.getgid(), "permissions.gid"),
        "file_mode": parse_permission_mode(permissions.get("file_mode"), DEFAULT_FILE_MODE, "permissions.file_mode"),
        "dir_mode": parse_permission_mode(permissions.get("dir_mode"), DEFAULT_DIR_MODE, "permissions.dir_mode"),
    }


def sha256_file(path):
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def row_to_dict(row):
    return dict(row) if row is not None else None


class NamedFileStream:
    def __init__(self, handle, name):
        self._handle = handle
        self.name = name

    def __getattr__(self, attr):
        return getattr(self._handle, attr)

    def read(self, size=-1):
        return self._handle.read(size)


class SyncState:
    def __init__(self, db_path):
        self.db_path = db_path
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        self.lock = threading.RLock()
        self.conn = sqlite3.connect(
            db_path,
            timeout=SQLITE_BUSY_TIMEOUT_SECONDS,
            check_same_thread=False,
        )
        self.conn.row_factory = sqlite3.Row
        self.conn.execute(
            f"PRAGMA busy_timeout = {SQLITE_BUSY_TIMEOUT_SECONDS * 1000}"
        )
        try:
            journal_mode = self.conn.execute("PRAGMA journal_mode=WAL").fetchone()[0]
            if journal_mode.lower() != "wal":
                logging.getLogger(__name__).warning(
                    "SQLite database %s is using journal mode %s instead of WAL; "
                    "concurrent read-only access may be degraded",
                    db_path,
                    journal_mode,
                )
        except sqlite3.OperationalError as exc:
            logging.getLogger(__name__).warning(
                "Could not enable SQLite WAL mode for %s: %s; "
                "concurrent read-only access may be degraded",
                db_path,
                exc,
            )
        self.closed = False
        self._init_db()

    def _init_db(self):
        with self.lock:
            self.conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS entries (
                    path TEXT PRIMARY KEY,
                    type TEXT NOT NULL,
                    parent_path TEXT NOT NULL,
                    remote_drivewsid TEXT,
                    remote_docwsid TEXT,
                    remote_etag TEXT,
                    remote_zone TEXT,
                    remote_shareid TEXT,
                    remote_itemid TEXT,
                    remote_unified_token TEXT,
                    size INTEGER NOT NULL DEFAULT 0,
                    mtime INTEGER NOT NULL DEFAULT 0,
                    hydrated INTEGER NOT NULL DEFAULT 0,
                    dirty INTEGER NOT NULL DEFAULT 0,
                    tombstone INTEGER NOT NULL DEFAULT 0,
                    sync_attempt_count INTEGER NOT NULL DEFAULT 0,
                    sync_next_attempt_at INTEGER,
                    sync_last_error TEXT,
                    hydrate_attempt_count INTEGER NOT NULL DEFAULT 0,
                    hydrate_next_attempt_at INTEGER,
                    hydrate_last_error TEXT,
                    failed INTEGER NOT NULL DEFAULT 0,
                    local_sha256 TEXT,
                    last_synced_at INTEGER,
                    synced_path TEXT
                );
                CREATE INDEX IF NOT EXISTS idx_entries_remote_drivewsid
                    ON entries(remote_drivewsid);
                """
            )
            columns = {
                row["name"]
                for row in self.conn.execute("PRAGMA table_info(entries)").fetchall()
            }
            if "remote_shareid" not in columns:
                self.conn.execute("ALTER TABLE entries ADD COLUMN remote_shareid TEXT")
            if "remote_itemid" not in columns:
                self.conn.execute("ALTER TABLE entries ADD COLUMN remote_itemid TEXT")
            if "remote_unified_token" not in columns:
                self.conn.execute(
                    "ALTER TABLE entries ADD COLUMN remote_unified_token TEXT"
                )
            if "sync_attempt_count" not in columns:
                self.conn.execute(
                    "ALTER TABLE entries ADD COLUMN sync_attempt_count INTEGER NOT NULL DEFAULT 0"
                )
            if "sync_next_attempt_at" not in columns:
                self.conn.execute(
                    "ALTER TABLE entries ADD COLUMN sync_next_attempt_at INTEGER"
                )
            if "sync_last_error" not in columns:
                self.conn.execute("ALTER TABLE entries ADD COLUMN sync_last_error TEXT")
            if "hydrate_attempt_count" not in columns:
                self.conn.execute(
                    "ALTER TABLE entries ADD COLUMN hydrate_attempt_count INTEGER NOT NULL DEFAULT 0"
                )
            if "hydrate_next_attempt_at" not in columns:
                self.conn.execute(
                    "ALTER TABLE entries ADD COLUMN hydrate_next_attempt_at INTEGER"
                )
            if "hydrate_last_error" not in columns:
                self.conn.execute(
                    "ALTER TABLE entries ADD COLUMN hydrate_last_error TEXT"
                )
            if "failed" not in columns:
                self.conn.execute(
                    "ALTER TABLE entries ADD COLUMN failed INTEGER NOT NULL DEFAULT 0"
                )
            self.conn.execute("DROP TABLE IF EXISTS pending_ops")
            dirty_index_columns = [
                row["name"]
                for row in self.conn.execute(
                    "PRAGMA index_info('idx_entries_dirty')"
                ).fetchall()
            ]
            if dirty_index_columns != [
                "dirty",
                "tombstone",
                "failed",
                "sync_next_attempt_at",
            ]:
                self.conn.execute("DROP INDEX IF EXISTS idx_entries_dirty")
                self.conn.execute(
                    """
                    CREATE INDEX idx_entries_dirty
                        ON entries(dirty, tombstone, failed, sync_next_attempt_at)
                    """
                )
            self.clear_sync_backoff()
            self.clear_hydrate_backoff()
            self.conn.commit()

    def close(self):
        with self.lock:
            if self.closed:
                return
            self.conn.close()
            self.closed = True

    def upsert_entry(self, entry):
        payload = {
            "path": entry["path"],
            "type": entry["type"],
            "parent_path": entry["parent_path"],
            "remote_drivewsid": entry.get("remote_drivewsid"),
            "remote_docwsid": entry.get("remote_docwsid"),
            "remote_etag": entry.get("remote_etag"),
            "remote_zone": entry.get("remote_zone"),
            "remote_shareid": self._encode_shareid(entry.get("remote_shareid")),
            "remote_itemid": entry.get("remote_itemid"),
            "remote_unified_token": entry.get("remote_unified_token"),
            "size": int(entry.get("size", 0) or 0),
            "mtime": int(entry.get("mtime", 0) or 0),
            "hydrated": int(bool(entry.get("hydrated", False))),
            "dirty": int(bool(entry.get("dirty", False))),
            "tombstone": int(bool(entry.get("tombstone", False))),
            "local_sha256": entry.get("local_sha256"),
            "last_synced_at": entry.get("last_synced_at"),
            "synced_path": entry.get("synced_path", entry["path"]),
        }
        with self.lock:
            self.conn.execute(
                """
                INSERT INTO entries (
                    path, type, parent_path, remote_drivewsid, remote_docwsid, remote_etag,
                    remote_zone, remote_shareid, remote_itemid, remote_unified_token,
                    size, mtime, hydrated, dirty, tombstone, local_sha256,
                    last_synced_at, synced_path
                ) VALUES (
                    :path, :type, :parent_path, :remote_drivewsid, :remote_docwsid, :remote_etag,
                    :remote_zone, :remote_shareid, :remote_itemid, :remote_unified_token,
                    :size, :mtime, :hydrated, :dirty, :tombstone, :local_sha256,
                    :last_synced_at, :synced_path
                )
                ON CONFLICT(path) DO UPDATE SET
                    type = excluded.type,
                    parent_path = excluded.parent_path,
                    remote_drivewsid = excluded.remote_drivewsid,
                    remote_docwsid = excluded.remote_docwsid,
                    remote_etag = excluded.remote_etag,
                    remote_zone = excluded.remote_zone,
                    remote_shareid = excluded.remote_shareid,
                    remote_itemid = excluded.remote_itemid,
                    remote_unified_token = excluded.remote_unified_token,
                    size = excluded.size,
                    mtime = excluded.mtime,
                    hydrated = excluded.hydrated,
                    dirty = excluded.dirty,
                    tombstone = excluded.tombstone,
                    local_sha256 = excluded.local_sha256,
                    last_synced_at = excluded.last_synced_at,
                    synced_path = excluded.synced_path
                """,
                payload,
            )
            self.conn.commit()

    def get_entry(self, path):
        with self.lock:
            row = self.conn.execute(
                "SELECT * FROM entries WHERE path = ?",
                (path,),
            ).fetchone()
        return self._decode_entry(row_to_dict(row))

    def get_entry_by_remote_id(self, remote_drivewsid):
        with self.lock:
            row = self.conn.execute(
                "SELECT * FROM entries WHERE remote_drivewsid = ?",
                (remote_drivewsid,),
            ).fetchone()
        return self._decode_entry(row_to_dict(row))

    def list_entries(self):
        with self.lock:
            rows = self.conn.execute("SELECT * FROM entries ORDER BY path").fetchall()
        return [self._decode_entry(dict(row)) for row in rows]

    def count_entries(self):
        with self.lock:
            row = self.conn.execute("SELECT COUNT(*) AS count FROM entries").fetchone()
        return int(row["count"])

    def list_unhydrated_paths(self):
        with self.lock:
            rows = self.conn.execute(
                """
                SELECT path FROM entries
                WHERE type = 'file'
                    AND tombstone = 0
                    AND hydrated = 0
                    AND hydrate_attempt_count < ?
                ORDER BY path
                """,
                (MAX_SYNC_ATTEMPTS,),
            ).fetchall()
        return [row["path"] for row in rows]

    def list_dirty_entries(self, include_deferred=False):
        query = """
            SELECT * FROM entries
            WHERE (dirty = 1 OR tombstone = 1)
                AND failed = 0
        """
        parameters = []
        if not include_deferred:
            query += """
                AND (
                    sync_next_attempt_at IS NULL
                    OR sync_next_attempt_at <= ?
                )
            """
            parameters.append(int(time.time()))
        query += " ORDER BY path"
        with self.lock:
            rows = self.conn.execute(query, parameters).fetchall()
        return [self._decode_entry(dict(row)) for row in rows]

    def record_sync_failure(self, path, error, classification, max_attempts):
        error_text = str(error)[:500]
        with self.lock:
            row = self.conn.execute(
                "SELECT sync_attempt_count FROM entries WHERE path = ?",
                (path,),
            ).fetchone()
            if row is None:
                return 0, False
            attempt = int(row["sync_attempt_count"]) + 1
            quarantined = (
                classification == SYNC_FAILURE_TERMINAL
                or attempt >= max_attempts
            )
            next_attempt_at = (
                None
                if quarantined
                else int(time.time()) + _retry_delay_for_attempt(attempt)
            )
            self.conn.execute(
                """
                UPDATE entries
                SET sync_attempt_count = ?,
                    sync_next_attempt_at = ?,
                    sync_last_error = ?,
                    failed = ?
                WHERE path = ?
                """,
                (attempt, next_attempt_at, error_text, int(quarantined), path),
            )
            self.conn.commit()
        return attempt, quarantined

    def clear_sync_failure(self, path):
        with self.lock:
            self.conn.execute(
                """
                UPDATE entries
                SET sync_attempt_count = 0,
                    sync_next_attempt_at = NULL,
                    sync_last_error = NULL,
                    failed = 0
                WHERE path = ?
                """,
                (path,),
            )
            self.conn.commit()

    def clear_failures_subtree(self, path=None):
        if path is None:
            where_clause = "1 = 1"
            parameters = ()
        else:
            path = normalize_icloud_path(path)
            if path == "/":
                where_clause = "1 = 1"
                parameters = ()
            else:
                escaped_path = (
                    path.replace("\\", "\\\\")
                    .replace("%", "\\%")
                    .replace("_", "\\_")
                )
                where_clause = "path = ? OR path LIKE ? ESCAPE '\\'"
                parameters = (path, escaped_path + "/%")
        with self.lock:
            cursor = self.conn.execute(
                """
                UPDATE entries
                SET sync_attempt_count = 0,
                    sync_next_attempt_at = NULL,
                    sync_last_error = NULL,
                    failed = 0,
                    hydrate_attempt_count = 0,
                    hydrate_next_attempt_at = NULL,
                    hydrate_last_error = NULL
                WHERE """ + where_clause,
                parameters,
            )
            self.conn.commit()
        return cursor.rowcount

    def defer_sync(self, path, delay, error=None):
        with self.lock:
            self.conn.execute(
                """
                UPDATE entries
                SET sync_next_attempt_at = ?,
                    sync_last_error = ?
                WHERE path = ?
                """,
                (
                    int(time.time()) + int(delay),
                    str(error)[:500] if error is not None else None,
                    path,
                ),
            )
            self.conn.commit()

    def clear_sync_backoff(self):
        with self.lock:
            self.conn.execute(
                """
                UPDATE entries
                SET sync_next_attempt_at = NULL
                WHERE sync_attempt_count > 0
                """
            )
            self.conn.commit()

    def record_hydrate_failure(self, path, error, classification, max_attempts):
        error_text = str(error)[:500]
        with self.lock:
            row = self.conn.execute(
                "SELECT hydrate_attempt_count FROM entries WHERE path = ?",
                (path,),
            ).fetchone()
            if row is None:
                return 0, False
            attempt = int(row["hydrate_attempt_count"]) + 1
            quarantined = (
                classification == SYNC_FAILURE_TERMINAL
                or attempt >= max_attempts
            )
            if quarantined:
                attempt = max(attempt, max_attempts)
            next_attempt_at = (
                None
                if quarantined
                else int(time.time()) + _retry_delay_for_attempt(attempt)
            )
            self.conn.execute(
                """
                UPDATE entries
                SET hydrate_attempt_count = ?,
                    hydrate_next_attempt_at = ?,
                    hydrate_last_error = ?
                WHERE path = ?
                """,
                (attempt, next_attempt_at, error_text, path),
            )
            self.conn.commit()
        return attempt, quarantined

    def clear_hydrate_failure(self, path):
        with self.lock:
            self.conn.execute(
                """
                UPDATE entries
                SET hydrate_attempt_count = 0,
                    hydrate_next_attempt_at = NULL,
                    hydrate_last_error = NULL
                WHERE path = ?
                """,
                (path,),
            )
            self.conn.commit()

    def defer_hydrate(self, path, delay, error=None):
        with self.lock:
            self.conn.execute(
                """
                UPDATE entries
                SET hydrate_next_attempt_at = ?,
                    hydrate_last_error = ?
                WHERE path = ?
                """,
                (
                    int(time.time()) + int(delay),
                    str(error)[:500] if error is not None else None,
                    path,
                ),
            )
            self.conn.commit()

    def clear_hydrate_backoff(self):
        with self.lock:
            self.conn.execute(
                """
                UPDATE entries
                SET hydrate_next_attempt_at = NULL
                WHERE hydrate_attempt_count > 0
                """
            )
            self.conn.commit()

    def count_quarantined_sync_entries(self):
        with self.lock:
            row = self.conn.execute(
                """
                SELECT COUNT(*) AS count FROM entries
                WHERE failed = 1
                    AND (dirty = 1 OR tombstone = 1)
                """
            ).fetchone()
        return int(row["count"])

    def mark_hydrated(self, path, local_sha256=None, size=None, mtime=None):
        with self.lock:
            self.conn.execute(
                """
                UPDATE entries
                SET hydrated = 1,
                    hydrate_attempt_count = 0,
                    hydrate_next_attempt_at = NULL,
                    hydrate_last_error = NULL,
                    local_sha256 = COALESCE(?, local_sha256),
                    size = COALESCE(?, size),
                    mtime = COALESCE(?, mtime)
                WHERE path = ?
                """,
                (local_sha256, size, mtime, path),
            )
            self.conn.commit()

    def mark_dirty(self, path, size=None, mtime=None, hydrated=None, local_sha256=None):
        with self.lock:
            self.conn.execute(
                """
                UPDATE entries
                SET dirty = 1,
                    tombstone = 0,
                    size = COALESCE(?, size),
                    mtime = COALESCE(?, mtime),
                    hydrated = COALESCE(?, hydrated),
                    local_sha256 = COALESCE(?, local_sha256),
                    sync_attempt_count = 0,
                    sync_next_attempt_at = NULL,
                    sync_last_error = NULL,
                    failed = 0
                WHERE path = ?
                """,
                (size, mtime, hydrated, local_sha256, path),
            )
            self.conn.commit()

    def mark_tombstone(self, path):
        with self.lock:
            self.conn.execute(
                """
                UPDATE entries
                SET tombstone = 1,
                    dirty = 1,
                    sync_attempt_count = 0,
                    sync_next_attempt_at = NULL,
                    sync_last_error = NULL,
                    failed = 0
                WHERE path = ?
                """,
                (path,),
            )
            self.conn.commit()

    def mark_clean(self, path, remote_meta=None, local_sha256=None):
        remote_meta = remote_meta or {}
        with self.lock:
            self.conn.execute(
                """
                UPDATE entries
                SET dirty = 0,
                    tombstone = 0,
                    hydrated = CASE
                        WHEN type = 'file' THEN hydrated
                        ELSE 1
                    END,
                    remote_drivewsid = COALESCE(?, remote_drivewsid),
                    remote_docwsid = COALESCE(?, remote_docwsid),
                    remote_etag = COALESCE(?, remote_etag),
                    remote_zone = COALESCE(?, remote_zone),
                    remote_shareid = COALESCE(?, remote_shareid),
                    remote_itemid = COALESCE(?, remote_itemid),
                    remote_unified_token = COALESCE(?, remote_unified_token),
                    size = COALESCE(?, size),
                    mtime = COALESCE(?, mtime),
                    local_sha256 = COALESCE(?, local_sha256),
                    last_synced_at = ?,
                    synced_path = path,
                    sync_attempt_count = 0,
                    sync_next_attempt_at = NULL,
                    sync_last_error = NULL,
                    failed = 0
                WHERE path = ?
                """,
                (
                    remote_meta.get("remote_drivewsid"),
                    remote_meta.get("remote_docwsid"),
                    remote_meta.get("remote_etag"),
                    remote_meta.get("remote_zone"),
                    self._encode_shareid(remote_meta.get("remote_shareid")),
                    remote_meta.get("remote_itemid"),
                    remote_meta.get("remote_unified_token"),
                    remote_meta.get("size"),
                    remote_meta.get("mtime"),
                    local_sha256,
                    int(time.time()),
                    path,
                ),
            )
            self.conn.commit()

    def remove_entry(self, path):
        with self.lock:
            self.conn.execute("DELETE FROM entries WHERE path = ?", (path,))
            self.conn.commit()

    def remove_subtree(self, path):
        prefix = path.rstrip("/") + "/"
        with self.lock:
            self.conn.execute(
                "DELETE FROM entries WHERE path = ? OR path LIKE ?",
                (path, prefix + "%"),
            )
            self.conn.commit()

    def rename_tree(
        self,
        oldpath,
        newpath,
        root_dirty=True,
        update_synced=False,
        replace_entry=None,
    ):
        entries = self._fetch_subtree(oldpath)
        if not entries:
            return
        prefix = oldpath.rstrip("/") + "/"
        with self.lock:
            if replace_entry is not None:
                destination_prefix = newpath.rstrip("/") + "/"
                self.conn.execute(
                    "DELETE FROM entries WHERE path = ? OR path LIKE ?",
                    (newpath, destination_prefix + "%"),
                )
            for entry in entries:
                current = entry["path"]
                suffix = "" if current == oldpath else current[len(prefix) :]
                updated = newpath if not suffix else newpath.rstrip("/") + "/" + suffix
                updated_parent = os.path.dirname(updated) or "/"
                dirty = 1 if (root_dirty and current == oldpath) else entry["dirty"]
                self.conn.execute(
                    """
                    UPDATE entries
                    SET path = ?,
                        parent_path = ?,
                        dirty = ?,
                        synced_path = CASE
                            WHEN ? = 1 AND synced_path = ? THEN ?
                            WHEN ? = 1 AND synced_path LIKE ? THEN ? || substr(synced_path, ?)
                            ELSE synced_path
                        END
                    WHERE path = ?
                    """,
                    (
                        updated,
                        updated_parent,
                        dirty,
                        int(update_synced),
                        oldpath,
                        newpath,
                        int(update_synced),
                        prefix + "%",
                        newpath.rstrip("/") + "/",
                        len(prefix) + 1,
                        current,
                    ),
                )
            if replace_entry is not None and replace_entry.get("remote_drivewsid"):
                self.conn.execute(
                    """
                    UPDATE entries
                    SET remote_drivewsid = ?,
                        remote_docwsid = ?,
                        remote_etag = ?,
                        remote_zone = ?,
                        remote_shareid = ?,
                        remote_itemid = ?,
                        remote_unified_token = ?,
                        dirty = 1,
                        tombstone = 0,
                        synced_path = ?
                    WHERE path = ?
                    """,
                    (
                        replace_entry.get("remote_drivewsid"),
                        replace_entry.get("remote_docwsid"),
                        replace_entry.get("remote_etag"),
                        replace_entry.get("remote_zone"),
                        self._encode_shareid(replace_entry.get("remote_shareid")),
                        replace_entry.get("remote_itemid"),
                        replace_entry.get("remote_unified_token"),
                        newpath,
                        newpath,
                    ),
                )
            self.conn.commit()

    def mark_synced_subtree(self, path):
        prefix = path.rstrip("/") + "/"
        with self.lock:
            self.conn.execute(
                """
                UPDATE entries
                SET synced_path = path,
                    dirty = CASE
                        WHEN path = ? THEN 0
                        ELSE dirty
                    END,
                    tombstone = CASE
                        WHEN path = ? THEN 0
                        ELSE tombstone
                    END,
                    sync_attempt_count = CASE
                        WHEN path = ? THEN 0
                        ELSE sync_attempt_count
                    END,
                    sync_next_attempt_at = CASE
                        WHEN path = ? THEN NULL
                        ELSE sync_next_attempt_at
                    END,
                    sync_last_error = CASE
                        WHEN path = ? THEN NULL
                        ELSE sync_last_error
                    END,
                    failed = CASE
                        WHEN path = ? THEN 0
                        ELSE failed
                    END,
                    last_synced_at = ?
                WHERE path = ? OR path LIKE ?
                """,
                (
                    path,
                    path,
                    path,
                    path,
                    path,
                    path,
                    int(time.time()),
                    path,
                    prefix + "%",
                ),
            )
            self.conn.commit()

    def detach_subtree_as_conflict(self, oldpath, newpath):
        entries = self._fetch_subtree(oldpath)
        if not entries:
            return
        prefix = oldpath.rstrip("/") + "/"
        with self.lock:
            for entry in entries:
                current = entry["path"]
                suffix = "" if current == oldpath else current[len(prefix) :]
                updated = newpath if not suffix else newpath.rstrip("/") + "/" + suffix
                updated_parent = os.path.dirname(updated) or "/"
                self.conn.execute(
                    """
                    UPDATE entries
                    SET path = ?,
                        parent_path = ?,
                        remote_drivewsid = NULL,
                        remote_docwsid = NULL,
                        remote_etag = NULL,
                        remote_zone = NULL,
                        remote_shareid = NULL,
                        remote_itemid = NULL,
                        remote_unified_token = NULL,
                        synced_path = NULL,
                        dirty = 1,
                        tombstone = 0
                    WHERE path = ?
                    """,
                    (updated, updated_parent, current),
                )
            self.conn.commit()

    def clear_remote_identity(self, path):
        with self.lock:
            self.conn.execute(
                """
                UPDATE entries
                SET remote_drivewsid = NULL,
                    remote_docwsid = NULL,
                    remote_etag = NULL,
                    remote_zone = NULL,
                    remote_shareid = NULL,
                    remote_itemid = NULL,
                    remote_unified_token = NULL,
                    synced_path = NULL,
                    dirty = 1,
                    tombstone = 0
                WHERE path = ?
                """,
                (path,),
            )
            self.conn.commit()

    def _fetch_subtree(self, path):
        prefix = path.rstrip("/") + "/"
        with self.lock:
            rows = self.conn.execute(
                """
                SELECT * FROM entries
                WHERE path = ? OR path LIKE ?
                ORDER BY LENGTH(path) ASC, path ASC
                """,
                (path, prefix + "%"),
            ).fetchall()
        return [self._decode_entry(dict(row)) for row in rows]

    def _encode_shareid(self, shareid):
        if not shareid:
            return None
        return json.dumps(shareid, sort_keys=True)

    def _decode_entry(self, entry):
        if entry is None:
            return None
        shareid = entry.get("remote_shareid")
        if isinstance(shareid, str) and shareid:
            try:
                entry["remote_shareid"] = json.loads(shareid)
            except json.JSONDecodeError:
                entry["remote_shareid"] = None
        return entry


class LocalMirror:
    def __init__(self, cache_dir, file_mode=DEFAULT_FILE_MODE, dir_mode=DEFAULT_DIR_MODE):
        self.cache_dir = cache_dir
        self.root = os.path.join(cache_dir, "mirror")
        self.tmp_dir = os.path.join(cache_dir, "tmp")
        self.file_mode = file_mode
        self.dir_mode = dir_mode
        os.makedirs(self.root, exist_ok=True)
        os.makedirs(self.tmp_dir, exist_ok=True)
        self._normalize_directory_path(self.root)

    def _set_mode_if_needed(self, local, mode):
        current_mode = stat.S_IMODE(os.lstat(local).st_mode)
        if current_mode != mode:
            os.chmod(local, mode)

    def _normalize_directory_path(self, local):
        current = local
        while current.startswith(self.root):
            if os.path.isdir(current):
                self._set_mode_if_needed(current, self.dir_mode)
            if current == self.root:
                break
            parent = os.path.dirname(current)
            if parent == current:
                break
            current = parent

    def _normalize_file_path(self, local):
        self._set_mode_if_needed(local, self.file_mode)
        self._normalize_directory_path(os.path.dirname(local))

    def normalize_file(self, path):
        self._normalize_file_path(self.local_path(path))

    def local_path(self, path):
        normalized = os.path.normpath(path)
        if normalized == ".":
            normalized = "/"
        if not normalized.startswith("/"):
            normalized = "/" + normalized
        relative = normalized.lstrip("/")
        local = os.path.abspath(os.path.join(self.root, relative))
        if local != self.root and not local.startswith(self.root + os.sep):
            raise ValueError(f"Path escapes mirror root: {path}")
        return local

    def ensure_dir(self, path):
        local = self.local_path(path)
        if os.path.exists(local) and not os.path.isdir(local):
            os.unlink(local)
        os.makedirs(local, exist_ok=True)
        self._normalize_directory_path(local)

    def ensure_parent(self, path):
        parent = os.path.dirname(path) or "/"
        parent_local = self.local_path(parent)
        os.makedirs(parent_local, exist_ok=True)
        self._normalize_directory_path(parent_local)

    def materialize_placeholder(self, path, size, mtime):
        local = self.local_path(path)
        self.ensure_parent(path)
        if os.path.isdir(local):
            shutil.rmtree(local)
        with open(local, "wb") as handle:
            handle.truncate(int(size or 0))
        os.utime(local, (mtime, mtime))
        self._normalize_file_path(local)

    def write_atomic_bytes(self, path, content, mtime=None):
        self.ensure_parent(path)
        local = self.local_path(path)
        fd, tmp_path = tempfile.mkstemp(dir=self.tmp_dir)
        try:
            with os.fdopen(fd, "wb") as handle:
                handle.write(content)
            os.replace(tmp_path, local)
            if mtime is not None:
                os.utime(local, (mtime, mtime))
            self._normalize_file_path(local)
        finally:
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)

    def write_atomic_stream(self, path, source, mtime=None, chunk_size=IO_CHUNK_SIZE):
        self.ensure_parent(path)
        local = self.local_path(path)
        fd, tmp_path = tempfile.mkstemp(dir=self.tmp_dir)
        try:
            with os.fdopen(fd, "wb") as handle:
                shutil.copyfileobj(source, handle, length=chunk_size)
            os.replace(tmp_path, local)
            if mtime is not None:
                os.utime(local, (mtime, mtime))
            self._normalize_file_path(local)
        finally:
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)

    def read(self, path, size, offset):
        local = self.local_path(path)
        with open(local, "rb") as handle:
            handle.seek(offset)
            return handle.read(size)

    def write(self, path, buf, offset):
        self.ensure_parent(path)
        local = self.local_path(path)
        mode = "r+b" if os.path.exists(local) else "w+b"
        with open(local, mode) as handle:
            handle.seek(offset)
            handle.write(buf)
            handle.flush()
        self._normalize_file_path(local)
        return len(buf)

    def truncate(self, path, length):
        self.ensure_parent(path)
        local = self.local_path(path)
        mode = "r+b" if os.path.exists(local) else "w+b"
        with open(local, mode) as handle:
            handle.truncate(length)
        self._normalize_file_path(local)

    def create_file(self, path):
        self.ensure_parent(path)
        local = self.local_path(path)
        with open(local, "ab"):
            pass
        self._normalize_file_path(local)

    def listdir(self, path):
        return os.listdir(self.local_path(path))

    def exists(self, path):
        return os.path.exists(self.local_path(path))

    def is_dir(self, path):
        return os.path.isdir(self.local_path(path))

    def remove_file(self, path):
        os.unlink(self.local_path(path))

    def remove_dir(self, path):
        os.rmdir(self.local_path(path))

    def remove_tree(self, path):
        local = self.local_path(path)
        if os.path.isdir(local):
            shutil.rmtree(local)
        elif os.path.exists(local):
            os.unlink(local)

    def rename_path(self, oldpath, newpath):
        self.ensure_parent(newpath)
        os.replace(self.local_path(oldpath), self.local_path(newpath))

    def stat_local(self, path):
        return os.lstat(self.local_path(path))

    def statvfs(self):
        return os.statvfs(self.root)

    def set_mtime(self, path, mtime):
        local = self.local_path(path)
        os.utime(local, (mtime, mtime))

    def file_sha256(self, path):
        return sha256_file(self.local_path(path))


class ICloudSyncEngine:
    def __init__(
        self,
        api,
        mirror,
        state,
        logger,
        warmup_mode="background",
        conflict_mode="copy",
        upload_interval_seconds=30,
        remote_refresh_interval_seconds=300,
        warmup_workers=1,
        sync_paths=None,
        exclude_paths=None,
        auto_sync=True,
    ):
        self.api = api
        self.mirror = mirror
        self.state = state
        self.logger = logger
        self.warmup_mode = warmup_mode if warmup_mode in {"background", "lazy"} else "background"
        self.conflict_mode = conflict_mode if conflict_mode in {"copy"} else "copy"
        self.upload_interval_seconds = upload_interval_seconds
        self.remote_refresh_interval_seconds = remote_refresh_interval_seconds
        self.warmup_workers = max(1, int(warmup_workers))
        self.auto_sync = bool(auto_sync)
        # An empty sync_paths value preserves unrestricted syncing.
        self.sync_paths = normalize_icloud_paths(sync_paths) or None
        self.exclude_paths = normalize_icloud_paths(exclude_paths)
        self.executor = ThreadPoolExecutor(max_workers=self.warmup_workers, thread_name_prefix="warmup")
        self._crawl_executor = None
        self._crawl_executor_suspect = False
        self._crawl_executor_lock = threading.Lock()
        self.stop_event = threading.Event()
        self.refresh_now_event = threading.Event()
        self.path_locks = {}
        self.path_locks_lock = threading.Lock()
        self.scheduled_downloads = set()
        self.downloads_lock = threading.Lock()
        self.download_retry_timers = {}
        self.sync_pass_lock = threading.Lock()
        self.sync_auth_cooldown_until = 0
        self.threads = []
        self.hydration_total = 0
        self.hydration_completed = 0
        self.hydration_progress_lock = threading.Lock()
        self.shutdown_lock = threading.Lock()
        self.is_shutdown = False
        # PyiCloud downloads appear sensitive to concurrent use of one session.
        self.download_semaphore = threading.Semaphore(1)

    def _log_sync(self, event, level=logging.INFO, **fields):
        details = " ".join(f"{key}={value!r}" for key, value in fields.items() if value is not None)
        if details:
            self.logger.log(level, "sync %s %s", event, details)
            return
        self.logger.log(level, "sync %s", event)

    def start(self):
        if self.has_persistent_cache():
            self.logger.info("Using persistent local cache from %s", self.mirror.root)
            self._reconcile_persistent_cache()
            if self.warmup_mode == "background":
                self._schedule_all_unhydrated()
        else:
            self.logger.info("Persistent cache not initialized yet; performing first remote crawl")
            self.initial_scan()
            if self.warmup_mode == "background":
                self._schedule_all_unhydrated()
        if self.auto_sync:
            self._start_background_threads()
        else:
            self.logger.info(
                "auto_sync disabled — background upload/refresh threads not started. "
                "Use 'icloudctl sync' to trigger a one-shot refresh on demand."
            )

    def _start_background_threads(self):
        upload_thread = threading.Thread(target=self._upload_loop, name="icloud-upload", daemon=True)
        refresh_thread = threading.Thread(target=self._refresh_loop, name="icloud-refresh", daemon=True)
        upload_thread.start()
        refresh_thread.start()
        self.threads.extend([upload_thread, refresh_thread])

    def shutdown(self):
        with self.shutdown_lock:
            if self.is_shutdown:
                return
            self.is_shutdown = True
            self.stop_event.set()
            self.refresh_now_event.set()
            with self.downloads_lock:
                timers = list(self.download_retry_timers.values())
                self.download_retry_timers.clear()
                self.scheduled_downloads.clear()
            for timer in timers:
                timer.cancel()
            try:
                self.executor.shutdown(wait=False, cancel_futures=True)
            except TypeError:
                self.executor.shutdown(wait=False)
            with self._crawl_executor_lock:
                crawl_executor = self._crawl_executor
                self._crawl_executor = None
                self._crawl_executor_suspect = False
            if crawl_executor is not None:
                try:
                    crawl_executor.shutdown(wait=False, cancel_futures=True)
                except TypeError:
                    crawl_executor.shutdown(wait=False)
            for thread in list(self.threads):
                thread.join(timeout=1)

    def has_persistent_cache(self):
        return self.state.count_entries() > 0 and os.path.isdir(self.mirror.root)

    def initial_scan(self):
        snapshot = self._crawl_remote_snapshot()
        self._apply_remote_snapshot(snapshot)

    def _get_crawl_executor(self):
        with self._crawl_executor_lock:
            if self._crawl_executor_suspect:
                crawl_executor = self._crawl_executor
                self._crawl_executor = None
                self._crawl_executor_suspect = False
                if crawl_executor is not None:
                    self.logger.warning(
                        "Abandoning suspect remote metadata crawl worker after a timeout"
                    )
                    try:
                        crawl_executor.shutdown(wait=False, cancel_futures=True)
                    except TypeError:
                        crawl_executor.shutdown(wait=False)
            if self._crawl_executor is None:
                self._crawl_executor = ThreadPoolExecutor(
                    max_workers=1,
                    thread_name_prefix="icloud-crawl",
                )
            return self._crawl_executor

    def _reconcile_persistent_cache(self):
        entries = self.state.list_entries()
        missing_files = 0
        recreated_dirs = 0
        queued_uploads = 0

        for entry in entries:
            path = entry["path"]
            if entry["tombstone"]:
                continue
            if self._is_directory_type(entry["type"]):
                was_dir = self.mirror.is_dir(path)
                self.mirror.ensure_dir(path)
                if not was_dir:
                    recreated_dirs += 1
                continue

            if self.mirror.exists(path):
                self.mirror.normalize_file(path)
                stats = self.mirror.stat_local(path)
                checksum = entry.get("local_sha256")
                hydrated = bool(entry["hydrated"])
                if entry["type"] == "file" and (hydrated or not entry["remote_drivewsid"]):
                    hydrated = True
                    # Only recompute the SHA256 if size or mtime changed since
                    # the last recorded sync — reading every file on startup is
                    # the cause of the 4-minute / 11 GB memory blowup at boot.
                    size_changed = stats.st_size != int(entry.get("size") or 0)
                    mtime_changed = int(stats.st_mtime) != int(entry.get("mtime") or 0)
                    if size_changed or mtime_changed or not checksum:
                        checksum = self.mirror.file_sha256(path)
                        if (
                            entry.get("local_sha256")
                            and checksum != entry["local_sha256"]
                            and not entry["dirty"]
                        ):
                            entry = {**entry, "dirty": True}
                            queued_uploads += 1
                            self.logger.info(
                                "Mirror file %s changed while the driver was not running "
                                "and has been queued for upload",
                                path,
                            )
                self.state.upsert_entry(
                    {
                        **entry,
                        "size": stats.st_size,
                        "mtime": int(stats.st_mtime),
                        "hydrated": hydrated,
                        "local_sha256": checksum,
                    }
                )
                continue

            missing_files += 1
            if entry["remote_drivewsid"]:
                self.mirror.materialize_placeholder(path, entry["size"], entry["mtime"])
                self.state.upsert_entry({**entry, "hydrated": entry["size"] == 0})
            else:
                self.mirror.create_file(path)
                stats = self.mirror.stat_local(path)
                checksum = self.mirror.file_sha256(path)
                self.state.upsert_entry(
                    {
                        **entry,
                        "size": stats.st_size,
                        "mtime": int(stats.st_mtime),
                        "hydrated": True,
                        "local_sha256": checksum,
                    }
                )

        self.logger.info(
            "Persistent cache ready: %s entries, %s directories recreated, "
            "%s files queued for hydration, %s files queued for upload",
            len(entries),
            recreated_dirs,
            missing_files,
            queued_uploads,
        )

    def _is_directory_type(self, node_type):
        return (node_type or "").lower() in DIRECTORY_NODE_TYPES

    def ensure_local_file(self, path):
        if not self._path_allowed(path):
            return
        entry = self.state.get_entry(path)
        if not entry or entry["type"] != "file" or entry["tombstone"]:
            return
        if entry["hydrated"] and self.mirror.exists(path):
            return

        lock = self._path_lock(path)
        with lock:
            entry = self.state.get_entry(path)
            if not entry or entry["type"] != "file" or entry["tombstone"]:
                return
            if entry["hydrated"] and self.mirror.exists(path):
                return
            if entry["hydrate_attempt_count"] >= MAX_SYNC_ATTEMPTS:
                raise HydrationFailed(
                    "hydration retry budget exhausted for {}".format(path)
                )
            if not entry["remote_drivewsid"]:
                self._log_sync("hydrate-local", level=logging.DEBUG, path=path)
                if not self.mirror.exists(path):
                    self.mirror.create_file(path)
                checksum = self.mirror.file_sha256(path)
                stats = self.mirror.stat_local(path)
                self.state.mark_hydrated(path, checksum, stats.st_size, int(stats.st_mtime))
                self._log_sync(
                    "hydrate-complete",
                    level=logging.INFO,
                    path=path,
                    source="local",
                    size=stats.st_size,
                )
                return

            self._log_sync(
                "hydrate-start",
                level=logging.INFO,
                path=path,
                drivewsid=entry.get("remote_drivewsid"),
                size=entry.get("size"),
            )
            self.logger.debug("Hydrating %s", path)
            with self.download_semaphore:
                self.logger.debug(
                    "Hydrating file path=%s drivewsid=%s docwsid=%s zone=%s size=%s",
                    path,
                    entry.get("remote_drivewsid"),
                    entry.get("remote_docwsid"),
                    entry.get("remote_zone"),
                    entry.get("size"),
                )
                node = self._node_from_entry(entry)
                with closing(self._open_remote_file(node, entry, path, stream=True)) as response:
                    self.mirror.write_atomic_stream(path, response.raw, entry["mtime"])
            stats = self.mirror.stat_local(path)
            checksum = self.mirror.file_sha256(path)
            self.state.mark_hydrated(path, checksum, stats.st_size, int(stats.st_mtime))
            self._log_sync("hydrate-complete", level=logging.INFO, path=path, source="remote", size=stats.st_size)

    def _crawl_remote_snapshot(self):
        self.logger.info("Starting remote metadata crawl")
        snapshot = RemoteSnapshot()
        queue = deque()
        root = self.api.drive.root
        queue.append((root, "/", root.data.get("shareID")))
        started_at = time.time()
        last_progress_log = started_at
        scanned_folders = 0
        crawl_executor = self._get_crawl_executor()

        while queue:
            node, path, inherited_shareid = queue.popleft()
            if inherited_shareid and not node.data.get("shareID"):
                node.data = {**node.data, "shareID": inherited_shareid}
            scanned_folders += 1
            try:
                future = crawl_executor.submit(node.get_children, True)
                children = future.result(timeout=CRAWL_FOLDER_TIMEOUT)
            except TimeoutError:
                snapshot.complete = False
                snapshot.failed_folders.append(path)
                with self._crawl_executor_lock:
                    if crawl_executor is self._crawl_executor:
                        self._crawl_executor_suspect = True
                self.logger.warning(
                    "Timed out enumerating %s after %ss; ending remote metadata crawl "
                    "because its worker is wedged",
                    path,
                    CRAWL_FOLDER_TIMEOUT,
                )
                break
            except Exception as exc:
                snapshot.complete = False
                snapshot.failed_folders.append(path)
                self.logger.error("Failed to enumerate %s: %s", path, exc)
                continue

            for child in children:
                child_path = "/" + child.name if path == "/" else path.rstrip("/") + "/" + child.name
                meta = self._node_to_meta(
                    child,
                    child_path,
                    inherited_shareid=node.data.get("shareID") or inherited_shareid,
                )
                if meta.get("remote_shareid") and not child.data.get("shareID"):
                    child.data = {**child.data, "shareID": meta["remote_shareid"]}
                snapshot[meta["remote_drivewsid"]] = meta
                if self._is_directory_type(meta["type"]):
                    # If sync_paths is set, only recurse into directories that are
                    # on the path to or inside a sync_path. This avoids crawling
                    # the entire iCloud Drive when only /Downloads is needed.
                    if self.sync_paths is not None:
                        should_recurse = False
                        for sp in self.sync_paths:
                            sp = sp.rstrip("/")
                            cp = child_path.rstrip("/")
                            # Recurse if child is a prefix of sync_path (ancestor)
                            # or if child is inside sync_path (descendant)
                            if sp.startswith(cp + "/") or sp == cp or cp.startswith(sp + "/"):
                                should_recurse = True
                                break
                        if not should_recurse:
                            continue
                    queue.append((child, child_path, meta.get("remote_shareid")))

            now = time.time()
            if scanned_folders == 1 or scanned_folders % 25 == 0 or now - last_progress_log >= 5:
                self.logger.info(
                    "Remote metadata crawl progress: %s folders scanned, %s entries discovered, %s folders queued",
                    scanned_folders,
                    len(snapshot),
                    len(queue),
                )
                last_progress_log = now

        self.logger.info(
            "Remote metadata crawl complete: %s entries across %s folders in %.1fs%s",
            len(snapshot),
            scanned_folders,
            time.time() - started_at,
            "" if snapshot.complete else "; incomplete folders: " + ", ".join(snapshot.failed_folders),
        )
        return snapshot

    def _apply_remote_snapshot(self, snapshot):
        remote_ids = set(snapshot.keys())

        for meta in snapshot.values():
            existing = self.state.get_entry_by_remote_id(meta["remote_drivewsid"])
            if existing and existing["dirty"] and self._entry_conflicts(existing, meta):
                self._resolve_conflict(existing)
                existing = None

            if existing is None:
                path_entry = self.state.get_entry(meta["path"])
                if path_entry and path_entry["dirty"]:
                    self._resolve_conflict(path_entry)
                self._materialize_remote_entry(meta)
                continue

            if existing["dirty"]:
                continue

            self._refresh_clean_entry(existing, meta)

        if not getattr(snapshot, "complete", True):
            self.logger.warning(
                "Skipping remote-deletion reconciliation because crawl was incomplete: %s",
                ", ".join(getattr(snapshot, "failed_folders", ())) or "unknown folder",
            )
            return

        for entry in self.state.list_entries():
            remote_id = entry["remote_drivewsid"]
            if not remote_id or remote_id in remote_ids:
                continue
            if entry["dirty"]:
                self.logger.warning("Remote deleted dirty path %s; keeping local copy for upload", entry["path"])
                self.state.clear_remote_identity(entry["path"])
                continue
            self.logger.info("Removing clean path deleted remotely: %s", entry["path"])
            self.mirror.remove_tree(entry["path"])
            self.state.remove_subtree(entry["path"])

    def _materialize_remote_entry(self, meta):
        local_path = meta["path"]
        self._log_sync(
            "remote-materialize",
            path=local_path,
            entry_type=meta["type"],
            drivewsid=meta.get("remote_drivewsid"),
            size=meta.get("size"),
        )
        if self._is_directory_type(meta["type"]):
            self.mirror.ensure_dir(local_path)
            hydrated = True
        else:
            self.mirror.materialize_placeholder(local_path, meta["size"], meta["mtime"])
            hydrated = meta["size"] == 0
        self.state.upsert_entry(
            {
                **meta,
                "hydrated": hydrated,
                "dirty": False,
                "tombstone": False,
                "synced_path": local_path,
            }
        )
        if meta["type"] == "file" and not hydrated:
            self._schedule_download(local_path)

    def _refresh_clean_entry(self, entry, meta):
        oldpath = entry["path"]
        newpath = meta["path"]
        if oldpath != newpath and self.mirror.exists(oldpath):
            self._log_sync("remote-rename", path=oldpath, target_path=newpath, entry_type=meta["type"])
            self.mirror.rename_path(oldpath, newpath)
            self.state.rename_tree(oldpath, newpath, root_dirty=False, update_synced=True)
            entry = self.state.get_entry(newpath)
        elif oldpath != newpath:
            self._log_sync("remote-rename", path=oldpath, target_path=newpath, entry_type=meta["type"])
            self.state.rename_tree(oldpath, newpath, root_dirty=False, update_synced=True)
            entry = self.state.get_entry(newpath)

        if self._is_directory_type(meta["type"]):
            self.mirror.ensure_dir(newpath)
            self.state.upsert_entry(
                {
                    **meta,
                    "hydrated": True,
                    "dirty": False,
                    "tombstone": False,
                    "local_sha256": entry.get("local_sha256") if entry else None,
                    "last_synced_at": entry.get("last_synced_at") if entry else None,
                    "synced_path": newpath,
                }
            )
            return

        should_replace = (
            entry is None
            or entry["remote_etag"] != meta["remote_etag"]
            or entry["size"] != meta["size"]
            or entry["mtime"] != meta["mtime"]
        )
        hydrated = bool(entry and entry["hydrated"] and not should_replace)
        if should_replace:
            self._log_sync(
                "remote-update",
                path=newpath,
                old_etag=entry.get("remote_etag") if entry else None,
                new_etag=meta.get("remote_etag"),
                size=meta.get("size"),
            )
            self.mirror.materialize_placeholder(newpath, meta["size"], meta["mtime"])
            hydrated = meta["size"] == 0
        self.state.upsert_entry(
            {
                **meta,
                "hydrated": hydrated,
                "dirty": False,
                "tombstone": False,
                "local_sha256": entry.get("local_sha256") if hydrated and entry else None,
                "last_synced_at": entry.get("last_synced_at") if entry else None,
                "synced_path": newpath,
            }
        )
        if not hydrated:
            self._schedule_download(newpath)

    def _resolve_conflict(self, entry):
        if self.conflict_mode != "copy":
            self.logger.warning("Unsupported conflict mode %s; falling back to copy", self.conflict_mode)
        conflict_path = self._conflict_path(entry["path"])
        self.logger.warning("Conflict on %s; preserving local version as %s", entry["path"], conflict_path)
        if self.mirror.exists(entry["path"]):
            self.mirror.rename_path(entry["path"], conflict_path)
        self.state.detach_subtree_as_conflict(entry["path"], conflict_path)

    def _schedule_all_unhydrated(self):
        paths = self.state.list_unhydrated_paths()
        total = len(paths)
        with self.hydration_progress_lock:
            self.hydration_total = total
            self.hydration_completed = 0
        if total:
            self.logger.info("Background cache warmup scheduled for %s files", total)
        else:
            self.logger.info("Background cache warmup skipped; all files already hydrated")
        for path in paths:
            self._schedule_download(path)

    def _schedule_download(self, path):
        self._schedule_download_with_delay(path, 0)

    def _schedule_download_with_delay(self, path, delay_seconds):
        if not self._path_allowed(path):
            return
        if self.stop_event.is_set() or self.is_shutdown:
            return
        entry = self.state.get_entry(path)
        if entry and entry["hydrate_attempt_count"] >= MAX_SYNC_ATTEMPTS:
            return

        with self.downloads_lock:
            if path in self.scheduled_downloads:
                return
            self.scheduled_downloads.add(path)

        self._log_sync(
            "download-scheduled",
            level=logging.DEBUG if delay_seconds <= 0 else logging.INFO,
            path=path,
            delay_seconds=delay_seconds,
        )

        if delay_seconds <= 0:
            try:
                self.executor.submit(self._download_job, path)
            except RuntimeError:
                with self.downloads_lock:
                    self.scheduled_downloads.discard(path)
            return

        timer = threading.Timer(delay_seconds, self._submit_retry_download, args=(path,))
        timer.daemon = True
        with self.downloads_lock:
            self.download_retry_timers[path] = timer
        timer.start()

    def _submit_retry_download(self, path):
        with self.downloads_lock:
            self.download_retry_timers.pop(path, None)
        if self.stop_event.is_set() or self.is_shutdown:
            with self.downloads_lock:
                self.scheduled_downloads.discard(path)
            return
        try:
            self.executor.submit(self._download_job, path)
        except RuntimeError:
            with self.downloads_lock:
                self.scheduled_downloads.discard(path)

    def _retry_delay_for_attempt(self, attempt):
        return _retry_delay_for_attempt(attempt)

    def _record_hydrate_failure(self, path, exc):
        classification = classify_sync_failure(exc, "download")
        if classification == SYNC_FAILURE_AUTH:
            self.state.defer_hydrate(
                path,
                self._retry_delay_for_attempt(1),
                exc,
            )
            return classification, 0, False
        attempt, exhausted = self.state.record_hydrate_failure(
            path,
            exc,
            classification,
            MAX_SYNC_ATTEMPTS,
        )
        return classification, attempt, exhausted

    def _download_job(self, path):
        retry_delay = None
        try:
            entry = self.state.get_entry(path)
            if entry and entry["hydrate_attempt_count"] >= MAX_SYNC_ATTEMPTS:
                self.logger.error(
                    "Warmup download failed for %s; hydration retry budget exhausted",
                    path,
                )
                return
            self.ensure_local_file(path)
            self._log_sync("download-complete", level=logging.INFO, path=path)
            with self.hydration_progress_lock:
                self.hydration_completed += 1
                completed = self.hydration_completed
                total = self.hydration_total
            if total and (completed == 1 or completed == total or completed % 25 == 0):
                self.logger.info(
                    "Background cache warmup progress: %s/%s files hydrated",
                    completed,
                    total,
                )
        except Exception as exc:
            classification, attempt, exhausted = self._record_hydrate_failure(
                path,
                exc,
            )
            if classification == SYNC_FAILURE_AUTH:
                self.logger.error(
                    "Warmup download blocked by expired iCloud authentication for %s: %s. "
                    "Run './icloudctl auth' and then './icloudctl restart'.",
                    path,
                    exc,
                )
                return
            if exhausted:
                self.logger.error(
                    "Warmup download failed for %s (attempt %s): %s; quarantined",
                    path,
                    attempt,
                    exc,
                )
                return
            retry_delay = self._retry_delay_for_attempt(attempt or 1)
            self.logger.error(
                "Warmup download failed for %s (attempt %s): %s; retrying in %ss",
                path,
                attempt or 1,
                exc,
                retry_delay,
            )
        finally:
            with self.downloads_lock:
                self.scheduled_downloads.discard(path)
                self.download_retry_timers.pop(path, None)
            if retry_delay is not None:
                self._schedule_download_with_delay(path, retry_delay)

    def _upload_loop(self):
        while not self.stop_event.wait(self.upload_interval_seconds):
            try:
                self.sync_dirty_entries()
            except Exception as exc:
                self.logger.error("Upload loop failed: %s", exc)

    def request_remote_refresh(self):
        self._log_sync("refresh-requested")
        self.refresh_now_event.set()

    def _run_remote_refresh(self, reason):
        try:
            self._log_sync("refresh-start", reason=reason)
            snapshot = self._crawl_remote_snapshot()
            self._apply_remote_snapshot(snapshot)
            self._log_sync("refresh-complete", reason=reason)
        except Exception as exc:
            self.logger.error("Remote refresh failed (%s): %s", reason, exc)

    def _refresh_loop(self):
        immediate = self.has_persistent_cache()
        if immediate:
            self.logger.info("Starting background remote refresh from persistent cache")
            self._run_remote_refresh("startup")
        while not self.stop_event.is_set():
            manual = self.refresh_now_event.wait(self.remote_refresh_interval_seconds)
            self.refresh_now_event.clear()
            if self.stop_event.is_set():
                break
            self._run_remote_refresh("manual" if manual else "scheduled")

    def sync_dirty_entries(self, include_deferred=False):
        with self.sync_pass_lock:
            quarantined_entries = self.state.count_quarantined_sync_entries()
            if self.sync_auth_cooldown_until > time.time():
                self.logger.info(
                    "Skipping dirty sync during iCloud authentication cooldown"
                )
                return quarantined_entries
            sync_context = SyncPassContext()
            dirty_entries = [
                entry for entry in self.state.list_dirty_entries(
                    include_deferred=include_deferred
                )
                if self._entry_allowed_to_sync(entry)
            ]
            if not dirty_entries:
                return quarantined_entries

            self._log_sync(
                "dirty-scan",
                dirty_count=len(dirty_entries),
                include_deferred=include_deferred,
            )

            tombstones = sorted(
                [entry for entry in dirty_entries if entry["tombstone"]],
                key=lambda entry: (entry["path"].count("/"), entry["path"]),
                reverse=True,
            )
            regular = sorted(
                [entry for entry in dirty_entries if not entry["tombstone"]],
                key=lambda entry: (entry["type"] != "folder", entry["path"].count("/"), entry["path"]),
            )

            for entry in tombstones:
                try:
                    self._sync_tombstone(entry, sync_context)
                except SyncAuthenticationBlocked:
                    return quarantined_entries

            for entry in regular:
                fresh = self.state.get_entry(entry["path"])
                if fresh is None or fresh["tombstone"] or not fresh["dirty"]:
                    continue
                try:
                    if fresh["type"] == "folder":
                        self._sync_directory(fresh, sync_context)
                    else:
                        self._sync_file(fresh, sync_context)
                except SyncAuthenticationBlocked:
                    return quarantined_entries
            return quarantined_entries

    def _record_sync_failure(self, entry, exc, operation, sync_context=None):
        classification = classify_sync_failure(exc, operation)
        if classification == SYNC_FAILURE_AUTH:
            self.state.defer_sync(
                entry["path"],
                self._retry_delay_for_attempt(1),
                exc,
            )
            self.sync_auth_cooldown_until = (
                time.time() + AUTH_SYNC_COOLDOWN_SECONDS
            )
            if sync_context is None or not sync_context.auth_failure_logged:
                self.logger.error(
                    "Sync %s blocked by iCloud authentication for %s: %s",
                    operation,
                    entry["path"],
                    exc,
                )
                if sync_context is not None:
                    sync_context.auth_failure_logged = True
            return True
        attempt, quarantined = self.state.record_sync_failure(
            entry["path"],
            exc,
            classification,
            MAX_SYNC_ATTEMPTS,
        )
        if quarantined:
            self.logger.error(
                "Sync %s failed for %s (attempt %s): %s; quarantined",
                operation,
                entry["path"],
                attempt,
                exc,
            )
            return False
        self.logger.error(
            "Sync %s failed for %s (attempt %s): %s; retrying later",
            operation,
            entry["path"],
            attempt,
            exc,
        )
        return False

    def _mark_remote_parent_failed(self, path, sync_context):
        sync_context.failed_remote_parents.add(path)

    def _sync_tombstone(self, entry, sync_context=None):
        is_sync_pass = sync_context is not None
        self._log_sync("delete-start", path=entry["path"], remote=bool(entry["remote_drivewsid"]))
        if entry["remote_drivewsid"]:
            try:
                node = self._node_from_entry(entry)
                node.delete()
            except Exception as exc:
                if not remote_item_is_absent(exc, "delete"):
                    if self._record_sync_failure(
                        entry, exc, "delete", sync_context
                    ):
                        if is_sync_pass:
                            raise SyncAuthenticationBlocked()
                    return
                self.logger.info(
                    "Remote path %s was already deleted; resolving tombstone",
                    entry["path"],
                )
        self.state.clear_sync_failure(entry["path"])
        self.state.remove_subtree(entry["path"])
        self._log_sync("delete-complete", path=entry["path"])

    def _sync_directory(self, entry, sync_context=None):
        is_sync_pass = sync_context is not None
        if sync_context is None:
            sync_context = SyncPassContext()
        try:
            parent_node = self._ensure_remote_parent(entry["path"], sync_context)
            self._log_sync(
                "directory-sync-start",
                path=entry["path"],
                remote_exists=bool(entry["remote_drivewsid"]),
                synced_path=entry.get("synced_path"),
            )
            is_shared = bool(entry.get("remote_shareid") or parent_node.data.get("shareID"))
            if not entry["remote_drivewsid"]:
                created_node = self._create_remote_directory(
                    parent_node,
                    os.path.basename(entry["path"]),
                )
                meta = (
                    self._node_to_meta(
                        created_node,
                        entry["path"],
                        inherited_shareid=parent_node.data.get("shareID"),
                    )
                    if created_node is not None
                    else self._reconcile_child_meta(
                        os.path.dirname(entry["path"]) or "/",
                        os.path.basename(entry["path"]),
                    )
                )
                self.state.mark_clean(entry["path"], meta)
                self._log_sync("directory-create-complete", path=entry["path"])
                return

            if entry["synced_path"] and entry["synced_path"] != entry["path"]:
                if is_shared:
                    self._sync_shared_directory(entry, parent_node)
                    self.state.clear_sync_failure(entry["path"])
                    self._log_sync("directory-sync-complete", path=entry["path"])
                    return
                self._sync_move_or_rename(entry)
            self.state.mark_synced_subtree(entry["path"])
            self._log_sync("directory-sync-complete", path=entry["path"])
        except SyncBlocked as exc:
            self.logger.info(
                "Sync directory %s blocked by ancestor: %s", entry["path"], exc
            )
        except SyncAuthenticationBlocked:
            raise
        except Exception as exc:
            if self._record_sync_failure(entry, exc, "upload", sync_context):
                if is_sync_pass:
                    raise SyncAuthenticationBlocked()
                return
            self._mark_remote_parent_failed(entry["path"], sync_context)

    def _sync_file(self, entry, sync_context=None):
        is_sync_pass = sync_context is not None
        if sync_context is None:
            sync_context = SyncPassContext()
        try:
            parent_node = self._ensure_remote_parent(entry["path"], sync_context)
            self._log_sync(
                "file-sync-start",
                path=entry["path"],
                remote_exists=bool(entry["remote_drivewsid"]),
                synced_path=entry.get("synced_path"),
            )
            if not self.mirror.exists(entry["path"]):
                self._log_sync(
                    "file-missing-quarantined",
                    level=logging.ERROR,
                    path=entry["path"],
                )
                raise MissingMirrorFile(
                    "Mirror file is missing; remote data was not deleted. "
                    "Restore the file in the mirror, or delete it through the "
                    "mounted filesystem if deletion is intended; manual "
                    "resolution is required."
                )

            try:
                self.ensure_local_file(entry["path"])
            except HydrationFailed as exc:
                self.logger.error(
                    "Sync file %s blocked by failed hydration: %s",
                    entry["path"],
                    exc,
                )
                return
            except Exception as exc:
                if entry["hydrated"]:
                    raise
                classification, attempt, exhausted = self._record_hydrate_failure(
                    entry["path"],
                    exc,
                )
                if classification == SYNC_FAILURE_AUTH:
                    self.sync_auth_cooldown_until = (
                        time.time() + AUTH_SYNC_COOLDOWN_SECONDS
                    )
                    if is_sync_pass:
                        raise SyncAuthenticationBlocked()
                    return
                if exhausted:
                    self.logger.error(
                        "Sync file %s blocked by failed hydration (attempt %s): %s",
                        entry["path"],
                        attempt,
                        exc,
                    )
                    return
                self._schedule_download_with_delay(
                    entry["path"],
                    self._retry_delay_for_attempt(attempt),
                )
                self.logger.error(
                    "Sync file %s deferred while hydration retries: %s",
                    entry["path"],
                    exc,
                )
                return
            is_shared = bool(entry.get("remote_shareid") or parent_node.data.get("shareID"))

            if (
                entry["remote_drivewsid"]
                and entry["synced_path"]
                and entry["synced_path"] != entry["path"]
                and not is_shared
            ):
                self._sync_move_or_rename(entry)
                entry = self.state.get_entry(entry["path"])

            if is_shared:
                meta = self._sync_shared_file(entry, parent_node)
            else:
                if entry["remote_drivewsid"]:
                    try:
                        self._delete_remote_node(self._node_from_entry(entry))
                    except Exception as exc:
                        if not remote_item_is_absent(exc, "delete"):
                            raise
                        self.logger.info(
                            "Remote path %s was already deleted; uploading replacement",
                            entry["path"],
                        )

                with open(self.mirror.local_path(entry["path"]), "rb") as handle:
                    stream = NamedFileStream(handle, os.path.basename(entry["path"]))
                    parent_node.upload(stream)

                meta = self._reconcile_child_meta(
                    os.path.dirname(entry["path"]) or "/",
                    os.path.basename(entry["path"]),
                )
                checksum = self.mirror.file_sha256(entry["path"])
                self.state.mark_clean(entry["path"], meta, checksum)
            self.state.clear_sync_failure(entry["path"])
            self._log_sync("file-sync-complete", path=entry["path"], size=meta.get("size"))
        except SyncBlocked as exc:
            self.logger.info(
                "Sync file %s blocked by ancestor: %s", entry["path"], exc
            )
        except SyncAuthenticationBlocked:
            raise
        except Exception as exc:
            if self._record_sync_failure(entry, exc, "upload", sync_context):
                if is_sync_pass:
                    raise SyncAuthenticationBlocked()
                return

    def _sync_move_or_rename(self, entry):
        synced_path = entry["synced_path"]
        if not synced_path:
            return
        old_parent = os.path.dirname(synced_path) or "/"
        new_parent = os.path.dirname(entry["path"]) or "/"
        old_name = os.path.basename(synced_path)
        new_name = os.path.basename(entry["path"])

        self._log_sync("move-start", path=synced_path, target_path=entry["path"])
        node = self._node_from_entry(entry)
        if old_parent != new_parent:
            destination = self._remote_node_for_path(new_parent)
            if destination is None:
                raise RuntimeError(f"Remote parent not available for {new_parent}")
            self._move_remote_nodes([node], destination)
            node = self._refresh_node_by_id(
                entry["remote_drivewsid"],
                entry.get("remote_shareid"),
            )
        if old_name != new_name:
            node.rename(new_name)
        self._log_sync("move-complete", path=synced_path, target_path=entry["path"])

    def _entry_allowed_to_sync(self, entry):
        paths = [entry["path"]]
        synced_path = entry.get("synced_path")
        if synced_path and synced_path != entry["path"]:
            paths.append(synced_path)

        if all(self._path_allowed(path) for path in paths):
            return True

        self._log_sync(
            "dirty-skip-disallowed",
            level=logging.WARNING,
            path=entry["path"],
            synced_path=synced_path,
        )
        return False

    def _ensure_remote_parent(self, path, sync_context=None):
        if sync_context is None:
            sync_context = SyncPassContext()
        parent_path = os.path.dirname(path) or "/"
        if parent_path == "/":
            return self.api.drive.root
        if parent_path in sync_context.failed_remote_parents:
            raise SyncBlocked(f"Remote parent unavailable: {parent_path}")
        parent_entry = self.state.get_entry(parent_path)
        if not parent_entry:
            self._mark_remote_parent_failed(parent_path, sync_context)
            raise SyncBlocked(f"Remote parent unavailable: {parent_path}")
        if parent_entry["failed"]:
            self._mark_remote_parent_failed(parent_path, sync_context)
            raise SyncBlocked(f"Remote parent quarantined: {parent_path}")
        next_attempt_at = parent_entry["sync_next_attempt_at"]
        if next_attempt_at is not None and next_attempt_at > int(time.time()):
            self._mark_remote_parent_failed(parent_path, sync_context)
            raise SyncBlocked(f"Remote parent deferred: {parent_path}")
        if parent_entry["dirty"]:
            self._sync_directory(parent_entry, sync_context)
            parent_entry = self.state.get_entry(parent_path)
        if parent_path in sync_context.failed_remote_parents:
            raise SyncBlocked(f"Remote parent unavailable: {parent_path}")
        if not parent_entry or not parent_entry["remote_drivewsid"]:
            self._mark_remote_parent_failed(parent_path, sync_context)
            raise SyncBlocked(f"Remote parent unavailable: {parent_path}")
        try:
            return self._node_from_entry(parent_entry)
        except Exception as exc:
            if self._record_sync_failure(
                parent_entry, exc, "upload", sync_context
            ):
                raise SyncAuthenticationBlocked()
            self._mark_remote_parent_failed(parent_path, sync_context)
            raise SyncBlocked(f"Remote parent unavailable: {parent_path}")

    def _refresh_child_meta(self, parent_path, child_name, inherited_shareid=None):
        parent = self._remote_node_for_path(parent_path)
        if parent is None:
            raise RuntimeError(f"Missing remote parent: {parent_path}")
        if inherited_shareid and not parent.data.get("shareID"):
            parent.data = {**parent.data, "shareID": inherited_shareid}
        for child in parent.get_children(force=True):
            if child.name == child_name:
                meta_shareid = child.data.get("shareID") or inherited_shareid
                if meta_shareid and not child.data.get("shareID"):
                    child.data = {**child.data, "shareID": meta_shareid}
                return self._node_to_meta(
                    child,
                    "/" + child.name if parent_path == "/" else parent_path.rstrip("/") + "/" + child.name,
                    inherited_shareid=meta_shareid,
                )
        raise KeyError(f"Missing child {child_name} under {parent_path}")

    def _reconcile_child_meta(self, parent_path, child_name):
        inherited_shareid = self._shareid_for_path(parent_path)
        try:
            return self._refresh_child_meta(
                parent_path,
                child_name,
                inherited_shareid=inherited_shareid,
            )
        except KeyError:
            if not inherited_shareid:
                raise
            snapshot = self._crawl_remote_snapshot()
            self._apply_remote_snapshot(snapshot)
            path = "/" + child_name if parent_path == "/" else parent_path.rstrip("/") + "/" + child_name
            entry = self.state.get_entry(path)
            if entry and entry.get("remote_drivewsid"):
                return entry
            raise

    def _sync_shared_directory(self, entry, parent_node):
        if entry.get("remote_shareid") and entry.get("remote_shareid") != parent_node.data.get("shareID"):
            raise RuntimeError(
                f"Cross-share directory moves are not supported for {entry['path']}"
            )
        old_node = self._node_from_entry(entry)
        created_node = self._create_remote_directory(
            parent_node,
            os.path.basename(entry["path"]),
        )
        if created_node is None:
            raise RuntimeError(f"Failed creating shared directory {entry['path']}")
        children = list(old_node.get_children(force=True))
        if children:
            self._move_remote_nodes(children, created_node)
        self._delete_remote_node(old_node)
        meta = self._node_to_meta(
            created_node,
            entry["path"],
            inherited_shareid=parent_node.data.get("shareID"),
        )
        self.state.mark_clean(entry["path"], meta)
        self.state.mark_synced_subtree(entry["path"])

    def _sync_shared_file(self, entry, parent_node):
        old_node = self._node_from_entry(entry) if entry.get("remote_drivewsid") else None
        synced_path = entry.get("synced_path")
        target_parent_path = os.path.dirname(entry["path"]) or "/"
        with open(self.mirror.local_path(entry["path"]), "rb") as handle:
            stream = NamedFileStream(handle, os.path.basename(entry["path"]))
            if old_node is not None and synced_path == entry["path"]:
                self._delete_remote_node(old_node)
                self._upload_file_to_parent(parent_node, stream)
            else:
                self._upload_file_to_parent(parent_node, stream)
                if old_node is not None:
                    self._delete_remote_node(old_node)
        meta = self._reconcile_child_meta(
            target_parent_path,
            os.path.basename(entry["path"]),
        )
        checksum = self.mirror.file_sha256(entry["path"])
        self.state.mark_clean(entry["path"], meta, checksum)
        return meta

    def _post_drive_service(self, endpoint, payload, shareid=None, content_type_text=False):
        request_payload = dict(payload)
        if shareid:
            request_payload["shareID"] = shareid
        headers = {"Content-Type": "text/plain"} if content_type_text else None
        request = self.api.drive.session.post(
            self.api.drive.service_root + endpoint,
            params=self.api.drive.params,
            headers=headers,
            json=request_payload,
            timeout=CONTROL_PLANE_TIMEOUT,
        )
        self.api.drive._raise_if_error(request)
        return request.json()

    def _create_remote_directory(self, parent_node, name):
        shareid = parent_node.data.get("shareID")
        if shareid:
            response = self._post_drive_service(
                "/createFolders",
                {
                    "destinationDrivewsId": parent_node.data["drivewsid"],
                    "folders": [
                        {
                            "clientId": f"FOLDER::UNKNOWN_ZONE::TempId-{uuid.uuid4()}",
                            "name": name,
                        }
                    ],
                },
                shareid=shareid,
                content_type_text=True,
            )
        else:
            response = parent_node.mkdir(name)
        folders = response.get("folders") or []
        if not folders:
            return None
        folder = folders[0]
        if shareid and not folder.get("shareID"):
            folder = {**folder, "shareID": shareid}
        return DriveNode(self.api.drive, folder)

    def _move_remote_nodes(self, nodes, destination):
        shareid = destination.data.get("shareID")
        if not shareid:
            return self.api.drive.move_nodes_to_node(nodes, destination)
        return self._post_drive_service(
            "/moveItems",
            {
                "destinationDrivewsId": destination.data["drivewsid"],
                "items": [
                    {
                        "drivewsid": node.data["drivewsid"],
                        "etag": node.data["etag"],
                        "clientId": node.data["drivewsid"],
                    }
                    for node in nodes
                ],
            },
            shareid=shareid,
        )

    def _put_document_item(self, item_id, payload):
        request = self.api.drive.session.put(
            f"{self.api.drive._document_root}/v1/item/{item_id}",
            headers={"Content-Type": "text/plain"},
            data=json.dumps(payload),
            timeout=CONTROL_PLANE_TIMEOUT,
        )
        self.api.drive._raise_if_error(request)
        return request.json()

    def _download_shared_file(self, node, **kwargs):
        item_id = node.data.get("item_id")
        if not item_id:
            raise RuntimeError(f"Missing shared item id for {node.name}")
        request = self.api.drive.session.get(
            f"{self.api.drive._document_root}/v1/item/{item_id}",
            params=self.api.drive.params,
            timeout=CONTROL_PLANE_TIMEOUT,
        )
        self.api.drive._raise_if_error(request)
        item_info = request.json().get("item_info", {})
        url = item_info.get("urls", {}).get("url_download")
        if not url:
            raise KeyError(f"Shared download URL missing for {node.name}")
        # The read timeout limits inactivity between bytes, not total transfer time.
        return self.api.drive.session.get(
            url,
            params=self.api.drive.params,
            timeout=DOWNLOAD_TIMEOUT,
            **kwargs,
        )

    def _open_remote_file(self, node, entry, path, **kwargs):
        if entry.get("remote_shareid"):
            try:
                return self._download_shared_file(node, **kwargs)
            except Exception as exc:
                self.logger.info(
                    "Shared item download failed for %s; falling back to generic open: %s",
                    path,
                    exc,
                )
        return node.open(**kwargs)

    def _shared_item_id(self, node):
        item_id = node.data.get("item_id")
        if item_id:
            return item_id
        refreshed = self._refresh_node_by_id(
            node.data["drivewsid"],
            node.data.get("shareID"),
        )
        node.data = {**node.data, **refreshed.data}
        item_id = node.data.get("item_id")
        if not item_id:
            raise RuntimeError(f"Missing shared item id for {node.name}")
        return item_id

    def _delete_remote_node(self, node):
        if node.data.get("shareID"):
            self._put_document_item(
                self._shared_item_id(node),
                {"info_to_update": {"parent_item_id": "trash"}},
            )
            return
        node.delete()

    def _cleanup_temporary_upload_folder(self, staging_node):
        try:
            children = list(staging_node.get_children(force=True))
        except Exception as exc:
            self.logger.warning("Failed listing staging folder %s: %s", staging_node.name, exc)
            children = []
        for child in children:
            try:
                child.delete()
            except Exception as exc:
                self.logger.warning("Failed deleting staged item %s: %s", child.name, exc)
        try:
            staging_node.delete()
        except Exception as exc:
            self.logger.warning("Failed deleting staging folder %s: %s", staging_node.name, exc)

    def _upload_file_to_parent(self, parent_node, file_object):
        if not parent_node.data.get("shareID"):
            parent_node.upload(file_object)
            return
        staging_name = f".icloud-linux-stage-{uuid.uuid4().hex}"
        staging_node = self._create_remote_directory(self.api.drive.root, staging_name)
        if staging_node is None:
            raise RuntimeError("Failed creating shared upload staging directory")
        try:
            staging_node.upload(file_object)
            target_name = os.path.basename(file_object.name)
            staged_child = None
            for child in staging_node.get_children(force=True):
                if child.name == target_name:
                    staged_child = child
                    break
            if staged_child is None:
                raise KeyError(f"Missing staged upload {target_name} in /{staging_name}")
            self._move_remote_nodes([staged_child], parent_node)
        finally:
            self._cleanup_temporary_upload_folder(staging_node)

    def _shareid_for_path(self, path):
        if path in ("", "/"):
            return None
        entry = self.state.get_entry(path)
        if not entry:
            return None
        return entry.get("remote_shareid")

    def _remote_node_for_path(self, path):
        if path == "/" or path == "":
            return self.api.drive.root
        entry = self.state.get_entry(path)
        if not entry or not entry["remote_drivewsid"]:
            return None
        return self._node_from_entry(entry)

    def _refresh_node_by_id(self, remote_drivewsid, remote_shareid=None):
        data = self.api.drive.get_node_data(remote_drivewsid, remote_shareid)
        return DriveNode(self.api.drive, data)

    def _node_from_entry(self, entry):
        data = {
            "drivewsid": entry["remote_drivewsid"],
            "docwsid": entry.get("remote_docwsid"),
            "etag": entry.get("remote_etag"),
            "zone": entry.get("remote_zone"),
            "shareID": entry.get("remote_shareid"),
            "item_id": entry.get("remote_itemid"),
            "unifiedToken": entry.get("remote_unified_token"),
            "size": int(entry.get("size", 0) or 0),
            "type": entry.get("type", "file").upper(),
            "name": os.path.basename(entry["path"].rstrip("/")) or "root",
        }
        return DriveNode(self.api.drive, data)

    def _node_to_meta(self, node, path, inherited_shareid=None):
        data = node.data
        node_type = data.get("type", "FILE").lower()
        if self._is_directory_type(node_type):
            size = 0
        else:
            size = int(data.get("size", 0) or 0)
        shareid = data.get("shareID") or inherited_shareid
        return {
            "path": path,
            "type": node_type,
            "parent_path": os.path.dirname(path) or "/",
            "remote_drivewsid": data.get("drivewsid"),
            "remote_docwsid": data.get("docwsid"),
            "remote_etag": data.get("etag"),
            "remote_zone": data.get("zone"),
            "remote_shareid": shareid,
            "remote_itemid": data.get("item_id"),
            "remote_unified_token": data.get("unifiedToken"),
            "size": size,
            "mtime": parse_remote_time(data.get("dateModified")),
        }

    def _entry_conflicts(self, entry, meta):
        return (
            (entry.get("synced_path") and entry["synced_path"] != meta["path"])
            or (entry.get("remote_etag") and entry["remote_etag"] != meta["remote_etag"])
        )

    def _conflict_path(self, path):
        dirname = os.path.dirname(path) or "/"
        basename = os.path.basename(path)
        stamp = datetime.datetime.utcnow().strftime("%Y%m%d%H%M%S")
        return (
            "/" + f"{basename}.local-conflict-{stamp}"
            if dirname == "/"
            else dirname.rstrip("/") + "/" + f"{basename}.local-conflict-{stamp}"
        )

    def _path_allowed(self, path):
        """Return True when a path may be hydrated or synchronized."""
        return path_allowed(path, self.sync_paths, self.exclude_paths)

    def _path_lock(self, path):
        with self.path_locks_lock:
            lock = self.path_locks.get(path)
            if lock is None:
                lock = threading.Lock()
                self.path_locks[path] = lock
            return lock


class ICloudFS(Fuse):
    def __init__(self, *args, **kw):
        super(ICloudFS, self).__init__(*args, **kw)
        self.logger = logging.getLogger("icloud")
        self.username = None
        self.password = None
        self.cache_dir = None
        self.api = None
        self.mirror = None
        self.state = None
        self.sync_engine = None
        self.mount_uid = os.getuid()
        self.mount_gid = os.getgid()
        self.file_mode = DEFAULT_FILE_MODE
        self.dir_mode = DEFAULT_DIR_MODE

    def _is_directory_type(self, node_type):
        return (node_type or "").lower() in DIRECTORY_NODE_TYPES

    def apply_fuse_options(self, fuse_options):
        if not fuse_options:
            return
        if not isinstance(fuse_options, dict):
            raise ValueError("fuse_options must be a mapping")

        for option, value in fuse_options.items():
            if not isinstance(option, str) or not option:
                raise ValueError("fuse option names must be non-empty strings")
            if isinstance(value, bool):
                if value:
                    self.fuse_args.add(option)
                continue
            if value is None:
                continue
            self.fuse_args.add(option, value)

    def apply_permissions_config(self, permissions):
        resolved = resolve_permissions_config(permissions)
        self.mount_uid = resolved["uid"]
        self.mount_gid = resolved["gid"]
        self.file_mode = resolved["file_mode"]
        self.dir_mode = resolved["dir_mode"]

    def _permission_mode_for_entry(self, entry_type):
        return self.dir_mode if self._is_directory_type(entry_type) else self.file_mode

    def _apply_presented_permissions(self, attrs, entry_type):
        file_type = stat.S_IFDIR if self._is_directory_type(entry_type) else stat.S_IFREG
        attrs.st_mode = file_type | self._permission_mode_for_entry(entry_type)
        attrs.st_uid = self.mount_uid
        attrs.st_gid = self.mount_gid

    def _log_file_op(self, op, path=None, level=logging.INFO, **fields):
        payload = {}
        if path is not None:
            payload["path"] = path
        payload.update(fields)
        details = " ".join(f"{key}={value!r}" for key, value in payload.items() if value is not None)
        if details:
            self.logger.log(level, "file-op %s %s", op, details)
            return
        self.logger.log(level, "file-op %s", op)

    def _record_foreground_hydration_failure(self, path, exc, operation):
        try:
            classification, attempt, exhausted = (
                self.sync_engine._record_hydrate_failure(path, exc)
            )
        except Exception as record_exc:
            self.logger.error(
                "Failed hydrating on %s for %s: %s; "
                "also failed recording durable hydration failure: %s",
                operation,
                path,
                exc,
                record_exc,
            )
            self._write_unrecorded_hydration_failure_marker(
                path, exc, operation, record_exc
            )
            return
        self.logger.error(
            "Failed hydrating on %s for %s: %s; "
            "recorded %s failure (attempt %s, exhausted=%s)",
            operation,
            path,
            exc,
            classification,
            attempt,
            exhausted,
        )

    def _write_unrecorded_hydration_failure_marker(
        self, path, exc, operation, record_exc
    ):
        try:
            marker_path = os.path.join(
                self.cache_dir, "unrecorded_failures.log"
            )
            record = {
                "timestamp": int(time.time()),
                "path": path,
                "operation": operation,
                "error": str(exc),
                "record_error": str(record_exc),
            }
            with open(marker_path, "a", encoding="utf-8") as handle:
                handle.write(json.dumps(record, sort_keys=True) + "\n")
                handle.flush()
                os.fsync(handle.fileno())
        except Exception as marker_exc:
            # Both the database and the fallback log are unwritable, so no
            # durable record of this failure can exist and the queue is no
            # longer a complete picture. This log line is the only remaining
            # signal, so make it impossible to miss.
            self.logger.critical(
                "QUEUE UNRELIABLE: could not record the hydration failure for %s "
                "in the database (%s) or in the fallback log (%s). This file is "
                "stuck and will NOT appear in './icloudctl queue'. Check free "
                "space and permissions on the cache directory %s, then run "
                "'./icloudctl retry %s'.",
                path,
                record_exc,
                marker_exc,
                self.cache_dir,
                path,
            )

    def _mutation_allowed(self, operation, *paths):
        if self.sync_engine is None:
            self._log_file_op(
                operation,
                level=logging.WARNING,
                reason="sync-engine-unavailable",
            )
            return False

        if all(self.sync_engine._path_allowed(path) for path in paths):
            return True

        self._log_file_op(
            operation,
            paths=", ".join(paths),
            level=logging.WARNING,
            reason="path-policy",
        )
        return False

    def shutdown(self):
        if self.sync_engine is not None:
            self.sync_engine.shutdown()
        if self.state is not None:
            self.state.close()

    def request_remote_refresh(self):
        if self.sync_engine is None:
            self.logger.warning("Remote refresh requested before sync engine was initialized")
            return
        self.sync_engine.request_remote_refresh()

    def init_icloud(self, username, password, cache_dir, cookie_dir=None, require_session=True):
        """Initialise iCloud API connection.

        If *require_session* is False (the default when called from the systemd
        service path), an auth failure sets self.api = None and logs a clear
        error rather than crashing the process.  The FUSE layer will return
        EACCES for all operations until a session is restored via
        './icloudctl auth' followed by './icloudctl restart'.
        """
        self.username = username
        self.password = password
        self.cache_dir = cache_dir
        os.makedirs(self.cache_dir, exist_ok=True)
        if cookie_dir:
            os.makedirs(cookie_dir, exist_ok=True)

        try:
            # Resolve Apple account partition (fixes 421 redirect for non-default shards)
            import requests as _req
            _r = _req.post("https://setup.icloud.com/setup/ws/1/validate", json={}, timeout=10)
            _partition = _r.headers.get("x-apple-user-partition")

            self.api = PyiCloudService(username, password,
                                       cookie_directory=cookie_dir,
                                       authenticate=False)
            install_pyi_cloud_session_timeouts(self.api, self.logger)
            if _partition:
                self.api._setup_endpoint = (
                    f"https://p{_partition}-setup.icloud.com/setup/ws/1"
                )
            self.api.authenticate()
            if self.api.requires_2fa:
                if sys.stdin.isatty():
                    print("Two-factor authentication required.")
                    code = input("Enter the verification code: ").strip()
                    result = self.api.validate_2fa_code(code)
                    print("Result: %s" % result)
                    if result and not self.api.is_trusted_session:
                        self.api.trust_session()
                else:
                    raise RuntimeError(
                        "2FA required, but no interactive terminal is available. "
                        "Run './icloudctl auth' first to establish a trusted session."
                    )

            if self.api.requires_2sa:
                if sys.stdin.isatty():
                    print("Two-step authentication required.")
                    devices = self.api.trusted_devices
                    for index, device in enumerate(devices):
                        label = device.get("deviceName") or f"SMS to {device.get('phoneNumber', 'unknown')}"
                        print(f"{index}: {label}")
                    selected = int(input("Select device index: ").strip() or "0")
                    device = devices[selected]
                    self.api.send_verification_code(device)
                    code = input("Enter the verification code: ").strip()
                    if not self.api.validate_verification_code(device, code):
                        raise RuntimeError("Failed to verify 2SA code")
                else:
                    raise RuntimeError(
                        "2SA required, but no interactive terminal is available. "
                        "Run './icloudctl auth' first to establish a trusted session."
                    )

            if self.api.requires_2fa or self.api.requires_2sa:
                raise RuntimeError("Additional authentication still required after code verification.")

        except Exception as exc:
            self.logger.error("Failed to connect to iCloud: %s", exc)
            if require_session:
                raise
            # Non-fatal path: park in unauthenticated state.  The FUSE layer
            # will return EACCES for all operations; the service stays up and
            # won't trigger Apple's lockout by crash-looping.
            self.logger.error(
                "Service starting in UNAUTHENTICATED mode.  "
                "Run './icloudctl auth' then './icloudctl restart' to restore access."
            )
            self.api = None

    def _is_authenticated(self):
        """Return True if a live iCloud session is available."""
        return self.api is not None

    def init_local_cache(
        self,
        cache_dir,
        warmup_mode,
        conflict_mode,
        upload_interval_seconds,
        remote_refresh_interval_seconds,
        warmup_workers,
        sync_paths=None,
        exclude_paths=None,
        auto_sync=True,
    ):
        self.mirror = LocalMirror(cache_dir, file_mode=self.file_mode, dir_mode=self.dir_mode)
        state_path = os.path.join(cache_dir, "state.sqlite3")
        self.state = SyncState(state_path)
        if not self._is_authenticated():
            self.logger.warning(
                "Skipping sync engine start — no iCloud session.  "
                "FUSE will serve cached data read-only until re-authenticated."
            )
            return
        self.sync_engine = ICloudSyncEngine(
            self.api,
            self.mirror,
            self.state,
            self.logger,
            warmup_mode=warmup_mode,
            conflict_mode=conflict_mode,
            upload_interval_seconds=upload_interval_seconds,
            remote_refresh_interval_seconds=remote_refresh_interval_seconds,
            warmup_workers=warmup_workers,
            sync_paths=sync_paths,
            exclude_paths=exclude_paths,
            auto_sync=auto_sync,
        )
        self.sync_engine.start()

    def getattr(self, path):
        now = int(time.time())
        entry = self.state.get_entry(path) if self.state else None
        attrs = Stat()

        if path == "/":
            try:
                stats = self.mirror.stat_local(path)
                self._apply_os_stat(attrs, stats)
            except Exception:
                self._apply_presented_permissions(attrs, "folder")
                attrs.st_nlink = 2
                attrs.st_size = 0
                attrs.st_ctime = now
                attrs.st_mtime = now
                attrs.st_atime = now
            return attrs

        if self.mirror and self.mirror.exists(path):
            stats = self.mirror.stat_local(path)
            self._apply_os_stat(attrs, stats)
            if entry and entry["type"] == "file" and not entry["hydrated"]:
                attrs.st_size = entry["size"]
                attrs.st_mtime = entry["mtime"]
                attrs.st_ctime = entry["mtime"]
            return attrs

        if entry and not entry["tombstone"]:
            self._apply_presented_permissions(attrs, entry["type"])
            attrs.st_nlink = 2 if self._is_directory_type(entry["type"]) else 1
            attrs.st_size = entry["size"]
            attrs.st_ctime = entry["mtime"] or now
            attrs.st_mtime = entry["mtime"] or now
            attrs.st_atime = now
            return attrs

        return -errno.ENOENT

    def readdir(self, path, offset):
        if not self.mirror.exists(path) or not self.mirror.is_dir(path):
            return -errno.ENOENT

        self._log_file_op("readdir", path, level=logging.DEBUG)
        entries = [".", ".."] + sorted(self.mirror.listdir(path))
        for entry in entries:
            yield fuse.Direntry(entry)

    def open(self, path, flags):
        self._log_file_op("open", path, level=logging.DEBUG, flags=flags)
        if flags & (os.O_CREAT | os.O_WRONLY | os.O_RDWR | os.O_APPEND | os.O_TRUNC):
            if not self._is_authenticated() or not self._mutation_allowed("open", path):
                return -errno.EACCES
        if not self.state.get_entry(path):
            if flags & (os.O_CREAT | os.O_WRONLY | os.O_RDWR | os.O_APPEND | os.O_TRUNC):
                return self.create(path, 0o644, flags)
            return -errno.ENOENT

        entry = self.state.get_entry(path)
        if (
            entry
            and entry["type"] == "file"
            and not entry["hydrated"]
            and entry["remote_drivewsid"]
        ):
            if not self._is_authenticated() or self.sync_engine is None:
                # No session: serve what we have locally; remote files return EIO
                if not self.mirror.exists(path):
                    self.logger.warning(
                        "Cannot hydrate %s: no iCloud session. Run './icloudctl auth' then restart.", path
                    )
                    return -errno.EIO
                return 0
            try:
                self.sync_engine.ensure_local_file(path)
            except Exception as exc:
                self._record_foreground_hydration_failure(path, exc, "open")
                return -errno.EIO
        return 0

    def create(self, path, mode, flags=None):
        if not self._is_authenticated():
            return -errno.EACCES
        if not self._mutation_allowed("create", path):
            return -errno.EACCES
        try:
            self.mirror.create_file(path)
            stats = self.mirror.stat_local(path)
            self.state.upsert_entry(
                {
                    "path": path,
                    "type": "file",
                    "parent_path": os.path.dirname(path) or "/",
                    "size": 0,
                    "mtime": int(stats.st_mtime),
                    "hydrated": True,
                    "dirty": True,
                    "tombstone": False,
                    "synced_path": None,
                }
            )
            self._log_file_op("create", path, mode=oct(mode), flags=flags)
            return 0
        except Exception as exc:
            self.logger.error("Error creating file %s: %s", path, exc)
            return -errno.EIO

    def read(self, path, size, offset):
        entry = self.state.get_entry(path)
        if not entry or entry["type"] != "file" or entry["tombstone"]:
            return -errno.ENOENT

        if not entry["hydrated"] and entry["remote_drivewsid"]:
            if self.sync_engine is None:
                # No session — cannot hydrate; if placeholder exists it has no data
                self.logger.warning(
                    "Cannot hydrate %s: no iCloud session. Run './icloudctl auth' then restart.", path
                )
                return -errno.EIO
            try:
                self.sync_engine.ensure_local_file(path)
            except Exception as exc:
                self._record_foreground_hydration_failure(path, exc, "read")
                return -errno.EIO
        try:
            self._log_file_op("read", path, level=logging.DEBUG, size=size, offset=offset)
            return self.mirror.read(path, size, offset)
        except Exception as exc:
            self.logger.error("Error reading %s: %s", path, exc)
            return -errno.EIO

    def write(self, path, buf, offset):
        if not self._is_authenticated():
            return -errno.EACCES
        if not self._mutation_allowed("write", path):
            return -errno.EACCES
        entry = self.state.get_entry(path)
        if entry and not entry["hydrated"] and entry["remote_drivewsid"]:
            try:
                self.sync_engine.ensure_local_file(path)
            except Exception as exc:
                self._record_foreground_hydration_failure(path, exc, "write")
                return -errno.EIO

        try:
            written = self.mirror.write(path, buf, offset)
            stats = self.mirror.stat_local(path)
            checksum = self.mirror.file_sha256(path)
            if not entry:
                self.state.upsert_entry(
                    {
                        "path": path,
                        "type": "file",
                        "parent_path": os.path.dirname(path) or "/",
                        "size": stats.st_size,
                        "mtime": int(stats.st_mtime),
                        "hydrated": True,
                        "dirty": True,
                        "tombstone": False,
                        "local_sha256": checksum,
                        "synced_path": None,
                    }
                )
            else:
                self.state.mark_dirty(path, stats.st_size, int(stats.st_mtime), 1, checksum)
            self._log_file_op("write", path, size=len(buf), offset=offset, written=written)
            return written
        except Exception as exc:
            self.logger.error("Error writing %s: %s", path, exc)
            return -errno.EIO

    def flush(self, path):
        return 0

    def release(self, path, flags):
        return 0

    def mkdir(self, path, mode):
        if not self._is_authenticated():
            return -errno.EACCES
        if not self._mutation_allowed("mkdir", path):
            return -errno.EACCES
        try:
            self.mirror.ensure_dir(path)
            stats = self.mirror.stat_local(path)
            self.state.upsert_entry(
                {
                    "path": path,
                    "type": "folder",
                    "parent_path": os.path.dirname(path) or "/",
                    "size": 0,
                    "mtime": int(stats.st_mtime),
                    "hydrated": True,
                    "dirty": True,
                    "tombstone": False,
                    "synced_path": None,
                }
            )
            self._log_file_op("mkdir", path, mode=oct(mode))
            return 0
        except Exception as exc:
            self.logger.error("Error creating directory %s: %s", path, exc)
            return -errno.EIO

    def rmdir(self, path):
        if not self._is_authenticated():
            return -errno.EACCES
        if not self._mutation_allowed("rmdir", path):
            return -errno.EACCES
        entry = self.state.get_entry(path)
        if not entry:
            return -errno.ENOENT

        try:
            self.mirror.remove_dir(path)
            if entry["remote_drivewsid"]:
                self.state.mark_tombstone(path)
            else:
                self.state.remove_subtree(path)
            self._log_file_op("rmdir", path)
            return 0
        except OSError as exc:
            if exc.errno:
                return -exc.errno
            self.logger.error("Error removing directory %s: %s", path, exc)
            return -errno.EIO

    def unlink(self, path):
        if not self._is_authenticated():
            return -errno.EACCES
        if not self._mutation_allowed("unlink", path):
            return -errno.EACCES
        entry = self.state.get_entry(path)
        if not entry:
            return -errno.ENOENT

        try:
            if self.mirror.exists(path):
                self.mirror.remove_file(path)
            if entry["remote_drivewsid"]:
                self.state.mark_tombstone(path)
            else:
                self.state.remove_entry(path)
            self._log_file_op("unlink", path)
            return 0
        except OSError as exc:
            if exc.errno:
                return -exc.errno
            self.logger.error("Error unlinking %s: %s", path, exc)
            return -errno.EIO

    def rename(self, oldpath, newpath):
        if not self._is_authenticated():
            return -errno.EACCES
        if not self._mutation_allowed("rename", oldpath, newpath):
            return -errno.EACCES
        entry = self.state.get_entry(oldpath)
        if not entry:
            return -errno.ENOENT

        try:
            existing = self.state.get_entry(newpath)
            if (
                existing
                and existing.get("remote_drivewsid")
                and entry.get("remote_drivewsid")
                and existing["remote_drivewsid"] != entry["remote_drivewsid"]
            ):
                self.logger.error(
                    "Refusing to replace remotely synced path %s with different remotely synced path %s",
                    newpath,
                    oldpath,
                )
                return -errno.EEXIST
            self.mirror.rename_path(oldpath, newpath)
            self.state.rename_tree(
                oldpath,
                newpath,
                root_dirty=True,
                replace_entry=existing,
            )
            self._log_file_op("rename", oldpath, target_path=newpath)
            return 0
        except OSError as exc:
            if exc.errno:
                return -exc.errno
            self.logger.error("Error renaming %s to %s: %s", oldpath, newpath, exc)
            return -errno.EIO
        except Exception as exc:
            self.logger.error("Error renaming %s to %s: %s", oldpath, newpath, exc)
            return -errno.EIO

    def truncate(self, path, length):
        if not self._is_authenticated():
            return -errno.EACCES
        if not self._mutation_allowed("truncate", path):
            return -errno.EACCES
        entry = self.state.get_entry(path)
        if entry and not entry["hydrated"] and entry["remote_drivewsid"]:
            try:
                self.sync_engine.ensure_local_file(path)
            except Exception as exc:
                self._record_foreground_hydration_failure(path, exc, "truncate")
                return -errno.EIO

        try:
            self.mirror.truncate(path, length)
            stats = self.mirror.stat_local(path)
            checksum = self.mirror.file_sha256(path)
            if not entry:
                self.state.upsert_entry(
                    {
                        "path": path,
                        "type": "file",
                        "parent_path": os.path.dirname(path) or "/",
                        "size": stats.st_size,
                        "mtime": int(stats.st_mtime),
                        "hydrated": True,
                        "dirty": True,
                        "tombstone": False,
                        "local_sha256": checksum,
                        "synced_path": None,
                    }
                )
            else:
                self.state.mark_dirty(path, stats.st_size, int(stats.st_mtime), 1, checksum)
            self._log_file_op("truncate", path, length=length)
            return 0
        except Exception as exc:
            self.logger.error("Error truncating %s: %s", path, exc)
            return -errno.EIO

    def mknod(self, path, mode, dev):
        if not stat.S_ISREG(mode):
            return -errno.ENOSYS
        return self.create(path, mode)

    def utime(self, path, times):
        if not self._is_authenticated():
            return -errno.EACCES
        if not self._mutation_allowed("utime", path):
            return -errno.EACCES
        try:
            if not self.mirror.exists(path):
                return -errno.ENOENT
            atime, mtime = times if times else (time.time(), time.time())
            self.mirror.set_mtime(path, int(mtime))
            stats = self.mirror.stat_local(path)
            if self.state.get_entry(path):
                self.state.mark_dirty(path, stats.st_size, int(stats.st_mtime))
            self._log_file_op("utime", path, atime=int(atime), mtime=int(mtime))
            return 0
        except Exception as exc:
            self.logger.error("Error setting utime for %s: %s", path, exc)
            return -errno.EIO

    def statfs(self):
        stats = self.mirror.statvfs()
        return {
            "f_bsize": stats.f_bsize,
            "f_frsize": stats.f_frsize,
            "f_blocks": stats.f_blocks,
            "f_bfree": stats.f_bfree,
            "f_bavail": stats.f_bavail,
            "f_files": stats.f_files,
            "f_ffree": stats.f_ffree,
            "f_namelen": stats.f_namemax,
        }

    def _apply_os_stat(self, attrs, stats):
        attrs.st_mode = stats.st_mode
        attrs.st_ino = stats.st_ino
        attrs.st_dev = stats.st_dev
        attrs.st_nlink = stats.st_nlink
        attrs.st_size = stats.st_size
        attrs.st_atime = int(stats.st_atime)
        attrs.st_mtime = int(stats.st_mtime)
        attrs.st_ctime = int(stats.st_ctime)
        self._apply_presented_permissions(attrs, "folder" if stat.S_ISDIR(stats.st_mode) else "file")


def parse_config(config_path):
    try:
        with open(config_path, "r", encoding="utf-8") as handle:
            config = yaml.safe_load(handle) or {}
        return config
    except Exception as exc:
        print(f"Error parsing config file: {exc}")
        sys.exit(1)


def main():
    usage = """
iCloud Linux: Mount iCloud Drive as a FUSE filesystem

%prog [options] mountpoint
"""
    fs = ICloudFS(version="%prog " + fuse.__version__, usage=usage, dash_s_do="setsingle")
    fs.parser.add_option(
        "-c",
        "--config",
        dest="config",
        default=os.path.expanduser("~/.config/icloud-linux/config.yaml"),
        help="Path to config file (default: ~/.config/icloud-linux/config.yaml)",
    )
    fs.parser.add_option("-v", "--debug", dest="debug", action="store_true", help="Enable debug logging")
    fs.parse(errex=1)
    args = fs.cmdline[0]

    log_level = logging.DEBUG if args.debug else logging.INFO
    log_path = os.environ.get("ICLOUD_LOG_PATH", os.path.expanduser("~/.local/state/icloud-linux/icloud.log"))
    os.makedirs(os.path.dirname(log_path), exist_ok=True)

    logging.basicConfig(
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        level=log_level,
        handlers=[logging.StreamHandler(), logging.FileHandler(log_path)],
    )
    logger = logging.getLogger("icloud")
    logging.getLogger("pyicloud.base").addFilter(IgnoreIcdrsWarning())

    config = parse_config(args.config)
    try:
        fs.apply_fuse_options(config.get("fuse_options"))
        fs.apply_permissions_config(config.get("permissions"))
    except ValueError as exc:
        logger.error("Invalid configuration: %s", exc)
        sys.exit(1)
    username = config.get("username")
    password = config.get("password")
    if not username or not password:
        logger.error("Username or password not provided in config file")
        sys.exit(1)

    cache_dir = os.path.expanduser(config.get("cache_dir", "~/.cache/icloud-linux"))
    cookie_dir = os.path.expanduser(config.get("cookie_dir", "~/.config/icloud-linux/cookies"))
    warmup_mode = config.get("warmup_mode", "background")
    conflict_mode = config.get("conflict_mode", "copy")
    upload_interval_seconds = int(config.get("upload_interval_seconds", 30))
    remote_refresh_interval_seconds = int(config.get("remote_refresh_interval_seconds", 300))
    warmup_workers = int(config.get("warmup_workers", 1))
    sync_paths = config.get("sync_paths", None)      # list of iCloud paths to hydrate, None=all
    exclude_paths = config.get("exclude_paths", None) # deny-list applied before sync_paths
    auto_sync = bool(config.get("auto_sync", True))   # False = manual sync only via icloudctl sync

    # When running under systemd (no TTY) we never want a failed auth to crash
    # the process — that would trigger Restart=on-failure and hammer Apple's
    # lockout threshold.  require_session=True is still the right default for
    # interactive invocations (e.g. debugging from a terminal with -f).
    interactive = sys.stdin.isatty()

    # Register signal handlers BEFORE init_local_cache() so they are live
    # during the reconcile pass (~75s).  Without this, SIGUSR1 arriving during
    # reconcile uses Python's default handler which kills the process.
    atexit.register(fs.shutdown)

    def handle_shutdown(signum, frame):
        logger.info("Received signal %s, shutting down background sync", signum)
        fs.shutdown()
        raise SystemExit(0)

    # SIGUSR1 — on-demand sync/refresh trigger (used by 'icloudctl sync' and 'icloudctl refresh').
    # Queues a one-shot remote crawl in a background thread so the signal
    # handler returns immediately and FUSE keeps serving requests.
    # If sync_engine is not ready yet (still reconciling), queues it to run
    # once the engine is available.
    def handle_sigusr1(signum, frame):
        def _one_shot():
            # Wait up to 120s for the sync engine to be ready after startup
            deadline = time.time() + 120
            while fs.sync_engine is None and time.time() < deadline:
                time.sleep(1)
            if fs.sync_engine is None:
                logger.warning("SIGUSR1: sync engine not available (unauthenticated or startup failed)")
                return
            logger.info("SIGUSR1: starting on-demand remote metadata crawl")
            quarantined_entries = 0
            try:
                fs.sync_engine.initial_scan()
                quarantined_entries = fs.sync_engine.sync_dirty_entries(
                    include_deferred=True
                )
                logger.info(
                    "SIGUSR1: on-demand sync skipped %s quarantined entries",
                    quarantined_entries,
                )
                logger.info("SIGUSR1: on-demand remote metadata crawl complete")
            except Exception as exc:
                logger.error("SIGUSR1: on-demand remote metadata crawl failed: %s", exc)
            finally:
                # Write completion marker so icloudctl sync can detect done.
                state_dir = os.path.expanduser("~/.local/state/icloud-linux")
                os.makedirs(state_dir, exist_ok=True)
                marker = os.path.join(state_dir, "sync_done")
                with open(marker, "w") as fh:
                    json.dump(
                        {
                            "completed_at": time.time(),
                            "quarantined_skipped": quarantined_entries,
                        },
                        fh,
                    )

        threading.Thread(target=_one_shot, name="icloud-on-demand-sync", daemon=True).start()

    signal.signal(signal.SIGTERM, handle_shutdown)
    signal.signal(signal.SIGINT, handle_shutdown)
    if hasattr(signal, "SIGUSR1"):
        signal.signal(signal.SIGUSR1, handle_sigusr1)

    fs.init_icloud(username, password, cache_dir, cookie_dir, require_session=interactive)
    fs.init_local_cache(
        cache_dir,
        warmup_mode,
        conflict_mode,
        upload_interval_seconds,
        remote_refresh_interval_seconds,
        warmup_workers,
        sync_paths=sync_paths,
        exclude_paths=exclude_paths,
        auto_sync=auto_sync,
    )

    try:
        fs.main()
    finally:
        fs.shutdown()


if __name__ == "__main__":
    main()
