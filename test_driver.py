import errno
import gzip
import io
import os
import shutil
import sqlite3
import socket
import stat
import tempfile
import threading
import time
import unittest
from unittest.mock import Mock, patch
from requests import Response
from requests.exceptions import ConnectionError, Timeout
from requests.models import RequestEncodingMixin
from urllib3.response import HTTPResponse

import driver
from driver import (
    ICloudFS,
    ICloudSyncEngine,
    LocalMirror,
    CONTROL_PLANE_TIMEOUT,
    DOWNLOAD_TIMEOUT,
    MAX_SYNC_ATTEMPTS,
    MissingMirrorFile,
    NamedFileStream,
    SYNC_FAILURE_AUTH,
    SYNC_FAILURE_TERMINAL,
    SYNC_FAILURE_TRANSIENT,
    SyncState,
    classify_sync_failure,
    install_pyi_cloud_session_timeouts,
)
from pyicloud.exceptions import (
    PyiCloud2FARequiredException,
    PyiCloud2SARequiredException,
    PyiCloudAPIResponseException,
    PyiCloudAuthRequiredException,
    PyiCloudFailedLoginException,
)
import queue_diagnostic
from queue_diagnostic import open_read_only
import queue_recovery


class NoUnboundedReadStream(io.BytesIO):
    def read(self, size=-1):
        if size is None or size < 0:
            raise AssertionError("stream was read without a chunk size")
        return super().read(size)


class FakeCrawlNode:
    def __init__(self, name, drivewsid, children=None, error=None, block_event=None):
        self.name = name
        self.data = {
            "type": "FOLDER",
            "drivewsid": drivewsid,
            "docwsid": f"doc-{drivewsid}",
            "etag": f"etag-{drivewsid}",
            "zone": "zone-1",
            "dateModified": "2026-04-06T00:00:00Z",
        }
        self.children = children or []
        self.error = error
        self.block_event = block_event
        self.started = threading.Event()
        self.get_children_calls = 0

    def get_children(self, force=False):
        self.get_children_calls += 1
        self.started.set()
        if self.block_event is not None:
            self.block_event.wait()
        if self.error is not None:
            raise self.error
        return self.children


class ImmediateCrawlExecutor:
    """Runs ordinary folders synchronously but leaves wedged folders running."""

    def __init__(self, *args, **kwargs):
        self.shutdown = Mock()
        self.threads = []

    def submit(self, callback, *args):
        node = callback.__self__
        future = Mock()
        if node.block_event is not None:
            thread = threading.Thread(target=callback, args=args, daemon=True)
            thread.start()
            self.threads.append(thread)
            future.result.side_effect = TimeoutError()
        else:
            future.result.side_effect = lambda timeout: callback(*args)
        return future


class DriverStateTests(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.mkdtemp(prefix="icloud-linux-test-")
        self.mirror = LocalMirror(self.root)
        self.state = SyncState(os.path.join(self.root, "state.sqlite3"))

    def tearDown(self):
        self.state.close()
        shutil.rmtree(self.root)

    def test_mirror_read_write_truncate(self):
        self.mirror.create_file("/docs/a.txt")
        self.mirror.write("/docs/a.txt", b"hello", 0)
        self.assertEqual(self.mirror.read("/docs/a.txt", 5, 0), b"hello")

        self.mirror.truncate("/docs/a.txt", 2)
        self.assertEqual(self.mirror.read("/docs/a.txt", 10, 0), b"he")

    def test_ensure_dir_replaces_file_placeholder(self):
        self.mirror.create_file("/Obsidian")

        self.mirror.ensure_dir("/Obsidian")

        self.assertTrue(self.mirror.is_dir("/Obsidian"))

    def test_write_atomic_stream_copies_in_chunks(self):
        stream = NoUnboundedReadStream(b"streamed content")

        written = self.mirror.write_atomic_stream("/docs/a.txt", stream)

        self.assertEqual(written, len(b"streamed content"))
        self.assertEqual(self.mirror.read("/docs/a.txt", 100, 0), b"streamed content")

    def test_write_atomic_stream_accepts_chunk_iterable(self):
        written = self.mirror.write_atomic_stream(
            "/docs/a.txt",
            (chunk for chunk in (b"streamed ", b"content")),
        )

        self.assertEqual(written, len(b"streamed content"))
        self.assertEqual(self.mirror.read("/docs/a.txt", 100, 0), b"streamed content")

    def test_named_file_stream_is_encoded_as_file_content(self):
        stream = NamedFileStream(io.BytesIO(b"content"), "note.md")

        body, content_type = RequestEncodingMixin._encode_files(
            {stream.name: stream},
            {},
        )

        self.assertIn(b"content", body)
        self.assertIn(b'filename="note.md"', body)
        self.assertTrue(content_type.startswith("multipart/form-data"))

    def test_rename_tree_preserves_old_synced_paths_for_local_rename(self):
        self.state.upsert_entry(
            {
                "path": "/docs",
                "type": "folder",
                "parent_path": "/",
                "hydrated": True,
                "dirty": False,
                "tombstone": False,
                "synced_path": "/docs",
            }
        )
        self.state.upsert_entry(
            {
                "path": "/docs/a.txt",
                "type": "file",
                "parent_path": "/docs",
                "hydrated": True,
                "dirty": False,
                "tombstone": False,
                "synced_path": "/docs/a.txt",
            }
        )

        self.state.rename_tree("/docs", "/archive", root_dirty=True)

        folder = self.state.get_entry("/archive")
        child = self.state.get_entry("/archive/a.txt")
        self.assertEqual(folder["synced_path"], "/docs")
        self.assertEqual(child["synced_path"], "/docs/a.txt")
        self.assertEqual(folder["dirty"], 1)
        self.assertEqual(child["dirty"], 0)

    def test_rename_tree_updates_synced_paths_for_remote_rename(self):
        self.state.upsert_entry(
            {
                "path": "/docs",
                "type": "folder",
                "parent_path": "/",
                "hydrated": True,
                "dirty": False,
                "tombstone": False,
                "synced_path": "/docs",
            }
        )
        self.state.upsert_entry(
            {
                "path": "/docs/a.txt",
                "type": "file",
                "parent_path": "/docs",
                "hydrated": True,
                "dirty": False,
                "tombstone": False,
                "synced_path": "/docs/a.txt",
            }
        )

        self.state.rename_tree("/docs", "/remote-docs", root_dirty=False, update_synced=True)

        folder = self.state.get_entry("/remote-docs")
        child = self.state.get_entry("/remote-docs/a.txt")
        self.assertEqual(folder["synced_path"], "/remote-docs")
        self.assertEqual(child["synced_path"], "/remote-docs/a.txt")

    def test_detach_subtree_as_conflict_clears_remote_identity(self):
        self.state.upsert_entry(
            {
                "path": "/docs",
                "type": "folder",
                "parent_path": "/",
                "remote_drivewsid": "folder-1",
                "hydrated": True,
                "dirty": True,
                "tombstone": False,
                "synced_path": "/docs",
            }
        )
        self.state.upsert_entry(
            {
                "path": "/docs/a.txt",
                "type": "file",
                "parent_path": "/docs",
                "remote_drivewsid": "file-1",
                "remote_docwsid": "doc-1",
                "remote_etag": "etag-1",
                "remote_zone": "zone",
                "hydrated": True,
                "dirty": True,
                "tombstone": False,
                "synced_path": "/docs/a.txt",
            }
        )

        self.state.detach_subtree_as_conflict("/docs", "/docs.local-conflict-123")

        folder = self.state.get_entry("/docs.local-conflict-123")
        child = self.state.get_entry("/docs.local-conflict-123/a.txt")
        self.assertIsNone(folder["remote_drivewsid"])
        self.assertIsNone(child["remote_docwsid"])
        self.assertEqual(folder["dirty"], 1)
        self.assertEqual(child["dirty"], 1)

    def test_reconcile_persistent_cache_keeps_placeholder_unhydrated(self):
        self.state.upsert_entry(
            {
                "path": "/docs/a.txt",
                "type": "file",
                "parent_path": "/docs",
                "remote_drivewsid": "file-1",
                "size": 128,
                "mtime": 123,
                "hydrated": False,
                "dirty": False,
                "tombstone": False,
                "synced_path": "/docs/a.txt",
            }
        )
        self.mirror.ensure_dir("/docs")
        self.mirror.materialize_placeholder("/docs/a.txt", 128, 123)

        api = Mock()
        api.drive.root = Mock()
        engine = ICloudSyncEngine(api, self.mirror, self.state, Mock())
        engine._reconcile_persistent_cache()

        entry = self.state.get_entry("/docs/a.txt")
        self.assertEqual(entry["hydrated"], 0)

    def test_reconcile_persistent_cache_replaces_app_library_placeholder(self):
        self.state.upsert_entry(
            {
                "path": "/Obsidian",
                "type": "app_library",
                "parent_path": "/",
                "remote_drivewsid": "folder-1",
                "hydrated": True,
                "dirty": False,
                "tombstone": False,
                "synced_path": "/Obsidian",
            }
        )
        self.mirror.create_file("/Obsidian")

        api = Mock()
        api.drive.root = Mock()
        engine = ICloudSyncEngine(api, self.mirror, self.state, Mock())

        engine._reconcile_persistent_cache()

        self.assertTrue(self.mirror.is_dir("/Obsidian"))

    def test_reconcile_persistent_cache_queues_changed_hydrated_file_for_upload(self):
        path = "/changed.txt"
        self.mirror.write(path, b"before", 0)
        stats = self.mirror.stat_local(path)
        checksum = self.mirror.file_sha256(path)
        self.state.upsert_entry(
            {
                "path": path,
                "type": "file",
                "parent_path": "/",
                "remote_drivewsid": "file-1",
                "size": stats.st_size,
                "mtime": int(stats.st_mtime),
                "hydrated": True,
                "dirty": False,
                "tombstone": False,
                "local_sha256": checksum,
                "synced_path": path,
            }
        )
        self.mirror.write(path, b"changed while offline", 0)
        logger = Mock()
        engine = ICloudSyncEngine(Mock(), self.mirror, self.state, logger)

        engine._reconcile_persistent_cache()

        entry = self.state.get_entry(path)
        report = queue_diagnostic.inspect_queue(self.state.db_path, self.root)
        self.assertEqual(entry["dirty"], 1)
        self.assertEqual(
            [item["path"] for item in report["pending_sync_entries"]],
            [path],
        )
        self.assertIn("1 sync-ready", queue_diagnostic.queue_summary(report))
        logger.info.assert_any_call(
            "Mirror file %s changed while the driver was not running and has been queued for upload",
            path,
        )
        logger.info.assert_any_call(
            "Persistent cache ready: %s entries, %s directories recreated, "
            "%s files queued for hydration, %s files queued for upload",
            1,
            0,
            0,
            1,
        )

    def test_reconcile_persistent_cache_keeps_unchanged_hydrated_file_clean(self):
        path = "/unchanged.txt"
        self.mirror.write(path, b"unchanged", 0)
        stats = self.mirror.stat_local(path)
        self.state.upsert_entry(
            {
                "path": path,
                "type": "file",
                "parent_path": "/",
                "remote_drivewsid": "file-1",
                "size": stats.st_size,
                "mtime": int(stats.st_mtime),
                "hydrated": True,
                "dirty": False,
                "tombstone": False,
                "local_sha256": self.mirror.file_sha256(path),
                "synced_path": path,
            }
        )
        logger = Mock()
        engine = ICloudSyncEngine(Mock(), self.mirror, self.state, logger)

        engine._reconcile_persistent_cache()

        self.assertEqual(self.state.get_entry(path)["dirty"], 0)
        logger.info.assert_called_once_with(
            "Persistent cache ready: %s entries, %s directories recreated, "
            "%s files queued for hydration, %s files queued for upload",
            1,
            0,
            0,
            0,
        )

    def test_reconcile_persistent_cache_does_not_queue_file_without_stored_checksum(self):
        path = "/unknown-checksum.txt"
        self.mirror.write(path, b"before", 0)
        stats = self.mirror.stat_local(path)
        self.state.upsert_entry(
            {
                "path": path,
                "type": "file",
                "parent_path": "/",
                "remote_drivewsid": "file-1",
                "size": stats.st_size,
                "mtime": int(stats.st_mtime),
                "hydrated": True,
                "dirty": False,
                "tombstone": False,
                "local_sha256": None,
                "synced_path": path,
            }
        )
        self.mirror.write(path, b"changed while offline", 0)
        logger = Mock()
        engine = ICloudSyncEngine(Mock(), self.mirror, self.state, logger)

        engine._reconcile_persistent_cache()

        entry = self.state.get_entry(path)
        self.assertEqual(entry["dirty"], 0)
        self.assertEqual(entry["local_sha256"], self.mirror.file_sha256(path))
        logger.info.assert_called_once_with(
            "Persistent cache ready: %s entries, %s directories recreated, "
            "%s files queued for hydration, %s files queued for upload",
            1,
            0,
            0,
            0,
        )

    def test_reconcile_persistent_cache_preserves_quarantine_while_queueing_change(self):
        path = "/quarantined.txt"
        self.mirror.write(path, b"before", 0)
        stats = self.mirror.stat_local(path)
        self.state.upsert_entry(
            {
                "path": path,
                "type": "file",
                "parent_path": "/",
                "remote_drivewsid": "file-1",
                "size": stats.st_size,
                "mtime": int(stats.st_mtime),
                "hydrated": True,
                "dirty": True,
                "tombstone": False,
                "local_sha256": self.mirror.file_sha256(path),
                "synced_path": path,
            }
        )
        for _ in range(MAX_SYNC_ATTEMPTS):
            self.state.record_sync_failure(
                path,
                RuntimeError("permanent failure"),
                SYNC_FAILURE_TRANSIENT,
                MAX_SYNC_ATTEMPTS,
            )
        self.mirror.write(path, b"changed while offline", 0)
        logger = Mock()
        engine = ICloudSyncEngine(Mock(), self.mirror, self.state, logger)

        engine._reconcile_persistent_cache()

        entry = self.state.get_entry(path)
        self.assertEqual(entry["dirty"], 1)
        self.assertEqual(entry["failed"], 1)
        self.assertEqual(entry["sync_attempt_count"], MAX_SYNC_ATTEMPTS)
        self.assertEqual(entry["sync_last_error"], "permanent failure")
        logger.info.assert_any_call(
            "Persistent cache ready: %s entries, %s directories recreated, "
            "%s files queued for hydration, %s files queued for upload",
            1,
            0,
            0,
            0,
        )

    def test_remote_shareid_round_trips_through_state(self):
        self.state.upsert_entry(
            {
                "path": "/shared/a.txt",
                "type": "file",
                "parent_path": "/shared",
                "remote_drivewsid": "file-1",
                "remote_docwsid": "doc-1",
                "remote_etag": "etag-1",
                "remote_zone": "zone-1",
                "remote_shareid": {"share-zone": "abc"},
                "hydrated": False,
                "dirty": False,
                "tombstone": False,
                "synced_path": "/shared/a.txt",
            }
        )

        entry = self.state.get_entry("/shared/a.txt")

        self.assertEqual(entry["remote_shareid"], {"share-zone": "abc"})

    def test_existing_state_db_migrates_durable_queue_schema_and_drops_pending_ops(self):
        legacy_db = os.path.join(self.root, "legacy.sqlite3")
        conn = sqlite3.connect(legacy_db)
        conn.execute(
            """
            CREATE TABLE entries (
                path TEXT PRIMARY KEY,
                type TEXT NOT NULL,
                parent_path TEXT NOT NULL,
                remote_drivewsid TEXT,
                remote_docwsid TEXT,
                remote_etag TEXT,
                remote_zone TEXT,
                size INTEGER NOT NULL DEFAULT 0,
                mtime INTEGER NOT NULL DEFAULT 0,
                hydrated INTEGER NOT NULL DEFAULT 0,
                dirty INTEGER NOT NULL DEFAULT 0,
                tombstone INTEGER NOT NULL DEFAULT 0,
                local_sha256 TEXT,
                last_synced_at INTEGER,
                synced_path TEXT
            )
            """
        )
        conn.execute(
            """
            INSERT INTO entries (path, type, parent_path)
            VALUES ('/legacy.txt', 'file', '/')
            """
        )
        conn.execute(
            """
            CREATE INDEX idx_entries_dirty
                ON entries(dirty, tombstone)
            """
        )
        conn.execute(
            """
            CREATE TABLE pending_ops (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                op TEXT NOT NULL,
                path TEXT NOT NULL,
                target_path TEXT,
                queued_at INTEGER NOT NULL,
                retry_count INTEGER NOT NULL DEFAULT 0,
                last_error TEXT
            )
            """
        )
        conn.execute(
            """
            INSERT INTO pending_ops (op, path, queued_at)
            VALUES ('update', '/legacy.txt', 1)
            """
        )
        conn.commit()
        conn.close()

        migrated = SyncState(legacy_db)
        columns = migrated.conn.execute("PRAGMA table_info(entries)").fetchall()
        column_names = {column["name"] for column in columns}
        entry = migrated.get_entry("/legacy.txt")
        index_columns = [
            column["name"]
            for column in migrated.conn.execute(
                "PRAGMA index_info('idx_entries_dirty')"
            ).fetchall()
        ]

        self.assertIn("remote_shareid", column_names)
        self.assertTrue(
            {
                "sync_attempt_count",
                "sync_next_attempt_at",
                "sync_last_error",
                "hydrate_attempt_count",
                "hydrate_next_attempt_at",
                "hydrate_last_error",
                "failed",
            }
            <= column_names
        )
        self.assertEqual(entry["sync_attempt_count"], 0)
        self.assertIsNone(entry["sync_next_attempt_at"])
        self.assertIsNone(entry["sync_last_error"])
        self.assertEqual(entry["hydrate_attempt_count"], 0)
        self.assertIsNone(entry["hydrate_next_attempt_at"])
        self.assertIsNone(entry["hydrate_last_error"])
        self.assertEqual(entry["failed"], 0)
        self.assertIsNone(
            migrated.conn.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'pending_ops'"
            ).fetchone()
        )
        self.assertEqual(
            index_columns,
            ["dirty", "tombstone", "failed", "sync_next_attempt_at"],
        )

    def test_reopening_state_does_not_rebuild_current_dirty_index(self):
        db_path = os.path.join(self.root, "reopen.sqlite3")
        initial = SyncState(db_path)
        initial.close()

        connection = sqlite3.connect(db_path)
        authorizer_actions = []
        connection.set_authorizer(
            lambda action, arg1, arg2, database, trigger: (
                authorizer_actions.append((action, arg1)) or sqlite3.SQLITE_OK
            )
        )
        with patch("driver.sqlite3.connect", return_value=connection):
            reopened = SyncState(db_path)
        reopened.close()

        self.assertNotIn(
            (sqlite3.SQLITE_DROP_INDEX, "idx_entries_dirty"),
            authorizer_actions,
        )

    def test_state_warns_when_wal_is_unavailable(self):
        db_path = os.path.join(self.root, "non-wal.sqlite3")
        connection = sqlite3.connect(db_path)
        connection.row_factory = sqlite3.Row
        execute = connection.execute

        def execute_without_wal(sql, *args):
            if sql == "PRAGMA journal_mode=WAL":
                return Mock(fetchone=Mock(return_value=("delete",)))
            return execute(sql, *args)

        wrapped_connection = Mock(wraps=connection)
        wrapped_connection.execute.side_effect = execute_without_wal
        with patch("driver.sqlite3.connect", return_value=wrapped_connection):
            with self.assertLogs("driver", level="WARNING") as logs:
                state = SyncState(db_path)
        state.close()

        warning = "\n".join(logs.output)
        self.assertIn(db_path, warning)
        self.assertIn("delete", warning)

    def test_state_uses_wal_and_supports_concurrent_read_only_access(self):
        self.state.upsert_entry(
            {
                "path": "/visible.txt",
                "type": "file",
                "parent_path": "/",
                "hydrated": True,
                "dirty": True,
                "tombstone": False,
                "synced_path": "/visible.txt",
            }
        )

        journal_mode = self.state.conn.execute("PRAGMA journal_mode").fetchone()[0]
        read_only = open_read_only(self.state.db_path)
        try:
            visible = read_only.execute(
                "SELECT path FROM entries WHERE path = '/visible.txt'"
            ).fetchone()
        finally:
            read_only.close()

        self.assertEqual(journal_mode, "wal")
        self.assertEqual(visible["path"], "/visible.txt")

    def test_state_sets_generous_busy_timeout(self):
        timeout = self.state.conn.execute("PRAGMA busy_timeout").fetchone()[0]

        self.assertEqual(timeout, 30000)


class SyncFailureClassificationTests(unittest.TestCase):
    def test_auth_exceptions_classify_as_auth(self):
        response = Mock()
        exceptions = (
            PyiCloud2FARequiredException("user@example.com", response),
            PyiCloud2SARequiredException("user@example.com"),
            PyiCloudAuthRequiredException("user@example.com", response),
            PyiCloudFailedLoginException("bad session"),
        )

        for exc in exceptions:
            with self.subTest(exc=type(exc).__name__):
                self.assertEqual(
                    classify_sync_failure(exc, "download"),
                    SYNC_FAILURE_AUTH,
                )

    def test_not_found_delete_classifies_as_terminal(self):
        exc = PyiCloudAPIResponseException("not found", 404)

        self.assertEqual(
            classify_sync_failure(exc, "delete"),
            SYNC_FAILURE_TERMINAL,
        )

    def test_not_found_upload_classifies_as_transient(self):
        exc = PyiCloudAPIResponseException("not found", 404)

        self.assertEqual(
            classify_sync_failure(exc, "upload"),
            SYNC_FAILURE_TRANSIENT,
        )

    def test_forbidden_classifies_as_transient(self):
        # pyicloud only raises a typed auth exception for HTTP 450, so an
        # expired session can surface as a bare 403. Retry rather than
        # quarantine; a genuine denial still quarantines once retries run out.
        response = Mock(status_code=403, text="")
        exc = PyiCloudAPIResponseException("forbidden", response=response)

        self.assertEqual(
            classify_sync_failure(exc, "download"),
            SYNC_FAILURE_TRANSIENT,
        )

    def test_generic_500_auth_message_classifies_as_transient(self):
        exc = PyiCloudAPIResponseException(
            "Authentication required for Account.",
            500,
        )

        self.assertEqual(
            classify_sync_failure(exc, "download"),
            SYNC_FAILURE_TRANSIENT,
        )

    def test_rate_limit_classifies_as_transient(self):
        exc = PyiCloudAPIResponseException("rate limited", 429)

        self.assertEqual(
            classify_sync_failure(exc, "upload"),
            SYNC_FAILURE_TRANSIENT,
        )

    def test_unrecognized_and_malformed_exceptions_classify_as_transient(self):
        malformed_response = RuntimeError("bad response")
        malformed_response.response = object()
        exceptions = (
            RuntimeError("network failure"),
            socket.timeout("timed out"),
            Timeout("request timed out"),
            ConnectionError("connection dropped"),
            malformed_response,
            PyiCloudAPIResponseException("forbidden", "403"),
        )

        for exc in exceptions:
            with self.subTest(exc=type(exc).__name__):
                self.assertEqual(
                    classify_sync_failure(exc, "download"),
                    SYNC_FAILURE_TRANSIENT,
                )


class PyiCloudSessionTimeoutTests(unittest.TestCase):
    def test_session_hook_uses_defaults_without_overriding_explicit_timeouts(self):
        api = type("Api", (), {})()
        session = type("Session", (), {})()
        original_request = Mock()
        session.request = original_request
        api.session = session
        logger = Mock()

        self.assertTrue(install_pyi_cloud_session_timeouts(api, logger))

        session.request("GET", "https://example.invalid/control")
        self.assertEqual(
            original_request.call_args.kwargs["timeout"],
            CONTROL_PLANE_TIMEOUT,
        )

        explicit_timeout = (1, 2)
        session.request(
            "GET",
            "https://example.invalid/explicit",
            timeout=explicit_timeout,
        )
        self.assertEqual(
            original_request.call_args.kwargs["timeout"],
            explicit_timeout,
        )

        session.request("GET", "https://example.invalid/download", stream=True)
        self.assertEqual(
            original_request.call_args.kwargs["timeout"],
            DOWNLOAD_TIMEOUT,
        )
        self.assertEqual(
            classify_sync_failure(Timeout("request timed out"), "download"),
            SYNC_FAILURE_TRANSIENT,
        )

    def test_wraps_api_session_before_accessing_drive(self):
        class Session:
            def __init__(self):
                self.request = Mock()

        class Api:
            def __init__(self):
                self.session = Session()
                self.original_request = self.session.request
                self.drive_session_was_wrapped = None
                self._drive = type("Drive", (), {"session": self.session})()

            @property
            def drive(self):
                self.drive_session_was_wrapped = (
                    self.session.request is not self.original_request
                )
                return self._drive

        api = Api()

        self.assertTrue(install_pyi_cloud_session_timeouts(api, Mock()))
        self.assertTrue(api.drive_session_was_wrapped)


class SyncEngineStartupTests(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.mkdtemp(prefix="icloud-linux-test-")
        self.mirror = LocalMirror(self.root)
        self.state = SyncState(os.path.join(self.root, "state.sqlite3"))
        self.logger = Mock()
        api = Mock()
        api.drive.root = Mock()
        self.engine = ICloudSyncEngine(api, self.mirror, self.state, self.logger)
        self.engine._start_background_threads = Mock()
        self.engine._schedule_all_unhydrated = Mock()
        self.engine.initial_scan = Mock()
        self.engine._reconcile_persistent_cache = Mock()

    def tearDown(self):
        self.engine.shutdown()
        self.state.close()
        shutil.rmtree(self.root)

    def _add_unhydrated_remote_file(self, path, size):
        self.state.upsert_entry(
            {
                "path": path,
                "type": "file",
                "parent_path": os.path.dirname(path),
                "remote_drivewsid": "file-1",
                "remote_docwsid": "doc-1",
                "remote_zone": "zone-1",
                "size": size,
                "mtime": 123,
                "hydrated": False,
                "dirty": False,
                "tombstone": False,
                "synced_path": path,
            }
        )
        self.mirror.ensure_dir(os.path.dirname(path))

    def _response(self, body, headers=None):
        response = Response()
        response.status_code = 200
        response.headers.update(headers or {})
        response.raw = io.BytesIO(body)
        return response

    def test_start_uses_persistent_cache_without_initial_scan(self):
        self.state.upsert_entry(
            {
                "path": "/docs",
                "type": "folder",
                "parent_path": "/",
                "hydrated": True,
                "dirty": False,
                "tombstone": False,
                "synced_path": "/docs",
            }
        )

        self.engine.start()

        self.engine.initial_scan.assert_not_called()
        self.engine._reconcile_persistent_cache.assert_called_once()
        self.engine._schedule_all_unhydrated.assert_called_once()
        self.engine._start_background_threads.assert_called_once()

    def test_start_performs_initial_scan_on_first_run(self):
        self.engine.start()

        self.engine.initial_scan.assert_called_once()
        self.engine._reconcile_persistent_cache.assert_not_called()
        self.engine._schedule_all_unhydrated.assert_called_once()
        self.engine._start_background_threads.assert_called_once()

    def test_timed_out_download_is_retried_with_backoff(self):
        self.engine.ensure_local_file = Mock(
            side_effect=Timeout("request timed out")
        )
        self.engine._schedule_download_with_delay = Mock()
        self.engine.scheduled_downloads.add("/docs/a.txt")

        self.engine._download_job("/docs/a.txt")

        self.engine._schedule_download_with_delay.assert_called_once()
        args = self.engine._schedule_download_with_delay.call_args[0]
        self.assertEqual(args[0], "/docs/a.txt")
        self.assertGreater(args[1], 0)

    def test_auth_failure_is_not_retried(self):
        self.engine.ensure_local_file = Mock(
            side_effect=PyiCloudFailedLoginException("bad session")
        )
        self.engine._schedule_download_with_delay = Mock()
        self.engine.scheduled_downloads.add("/docs/a.txt")

        self.engine._download_job("/docs/a.txt")

        self.engine._schedule_download_with_delay.assert_not_called()

    def test_generic_500_auth_message_is_still_retried(self):
        self.engine.ensure_local_file = Mock(
            side_effect=PyiCloudAPIResponseException(
                "Authentication required for Account.",
                500,
            )
        )
        self.engine._schedule_download_with_delay = Mock()
        self.engine.scheduled_downloads.add("/docs/a.txt")

        self.engine._download_job("/docs/a.txt")

        self.engine._schedule_download_with_delay.assert_called_once()

    def test_schedule_download_ignores_executor_shutdown_race(self):
        self.engine.executor.submit = Mock(side_effect=RuntimeError("cannot schedule new futures after interpreter shutdown"))

        self.engine._schedule_download_with_delay("/docs/a.txt", 0)

        self.assertNotIn("/docs/a.txt", self.engine.scheduled_downloads)

    def test_request_remote_refresh_sets_wakeup_event(self):
        self.assertFalse(self.engine.refresh_now_event.is_set())

        self.engine.request_remote_refresh()

        self.assertTrue(self.engine.refresh_now_event.is_set())

    def test_node_from_entry_reuses_persisted_file_metadata(self):
        shareid = {"share-zone": "abc"}
        node = self.engine._node_from_entry(
            {
                "path": "/docs/a.txt",
                "type": "file",
                "remote_drivewsid": "file-1",
                "remote_docwsid": "doc-1",
                "remote_etag": "etag-1",
                "remote_zone": "zone-1",
                "remote_shareid": shareid,
                "size": 5,
            }
        )

        self.engine.api.drive.get_node_data.assert_not_called()
        self.assertEqual(node.data["docwsid"], "doc-1")
        self.assertEqual(node.data["shareID"], shareid)
        self.assertEqual(node.data["size"], 5)

    def test_crawl_descends_into_app_library_nodes(self):
        note = Mock()
        note.name = "vault.md"
        note.data = {
            "type": "FILE",
            "drivewsid": "file-1",
            "docwsid": "doc-1",
            "etag": "etag-1",
            "zone": "zone-1",
            "size": 12,
            "dateModified": "2026-04-06T00:00:00Z",
        }
        obsidian = Mock()
        obsidian.name = "Obsidian"
        obsidian.data = {
            "type": "APP_LIBRARY",
            "drivewsid": "folder-1",
            "docwsid": "documents",
            "etag": "etag-folder",
            "zone": "zone-1",
            "dateModified": "2026-04-06T00:00:00Z",
        }
        obsidian.get_children.return_value = [note]
        root = Mock()
        root.get_children.return_value = [obsidian]
        self.engine.api.drive.root = root

        snapshot = self.engine._crawl_remote_snapshot()

        self.assertIn("folder-1", snapshot)
        self.assertIn("file-1", snapshot)
        self.assertEqual(snapshot["folder-1"]["path"], "/Obsidian")
        self.assertEqual(snapshot["folder-1"]["type"], "app_library")
        self.assertEqual(snapshot["file-1"]["path"], "/Obsidian/vault.md")

    def test_crawl_uses_visibility_policy_for_sync_path_ancestors_and_descendants(self):
        work_notes = FakeCrawlNode("Work Notes", "work-notes")
        personal = FakeCrawlNode("Mikael Personal", "personal")
        obsidian = FakeCrawlNode(
            "Obsidian",
            "obsidian",
            children=[work_notes, personal],
        )
        documents = FakeCrawlNode("Documents", "documents")
        self.engine.api.drive.root = FakeCrawlNode(
            "root",
            "root",
            children=[obsidian, documents],
        )
        self.engine.sync_paths = ["/Obsidian/Work Notes"]

        with patch("driver.path_visible", wraps=driver.path_visible) as visible:
            self.engine._crawl_remote_snapshot()

        visible.assert_any_call(
            "/Documents",
            ["/Obsidian/Work Notes"],
            [],
        )
        self.assertEqual(obsidian.get_children_calls, 1)
        self.assertEqual(work_notes.get_children_calls, 1)
        self.assertEqual(personal.get_children_calls, 0)
        self.assertEqual(documents.get_children_calls, 0)

    def test_incomplete_crawl_does_not_prune_clean_cached_entries(self):
        self.state.upsert_entry(
            {
                "path": "/still-remote.txt",
                "type": "file",
                "parent_path": "/",
                "remote_drivewsid": "file-still-remote",
                "size": 5,
                "mtime": 123,
                "hydrated": True,
                "dirty": False,
                "tombstone": False,
                "synced_path": "/still-remote.txt",
            }
        )
        self.mirror.write("/still-remote.txt", b"keep!", 0)
        future = Mock()
        future.result.side_effect = TimeoutError()
        crawl_executor = Mock()
        crawl_executor.submit.return_value = future

        with patch("driver.ThreadPoolExecutor", return_value=crawl_executor):
            snapshot = self.engine._crawl_remote_snapshot()

        self.engine._apply_remote_snapshot(snapshot)

        self.assertFalse(snapshot.complete)
        self.assertEqual(snapshot.failed_folders, ["/"])
        self.assertIsNotNone(self.state.get_entry("/still-remote.txt"))
        self.assertTrue(self.mirror.exists("/still-remote.txt"))

    def test_complete_crawl_prunes_clean_entries_deleted_remotely(self):
        self.state.upsert_entry(
            {
                "path": "/gone.txt",
                "type": "file",
                "parent_path": "/",
                "remote_drivewsid": "file-gone",
                "size": 4,
                "mtime": 123,
                "hydrated": True,
                "dirty": False,
                "tombstone": False,
                "synced_path": "/gone.txt",
            }
        )
        self.mirror.write("/gone.txt", b"gone", 0)
        root = Mock()
        root.data = {}
        root.get_children.return_value = []
        self.engine.api.drive.root = root

        snapshot = self.engine._crawl_remote_snapshot()
        self.engine._apply_remote_snapshot(snapshot)

        self.assertTrue(snapshot.complete)
        self.assertIsNone(self.state.get_entry("/gone.txt"))
        self.assertFalse(self.mirror.exists("/gone.txt"))

    def test_crawl_executor_is_shutdown_with_engine(self):
        future = Mock()
        future.result.return_value = []
        crawl_executor = Mock()
        crawl_executor.submit.return_value = future
        root = Mock()
        root.data = {}
        self.engine.api.drive.root = root

        with patch("driver.ThreadPoolExecutor", return_value=crawl_executor):
            self.engine._crawl_remote_snapshot()

        crawl_executor.shutdown.assert_not_called()
        self.engine.shutdown()
        crawl_executor.shutdown.assert_called_once_with(wait=False, cancel_futures=True)

    def test_materialize_remote_entry_treats_app_library_as_directory(self):
        self.engine._materialize_remote_entry(
            {
                "path": "/Obsidian",
                "type": "app_library",
                "parent_path": "/",
                "remote_drivewsid": "folder-1",
                "remote_docwsid": "documents",
                "remote_etag": "etag-folder",
                "remote_zone": "zone-1",
                "size": 0,
                "mtime": 123,
            }
        )

        self.assertTrue(self.mirror.is_dir("/Obsidian"))
        entry = self.state.get_entry("/Obsidian")
        self.assertEqual(entry["hydrated"], 1)

    def test_ensure_local_file_streams_remote_content_in_chunks(self):
        self.state.upsert_entry(
            {
                "path": "/docs/a.txt",
                "type": "file",
                "parent_path": "/docs",
                "remote_drivewsid": "file-1",
                "remote_docwsid": "doc-1",
                "remote_zone": "zone-1",
                "size": 16,
                "mtime": 123,
                "hydrated": False,
                "dirty": False,
                "tombstone": False,
                "synced_path": "/docs/a.txt",
            }
        )
        self.mirror.ensure_dir("/docs")
        response = self._response(b"chunked download", {"Content-Length": "16"})
        response.raw = NoUnboundedReadStream(b"chunked download")
        node = Mock()
        node.open.return_value = response
        self.engine._node_from_entry = Mock(return_value=node)

        self.engine.ensure_local_file("/docs/a.txt")

        self.assertEqual(self.mirror.read("/docs/a.txt", 100, 0), b"chunked download")

    def test_ensure_local_file_replays_consumed_json_response_content(self):
        path = "/docs/settings.json"
        content = b'{"setting": true}'
        self._add_unhydrated_remote_file(path, len(content))
        response = self._response(content, {"Content-Length": str(len(content))})
        self.assertEqual(response.content, content)
        node = Mock()
        node.open.return_value = response
        self.engine._node_from_entry = Mock(return_value=node)

        self.engine.ensure_local_file(path)

        self.assertEqual(self.mirror.read(path, 100, 0), content)
        self.assertEqual(self.state.get_entry(path)["hydrated"], 1)

    def test_ensure_local_file_rejects_zero_byte_download_for_nonempty_entry(self):
        path = "/docs/missing-content.json"
        self._add_unhydrated_remote_file(path, 10)
        response = self._response(b"")
        node = Mock()
        node.open.return_value = response
        self.engine._node_from_entry = Mock(return_value=node)

        with self.assertRaises(Exception) as caught:
            self.engine.ensure_local_file(path)

        self.assertEqual(type(caught.exception).__name__, "HydrationTruncated")
        self.assertEqual(self.state.get_entry(path)["hydrated"], 0)
        self.assertFalse(self.mirror.exists(path))

    def test_ensure_local_file_allows_decoded_gzip_size_to_differ_from_header(self):
        path = "/docs/compressed.json"
        content = b'{"decoded": "content"}'
        compressed = gzip.compress(content)
        self._add_unhydrated_remote_file(path, len(content))
        response = self._response(
            b"",
            {
                "Content-Encoding": "gzip",
                "Content-Length": str(len(compressed)),
            },
        )
        response.raw = HTTPResponse(
            body=io.BytesIO(compressed),
            headers=response.headers,
            preload_content=False,
            decode_content=False,
        )
        node = Mock()
        node.open.return_value = response
        self.engine._node_from_entry = Mock(return_value=node)

        self.engine.ensure_local_file(path)

        self.assertEqual(self.mirror.read(path, 100, 0), content)
        self.assertEqual(self.state.get_entry(path)["hydrated"], 1)

    def test_ensure_local_file_allows_legitimately_empty_remote_file(self):
        path = "/docs/empty.json"
        self._add_unhydrated_remote_file(path, 0)
        response = self._response(b"", {"Content-Length": "0"})
        node = Mock()
        node.open.return_value = response
        self.engine._node_from_entry = Mock(return_value=node)

        self.engine.ensure_local_file(path)

        self.assertEqual(self.mirror.read(path, 100, 0), b"")
        self.assertEqual(self.state.get_entry(path)["hydrated"], 1)

    def test_sync_file_uploads_stream_without_buffering_entire_file(self):
        self.mirror.create_file("/docs/a.txt")
        self.mirror.write("/docs/a.txt", b"hello world", 0)
        self.state.upsert_entry(
            {
                "path": "/docs/a.txt",
                "type": "file",
                "parent_path": "/docs",
                "remote_drivewsid": None,
                "hydrated": True,
                "dirty": True,
                "tombstone": False,
                "synced_path": "/docs/a.txt",
            }
        )
        upload_state = {}

        def capture_upload(stream):
            upload_state["class_name"] = stream.__class__.__name__
            upload_state["name"] = stream.name
            upload_state["prefix"] = stream.read(5)

        parent_node = Mock()
        parent_node.data = {}
        parent_node.upload.side_effect = capture_upload
        self.engine._ensure_remote_parent = Mock(return_value=parent_node)
        self.engine.ensure_local_file = Mock()
        self.engine._refresh_child_meta = Mock(
            return_value={
                "path": "/docs/a.txt",
                "type": "file",
                "parent_path": "/docs",
                "remote_drivewsid": "file-1",
                "remote_docwsid": "doc-1",
                "remote_etag": "etag-1",
                "remote_zone": "zone-1",
                "size": 11,
                "mtime": 123,
            }
        )

        self.engine._sync_file(self.state.get_entry("/docs/a.txt"))

        self.assertEqual(upload_state["class_name"], "NamedFileStream")
        self.assertEqual(upload_state["name"], "a.txt")
        self.assertEqual(upload_state["prefix"], b"hello")


class CrawlExecutorTests(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.mkdtemp(prefix="icloud-linux-test-")
        self.mirror = LocalMirror(self.root)
        self.state = SyncState(os.path.join(self.root, "state.sqlite3"))
        self.logger = Mock()
        self.api = Mock()
        self.api.drive = Mock()
        self.engine = ICloudSyncEngine(self.api, self.mirror, self.state, self.logger)
        self.block_events = []

    def tearDown(self):
        for event in self.block_events:
            event.set()
        self.engine.shutdown()
        self.state.close()
        shutil.rmtree(self.root)

    def test_wedged_folder_stops_crawl_before_later_folders(self):
        release_wedged_folder = threading.Event()
        self.block_events.append(release_wedged_folder)
        wedged_folder = FakeCrawlNode(
            "wedged",
            "wedged-folder",
            block_event=release_wedged_folder,
        )
        later_folder = FakeCrawlNode("later", "later-folder")
        self.api.drive.root = FakeCrawlNode(
            "root",
            "root",
            children=[wedged_folder, later_folder],
        )

        with patch("driver.CRAWL_FOLDER_TIMEOUT", 0.01):
            snapshot = self.engine._crawl_remote_snapshot()

        self.assertTrue(wedged_folder.started.wait(timeout=1))
        self.assertFalse(snapshot.complete)
        self.assertEqual(snapshot.failed_folders, ["/wedged"])
        self.assertEqual(later_folder.get_children_calls, 0)

    def test_folder_exception_continues_with_durable_executor(self):
        failed_folder = FakeCrawlNode(
            "failed",
            "failed-folder",
            error=RuntimeError("remote metadata error"),
        )
        later_folder = FakeCrawlNode("later", "later-folder")
        self.api.drive.root = FakeCrawlNode(
            "root",
            "root",
            children=[failed_folder, later_folder],
        )

        snapshot = self.engine._crawl_remote_snapshot()

        self.assertFalse(snapshot.complete)
        self.assertEqual(snapshot.failed_folders, ["/failed"])
        self.assertEqual(later_folder.get_children_calls, 1)
        self.assertIsNotNone(self.engine._crawl_executor)

    def test_reuses_healthy_executor_and_replaces_suspect_executor(self):
        executors = [ImmediateCrawlExecutor() for _ in range(4)]
        executor_factory = Mock(side_effect=executors)
        healthy_root = FakeCrawlNode("root", "root")
        release_wedged_folder = threading.Event()
        self.block_events.append(release_wedged_folder)
        wedged_root = FakeCrawlNode(
            "root",
            "root",
            block_event=release_wedged_folder,
        )

        with patch("driver.ThreadPoolExecutor", executor_factory):
            self.api.drive.root = healthy_root
            self.engine._crawl_remote_snapshot()
            healthy_executor = self.engine._crawl_executor

            self.engine._crawl_remote_snapshot()
            self.assertIs(self.engine._crawl_executor, healthy_executor)
            self.assertEqual(executor_factory.call_count, 1)

            self.api.drive.root = wedged_root
            snapshot = self.engine._crawl_remote_snapshot()
            self.assertFalse(snapshot.complete)

            self.api.drive.root = healthy_root
            self.engine._crawl_remote_snapshot()
            self.assertIsNot(self.engine._crawl_executor, healthy_executor)
            self.assertEqual(executor_factory.call_count, 2)


class DurableSyncQueueTests(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.mkdtemp(prefix="icloud-linux-test-")
        self.mirror = LocalMirror(self.root)
        self.state = SyncState(os.path.join(self.root, "state.sqlite3"))
        self.logger = Mock()
        self.api = Mock()
        self.api.drive.root = Mock()
        self.engine = ICloudSyncEngine(
            self.api,
            self.mirror,
            self.state,
            self.logger,
        )

    def tearDown(self):
        self.engine.shutdown()
        self.state.close()
        shutil.rmtree(self.root)

    def _add_entry(self, path, entry_type="file", remote_drivewsid=None):
        self.state.upsert_entry(
            {
                "path": path,
                "type": entry_type,
                "parent_path": os.path.dirname(path) or "/",
                "remote_drivewsid": remote_drivewsid,
                "hydrated": True,
                "dirty": True,
                "tombstone": False,
                "synced_path": path,
            }
        )

    def test_transient_failure_defers_dirty_entry(self):
        self._add_entry("/queued.txt")

        attempt, quarantined = self.state.record_sync_failure(
            "/queued.txt",
            RuntimeError("temporary outage"),
            SYNC_FAILURE_TRANSIENT,
            MAX_SYNC_ATTEMPTS,
        )

        entry = self.state.get_entry("/queued.txt")
        self.assertEqual(attempt, 1)
        self.assertFalse(quarantined)
        self.assertEqual(entry["sync_attempt_count"], 1)
        self.assertGreater(entry["sync_next_attempt_at"], int(time.time()))
        self.assertEqual(self.state.list_dirty_entries(), [])
        self.assertEqual(
            [item["path"] for item in self.state.list_dirty_entries(include_deferred=True)],
            ["/queued.txt"],
        )

    def test_request_timeout_defers_entry_without_quarantining(self):
        self._add_entry("/queued.txt")
        self.mirror.write("/queued.txt", b"content", 0)
        parent = Mock()
        parent.data = {}
        self.engine._ensure_remote_parent = Mock(return_value=parent)
        self.engine.ensure_local_file = Mock(
            side_effect=Timeout("request timed out")
        )

        self.engine._sync_file(self.state.get_entry("/queued.txt"))

        entry = self.state.get_entry("/queued.txt")
        self.assertEqual(entry["sync_attempt_count"], 1)
        self.assertEqual(entry["failed"], 0)
        self.assertGreater(entry["sync_next_attempt_at"], int(time.time()))

    def test_retry_budget_quarantines_entry_after_eight_failures(self):
        self._add_entry("/queued.txt")

        for _ in range(MAX_SYNC_ATTEMPTS):
            attempt, quarantined = self.state.record_sync_failure(
                "/queued.txt",
                RuntimeError("temporary outage"),
                SYNC_FAILURE_TRANSIENT,
                MAX_SYNC_ATTEMPTS,
            )

        entry = self.state.get_entry("/queued.txt")
        self.assertEqual(attempt, MAX_SYNC_ATTEMPTS)
        self.assertTrue(quarantined)
        self.assertEqual(entry["failed"], 1)

    def test_quarantined_entry_is_not_retried(self):
        self._add_entry("/queued.txt")
        for _ in range(MAX_SYNC_ATTEMPTS):
            self.state.record_sync_failure(
                "/queued.txt",
                RuntimeError("temporary outage"),
                SYNC_FAILURE_TRANSIENT,
                MAX_SYNC_ATTEMPTS,
            )
        self.engine._sync_file = Mock()

        self.engine.sync_dirty_entries()

        self.engine._sync_file.assert_not_called()
        self.assertEqual(self.state.list_dirty_entries(), [])

    def test_explicit_sync_ignores_backoff_and_counts_quarantined_entries(self):
        self._add_entry("/deferred.txt")
        self._add_entry("/quarantined.txt")
        self.state.record_sync_failure(
            "/deferred.txt",
            RuntimeError("temporary outage"),
            SYNC_FAILURE_TRANSIENT,
            MAX_SYNC_ATTEMPTS,
        )
        for _ in range(MAX_SYNC_ATTEMPTS):
            self.state.record_sync_failure(
                "/quarantined.txt",
                RuntimeError("permanent outage"),
                SYNC_FAILURE_TRANSIENT,
                MAX_SYNC_ATTEMPTS,
            )
        self.engine._sync_file = Mock()

        skipped = self.engine.sync_dirty_entries(include_deferred=True)

        self.engine._sync_file.assert_called_once()
        self.assertEqual(
            self.engine._sync_file.call_args[0][0]["path"],
            "/deferred.txt",
        )
        self.assertEqual(skipped, 1)

    def test_auth_failure_does_not_consume_retry_budget(self):
        self.mirror.write("/queued.txt", b"content", 0)
        self._add_entry("/queued.txt")
        parent = Mock()
        parent.data = {}
        self.engine._ensure_remote_parent = Mock(return_value=parent)
        self.engine.ensure_local_file = Mock(
            side_effect=PyiCloudFailedLoginException("expired session")
        )

        self.engine._sync_file(self.state.get_entry("/queued.txt"))

        entry = self.state.get_entry("/queued.txt")
        self.assertEqual(entry["sync_attempt_count"], 0)
        self.assertIsNotNone(entry["sync_next_attempt_at"])
        self.assertEqual(entry["sync_last_error"], "expired session")

    def test_preupload_delete_not_found_uploads_replacement(self):
        self.mirror.write("/replaced.txt", b"replacement", 0)
        self._add_entry("/replaced.txt", remote_drivewsid="file-1")
        parent = Mock()
        parent.data = {}
        self.engine._ensure_remote_parent = Mock(return_value=parent)
        self.engine.ensure_local_file = Mock()
        self.engine._node_from_entry = Mock()
        self.engine._delete_remote_node = Mock(
            side_effect=PyiCloudAPIResponseException("not found", 404)
        )
        self.engine._reconcile_child_meta = Mock(return_value={})

        self.engine._sync_file(self.state.get_entry("/replaced.txt"))

        self.engine._delete_remote_node.assert_called_once()
        parent.upload.assert_called_once()
        self.assertFalse(self.state.get_entry("/replaced.txt")["dirty"])
        self.logger.info.assert_called_once_with(
            "Remote path %s was already deleted; uploading replacement",
            "/replaced.txt",
        )

    def test_preupload_delete_failures_are_durable_and_skip_upload(self):
        failures = (
            ("timeout", Timeout("request timed out")),
            (
                "server error",
                PyiCloudAPIResponseException("server error", 500),
            ),
        )

        for name, failure in failures:
            with self.subTest(name=name):
                path = f"/{name}.txt"
                self.mirror.write(path, b"content", 0)
                self._add_entry(path, remote_drivewsid=f"file-{name}")
                parent = Mock()
                parent.data = {}
                self.engine._ensure_remote_parent = Mock(return_value=parent)
                self.engine.ensure_local_file = Mock()
                self.engine._node_from_entry = Mock()
                self.engine._delete_remote_node = Mock(side_effect=failure)

                self.engine._sync_file(self.state.get_entry(path))

                entry = self.state.get_entry(path)
                parent.upload.assert_not_called()
                self.assertEqual(entry["dirty"], 1)
                self.assertEqual(entry["sync_attempt_count"], 1)
                self.assertIn(
                    path,
                    [
                        item["path"]
                        for item in self.state.list_dirty_entries(include_deferred=True)
                    ],
                )

    def test_preupload_delete_auth_failure_aborts_sync_pass_without_retry(self):
        for path in ("/first.txt", "/second.txt"):
            self.mirror.write(path, b"content", 0)
            self._add_entry(path, remote_drivewsid=f"file-{path}")
        parent = Mock()
        parent.data = {}
        self.engine._ensure_remote_parent = Mock(return_value=parent)
        self.engine.ensure_local_file = Mock()
        self.engine._node_from_entry = Mock()
        self.engine._delete_remote_node = Mock(
            side_effect=PyiCloudFailedLoginException("expired session")
        )

        self.engine.sync_dirty_entries()

        self.engine._delete_remote_node.assert_called_once()
        parent.upload.assert_not_called()
        first = self.state.get_entry("/first.txt")
        second = self.state.get_entry("/second.txt")
        self.assertEqual(first["sync_attempt_count"], 0)
        self.assertIsNotNone(first["sync_next_attempt_at"])
        self.assertEqual(second["sync_attempt_count"], 0)
        self.assertTrue(second["dirty"])

    def test_restart_preserves_auth_sync_deferral(self):
        self.mirror.write("/queued.txt", b"content", 0)
        self._add_entry("/queued.txt")
        parent = Mock()
        parent.data = {}
        self.engine._ensure_remote_parent = Mock(return_value=parent)
        self.engine.ensure_local_file = Mock(
            side_effect=PyiCloudFailedLoginException("expired session")
        )

        self.engine._sync_file(self.state.get_entry("/queued.txt"))
        original_deadline = self.state.get_entry("/queued.txt")[
            "sync_next_attempt_at"
        ]
        self.state.close()
        self.state = SyncState(os.path.join(self.root, "state.sqlite3"))

        entry = self.state.get_entry("/queued.txt")
        self.assertEqual(entry["sync_attempt_count"], 0)
        self.assertEqual(entry["sync_last_error"], "expired session")
        self.assertEqual(entry["sync_next_attempt_at"], original_deadline)

    def test_delete_not_found_resolves_tombstone_without_quarantine(self):
        self._add_entry("/removed", entry_type="folder", remote_drivewsid="folder-1")
        self.state.mark_tombstone("/removed")
        self._add_entry("/removed/child.txt", remote_drivewsid="file-1")
        self.state.mark_tombstone("/removed/child.txt")
        node = Mock()
        node.delete.side_effect = PyiCloudAPIResponseException("not found", 404)
        self.engine._node_from_entry = Mock(return_value=node)

        self.engine._sync_tombstone(self.state.get_entry("/removed"))

        self.assertIsNone(self.state.get_entry("/removed"))
        self.assertIsNone(self.state.get_entry("/removed/child.txt"))

    def test_non_404_terminal_delete_preserves_tombstone_and_quarantines(self):
        self._add_entry("/removed.txt", remote_drivewsid="file-1")
        self.state.mark_tombstone("/removed.txt")
        node = Mock()
        node.delete.side_effect = MissingMirrorFile("mirror file is missing")
        self.engine._node_from_entry = Mock(return_value=node)

        self.engine._sync_tombstone(self.state.get_entry("/removed.txt"))

        entry = self.state.get_entry("/removed.txt")
        node.delete.assert_called_once_with()
        self.assertIsNotNone(entry)
        self.assertEqual(entry["tombstone"], 1)
        self.assertEqual(entry["failed"], 1)
        self.assertIn("mirror file is missing", entry["sync_last_error"])

    def test_success_clears_retry_state(self):
        self._add_entry("/queued.txt")
        self.state.record_sync_failure(
            "/queued.txt",
            RuntimeError("temporary outage"),
            SYNC_FAILURE_TRANSIENT,
            MAX_SYNC_ATTEMPTS,
        )

        self.state.mark_clean("/queued.txt")

        entry = self.state.get_entry("/queued.txt")
        self.assertEqual(entry["sync_attempt_count"], 0)
        self.assertIsNone(entry["sync_last_error"])
        self.assertIsNone(entry["sync_next_attempt_at"])
        self.assertEqual(entry["failed"], 0)

    def test_clear_sync_backoff_preserves_attempts_and_quarantine(self):
        self._add_entry("/queued.txt")
        self.state.record_sync_failure(
            "/queued.txt",
            RuntimeError("temporary outage"),
            SYNC_FAILURE_TRANSIENT,
            MAX_SYNC_ATTEMPTS,
        )
        self.state.conn.execute(
            "UPDATE entries SET failed = 1 WHERE path = ?",
            ("/queued.txt",),
        )
        self.state.conn.commit()

        self.state.clear_sync_backoff()

        entry = self.state.get_entry("/queued.txt")
        self.assertEqual(entry["sync_attempt_count"], 1)
        self.assertEqual(entry["failed"], 1)
        self.assertIsNone(entry["sync_next_attempt_at"])

    def test_clear_failures_subtree_clears_parent_and_children_only(self):
        for path in ("/parent", "/parent/child.txt", "/sibling.txt"):
            self._add_entry(
                path,
                entry_type="folder" if path == "/parent" else "file",
            )
            self.state.record_sync_failure(
                path,
                RuntimeError("sync failed"),
                SYNC_FAILURE_TRANSIENT,
                MAX_SYNC_ATTEMPTS,
            )
            self.state.record_hydrate_failure(
                path,
                RuntimeError("download failed"),
                SYNC_FAILURE_TRANSIENT,
                MAX_SYNC_ATTEMPTS,
            )

        cleared = self.state.clear_failures_subtree("/parent")

        self.assertEqual(cleared, 2)
        for path in ("/parent", "/parent/child.txt"):
            entry = self.state.get_entry(path)
            self.assertEqual(entry["sync_attempt_count"], 0)
            self.assertIsNone(entry["sync_next_attempt_at"])
            self.assertIsNone(entry["sync_last_error"])
            self.assertEqual(entry["failed"], 0)
            self.assertEqual(entry["hydrate_attempt_count"], 0)
            self.assertIsNone(entry["hydrate_next_attempt_at"])
            self.assertIsNone(entry["hydrate_last_error"])
        sibling = self.state.get_entry("/sibling.txt")
        self.assertEqual(sibling["sync_attempt_count"], 1)
        self.assertEqual(sibling["hydrate_attempt_count"], 1)

    def test_missing_remote_parent_does_not_penalize_child(self):
        self._add_entry("/missing/child.txt")

        self.engine._sync_file(self.state.get_entry("/missing/child.txt"))

        entry = self.state.get_entry("/missing/child.txt")
        self.assertEqual(entry["sync_attempt_count"], 0)
        self.assertEqual(entry["failed"], 0)

    def test_missing_mirror_file_is_quarantined_without_remote_delete(self):
        self._add_entry("/missing.txt", remote_drivewsid="file-1")
        parent_node = Mock()
        parent_node.data = {}
        remote_node = Mock()
        self.engine._ensure_remote_parent = Mock(return_value=parent_node)
        self.engine._node_from_entry = Mock(return_value=remote_node)

        self.engine.sync_dirty_entries()
        entry = self.state.get_entry("/missing.txt")
        self.assertEqual(entry["failed"], 1)
        self.assertEqual(entry["tombstone"], 0)
        self.assertIn("mirror file is missing", entry["sync_last_error"].lower())

        self.engine.sync_dirty_entries()

        self.assertEqual(self.state.list_dirty_entries(), [])
        remote_node.delete.assert_not_called()

    def test_child_recovers_after_parent_quarantine_is_cleared(self):
        self.mirror.ensure_dir("/parent")
        self.mirror.write("/parent/child.txt", b"content", 0)
        self._add_entry("/parent", entry_type="folder", remote_drivewsid="parent-1")
        self._add_entry("/parent/child.txt")
        for _ in range(MAX_SYNC_ATTEMPTS):
            self.state.record_sync_failure(
                "/parent",
                RuntimeError("parent failure"),
                SYNC_FAILURE_TRANSIENT,
                MAX_SYNC_ATTEMPTS,
            )

        self.engine.sync_dirty_entries()

        child = self.state.get_entry("/parent/child.txt")
        self.assertEqual(child["sync_attempt_count"], 0)
        self.assertEqual(child["failed"], 0)

        self.state.clear_sync_failure("/parent")
        parent_node = Mock()
        parent_node.data = {}
        self.engine._node_from_entry = Mock(return_value=parent_node)
        self.engine._refresh_child_meta = Mock(
            return_value={
                "path": "/parent/child.txt",
                "type": "file",
                "parent_path": "/parent",
                "remote_drivewsid": "child-1",
                "remote_docwsid": "child-doc-1",
                "remote_etag": "child-etag-1",
                "remote_zone": "zone-1",
                "size": 7,
                "mtime": 123,
            }
        )

        self.engine.sync_dirty_entries()

        child = self.state.get_entry("/parent/child.txt")
        self.assertFalse(child["dirty"])
        self.assertEqual(child["sync_attempt_count"], 0)

    def test_parent_backoff_blocks_child_without_retrying_parent(self):
        self._add_entry("/parent", entry_type="folder", remote_drivewsid="parent-1")
        self._add_entry("/parent/child.txt")
        self.state.record_sync_failure(
            "/parent",
            RuntimeError("parent failure"),
            SYNC_FAILURE_TRANSIENT,
            MAX_SYNC_ATTEMPTS,
        )
        self.engine._sync_directory = Mock()
        self.engine._node_from_entry = Mock()

        self.engine.sync_dirty_entries()

        child = self.state.get_entry("/parent/child.txt")
        self.engine._sync_directory.assert_not_called()
        self.engine._node_from_entry.assert_not_called()
        self.assertEqual(child["sync_attempt_count"], 0)
        self.assertEqual(child["failed"], 0)

    def test_overlapping_sync_passes_are_serialized(self):
        self._add_entry("/queued.txt")
        started = threading.Event()
        release = threading.Event()
        active_lock = threading.Lock()
        active = 0
        max_active = 0
        errors = []

        def sync_file(entry, sync_context=None):
            nonlocal active, max_active
            with active_lock:
                active += 1
                max_active = max(max_active, active)
            started.set()
            release.wait(2)
            with active_lock:
                active -= 1

        def run_sync():
            try:
                self.engine.sync_dirty_entries()
            except Exception as exc:
                errors.append(exc)

        self.engine._sync_file = Mock(side_effect=sync_file)
        first = threading.Thread(target=run_sync)
        second = threading.Thread(target=run_sync)
        first.start()
        self.assertTrue(started.wait(1))
        second.start()
        time.sleep(0.1)
        release.set()
        first.join(2)
        second.join(2)

        self.assertFalse(first.is_alive())
        self.assertFalse(second.is_alive())
        self.assertEqual(errors, [])
        self.assertEqual(max_active, 1)

    def test_auth_failure_aborts_remaining_sync_entries(self):
        self.mirror.write("/first.txt", b"first", 0)
        self.mirror.write("/second.txt", b"second", 0)
        self._add_entry("/first.txt")
        self._add_entry("/second.txt")
        self.engine.ensure_local_file = Mock(
            side_effect=PyiCloudFailedLoginException("expired session")
        )

        self.engine.sync_dirty_entries()

        self.engine.ensure_local_file.assert_called_once_with("/first.txt")
        self.assertEqual(
            self.state.get_entry("/first.txt")["sync_attempt_count"],
            0,
        )
        self.assertEqual(
            self.state.get_entry("/second.txt")["sync_attempt_count"],
            0,
        )
        self.assertGreater(self.engine.sync_auth_cooldown_until, time.time())
        self.logger.error.assert_called_once()

    def test_failed_parent_is_attempted_once_per_sync_pass(self):
        self._add_entry("/parent", entry_type="folder")
        self._add_entry("/parent/first.txt")
        self._add_entry("/parent/second.txt")
        self.engine._create_remote_directory = Mock(
            side_effect=RuntimeError("remote unavailable")
        )

        self.engine.sync_dirty_entries()

        self.engine._create_remote_directory.assert_called_once()


class ICloudFSInitializationTests(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.mkdtemp(prefix="icloud-linux-test-")
        self.fs = ICloudFS.__new__(ICloudFS)
        self.fs.logger = Mock()

    def tearDown(self):
        shutil.rmtree(self.root)

    def test_init_icloud_limits_partition_request_timeout(self):
        response = Mock()
        response.headers = {"x-apple-user-partition": "1"}
        api = Mock()
        api.requires_2fa = False
        api.requires_2sa = False

        with patch("requests.post", return_value=response) as post:
            with patch("driver.PyiCloudService", return_value=api):
                self.fs.init_icloud("user", "password", self.root)

        post.assert_called_once_with(
            "https://setup.icloud.com/setup/ws/1/validate",
            json={},
            timeout=10,
        )


class ICloudFSPathPolicyTests(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.mkdtemp(prefix="icloud-linux-test-")
        self.mirror = LocalMirror(self.root)
        self.state = SyncState(os.path.join(self.root, "state.sqlite3"))
        self.api = Mock()
        self.api.drive.root = Mock()
        self.engine = ICloudSyncEngine(
            self.api,
            self.mirror,
            self.state,
            Mock(),
            sync_paths=["/allowed/"],
            exclude_paths=["/allowed/excluded/"],
        )
        self.fs = ICloudFS.__new__(ICloudFS)
        self.fs.logger = Mock()
        self.fs.api = self.api
        self.fs.mirror = self.mirror
        self.fs.state = self.state
        self.fs.sync_engine = self.engine

    def tearDown(self):
        self.engine.shutdown()
        shutil.rmtree(self.root)

    def _add_entry(self, path, entry_type="file", content=b"existing"):
        if entry_type == "folder":
            self.mirror.ensure_dir(path)
        else:
            self.mirror.write(path, content, 0)
        stats = self.mirror.stat_local(path)
        self.state.upsert_entry(
            {
                "path": path,
                "type": entry_type,
                "parent_path": os.path.dirname(path) or "/",
                "remote_drivewsid": f"remote-{path}",
                "size": stats.st_size,
                "mtime": int(stats.st_mtime),
                "hydrated": True,
                "dirty": False,
                "tombstone": False,
                "synced_path": path,
            }
        )

    def test_allowed_mutations_are_accepted(self):
        self.assertEqual(self.fs.create("/allowed/queued.txt", 0o644), 0)
        self.assertEqual(self.fs.mkdir("/allowed/newdir", 0o755), 0)
        self.assertEqual(self.fs.create("/allowed/newdir/file.txt", 0o644), 0)
        self.assertEqual(self.fs.write("/allowed/newdir/file.txt", b"hello", 0), 5)
        self.assertEqual(self.fs.truncate("/allowed/newdir/file.txt", 2), 0)
        self.assertEqual(
            self.fs.rename("/allowed/newdir/file.txt", "/allowed/newdir/renamed.txt"),
            0,
        )
        self.assertEqual(self.fs.unlink("/allowed/newdir/renamed.txt"), 0)
        self.assertEqual(self.fs.rmdir("/allowed/newdir"), 0)
        self.assertEqual(
            self.state.get_entry("/allowed/queued.txt")["dirty"],
            1,
        )

    def test_write_returns_eio_when_mark_dirty_fails_after_mirror_write(self):
        self._add_entry("/allowed/file.txt")
        self.state.mark_dirty = Mock(
            side_effect=sqlite3.OperationalError("database is locked")
        )

        result = self.fs.write("/allowed/file.txt", b"changed", 0)

        self.assertEqual(result, -errno.EIO)
        self.assertEqual(
            self.mirror.read("/allowed/file.txt", 100, 0),
            b"changedg",
        )
        self.assertEqual(self.state.get_entry("/allowed/file.txt")["dirty"], 0)
        self.fs.logger.error.assert_called_once()

    def test_unlink_of_missing_mirror_file_recovers_quarantined_tombstone(self):
        self._add_entry("/allowed/removed.txt")
        self.mirror.remove_file("/allowed/removed.txt")
        self.state.mark_dirty("/allowed/removed.txt")
        parent_node = Mock()
        parent_node.data = {}
        remote_node = Mock()
        self.engine._ensure_remote_parent = Mock(return_value=parent_node)
        self.engine._node_from_entry = Mock(return_value=remote_node)

        self.engine.sync_dirty_entries()

        quarantined = self.state.get_entry("/allowed/removed.txt")
        self.assertEqual(quarantined["failed"], 1)
        self.assertEqual(quarantined["tombstone"], 0)

        self.assertEqual(self.fs.unlink("/allowed/removed.txt"), 0)

        entry = self.state.get_entry("/allowed/removed.txt")
        self.assertEqual(entry["tombstone"], 1)
        self.assertEqual(entry["failed"], 0)
        self.assertEqual(entry["sync_attempt_count"], 0)
        self.assertIsNone(entry["sync_next_attempt_at"])
        self.assertIsNone(entry["sync_last_error"])
        self.assertFalse(self.mirror.exists("/allowed/removed.txt"))

        self.engine.sync_dirty_entries()

        remote_node.delete.assert_called_once_with()
        self.assertIsNone(self.state.get_entry("/allowed/removed.txt"))

    def test_atomic_file_replacement_preserves_destination_remote_identity(self):
        self._add_entry("/allowed/note.md", content=b"old")
        self.assertEqual(self.fs.create("/allowed/note.md.tmp", 0o644), 0)
        self.assertEqual(self.fs.write("/allowed/note.md.tmp", b"new", 0), 3)

        result = self.fs.rename("/allowed/note.md.tmp", "/allowed/note.md")

        self.assertEqual(result, 0)
        self.assertEqual(self.mirror.read("/allowed/note.md", 100, 0), b"new")
        self.assertFalse(self.mirror.exists("/allowed/note.md.tmp"))
        self.assertIsNone(self.state.get_entry("/allowed/note.md.tmp"))
        entry = self.state.get_entry("/allowed/note.md")
        self.assertEqual(entry["remote_drivewsid"], "remote-/allowed/note.md")
        self.assertEqual(entry["synced_path"], "/allowed/note.md")
        self.assertEqual(entry["dirty"], 1)
        self.assertEqual(entry["tombstone"], 0)

    def test_replacing_remote_file_with_different_remote_file_is_rejected(self):
        self._add_entry("/allowed/source.md", content=b"source")
        self._add_entry("/allowed/destination.md", content=b"destination")

        result = self.fs.rename("/allowed/source.md", "/allowed/destination.md")

        self.assertEqual(result, -errno.EEXIST)
        self.assertEqual(self.mirror.read("/allowed/source.md", 100, 0), b"source")
        self.assertEqual(
            self.mirror.read("/allowed/destination.md", 100, 0),
            b"destination",
        )
        self.assertEqual(
            self.state.get_entry("/allowed/source.md")["remote_drivewsid"],
            "remote-/allowed/source.md",
        )
        self.assertEqual(
            self.state.get_entry("/allowed/destination.md")["remote_drivewsid"],
            "remote-/allowed/destination.md",
        )

    def test_rename_type_collision_returns_native_errno(self):
        self.assertEqual(self.fs.create("/allowed/source.md", 0o644), 0)
        self.assertEqual(self.fs.mkdir("/allowed/destination", 0o755), 0)

        result = self.fs.rename("/allowed/source.md", "/allowed/destination")

        self.assertEqual(result, -errno.EISDIR)
        self.assertTrue(self.mirror.exists("/allowed/source.md"))
        self.assertTrue(self.mirror.is_dir("/allowed/destination"))

    def test_excluded_and_out_of_scope_mutations_are_rejected_without_queueing(self):
        self._add_entry("/allowed/source.txt")
        self._add_entry("/allowed/excluded/file.txt")
        self._add_entry("/outside/file.txt")
        self._add_entry("/outside/dir", entry_type="folder")

        entry_paths_before = [entry["path"] for entry in self.state.list_entries()]

        attempts = [
            self.fs.create("/allowed/excluded/new.txt", 0o644),
            self.fs.write("/outside/file.txt", b"changed", 0),
            self.fs.truncate("/allowed/excluded/file.txt", 0),
            self.fs.mkdir("/outside/newdir", 0o755),
            self.fs.unlink("/allowed/excluded/file.txt"),
            self.fs.rmdir("/outside/dir"),
            self.fs.rename("/allowed/source.txt", "/outside/destination.txt"),
            self.fs.rename("/outside/file.txt", "/allowed/destination.txt"),
            self.fs.open("/outside/file.txt", os.O_WRONLY),
            self.fs.mknod("/outside/node.txt", stat.S_IFREG | 0o644, 0),
            self.fs.utime("/outside/file.txt", None),
        ]

        self.assertEqual(attempts, [-errno.EACCES] * len(attempts))
        self.assertTrue(
            all(entry["dirty"] == 0 for entry in self.state.list_entries())
        )
        self.assertEqual(
            [entry["path"] for entry in self.state.list_entries()],
            entry_paths_before,
        )
        self.assertEqual(self.mirror.read("/outside/file.txt", 100, 0), b"existing")
        self.assertTrue(self.mirror.exists("/allowed/source.txt"))
        self.assertFalse(self.mirror.exists("/outside/destination.txt"))

    def test_uploader_skips_disallowed_dirty_entries(self):
        self._add_entry("/allowed/file.txt")
        self._add_entry("/allowed/excluded/file.txt")
        self._add_entry("/outside/file.txt")
        self._add_entry("/allowed/moved.txt")
        self.state.mark_dirty("/allowed/file.txt")
        self.state.mark_dirty("/allowed/excluded/file.txt")
        self.state.mark_dirty("/outside/file.txt")
        self.state.upsert_entry(
            {
                **self.state.get_entry("/allowed/moved.txt"),
                "dirty": True,
                "synced_path": "/outside/original.txt",
            }
        )
        self.engine._sync_file = Mock()

        self.engine.sync_dirty_entries()

        self.engine._sync_file.assert_called_once()
        self.assertEqual(
            self.engine._sync_file.call_args.args[0],
            self.state.get_entry("/allowed/file.txt"),
        )

    def test_empty_sync_paths_preserve_unrestricted_behavior(self):
        engine = ICloudSyncEngine(
            self.api,
            self.mirror,
            self.state,
            Mock(),
            sync_paths=[],
        )
        try:
            self.assertTrue(engine._path_allowed("/outside/file.txt"))
        finally:
            engine.shutdown()


class ICloudFSVisibilityTests(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.mkdtemp(prefix="icloud-linux-test-")
        self.mirror = LocalMirror(self.root)
        self.state = SyncState(os.path.join(self.root, "state.sqlite3"))
        self.api = Mock()
        self.api.drive.root = Mock()
        self.engine = ICloudSyncEngine(
            self.api,
            self.mirror,
            self.state,
            Mock(),
            sync_paths=["/Obsidian/Work Notes"],
        )
        self.fs = ICloudFS.__new__(ICloudFS)
        self.fs.logger = Mock()
        self.fs.api = self.api
        self.fs.mirror = self.mirror
        self.fs.state = self.state
        self.fs.sync_engine = self.engine
        self.fs.mount_uid = os.getuid()
        self.fs.mount_gid = os.getgid()
        self.fs.file_mode = 0o644
        self.fs.dir_mode = 0o755

    def tearDown(self):
        self.engine.shutdown()
        self.state.close()
        shutil.rmtree(self.root)

    def _add_entry(self, path, entry_type="file", content=b"content", hydrated=True):
        if entry_type == "folder":
            self.mirror.ensure_dir(path)
        elif hydrated:
            self.mirror.write(path, content, 0)
        else:
            self.mirror.materialize_placeholder(path, len(content), 123)
        stats = self.mirror.stat_local(path)
        self.state.upsert_entry(
            {
                "path": path,
                "type": entry_type,
                "parent_path": os.path.dirname(path) or "/",
                "remote_drivewsid": f"remote-{path}",
                "size": len(content) if entry_type == "file" else 0,
                "mtime": int(stats.st_mtime),
                "hydrated": hydrated,
                "dirty": False,
                "tombstone": False,
                "synced_path": path,
            }
        )

    def _populate_scope_tree(self):
        self._add_entry("/Obsidian", "folder")
        self._add_entry("/Obsidian/Work Notes", "folder")
        self._add_entry("/Obsidian/Work Notes/note.md")
        self._add_entry("/Obsidian/Mikael Personal", "folder")
        self._add_entry("/Documents", "folder")

    def test_readdir_hides_out_of_scope_entries_but_keeps_sync_ancestors(self):
        self._populate_scope_tree()

        self.assertEqual(
            [entry.name for entry in self.fs.readdir("/", 0)],
            [".", "..", "Obsidian"],
        )
        self.assertEqual(
            [entry.name for entry in self.fs.readdir("/Obsidian", 0)],
            [".", "..", "Work Notes"],
        )

    def test_getattr_hides_out_of_scope_entries_but_keeps_ancestors_and_scope(self):
        self._populate_scope_tree()

        self.assertEqual(self.fs.getattr("/Documents"), -errno.ENOENT)
        self.assertEqual(self.fs.getattr("/Obsidian/Mikael Personal"), -errno.ENOENT)
        self.assertNotEqual(self.fs.getattr("/Obsidian"), -errno.ENOENT)
        self.assertNotEqual(
            self.fs.getattr("/Obsidian/Work Notes/note.md"),
            -errno.ENOENT,
        )

    def test_reading_out_of_scope_placeholder_returns_eio_not_zero_bytes(self):
        path = "/Documents/placeholder.txt"
        self._add_entry("/Documents", "folder")
        self._add_entry(path, content=b"expected", hydrated=False)
        self.engine.ensure_local_file = Mock()

        self.assertEqual(self.fs.read(path, 256, 0), -errno.EIO)
        self.engine.ensure_local_file.assert_not_called()

    def test_reading_still_unhydrated_remote_file_returns_eio(self):
        path = "/Obsidian/Work Notes/placeholder.txt"
        self._add_entry("/Obsidian", "folder")
        self._add_entry("/Obsidian/Work Notes", "folder")
        self._add_entry(path, content=b"expected", hydrated=False)
        self.engine.ensure_local_file = Mock()

        self.assertEqual(self.fs.read(path, 256, 0), -errno.EIO)
        self.engine.ensure_local_file.assert_called_once_with(path)

    def test_excluded_paths_are_hidden(self):
        self.engine.shutdown()
        self.engine = ICloudSyncEngine(
            self.api,
            self.mirror,
            self.state,
            Mock(),
            exclude_paths=["/Obsidian/Work Notes/private"],
        )
        self.fs.sync_engine = self.engine
        self._add_entry("/Obsidian", "folder")
        self._add_entry("/Obsidian/Work Notes", "folder")
        self._add_entry("/Obsidian/Work Notes/private", "folder")

        self.assertEqual(
            self.fs.getattr("/Obsidian/Work Notes/private"),
            -errno.ENOENT,
        )

    def test_unrestricted_sync_paths_do_not_hide_entries(self):
        self.engine.shutdown()
        self.engine = ICloudSyncEngine(self.api, self.mirror, self.state, Mock())
        self.fs.sync_engine = self.engine
        self._add_entry("/Documents", "folder")

        self.assertIn("Documents", [entry.name for entry in self.fs.readdir("/", 0)])
        self.assertNotEqual(self.fs.getattr("/Documents"), -errno.ENOENT)


class DurableHydrationQueueTests(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.mkdtemp(prefix="icloud-linux-test-")
        self.mirror = LocalMirror(self.root)
        self.state = SyncState(os.path.join(self.root, "state.sqlite3"))
        self.logger = Mock()
        self.api = Mock()
        self.api.drive.root = Mock()
        self.engine = ICloudSyncEngine(
            self.api,
            self.mirror,
            self.state,
            self.logger,
        )

    def tearDown(self):
        self.engine.shutdown()
        self.state.close()
        shutil.rmtree(self.root)

    def _add_unhydrated_entry(self, path, dirty=False):
        self.state.upsert_entry(
            {
                "path": path,
                "type": "file",
                "parent_path": os.path.dirname(path) or "/",
                "remote_drivewsid": "remote-" + path,
                "size": 10,
                "mtime": 123,
                "hydrated": False,
                "dirty": dirty,
                "tombstone": False,
                "synced_path": path,
            }
        )

    def test_transient_download_failure_persists_hydration_backoff(self):
        self._add_unhydrated_entry("/queued.txt")
        self.engine.ensure_local_file = Mock(side_effect=Timeout("timed out"))
        self.engine._schedule_download_with_delay = Mock()
        self.engine.scheduled_downloads.add("/queued.txt")

        self.engine._download_job("/queued.txt")

        entry = self.state.get_entry("/queued.txt")
        self.assertEqual(entry["hydrate_attempt_count"], 1)
        self.assertGreater(entry["hydrate_next_attempt_at"], int(time.time()))
        self.assertEqual(entry["hydrate_last_error"], "timed out")

    def test_short_download_records_queue_visible_hydration_failure(self):
        path = "/queued.txt"
        self._add_unhydrated_entry(path)
        response = Response()
        response.status_code = 200
        response.headers["Content-Length"] = "10"
        response.raw = io.BytesIO(b"short")
        node = Mock()
        node.open.return_value = response
        self.engine._node_from_entry = Mock(return_value=node)
        self.engine._schedule_download_with_delay = Mock()
        self.engine.scheduled_downloads.add(path)

        self.engine._download_job(path)

        entry = self.state.get_entry(path)
        self.assertEqual(entry["hydrated"], 0)
        self.assertEqual(entry["hydrate_attempt_count"], 1)
        self.assertIn("expected 10 bytes, got 5", entry["hydrate_last_error"])
        report = queue_diagnostic.inspect_queue(self.state.db_path, self.root)
        self.assertEqual(
            [item["path"] for item in report["pending_hydration_retries"]],
            [path],
        )
        self.assertIn("1 hydrate-pending", queue_diagnostic.queue_summary(report))

    def test_restart_preserves_hydration_attempts_and_clears_backoff(self):
        self._add_unhydrated_entry("/queued.txt")
        self.state.record_hydrate_failure(
            "/queued.txt",
            RuntimeError("temporary outage"),
            SYNC_FAILURE_TRANSIENT,
            MAX_SYNC_ATTEMPTS,
        )
        self.state.close()
        self.state = SyncState(os.path.join(self.root, "state.sqlite3"))

        entry = self.state.get_entry("/queued.txt")
        self.assertEqual(entry["hydrate_attempt_count"], 1)
        self.assertEqual(entry["hydrate_last_error"], "temporary outage")
        self.assertIsNone(entry["hydrate_next_attempt_at"])

    def test_restart_preserves_auth_hydration_deferral(self):
        self._add_unhydrated_entry("/queued.txt")
        self.engine.ensure_local_file = Mock(
            side_effect=PyiCloudFailedLoginException("expired session")
        )
        self.engine.scheduled_downloads.add("/queued.txt")
        self.engine._download_job("/queued.txt")
        original_deadline = self.state.get_entry("/queued.txt")[
            "hydrate_next_attempt_at"
        ]
        self.state.close()
        self.state = SyncState(os.path.join(self.root, "state.sqlite3"))

        entry = self.state.get_entry("/queued.txt")
        self.assertEqual(entry["hydrate_attempt_count"], 0)
        self.assertEqual(entry["hydrate_last_error"], "expired session")
        self.assertEqual(entry["hydrate_next_attempt_at"], original_deadline)

    def test_hydration_exhaustion_stops_retry_scheduling(self):
        self._add_unhydrated_entry("/queued.txt")
        self.engine.ensure_local_file = Mock(side_effect=Timeout("timed out"))
        self.engine._schedule_download_with_delay = Mock()

        for _ in range(MAX_SYNC_ATTEMPTS):
            self.engine.scheduled_downloads.add("/queued.txt")
            self.engine._download_job("/queued.txt")

        entry = self.state.get_entry("/queued.txt")
        self.assertEqual(entry["hydrate_attempt_count"], MAX_SYNC_ATTEMPTS)
        self.assertIsNone(entry["hydrate_next_attempt_at"])
        self.assertEqual(
            self.engine._schedule_download_with_delay.call_count,
            MAX_SYNC_ATTEMPTS - 1,
        )

    def test_successful_hydration_clears_hydration_retry_state(self):
        self._add_unhydrated_entry("/queued.txt")
        self.state.record_hydrate_failure(
            "/queued.txt",
            RuntimeError("temporary outage"),
            SYNC_FAILURE_TRANSIENT,
            MAX_SYNC_ATTEMPTS,
        )
        response = Response()
        response.status_code = 200
        response.headers["Content-Length"] = "10"
        response.raw = io.BytesIO(b"downloaded")
        node = Mock()
        node.open.return_value = response
        self.engine._node_from_entry = Mock(return_value=node)

        self.engine._download_job("/queued.txt")

        entry = self.state.get_entry("/queued.txt")
        self.assertEqual(entry["hydrate_attempt_count"], 0)
        self.assertIsNone(entry["hydrate_next_attempt_at"])
        self.assertIsNone(entry["hydrate_last_error"])

    def test_download_auth_failure_does_not_consume_hydration_budget(self):
        self._add_unhydrated_entry("/queued.txt")
        self.engine.ensure_local_file = Mock(
            side_effect=PyiCloudFailedLoginException("expired session")
        )
        self.engine._schedule_download_with_delay = Mock()
        self.engine.scheduled_downloads.add("/queued.txt")

        self.engine._download_job("/queued.txt")

        entry = self.state.get_entry("/queued.txt")
        self.assertEqual(entry["hydrate_attempt_count"], 0)
        self.assertIsNotNone(entry["hydrate_next_attempt_at"])
        self.assertEqual(entry["hydrate_last_error"], "expired session")
        self.engine._schedule_download_with_delay.assert_not_called()

    def test_recovery_reschedules_auth_blocked_hydration(self):
        path = "/queued.txt"
        self._add_unhydrated_entry(path)
        self.engine.ensure_local_file = Mock(
            side_effect=PyiCloudFailedLoginException("expired session")
        )
        self.engine.scheduled_downloads.add("/queued.txt")
        self.engine._download_job("/queued.txt")
        self.engine._schedule_download = Mock()
        marker_path = os.path.join(self.root, "unrecorded_failures.log")
        with open(marker_path, "w", encoding="utf-8") as handle:
            handle.write(
                '{"timestamp": 1, "path": "/queued.txt", "operation": "open", '
                '"error": "offline", "record_error": "database is locked"}\n'
            )

        cleared, path, _markers = queue_recovery.clear_failures(self.state.db_path)
        self.engine._schedule_all_unhydrated()

        self.assertEqual((cleared, path), (1, None))
        self.assertFalse(os.path.exists(marker_path))
        self.assertIsNone(
            self.state.get_entry("/queued.txt")["hydrate_next_attempt_at"]
        )
        self.engine._schedule_download.assert_called_once_with("/queued.txt")

    def test_hydration_exhaustion_does_not_block_upload_sync(self):
        self._add_unhydrated_entry("/queued.txt", dirty=True)
        self._add_unhydrated_entry("/sibling.txt", dirty=True)
        for _ in range(MAX_SYNC_ATTEMPTS):
            self.state.record_hydrate_failure(
                "/queued.txt",
                RuntimeError("temporary outage"),
                SYNC_FAILURE_TRANSIENT,
                MAX_SYNC_ATTEMPTS,
            )
        self.engine._sync_file = Mock()

        self.engine.sync_dirty_entries()

        self.assertEqual(
            {call.args[0]["path"] for call in self.engine._sync_file.call_args_list},
            {"/queued.txt", "/sibling.txt"},
        )
        self.assertEqual(self.state.get_entry("/queued.txt")["failed"], 0)

    def test_hydration_exhaustion_does_not_consume_sync_budget(self):
        self._add_unhydrated_entry("/queued.txt", dirty=True)
        self.mirror.materialize_placeholder("/queued.txt", 10, 123)
        for _ in range(MAX_SYNC_ATTEMPTS):
            self.state.record_hydrate_failure(
                "/queued.txt",
                RuntimeError("temporary outage"),
                SYNC_FAILURE_TRANSIENT,
                MAX_SYNC_ATTEMPTS,
            )
        parent = Mock()
        parent.data = {}
        self.engine._ensure_remote_parent = Mock(return_value=parent)

        self.engine._sync_file(self.state.get_entry("/queued.txt"))

        entry = self.state.get_entry("/queued.txt")
        self.assertEqual(entry["sync_attempt_count"], 0)
        self.assertEqual(entry["failed"], 0)


class HydrationFailureFUSETests(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.mkdtemp(prefix="icloud-linux-test-")
        self.mirror = LocalMirror(self.root)
        self.state = SyncState(os.path.join(self.root, "state.sqlite3"))
        self.api = Mock()
        self.api.drive.root = Mock()
        self.engine = ICloudSyncEngine(self.api, self.mirror, self.state, Mock())
        self.fs = ICloudFS.__new__(ICloudFS)
        self.fs.logger = Mock()
        self.fs.api = self.api
        self.fs.mirror = self.mirror
        self.fs.state = self.state
        self.fs.sync_engine = self.engine
        self.fs.cache_dir = self.root
        self.fs.file_mode = 0o644
        self.fs.dir_mode = 0o755
        self.fs.mount_uid = os.getuid()
        self.fs.mount_gid = os.getgid()

    def tearDown(self):
        self.engine.shutdown()
        self.state.close()
        shutil.rmtree(self.root)

    def test_reading_hydration_exhausted_entry_returns_eio(self):
        path = "/unavailable.txt"
        self.state.upsert_entry(
            {
                "path": path,
                "type": "file",
                "parent_path": "/",
                "remote_drivewsid": "remote-unavailable",
                "size": 10,
                "mtime": 123,
                "hydrated": False,
                "dirty": False,
                "tombstone": False,
                "synced_path": path,
            }
        )
        self.mirror.materialize_placeholder(path, 10, 123)
        for _ in range(MAX_SYNC_ATTEMPTS):
            self.state.record_hydrate_failure(
                path,
                RuntimeError("temporary outage"),
                SYNC_FAILURE_TRANSIENT,
                MAX_SYNC_ATTEMPTS,
            )

        self.assertEqual(self.fs.read(path, 10, 0), -errno.EIO)

    def test_utime_dirty_unhydrated_entry_returns_eio_but_remains_visible(self):
        path = "/unavailable.txt"
        self.state.upsert_entry(
            {
                "path": path,
                "type": "file",
                "parent_path": "/",
                "remote_drivewsid": "remote-unavailable",
                "size": 10,
                "mtime": 123,
                "hydrated": False,
                "dirty": False,
                "tombstone": False,
                "synced_path": path,
            }
        )
        self.mirror.materialize_placeholder(path, 10, 123)
        for _ in range(MAX_SYNC_ATTEMPTS):
            self.state.record_hydrate_failure(
                path,
                RuntimeError("temporary outage"),
                SYNC_FAILURE_TRANSIENT,
                MAX_SYNC_ATTEMPTS,
            )
        self.fs._is_authenticated = Mock(return_value=True)
        self.fs._mutation_allowed = Mock(return_value=True)

        self.assertEqual(self.fs.utime(path, (123, 124)), 0)
        self.assertTrue(self.state.get_entry(path)["dirty"])
        self.assertEqual(self.fs.read(path, 10, 0), -errno.EIO)
        self.assertEqual(self.fs.getattr(path).st_size, 10)
        self.assertIn(
            "unavailable.txt",
            [entry.name for entry in self.fs.readdir("/", 0)],
        )

    def test_dirty_unhydrated_local_file_reads_local_content(self):
        path = "/local.txt"
        self.mirror.create_file(path)
        self.mirror.write(path, b"local content", 0)
        stats = self.mirror.stat_local(path)
        self.state.upsert_entry(
            {
                "path": path,
                "type": "file",
                "parent_path": "/",
                "size": stats.st_size,
                "mtime": int(stats.st_mtime),
                "hydrated": False,
                "dirty": True,
                "tombstone": False,
                "synced_path": None,
            }
        )
        self.engine.ensure_local_file = Mock()

        self.assertEqual(self.fs.read(path, 100, 0), b"local content")
        self.engine.ensure_local_file.assert_not_called()

    def test_open_hydration_failure_records_pending_queue_entry(self):
        path = "/offline.txt"
        self.state.upsert_entry(
            {
                "path": path,
                "type": "file",
                "parent_path": "/",
                "remote_drivewsid": "remote-offline",
                "size": 10,
                "mtime": 123,
                "hydrated": False,
                "dirty": False,
                "tombstone": False,
                "synced_path": path,
            }
        )
        self.fs._is_authenticated = Mock(return_value=True)
        self.engine.ensure_local_file = Mock(side_effect=Timeout("offline"))

        self.assertEqual(self.fs.open(path, os.O_RDONLY), -errno.EIO)

        entry = self.state.get_entry(path)
        self.assertEqual(entry["hydrate_attempt_count"], 1)
        self.assertEqual(entry["hydrate_last_error"], "offline")
        self.assertGreater(entry["hydrate_next_attempt_at"], int(time.time()))
        report = queue_diagnostic.inspect_queue(self.state.db_path, self.root)
        self.assertEqual(
            [item["path"] for item in report["pending_hydration_retries"]],
            [path],
        )
        self.assertIn("1 hydrate-pending", queue_diagnostic.queue_summary(report))

    def test_open_hydration_recording_failure_writes_fallback_marker(self):
        path = "/unrecorded.txt"
        self.state.upsert_entry(
            {
                "path": path,
                "type": "file",
                "parent_path": "/",
                "remote_drivewsid": "remote-unrecorded",
                "size": 10,
                "mtime": 123,
                "hydrated": False,
                "dirty": False,
                "tombstone": False,
                "synced_path": path,
            }
        )
        self.fs._is_authenticated = Mock(return_value=True)
        self.engine.ensure_local_file = Mock(side_effect=Timeout("offline"))
        self.engine._record_hydrate_failure = Mock(
            side_effect=sqlite3.OperationalError("database is locked")
        )

        self.assertEqual(self.fs.open(path, os.O_RDONLY), -errno.EIO)
        marker_path = os.path.join(self.root, "unrecorded_failures.log")
        self.assertTrue(os.path.exists(marker_path))
        with open(marker_path, encoding="utf-8") as handle:
            marker = handle.read()
        self.assertIn(path, marker)
        self.assertIn("open", marker)

    def test_open_hydration_recording_marker_write_failure_returns_eio(self):
        path = "/marker-write-failure.txt"
        self.state.upsert_entry(
            {
                "path": path,
                "type": "file",
                "parent_path": "/",
                "remote_drivewsid": "remote-marker-write-failure",
                "size": 10,
                "mtime": 123,
                "hydrated": False,
                "dirty": False,
                "tombstone": False,
                "synced_path": path,
            }
        )
        self.fs._is_authenticated = Mock(return_value=True)
        self.engine.ensure_local_file = Mock(side_effect=Timeout("offline"))
        self.engine._record_hydrate_failure = Mock(
            side_effect=sqlite3.OperationalError("database is locked")
        )

        with patch("builtins.open", side_effect=OSError("disk full")):
            self.assertEqual(self.fs.open(path, os.O_RDONLY), -errno.EIO)

        # Neither the database nor the fallback log could record this, so the
        # log is the only remaining signal and must be CRITICAL and explicit.
        self.assertEqual(self.fs.logger.critical.call_count, 1)
        message = self.fs.logger.critical.call_args[0][0]
        self.assertIn("QUEUE UNRELIABLE", message)

    def test_open_auth_hydration_failure_is_queue_blocked_without_attempt(self):
        path = "/expired-session.txt"
        self.state.upsert_entry(
            {
                "path": path,
                "type": "file",
                "parent_path": "/",
                "remote_drivewsid": "remote-expired",
                "size": 10,
                "mtime": 123,
                "hydrated": False,
                "dirty": False,
                "tombstone": False,
                "synced_path": path,
            }
        )
        self.fs._is_authenticated = Mock(return_value=True)
        self.engine.ensure_local_file = Mock(
            side_effect=PyiCloudFailedLoginException("expired session")
        )

        self.assertEqual(self.fs.open(path, os.O_RDONLY), -errno.EIO)

        entry = self.state.get_entry(path)
        self.assertEqual(entry["hydrate_attempt_count"], 0)
        self.assertEqual(entry["hydrate_last_error"], "expired session")
        self.assertGreater(entry["hydrate_next_attempt_at"], int(time.time()))
        report = queue_diagnostic.inspect_queue(self.state.db_path, self.root)
        self.assertEqual(
            [(item["path"], item["operation"]) for item in report["auth_blocked_entries"]],
            [(path, "hydration")],
        )


if __name__ == "__main__":
    unittest.main()
