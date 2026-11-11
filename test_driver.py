import errno
import io
import os
import shutil
import sqlite3
import stat
import tempfile
import threading
import unittest
from unittest.mock import Mock, patch
from requests.models import RequestEncodingMixin, Response

from driver import (
    IO_CHUNK_SIZE, ICloudFS, ICloudSyncEngine, LocalMirror, NamedFileStream,
    SyncState, complete_on_demand_scan,
)
from pyicloud.exceptions import PyiCloudAPIResponseException, PyiCloudFailedLoginException
from pyicloud.services.drive import DriveNode


class NoUnboundedReadStream(io.BytesIO):
    def read(self, size=-1):
        if size is None or size < 0:
            raise AssertionError("stream was read without a chunk size")
        return super().read(size)


class CallbackReadStream(NoUnboundedReadStream):
    def __init__(self, content, callback=None):
        super().__init__(content)
        self.callback = callback
        self.read_sizes = []

    def read(self, size=-1):
        self.read_sizes.append(size)
        if self.callback is not None:
            callback, self.callback = self.callback, None
            callback()
        return super().read(size)


class DriverStateTests(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.mkdtemp(prefix="icloud-linux-test-")
        self.mirror = LocalMirror(self.root)
        self.state = SyncState(os.path.join(self.root, "state.sqlite3"))

    def tearDown(self):
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

        self.mirror.write_atomic_stream("/docs/a.txt", stream)

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

    def test_existing_state_db_is_migrated_for_remote_shareid(self):
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
        conn.commit()
        conn.close()

        migrated = SyncState(legacy_db)
        columns = migrated.conn.execute("PRAGMA table_info(entries)").fetchall()

        self.assertIn("remote_shareid", {column["name"] for column in columns})


class OfflineCacheModeTests(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.mkdtemp(prefix="icloud-linux-offline-test-")
        self.cache_dir = os.path.join(self.root, "cache")
        self.mirror = LocalMirror(self.cache_dir)
        self.state = SyncState(os.path.join(self.cache_dir, "state.sqlite3"))
        self.fs = ICloudFS()
        self.fs.api = None
        self.fs.cache_dir = self.cache_dir
        self.fs.mirror = self.mirror
        self.fs.state = self.state
        self.fs.sync_engine = None

    def tearDown(self):
        shutil.rmtree(self.root)

    def test_hydrated_cached_file_is_readable_without_session(self):
        self.mirror.ensure_dir("/docs")
        self.mirror.write("/docs/a.txt", b"cached", 0)
        self.state.upsert_entry(
            {
                "path": "/docs/a.txt",
                "type": "file",
                "parent_path": "/docs",
                "remote_drivewsid": "file-1",
                "size": 6,
                "mtime": 123,
                "hydrated": True,
                "dirty": False,
                "tombstone": False,
                "synced_path": "/docs/a.txt",
            }
        )

        self.assertEqual(self.fs.open("/docs/a.txt", os.O_RDONLY), 0)
        self.assertEqual(self.fs.read("/docs/a.txt", 100, 0), b"cached")

    def test_metadata_only_entries_do_not_crash_getattr_without_session(self):
        self.state.upsert_entry(
            {
                "path": "/remote-only",
                "type": "folder",
                "parent_path": "/",
                "remote_drivewsid": "folder-1",
                "size": 0,
                "mtime": 123,
                "hydrated": False,
                "dirty": False,
                "tombstone": False,
                "synced_path": "/remote-only",
            }
        )
        self.state.upsert_entry(
            {
                "path": "/remote-only/a.txt",
                "type": "file",
                "parent_path": "/remote-only",
                "remote_drivewsid": "file-1",
                "size": 5,
                "mtime": 123,
                "hydrated": False,
                "dirty": False,
                "tombstone": False,
                "synced_path": "/remote-only/a.txt",
            }
        )

        folder_attrs = self.fs.getattr("/remote-only")
        file_attrs = self.fs.getattr("/remote-only/a.txt")

        self.assertTrue(stat.S_ISDIR(folder_attrs.st_mode))
        self.assertTrue(stat.S_ISREG(file_attrs.st_mode))
        self.assertEqual(file_attrs.st_size, 5)

    def test_writes_are_denied_without_session(self):
        self.assertEqual(self.fs.create("/offline.txt", os.O_WRONLY, 0o644), -errno.EACCES)


class RemoteSnapshotRaceTests(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.mkdtemp(prefix="icloud-linux-test-")
        self.mirror = LocalMirror(self.root)
        self.state = SyncState(os.path.join(self.root, "state.sqlite3"))
        api = Mock()
        api.drive.root = Mock()
        self.engine = ICloudSyncEngine(api, self.mirror, self.state, Mock())

    def tearDown(self):
        self.engine.shutdown()
        shutil.rmtree(self.root)

    def _add_clean_remote_file(self, path, last_synced_at):
        self.mirror.write(path, b"content", 0)
        self.state.upsert_entry(
            {
                "path": path,
                "type": "file",
                "parent_path": os.path.dirname(path) or "/",
                "remote_drivewsid": f"remote-{path}",
                "size": 7,
                "mtime": 123,
                "hydrated": True,
                "dirty": False,
                "tombstone": False,
                "last_synced_at": last_synced_at,
                "synced_path": path,
            }
        )

    def test_snapshot_does_not_remove_file_synced_after_crawl_started(self):
        self._add_clean_remote_file("/new.txt", last_synced_at=200)

        self.engine._apply_remote_snapshot({}, crawl_started_at=200)

        self.assertIsNotNone(self.state.get_entry("/new.txt"))
        self.assertTrue(self.mirror.exists("/new.txt"))

    def test_snapshot_still_removes_file_synced_before_crawl_started(self):
        self._add_clean_remote_file("/old.txt", last_synced_at=199)

        self.engine._apply_remote_snapshot({}, crawl_started_at=200)

        self.assertIsNone(self.state.get_entry("/old.txt"))
        self.assertFalse(self.mirror.exists("/old.txt"))

    def test_failed_root_crawl_preserves_cached_file(self):
        self._add_clean_remote_file("/old.txt", last_synced_at=1)
        self.engine.api.drive.root.data = {}
        self.engine.api.drive.root.get_children.side_effect = RuntimeError("offline")

        self.engine._run_remote_refresh("test")

        self.assertIsNotNone(self.state.get_entry("/old.txt"))
        self.assertTrue(self.mirror.exists("/old.txt"))

    def test_incomplete_folder_response_cannot_reuse_old_items(self):
        self._add_clean_remote_file("/old.txt", last_synced_at=1)
        connection = Mock()
        connection.get_node_data.return_value = {"status": "incomplete"}
        self.engine.api.drive.root = DriveNode(connection, {
            "drivewsid": "root",
            "items": [],
        })

        self.engine._run_remote_refresh("test")

        self.assertIsNotNone(self.state.get_entry("/old.txt"))
        self.assertTrue(self.mirror.exists("/old.txt"))

    def test_incomplete_or_malformed_real_folder_preserves_cache(self):
        cases = [
            {"status": "incomplete", "items": []},
            {"status": "incomplete", "items": [{"drivewsid": "irrelevant"}]},
            {"status": None, "items": []},
            {"status": "NOT_FOUND", "items": []},
            {"items": {}},
            {"items": None},
            {},
        ]
        for nested in (False, True):
            for payload in cases:
                with self.subTest(nested=nested, payload=payload):
                    self._add_clean_remote_file("/old.txt", last_synced_at=1)
                    connection = Mock()
                    folder = {"drivewsid": "folder", "name": "folder", "type": "FOLDER"}
                    connection.get_node_data.side_effect = lambda identity, share: (
                        {"items": [folder]} if nested and identity == "root" else payload
                    )
                    self.engine.api.drive.root = DriveNode(connection, {"drivewsid": "root"})
                    self.engine._run_remote_refresh("test")
                    self.assertIsNotNone(self.state.get_entry("/old.txt"))
                    self.assertTrue(self.mirror.exists("/old.txt"))

    def test_complete_real_empty_folder_can_remove_missing_cache(self):
        for nested in (False, True):
            folder_id = "folder" if nested else "root"
            for payload in ({"items": []}, {"status": "OK", "items": []},
                            {"status": "OK", "drivewsid": folder_id, "items": []}):
                with self.subTest(nested=nested, payload=payload):
                    self._add_clean_remote_file("/old.txt", last_synced_at=1)
                    connection = Mock()
                    folder = {"drivewsid": "folder", "name": "folder", "type": "FOLDER"}
                    connection.get_node_data.side_effect = lambda identity, share: (
                        {"items": [folder]} if nested and identity == "root" else payload
                    )
                    self.engine.api.drive.root = DriveNode(connection, {"drivewsid": "root"})
                    self.engine._run_remote_refresh("test")
                    self.assertIsNone(self.state.get_entry("/old.txt"))
                    self.assertFalse(self.mirror.exists("/old.txt"))

    def test_wrong_refreshed_folder_identity_preserves_cache(self):
        for nested in (False, True):
            with self.subTest(nested=nested):
                self._add_clean_remote_file("/old.txt", last_synced_at=1)
                connection = Mock()
                folder = {"drivewsid": "folder", "name": "folder", "type": "FOLDER"}
                connection.get_node_data.side_effect = lambda identity, share: (
                    {"drivewsid": "root", "items": [folder]}
                    if nested and identity == "root"
                    else {"status": "OK", "drivewsid": "different", "items": []}
                )
                self.engine.api.drive.root = DriveNode(connection, {"drivewsid": "root"})
                self.engine._run_remote_refresh("test")
                self.assertIsNotNone(self.state.get_entry("/old.txt"))
                self.assertEqual(self.mirror.read("/old.txt", 7, 0), b"content")

    def test_invalid_remote_file_size_preserves_existing_bytes(self):
        missing = object()
        for nested in (False, True):
            for size in (missing, None, -1, "-1", "", "bad", True, 1.5, 2**63):
                with self.subTest(nested=nested, size=size):
                    self._add_clean_remote_file("/old.txt", last_synced_at=1)
                    data = {"name": "old.txt", "type": "FILE", "drivewsid": "remote-/old.txt",
                            "docwsid": "doc", "zone": "zone"}
                    if size is not missing:
                        data["size"] = size
                    folder = {"drivewsid": "folder", "name": "folder", "type": "FOLDER"}
                    connection = Mock()
                    connection.get_node_data.side_effect = lambda identity, share: (
                        {"items": [folder]} if nested and identity == "root" else {"items": [data]}
                    )
                    self.engine.api.drive.root = DriveNode(connection, {"drivewsid": "root"})
                    self.engine._run_remote_refresh("test")
                    self.assertEqual(self.mirror.read("/old.txt", 7, 0), b"content")
                    self.assertEqual(self.state.get_entry("/old.txt")["size"], 7)

    def test_valid_zero_size_file_is_applied(self):
        for size in (0, "0"):
            with self.subTest(size=size):
                self._add_clean_remote_file("/old.txt", last_synced_at=1)
                connection = Mock()
                connection.get_node_data.return_value = {"items": [{
                    "name": "old.txt", "type": "FILE", "drivewsid": "remote-/old.txt",
                    "docwsid": "doc", "zone": "zone", "size": size,
                }]}
                self.engine.api.drive.root = DriveNode(connection, {"drivewsid": "root"})
                self.engine._run_remote_refresh("test")
                self.assertEqual(self.mirror.read("/old.txt", 7, 0), b"")
                self.assertEqual(self.state.get_entry("/old.txt")["size"], 0)
                self.assertTrue(self.state.get_entry("/old.txt")["hydrated"])

    def test_overlapping_refreshes_cannot_publish_older_snapshot_last(self):
        old_entered = threading.Event()
        release_old = threading.Event()
        new_done = threading.Event()
        meta = {"path": "/new.txt", "parent_path": "/", "type": "file",
                "remote_drivewsid": "new-id", "size": 0, "mtime": 1}

        def crawl():
            if threading.current_thread().name == "older-crawl":
                old_entered.set()
                if not release_old.wait(5):
                    raise TimeoutError("test timed out")
                return {}
            return {"new-id": meta}

        self.engine._crawl_remote_snapshot = Mock(side_effect=crawl)
        older = threading.Thread(target=self.engine._run_remote_refresh,
                                 args=("background",), name="older-crawl")

        def on_demand():
            try:
                complete_on_demand_scan(self.engine, os.path.join(self.root, "markers"))
            finally:
                new_done.set()

        newer = threading.Thread(target=on_demand, name="newer-crawl")
        older.start()
        self.assertTrue(old_entered.wait(5))
        newer.start()
        new_done.wait(0.3)
        release_old.set()
        older.join(5)
        newer.join(5)
        self.assertFalse(older.is_alive())
        self.assertFalse(newer.is_alive())
        self.assertIsNotNone(self.state.get_entry("/new.txt"))
        self.assertTrue(self.mirror.exists("/new.txt"))

    def test_failed_scan_does_not_block_later_scan(self):
        self.engine._crawl_remote_snapshot = Mock(side_effect=[RuntimeError("offline"), {}])
        self.engine._run_remote_refresh("failed")
        done = threading.Event()
        worker = threading.Thread(target=lambda: (self.engine.initial_scan(), done.set()))
        worker.start()
        worker.join(5)
        self.assertTrue(done.is_set())
        self.assertFalse(worker.is_alive())

    def test_shared_reconciliation_serializes_with_background_scan(self):
        old_entered = threading.Event()
        release_old = threading.Event()
        reconcile_done = threading.Event()
        meta = {"path": "/Shared/new.txt", "parent_path": "/Shared", "type": "file",
                "remote_drivewsid": "new-id", "size": 0, "mtime": 1}

        def crawl():
            if threading.current_thread().name == "older-crawl":
                old_entered.set()
                if not release_old.wait(5):
                    raise TimeoutError("test timed out")
                return {}
            return {"new-id": meta}

        self.engine._crawl_remote_snapshot = Mock(side_effect=crawl)
        self.engine._shareid_for_path = Mock(return_value={"zone": "shared"})
        self.engine._refresh_child_meta = Mock(side_effect=KeyError("not found"))
        older = threading.Thread(target=self.engine._run_remote_refresh,
                                 args=("background",), name="older-crawl")
        results = []

        def reconcile():
            try:
                results.append(self.engine._reconcile_child_meta("/Shared", "new.txt"))
            finally:
                reconcile_done.set()

        newer = threading.Thread(target=reconcile)
        older.start()
        self.assertTrue(old_entered.wait(5))
        newer.start()
        reconcile_done.wait(0.3)
        release_old.set()
        older.join(5)
        newer.join(5)
        self.assertFalse(older.is_alive())
        self.assertFalse(newer.is_alive())
        self.assertEqual(results[0]["remote_drivewsid"], "new-id")
        self.assertIsNotNone(self.state.get_entry("/Shared/new.txt"))

    def test_missing_parent_does_not_block_another_file_across_restart(self):
        for path in ("/a.txt", "/b.txt"):
            self._add_clean_remote_file(path, last_synced_at=1)
            self.state.mark_dirty(path)
        self.engine.metadata_ready = True
        parent = Mock()
        parent.data = {}
        self.engine._ensure_remote_parent = Mock(
            side_effect=lambda path: None if path == "/a.txt" else parent
        )
        self.engine.ensure_local_file = Mock()
        self.engine._node_from_entry = Mock()
        self.engine._delete_remote_node = Mock()
        self.engine._reconcile_child_meta = Mock(return_value={
            "remote_drivewsid": "remote-/b.txt", "size": 7, "mtime": 123,
        })

        self.engine.sync_dirty_entries()
        reopened = SyncState(os.path.join(self.root, "state.sqlite3"))
        self.addCleanup(reopened.close)
        engine = ICloudSyncEngine(self.engine.api, self.mirror, reopened, Mock())
        self.addCleanup(engine.shutdown)
        engine.metadata_ready = True
        engine._ensure_remote_parent = self.engine._ensure_remote_parent
        engine.ensure_local_file = self.engine.ensure_local_file
        engine._node_from_entry = self.engine._node_from_entry
        engine._delete_remote_node = self.engine._delete_remote_node
        engine._reconcile_child_meta = self.engine._reconcile_child_meta
        engine.sync_dirty_entries()

        parent.upload.assert_called_once()
        self.assertTrue(reopened.get_entry("/a.txt")["dirty"])
        self.assertFalse(reopened.get_entry("/b.txt")["dirty"])

    def test_existing_parent_allows_file_upload(self):
        self._add_clean_remote_file("/b.txt", last_synced_at=1)
        self.state.mark_dirty("/b.txt")
        parent = Mock()
        parent.data = {}
        self.engine._ensure_remote_parent = Mock(return_value=parent)
        self.engine.ensure_local_file = Mock()
        self.engine._node_from_entry = Mock()
        self.engine._delete_remote_node = Mock()
        self.engine._reconcile_child_meta = Mock(return_value={
            "remote_drivewsid": "remote-/b.txt", "size": 7, "mtime": 123,
        })

        self.engine._sync_file(self.state.get_entry("/b.txt"))

        parent.upload.assert_called_once()
        self.assertFalse(self.state.get_entry("/b.txt")["dirty"])

    def test_failed_nested_crawl_does_not_apply_partial_snapshot(self):
        self._add_clean_remote_file("/old.txt", last_synced_at=1)
        child = Mock(name="folder")
        child.name = "folder"
        child.data = {"type": "FOLDER", "drivewsid": "folder-1"}
        child.get_children.side_effect = RuntimeError("offline")
        root = self.engine.api.drive.root
        root.data = {}
        root.get_children.return_value = [child]

        self.engine._run_remote_refresh("test")

        self.assertIsNotNone(self.state.get_entry("/old.txt"))
        self.assertIsNone(self.state.get_entry("/folder"))

    def test_timed_out_crawl_does_not_apply_snapshot(self):
        self._add_clean_remote_file("/old.txt", last_synced_at=1)
        self.engine.api.drive.root.data = {}
        executor = Mock()
        executor.submit.return_value.result.side_effect = TimeoutError()
        with patch("driver.ThreadPoolExecutor", return_value=executor):
            self.engine._run_remote_refresh("test")

        self.assertIsNotNone(self.state.get_entry("/old.txt"))
        self.assertTrue(self.mirror.exists("/old.txt"))

    def test_incomplete_on_demand_scan_does_not_report_completion(self):
        self._add_clean_remote_file("/old.txt", last_synced_at=1)
        self.engine.api.drive.root.data = {}
        self.engine.api.drive.root.get_children.side_effect = RuntimeError("offline")
        marker_dir = os.path.join(self.root, "markers")
        with self.assertRaises(RuntimeError):
            complete_on_demand_scan(self.engine, marker_dir)
        self.assertFalse(os.path.exists(os.path.join(marker_dir, "sync_done")))
        self.assertIsNotNone(self.state.get_entry("/old.txt"))

    def test_complete_on_demand_scan_reports_completion(self):
        self.engine.api.drive.root.data = {}
        self.engine.api.drive.root.get_children.return_value = []
        marker_dir = os.path.join(self.root, "markers")
        complete_on_demand_scan(self.engine, marker_dir)
        with open(os.path.join(marker_dir, "sync_done")) as marker:
            self.assertGreater(float(marker.read()), 0)

    def test_malformed_child_identity_preserves_entire_cached_snapshot(self):
        valid = {
            "name": "new.txt", "type": "FILE", "drivewsid": "new-id",
            "docwsid": "new-doc", "zone": "zone-1", "size": 0,
        }
        cases = [
            dict(valid, drivewsid=None), dict(valid, drivewsid=""),
            dict(valid, docwsid=None), dict(valid, zone=None),
            dict(valid, type=None), dict(valid, name="../escape"),
            dict(valid, name="."), dict(valid, name=""),
        ]
        # A missing raw name has a documented drivewsid fallback. An empty
        # drivewsid is never a usable fallback or a remote identity.
        cases[-1]["drivewsid"] = None
        for data in cases:
            with self.subTest(data=data):
                self.state.remove_subtree("/new.txt")
                self._add_clean_remote_file("/old.txt", last_synced_at=1)
                root = self.engine.api.drive.root
                root.data = {"drivewsid": "root"}
                root.get_children.return_value = [DriveNode(Mock(), data)]
                self.engine._run_remote_refresh("test")
                self.assertIsNotNone(self.state.get_entry("/old.txt"))
                self.assertTrue(self.mirror.exists("/old.txt"))
                self.assertIsNone(self.state.get_entry("/new.txt"))

    def test_duplicate_remote_paths_and_file_identities_abort_whole_snapshot(self):
        first = {
            "name": "new.txt", "type": "FILE", "drivewsid": "new-id",
            "docwsid": "new-doc", "zone": "zone-1", "size": 0,
        }
        duplicates = [
            dict(first, name="other.txt"),
            dict(first, drivewsid="other-id", docwsid="other-doc"),
            dict(first, name="other.txt", drivewsid="other-id"),
        ]
        for second in duplicates:
            with self.subTest(second=second):
                self.state.remove_subtree("/new.txt")
                self.state.remove_subtree("/other.txt")
                self._add_clean_remote_file("/old.txt", last_synced_at=1)
                root = self.engine.api.drive.root
                root.data = {"drivewsid": "root"}
                root.get_children.return_value = [DriveNode(Mock(), first), DriveNode(Mock(), second)]
                self.engine._run_remote_refresh("test")
                self.assertIsNotNone(self.state.get_entry("/old.txt"))
                self.assertIsNone(self.state.get_entry("/new.txt"))

    def test_known_special_nodes_and_shared_namespaces_remain_valid(self):
        nodes = [
            {"drivewsid": "TRASH_ROOT"},
            {"drivewsid": "app-1", "type": "APP_LIBRARY", "docwsid": "documents"},
            {"drivewsid": "app-2", "type": "APP_LIBRARY", "docwsid": "documents"},
            {"name": "shared-a", "drivewsid": "file-a", "type": "FILE",
             "docwsid": "same-doc", "zone": "zone", "size": 0, "shareID": {"zone": "share-a"}},
            {"name": "shared-b", "drivewsid": "file-b", "type": "FILE",
             "docwsid": "same-doc", "zone": "zone", "size": 0, "shareID": {"zone": "share-b"}},
        ]
        children = [DriveNode(Mock(), data) for data in nodes]
        for child in children[1:3]:
            child.get_children = Mock(return_value=[])
        root = self.engine.api.drive.root
        root.data = {"drivewsid": "root"}
        root.get_children.return_value = children
        snapshot = self.engine._crawl_remote_snapshot()
        self.assertEqual(set(snapshot), {data["drivewsid"] for data in nodes})

    def test_initial_scan_failure_keeps_mount_and_allows_later_metadata_retry(self):
        for auto_sync in (True, False):
            with self.subTest(auto_sync=auto_sync):
                fs = ICloudFS()
                fs.api = self.engine.api
                fs.logger = Mock()
                root = fs.api.drive.root
                root.data = {"drivewsid": "root"}
                root.get_children.reset_mock()
                root.get_children.side_effect = RuntimeError("secret-failure-payload")
                cache = os.path.join(self.root, f"startup-{auto_sync}")
                try:
                    with patch.object(ICloudSyncEngine, "_start_background_threads") as workers:
                        fs.init_local_cache(cache, "lazy", "copy", 30, 300, 1, auto_sync=auto_sync)
                    self.assertEqual(workers.call_count, int(auto_sync))
                    self.assertFalse(fs.sync_engine.stop_event.is_set())
                    self.assertFalse(fs.sync_engine.metadata_ready)
                    self.assertEqual(fs.state.count_entries(), 0)
                    self.assertEqual(len(list(fs.readdir("/", 0))), 2)
                    self.assertIn("RuntimeError", str(fs.logger.error.call_args))
                    self.assertNotIn("secret-failure-payload", str(fs.logger.mock_calls))
                    with patch.object(fs.state, "list_dirty_entries") as pending:
                        fs.sync_engine.sync_dirty_entries()
                        pending.assert_not_called()
                    root.get_children.side_effect = None
                    root.get_children.return_value = []
                    fs.sync_engine._run_remote_refresh("scheduled" if auto_sync else "manual")
                    self.assertTrue(fs.sync_engine.metadata_ready)
                    self.assertEqual(root.get_children.call_count, 2)
                finally:
                    fs.shutdown()


class PermissionCallbackTests(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.mkdtemp(prefix="icloud-linux-test-")
        self.mirror = LocalMirror(self.root)
        self.state = SyncState(os.path.join(self.root, "state.sqlite3"))
        self.mirror.write("/copied.txt", b"content", 0)
        stats = self.mirror.stat_local("/copied.txt")
        self.state.upsert_entry(
            {
                "path": "/copied.txt",
                "type": "file",
                "parent_path": "/",
                "remote_drivewsid": None,
                "size": stats.st_size,
                "mtime": int(stats.st_mtime),
                "hydrated": True,
                "dirty": False,
                "tombstone": False,
                "synced_path": None,
            }
        )
        self.fs = ICloudFS.__new__(ICloudFS)
        self.fs.logger = Mock()
        self.fs.mirror = self.mirror
        self.fs.state = self.state
        self.fs.api = Mock()
        self.fs.sync_engine = Mock()
        self.fs.sync_engine._path_allowed = Mock(return_value=True)
        self.fs.apply_permissions_config(None)
        self.fs.GetContext = Mock(return_value={"uid": os.getuid(), "gid": os.getgid()})

    def tearDown(self):
        self.state.conn.close()
        shutil.rmtree(self.root)

    def test_permission_callbacks_do_not_dirty_or_queue_existing_entries(self):
        pending_before = self.state.conn.execute(
            "SELECT COUNT(*) FROM pending_ops"
        ).fetchone()[0]

        self.assertEqual(self.fs.chmod("/copied.txt", 0o600), 0)
        self.assertEqual(stat.S_IMODE(self.fs.getattr("/copied.txt").st_mode), 0o600)
        self.assertEqual(self.fs.chown("/copied.txt", os.getuid(), os.getgid()), 0)

        entry = self.state.get_entry("/copied.txt")
        self.assertIsNotNone(entry)
        self.assertEqual(entry["dirty"] if entry else None, 0)
        pending_after = self.state.conn.execute(
            "SELECT COUNT(*) FROM pending_ops"
        ).fetchone()[0]
        self.assertEqual(pending_after, pending_before)

    def test_permission_callbacks_reject_missing_paths(self):
        self.assertEqual(self.fs.chmod("/missing.txt", 0o600), -errno.ENOENT)
        self.assertEqual(
            self.fs.chown("/missing.txt", os.getuid(), os.getgid()),
            -errno.ENOENT,
        )


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
        shutil.rmtree(self.root)

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

    def test_shutdown_is_reentrant_for_signal_handler(self):
        # A SIGTERM arriving while shutdown() is already running re-enters it on
        # the same thread, because Python runs signal handlers on the main
        # thread. Simulate that by re-entering from inside the thread join loop.
        reentered = []

        def rejoin(timeout=None):
            if not reentered:
                reentered.append(True)
                self.engine.shutdown()

        worker = Mock()
        worker.join = rejoin
        self.engine.threads = [worker]

        finished = threading.Event()

        def run_shutdown():
            self.engine.shutdown()
            finished.set()

        thread = threading.Thread(target=run_shutdown, daemon=True)
        thread.start()
        thread.join(timeout=10)

        self.assertTrue(
            finished.is_set(),
            "shutdown() deadlocked when re-entered on the same thread",
        )
        self.assertTrue(reentered)

    def test_start_performs_initial_scan_on_first_run(self):
        self.engine.start()

        self.engine.initial_scan.assert_called_once()
        self.engine._reconcile_persistent_cache.assert_not_called()
        self.engine._schedule_all_unhydrated.assert_called_once()
        self.engine._start_background_threads.assert_called_once()

    def test_failed_download_is_retried_with_backoff(self):
        self.engine.ensure_local_file = Mock(side_effect=RuntimeError("500"))
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
        root.data = {}
        root.get_children.return_value = [obsidian]
        self.engine.api.drive.root = root

        snapshot = self.engine._crawl_remote_snapshot()

        self.assertIn("folder-1", snapshot)
        self.assertIn("file-1", snapshot)
        self.assertEqual(snapshot["folder-1"]["path"], "/Obsidian")
        self.assertEqual(snapshot["folder-1"]["type"], "app_library")
        self.assertEqual(snapshot["file-1"]["path"], "/Obsidian/vault.md")

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
        response = Mock()
        response.raw = NoUnboundedReadStream(b"chunked download")
        response.close = Mock()
        node = Mock()
        node.open.return_value = response
        self.engine._node_from_entry = Mock(return_value=node)

        self.engine.ensure_local_file("/docs/a.txt")

        self.assertEqual(self.mirror.read("/docs/a.txt", 100, 0), b"chunked download")
        response.close.assert_called_once()

    def test_ensure_local_file_preserves_consumed_json_response(self):
        payload = b'{"enabled":true}'
        response = Response()
        response.status_code = 200
        response.raw = io.BytesIO(payload)
        self.assertEqual(response.json(), {"enabled": True})
        self.state.upsert_entry(
            {
                "path": "/docs/settings.json",
                "type": "file",
                "parent_path": "/docs",
                "remote_drivewsid": "file-1",
                "size": len(payload),
                "mtime": 123,
                "hydrated": False,
                "dirty": False,
                "tombstone": False,
            }
        )
        node = Mock()
        node.open.return_value = response
        self.engine._node_from_entry = Mock(return_value=node)

        self.engine.ensure_local_file("/docs/settings.json")

        self.assertEqual(self.mirror.read("/docs/settings.json", 100, 0), payload)
        entry = self.state.get_entry("/docs/settings.json")
        self.assertEqual(entry["size"], len(payload))
        self.assertEqual(entry["hydrated"], 1)
        self.assertEqual(entry["dirty"], 0)

    def test_ensure_local_file_uses_real_node_for_consumed_json_response(self):
        payload = b'{"enabled":true}'
        payload += b" " * (696 - len(payload))
        response = Response()
        response.status_code = 200
        response.headers["Content-Type"] = "application/json"
        response.headers["Content-Length"] = str(len(payload))
        response.raw = io.BytesIO(payload)
        self.assertEqual(response.json(), {"enabled": True})
        self.state.upsert_entry(
            {
                "path": "/docs/core-plugins.json",
                "type": "file",
                "parent_path": "/docs",
                "remote_drivewsid": "file-1",
                "remote_docwsid": "doc-1",
                "remote_zone": "zone-1",
                "size": len(payload),
                "mtime": 123,
                "hydrated": False,
                "dirty": False,
                "tombstone": False,
            }
        )
        self.engine.api.drive.get_file.return_value = response

        self.engine.ensure_local_file("/docs/core-plugins.json")

        self.engine.api.drive.get_file.assert_called_once_with(
            "doc-1", zone="zone-1", stream=True
        )
        self.assertEqual(self.mirror.read("/docs/core-plugins.json", 1000, 0), payload)
        entry = self.state.get_entry("/docs/core-plugins.json")
        self.assertEqual(entry["size"], 696)
        self.assertEqual(entry["hydrated"], 1)
        self.assertEqual(entry["dirty"], 0)

    def test_ensure_local_file_preserves_empty_response(self):
        for consumed in (False, True):
            with self.subTest(consumed=consumed):
                path = f"/docs/empty-{consumed}.txt"
                response = Response()
                response.status_code = 200
                response.raw = io.BytesIO(b"")
                if consumed:
                    self.assertEqual(response.content, b"")
                self.state.upsert_entry(
                    {
                        "path": path,
                        "type": "file",
                        "parent_path": "/docs",
                        "remote_drivewsid": "file-1",
                        "size": 0,
                        "mtime": 123,
                        "hydrated": False,
                        "dirty": False,
                        "tombstone": False,
                    }
                )
                node = Mock()
                node.open.return_value = response
                self.engine._node_from_entry = Mock(return_value=node)

                self.engine.ensure_local_file(path)

                self.assertTrue(self.mirror.exists(path))
                self.assertEqual(self.mirror.read(path, 100, 0), b"")
                entry = self.state.get_entry(path)
                self.assertEqual(entry["size"], 0)
                self.assertEqual(entry["hydrated"], 1)
                self.assertEqual(entry["dirty"], 0)

    def _prepare_integrity_download(self, payload, expected_size=5, consumed=False, callback=None):
        path = "/docs/integrity.txt"
        self.mirror.write_atomic_bytes(path, b"cached bytes", mtime=100)
        self.state.upsert_entry(
            {
                "path": path,
                "type": "file",
                "parent_path": "/docs",
                "remote_drivewsid": "file-1",
                "remote_docwsid": "doc-1",
                "remote_etag": "metadata-etag",
                "remote_zone": "zone-1",
                "remote_itemid": "item-1",
                "remote_unified_token": "token-1",
                "size": expected_size,
                "mtime": 123,
                "local_sha256": "previous-checksum",
                "hydrated": False,
                "dirty": False,
                "tombstone": False,
            }
        )
        response = Response()
        response.status_code = 200
        response.headers["Content-Length"] = str(len(payload))
        response.headers["ETag"] = "unrelated-http-etag"
        response.raw = CallbackReadStream(payload, callback)
        if consumed:
            self.assertEqual(response.content, payload)
        response.close = Mock(wraps=response.close)
        self.engine._open_remote_file = Mock(return_value=response)
        return path, response

    def test_hydration_rejects_size_mismatch_without_changing_cache_or_metadata(self):
        for consumed in (False, True):
            for payload, expected_size in ((b"", 5), (b"four", 5), (b"longer", 5), (b"x", 0)):
                with self.subTest(consumed=consumed, payload=payload, expected_size=expected_size):
                    path, response = self._prepare_integrity_download(
                        payload, expected_size, consumed
                    )
                    before = self.state.get_entry(path)
                    with self.assertRaises(OSError):
                        self.engine.ensure_local_file(path)
                    self.assertEqual(self.mirror.read(path, 100, 0), b"cached bytes")
                    self.assertEqual(self.state.get_entry(path), before)
                    self.assertEqual(self.state.get_entry(path)["hydrated"], 0)
                    self.assertEqual(self.state.get_entry(path)["dirty"], 0)
                    self.assertEqual(self.state.get_entry(path)["size"], expected_size)
                    self.assertEqual(int(self.mirror.stat_local(path).st_mtime), 100)
                    self.assertEqual(os.listdir(self.mirror.tmp_dir), [])
                    response.close.assert_called_once()
                    if not consumed:
                        self.assertTrue(response.raw.closed)

    def test_hydration_skips_already_dirty_entry(self):
        path, response = self._prepare_integrity_download(b"valid")
        self.state.mark_dirty(path)
        before = self.state.get_entry(path)

        self.engine.ensure_local_file(path)

        self.assertEqual(self.mirror.read(path, 100, 0), b"cached bytes")
        self.assertEqual(self.state.get_entry(path), before)
        self.engine._open_remote_file.assert_not_called()
        self.assertEqual(os.listdir(self.mirror.tmp_dir), [])

    def test_hydration_rejects_state_changes_during_download(self):
        changes = {
            "dirty": 1,
            "tombstone": 1,
            "hydrated": 1,
            "type": "folder",
            "parent_path": "/other",
            "synced_path": "/other/integrity.txt",
            "remote_drivewsid": "file-2",
            "remote_docwsid": "doc-2",
            "remote_etag": "new-metadata-etag",
            "remote_zone": "zone-2",
            "remote_shareid": {"zone": "shared-zone"},
            "remote_itemid": "item-2",
            "remote_unified_token": "token-2",
            "size": 10,
            "mtime": 456,
        }
        for field, value in changes.items():
            with self.subTest(field=field):
                changed = {}

                def mutate():
                    entry = self.state.get_entry(path)
                    self.state.upsert_entry({**entry, field: value})
                    changed["entry"] = self.state.get_entry(path)
                    if field == "dirty":
                        self.mirror.write_atomic_bytes(path, b"local edit", mtime=200)

                path, response = self._prepare_integrity_download(b"valid", callback=mutate)
                with self.assertRaises(OSError):
                    self.engine.ensure_local_file(path)
                expected = b"local edit" if field == "dirty" else b"cached bytes"
                self.assertEqual(self.mirror.read(path, 100, 0), expected)
                self.assertEqual(self.state.get_entry(path), changed["entry"])
                self.assertEqual(os.listdir(self.mirror.tmp_dir), [])
                response.close.assert_called_once()
                self.assertTrue(response.raw.closed)

    def test_write_after_placeholder_rename_preserves_remote_content(self):
        path, response = self._prepare_integrity_download(b"hello")
        entry = self.state.get_entry(path)
        self.state.upsert_entry({**entry, "local_sha256": None, "synced_path": path})
        self.mirror.materialize_placeholder(path, 5, 123)
        fs = ICloudFS.__new__(ICloudFS)
        fs.state = self.state
        fs.mirror = self.mirror
        fs.sync_engine = self.engine
        fs.logger = Mock()
        fs._is_authenticated = Mock(return_value=True)
        fs._mutation_allowed = Mock(return_value=True)
        renamed = "/docs/renamed.txt"

        self.assertEqual(fs.rename(path, renamed), 0)
        queued = self.state.conn.execute("SELECT * FROM pending_ops").fetchall()
        self.engine.ensure_local_file(renamed)

        self.assertEqual(self.mirror.read(renamed, 100, 0), b"hello")
        self.assertEqual(self.state.get_entry(renamed)["dirty"], 1)
        self.assertEqual(self.state.get_entry(renamed)["synced_path"], path)
        self.assertEqual(self.state.conn.execute("SELECT * FROM pending_ops").fetchall(), queued)
        self.assertEqual(fs.write(renamed, b"X", 0), 1)
        self.assertEqual(self.mirror.read(renamed, 100, 0), b"Xello")
        self.assertEqual(self.state.get_entry(renamed)["dirty"], 1)
        response.close.assert_called_once()

    def test_hydration_rejects_deleted_or_moved_entry_during_download(self):
        for move in (False, True):
            with self.subTest(move=move):
                def mutate():
                    if move:
                        self.state.rename_tree(path, "/docs/moved.txt")
                        self.mirror.rename_path(path, "/docs/moved.txt")
                    else:
                        self.state.remove_entry(path)

                path, response = self._prepare_integrity_download(b"valid", callback=mutate)
                with self.assertRaises(OSError):
                    self.engine.ensure_local_file(path)
                self.assertIsNone(self.state.get_entry(path))
                if move:
                    self.assertFalse(self.mirror.exists(path))
                    self.assertEqual(self.mirror.read("/docs/moved.txt", 100, 0), b"cached bytes")
                    self.assertEqual(self.state.get_entry("/docs/moved.txt")["hydrated"], 0)
                else:
                    self.assertEqual(self.mirror.read(path, 100, 0), b"cached bytes")
                self.assertEqual(os.listdir(self.mirror.tmp_dir), [])
                response.close.assert_called_once()

    def test_hydration_stream_error_preserves_cache_and_closes_response(self):
        def fail():
            raise OSError("download interrupted")

        def interrupt_next_read():
            response.raw.callback = fail

        path, response = self._prepare_integrity_download(b"valid", callback=interrupt_next_read)
        before = self.state.get_entry(path)
        with self.assertRaisesRegex(OSError, "download interrupted"):
            self.engine.ensure_local_file(path)
        self.assertEqual(self.mirror.read(path, 100, 0), b"cached bytes")
        self.assertEqual(self.state.get_entry(path), before)
        self.assertEqual(os.listdir(self.mirror.tmp_dir), [])
        response.close.assert_called_once()
        self.assertTrue(response.raw.closed)

    def test_hydration_cannot_resurrect_in_progress_fuse_unlink(self):
        publication_attempted = threading.Event()
        local_removed = threading.Event()
        download_finished = threading.Event()
        publication_acquired = []
        errors = []
        original_lock = self.state.lock

        class ObservedLock:
            def __enter__(self):
                if (
                    threading.current_thread().name == "integrity-download"
                    and local_removed.is_set()
                    and not publication_attempted.is_set()
                ):
                    acquired = original_lock.acquire(blocking=False)
                    publication_acquired.append(acquired)
                    publication_attempted.set()
                    if not acquired:
                        original_lock.acquire()
                else:
                    original_lock.acquire()
                return self

            def __exit__(self, *args):
                original_lock.release()

        self.state.lock = ObservedLock()
        fs = ICloudFS.__new__(ICloudFS)
        fs.state = self.state
        fs.mirror = self.mirror
        fs.logger = Mock()
        fs._is_authenticated = Mock(return_value=True)
        fs._mutation_allowed = Mock(return_value=True)
        original_remove = self.mirror.remove_file

        def remove_and_pause(target):
            original_remove(target)
            local_removed.set()
            if not publication_attempted.wait(5):
                raise AssertionError("Download did not attempt publication")
            if publication_acquired[0] and not download_finished.wait(5):
                raise AssertionError("Unprotected publication did not finish")

        self.mirror.remove_file = remove_and_pause
        results = []

        def unlink():
            results.append(fs.unlink(path))

        remover = threading.Thread(target=unlink, name="integrity-unlink")

        def start_unlink():
            remover.start()
            if not local_removed.wait(5):
                raise AssertionError("FUSE unlink did not reach the filesystem")

        path, response = self._prepare_integrity_download(b"valid", callback=start_unlink)

        def hydrate():
            try:
                self.engine.ensure_local_file(path)
            except OSError as exc:
                errors.append(exc)
            finally:
                download_finished.set()

        downloader = threading.Thread(target=hydrate, name="integrity-download")
        downloader.start()
        downloader.join(10)
        remover.join(10)
        self.assertFalse(downloader.is_alive())
        self.assertFalse(remover.is_alive())
        self.assertEqual(results, [0])
        self.assertEqual([exc.errno for exc in errors], [errno.EAGAIN])
        self.assertFalse(self.mirror.exists(path))
        self.assertEqual(self.state.get_entry(path)["dirty"], 1)
        self.assertEqual(self.state.get_entry(path)["tombstone"], 1)
        self.assertEqual(os.listdir(self.mirror.tmp_dir), [])
        response.close.assert_called_once()

    def test_hydration_large_download_keeps_streaming_bounded(self):
        payload = b"x" * (2 * IO_CHUNK_SIZE + 17)
        path, response = self._prepare_integrity_download(payload, len(payload))
        response.headers["Content-Length"] = "1"

        self.engine.ensure_local_file(path)

        self.assertEqual(self.mirror.read(path, len(payload), 0), payload)
        self.assertEqual(self.state.get_entry(path)["hydrated"], 1)
        self.assertEqual(self.state.get_entry(path)["size"], len(payload))
        self.assertGreater(len(response.raw.read_sizes), 2)
        self.assertTrue(all(0 < size <= IO_CHUNK_SIZE for size in response.raw.read_sizes))
        response.close.assert_called_once()
        self.assertEqual(os.listdir(self.mirror.tmp_dir), [])

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
        self.fs.apply_permissions_config(None)
        self.fs.GetContext = Mock(return_value={"uid": os.getuid(), "gid": os.getgid()})

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

    def _pending_op_count(self):
        return self.state.conn.execute("SELECT COUNT(*) FROM pending_ops").fetchone()[0]

    def test_allowed_mutations_are_accepted(self):
        self.assertEqual(self.fs.create("/allowed/queued.txt", os.O_WRONLY, 0o644), 0)
        self.assertEqual(self.fs.mkdir("/allowed/newdir", 0o755), 0)
        self.assertEqual(self.fs.create("/allowed/newdir/file.txt", os.O_WRONLY, 0o644), 0)
        self.assertEqual(self.fs.write("/allowed/newdir/file.txt", b"hello", 0), 5)
        self.assertEqual(self.fs.truncate("/allowed/newdir/file.txt", 2), 0)
        self.assertEqual(
            self.fs.rename("/allowed/newdir/file.txt", "/allowed/newdir/renamed.txt"),
            0,
        )
        self.assertEqual(self.fs.unlink("/allowed/newdir/renamed.txt"), 0)
        self.assertEqual(self.fs.rmdir("/allowed/newdir"), 0)
        self.assertGreater(self._pending_op_count(), 0)

    def test_atomic_file_replacement_preserves_destination_remote_identity(self):
        self._add_entry("/allowed/note.md", content=b"old")
        self.assertEqual(self.fs.create("/allowed/note.md.tmp", os.O_WRONLY, 0o644), 0)
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
        self.assertEqual(self.fs.create("/allowed/source.md", os.O_WRONLY, 0o644), 0)
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
            self.fs.create("/allowed/excluded/new.txt", os.O_WRONLY, 0o644),
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
        self.assertEqual(self._pending_op_count(), 0)
        self.assertEqual(
            [entry["path"] for entry in self.state.list_entries()],
            entry_paths_before,
        )
        self.assertEqual(self.mirror.read("/outside/file.txt", 100, 0), b"existing")
        self.assertTrue(self.mirror.exists("/allowed/source.txt"))
        self.assertFalse(self.mirror.exists("/outside/destination.txt"))

    def test_uploader_skips_disallowed_dirty_entries(self):
        self.engine.metadata_ready = True
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

        self.engine._sync_file.assert_called_once_with(
            self.state.get_entry("/allowed/file.txt")
        )

    def test_rename_rejects_allowed_directory_with_excluded_descendant(self):
        self.engine.exclude_paths = ["/allowed/parent/excluded"]
        self._add_entry("/allowed/parent", entry_type="folder")
        self._add_entry("/allowed/parent/excluded", entry_type="folder")
        self._add_entry("/allowed/parent/excluded/file.txt")

        result = self.fs.rename("/allowed/parent", "/allowed/newparent")

        self.assertEqual(result, -errno.EACCES)
        self.assertTrue(self.mirror.exists("/allowed/parent/excluded/file.txt"))
        self.assertFalse(self.mirror.exists("/allowed/newparent"))
        self.assertIsNotNone(self.state.get_entry("/allowed/parent/excluded/file.txt"))
        self.assertIsNone(self.state.get_entry("/allowed/newparent"))

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


if __name__ == "__main__":
    unittest.main()
