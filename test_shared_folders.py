"""Tests for the OpenClaw shared-folder engine and presented-permissions layer.

These cover functionality carried forward on top of upstream icloud-linux:
iCloud shared-folder read/write/move/delete and the configurable mount
ownership/permissions used for containerized (Incus/OpenClaw) access.
"""

import contextlib
import io
import json
import os
import shutil
import sqlite3
import stat
import tempfile
import unittest
from unittest.mock import Mock, call, patch

import fuse
from requests import Response

from driver import ICloudFS, ICloudSyncEngine, LocalMirror, SyncState, resolve_permissions_config
from pyicloud.exceptions import PyiCloudAPIResponseException, PyiCloudFailedLoginException


class NoUnboundedReadStream(io.BytesIO):
    def read(self, size=-1):
        if size is None or size < 0:
            raise AssertionError("stream was read without a chunk size")
        return super().read(size)


@contextlib.contextmanager
def temporary_umask(mask):
    previous = os.umask(mask)
    try:
        yield
    finally:
        os.umask(previous)


class SharedFolderMirrorStateTests(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.mkdtemp(prefix="icloud-linux-test-")
        self.mirror = LocalMirror(self.root)
        self.state = SyncState(os.path.join(self.root, "state.sqlite3"))

    def tearDown(self):
        self.state.close()
        shutil.rmtree(self.root)

    def test_mirror_normalizes_created_file_and_directory_modes(self):
        with temporary_umask(0o077):
            self.mirror.create_file("/Obsidian/vault.md")
            self.mirror.write("/Obsidian/vault.md", b"hello", 0)

        note_mode = stat.S_IMODE(self.mirror.stat_local("/Obsidian/vault.md").st_mode)
        dir_mode = stat.S_IMODE(self.mirror.stat_local("/Obsidian").st_mode)

        self.assertEqual(note_mode, 0o644)
        self.assertEqual(dir_mode, 0o755)

    def test_mirror_normalizes_placeholder_and_streamed_download_modes(self):
        with temporary_umask(0o077):
            self.mirror.materialize_placeholder("/docs/a.txt", 16, 123)
            self.mirror.write_atomic_stream("/docs/b.txt", io.BytesIO(b"streamed content"))

        placeholder_mode = stat.S_IMODE(self.mirror.stat_local("/docs/a.txt").st_mode)
        streamed_mode = stat.S_IMODE(self.mirror.stat_local("/docs/b.txt").st_mode)
        dir_mode = stat.S_IMODE(self.mirror.stat_local("/docs").st_mode)

        self.assertEqual(placeholder_mode, 0o644)
        self.assertEqual(streamed_mode, 0o644)
        self.assertEqual(dir_mode, 0o755)

    def test_mirror_uses_custom_modes_for_shared_write_layouts(self):
        shared_mirror = LocalMirror(self.root, file_mode=0o666, dir_mode=0o777)

        with temporary_umask(0o077):
            shared_mirror.create_file("/docs/a.txt")

        note_mode = stat.S_IMODE(shared_mirror.stat_local("/docs/a.txt").st_mode)
        dir_mode = stat.S_IMODE(shared_mirror.stat_local("/docs").st_mode)

        self.assertEqual(note_mode, 0o666)
        self.assertEqual(dir_mode, 0o777)

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
                "remote_itemid": "item-1",
                "remote_unified_token": "token-1",
                "hydrated": False,
                "dirty": False,
                "tombstone": False,
                "synced_path": "/shared/a.txt",
            }
        )

        entry = self.state.get_entry("/shared/a.txt")

        self.assertEqual(entry["remote_shareid"], {"share-zone": "abc"})
        self.assertEqual(entry["remote_itemid"], "item-1")
        self.assertEqual(entry["remote_unified_token"], "token-1")

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

        column_names = {column["name"] for column in columns}
        self.assertIn("remote_shareid", column_names)
        self.assertIn("remote_itemid", column_names)
        self.assertIn("remote_unified_token", column_names)
        migrated.close()


class SharedFolderEngineTests(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.mkdtemp(prefix="icloud-linux-test-")
        self.mirror = LocalMirror(self.root)
        self.state = SyncState(os.path.join(self.root, "state.sqlite3"))
        self.logger = Mock()
        api = Mock()
        api.drive.service_root = "https://example.invalid/drivews"
        api.drive.params = {"clientId": "test-client"}
        api.drive.session = Mock()
        api.drive._raise_if_error = Mock()
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
                "remote_itemid": "item-1",
                "remote_unified_token": "token-1",
                "size": 5,
            }
        )

        self.engine.api.drive.get_node_data.assert_not_called()
        self.assertEqual(node.data["docwsid"], "doc-1")
        self.assertEqual(node.data["shareID"], shareid)
        self.assertEqual(node.data["item_id"], "item-1")
        self.assertEqual(node.data["unifiedToken"], "token-1")
        self.assertEqual(node.data["size"], 5)

    def test_node_to_meta_inherits_parent_shareid_for_shared_child(self):
        shareid = {"share-zone": "abc"}
        node = Mock()
        node.data = {
            "type": "FOLDER",
            "drivewsid": "folder-1",
            "docwsid": "documents",
            "etag": "etag-folder",
            "zone": "zone-1",
            "item_id": "item-1",
            "unifiedToken": "token-1",
            "dateModified": "2026-04-06T00:00:00Z",
        }

        meta = self.engine._node_to_meta(
            node,
            "/Shared/child",
            inherited_shareid=shareid,
        )

        self.assertEqual(meta["remote_shareid"], shareid)
        self.assertEqual(meta["remote_itemid"], "item-1")
        self.assertEqual(meta["remote_unified_token"], "token-1")

    def test_ensure_local_file_downloads_shared_file_via_item_endpoint(self):
        shareid = {"share-zone": "abc"}
        self.state.upsert_entry(
            {
                "path": "/Shared/a.txt",
                "type": "file",
                "parent_path": "/Shared",
                "remote_drivewsid": "file-1",
                "remote_docwsid": "doc-shared",
                "remote_zone": "zone-1",
                "remote_shareid": shareid,
                "remote_itemid": "item-shared",
                "size": 14,
                "mtime": 123,
                "hydrated": False,
                "dirty": False,
                "tombstone": False,
                "synced_path": "/Shared/a.txt",
            }
        )
        self.mirror.ensure_dir("/Shared")
        meta_response = Mock()
        meta_response.json.return_value = {
            "item_info": {
                "urls": {
                    "url_download": "https://download.example/shared-a.txt",
                }
            }
        }
        response = Response()
        response.status_code = 200
        response.headers["Content-Length"] = "14"
        response.raw = NoUnboundedReadStream(b"shared content")
        response.close = Mock(wraps=response.close)
        node = Mock()
        node.name = "a.txt"
        node.data = {
            "drivewsid": "file-1",
            "docwsid": "doc-shared",
            "zone": "zone-1",
            "shareID": shareid,
            "item_id": "item-shared",
        }
        self.engine._node_from_entry = Mock(return_value=node)
        self.engine.api.drive.session.get.side_effect = [meta_response, response]

        self.engine.ensure_local_file("/Shared/a.txt")

        self.assertEqual(self.mirror.read("/Shared/a.txt", 100, 0), b"shared content")
        refreshed = self.state.get_entry("/Shared/a.txt")
        self.assertTrue(refreshed["hydrated"])
        first_call = self.engine.api.drive.session.get.call_args_list[0]
        self.assertTrue(first_call.args[0].endswith("/v1/item/item-shared"))
        self.assertEqual(first_call.kwargs["timeout"], (10, 60))
        self.assertEqual(
            self.engine.api.drive._raise_if_error.call_args_list,
            [call(meta_response), call(response)],
        )
        second_call = self.engine.api.drive.session.get.call_args_list[1]
        self.assertEqual(second_call.args[0], "https://download.example/shared-a.txt")
        self.assertEqual(second_call.kwargs["timeout"], (10, 300))
        node.open.assert_not_called()
        response.close.assert_called_once()

    def test_sync_file_uses_share_aware_upload_for_new_shared_file(self):
        shareid = {"share-zone": "abc"}
        self.mirror.create_file("/Shared/note.md")
        self.mirror.write("/Shared/note.md", b"hello world", 0)
        self.state.upsert_entry(
            {
                "path": "/Shared",
                "type": "folder",
                "parent_path": "/",
                "remote_drivewsid": "shared-root",
                "remote_docwsid": "shared-doc",
                "remote_shareid": shareid,
                "remote_zone": "zone-1",
                "hydrated": True,
                "dirty": False,
                "tombstone": False,
                "synced_path": "/Shared",
            }
        )
        self.state.upsert_entry(
            {
                "path": "/Shared/note.md",
                "type": "file",
                "parent_path": "/Shared",
                "remote_drivewsid": None,
                "hydrated": True,
                "dirty": True,
                "tombstone": False,
                "synced_path": "/Shared/note.md",
            }
        )
        parent_node = Mock()
        parent_node.data = {
            "drivewsid": "shared-root",
            "docwsid": "shared-doc",
            "shareID": shareid,
            "zone": "zone-1",
        }
        self.engine._ensure_remote_parent = Mock(return_value=parent_node)
        self.engine.ensure_local_file = Mock()
        self.engine._upload_file_to_parent = Mock()
        self.engine._reconcile_child_meta = Mock(
            return_value={
                "path": "/Shared/note.md",
                "type": "file",
                "parent_path": "/Shared",
                "remote_drivewsid": "file-1",
                "remote_docwsid": "doc-1",
                "remote_etag": "etag-1",
                "remote_zone": "zone-1",
                "remote_shareid": shareid,
                "size": 11,
                "mtime": 123,
            }
        )

        self.engine._sync_file(self.state.get_entry("/Shared/note.md"))

        self.engine._upload_file_to_parent.assert_called_once()
        self.assertIs(
            self.engine._upload_file_to_parent.call_args[0][0],
            parent_node,
        )
        stream = self.engine._upload_file_to_parent.call_args[0][1]
        self.assertEqual(stream.__class__.__name__, "NamedFileStream")
        self.assertEqual(stream.name, "note.md")

    def test_sync_file_replaces_existing_shared_file_via_item_trash_then_upload(self):
        shareid = {"share-zone": "abc"}
        self.mirror.create_file("/Shared/note.md")
        self.mirror.write("/Shared/note.md", b"hello world", 0)
        self.state.upsert_entry(
            {
                "path": "/Shared/note.md",
                "type": "file",
                "parent_path": "/Shared",
                "remote_drivewsid": "file-1",
                "remote_docwsid": "doc-1",
                "remote_etag": "etag-1",
                "remote_zone": "zone-1",
                "remote_shareid": shareid,
                "remote_itemid": "item-1",
                "hydrated": True,
                "dirty": True,
                "tombstone": False,
                "synced_path": "/Shared/note.md",
            }
        )
        parent_node = Mock()
        parent_node.data = {"drivewsid": "shared-root", "shareID": shareid}
        old_node = Mock()
        old_node.data = {"drivewsid": "file-1", "shareID": shareid, "item_id": "item-1"}
        self.engine._ensure_remote_parent = Mock(return_value=parent_node)
        self.engine._node_from_entry = Mock(return_value=old_node)
        self.engine.ensure_local_file = Mock()
        self.engine._reconcile_child_meta = Mock(
            return_value={
                "path": "/Shared/note.md",
                "type": "file",
                "parent_path": "/Shared",
                "remote_drivewsid": "file-2",
                "remote_docwsid": "doc-2",
                "remote_etag": "etag-2",
                "remote_zone": "zone-1",
                "remote_shareid": shareid,
                "remote_itemid": "item-2",
                "size": 11,
                "mtime": 123,
            }
        )
        calls = []
        self.engine._delete_remote_node = Mock(side_effect=lambda node: calls.append(("delete", node)))
        self.engine._upload_file_to_parent = Mock(
            side_effect=lambda parent, stream: calls.append(("upload", parent, stream.name))
        )

        self.engine._sync_file(self.state.get_entry("/Shared/note.md"))

        self.assertEqual([call[0] for call in calls], ["delete", "upload"])
        self.assertEqual(calls[1][2], "note.md")

    def test_sync_file_renamed_shared_file_uploads_before_deleting_old_remote(self):
        shareid = {"share-zone": "abc"}
        self.mirror.create_file("/SharedRenamed/note.md")
        self.mirror.write("/SharedRenamed/note.md", b"hello world", 0)
        self.state.upsert_entry(
            {
                "path": "/SharedRenamed/note.md",
                "type": "file",
                "parent_path": "/SharedRenamed",
                "remote_drivewsid": "file-1",
                "remote_docwsid": "doc-1",
                "remote_etag": "etag-1",
                "remote_zone": "zone-1",
                "remote_shareid": shareid,
                "remote_itemid": "item-1",
                "hydrated": True,
                "dirty": True,
                "tombstone": False,
                "synced_path": "/SharedOld/note.md",
            }
        )
        parent_node = Mock()
        parent_node.data = {"drivewsid": "shared-root", "shareID": shareid}
        old_node = Mock()
        old_node.data = {"drivewsid": "file-1", "shareID": shareid, "item_id": "item-1"}
        self.engine._ensure_remote_parent = Mock(return_value=parent_node)
        self.engine._node_from_entry = Mock(return_value=old_node)
        self.engine.ensure_local_file = Mock()
        self.engine._sync_move_or_rename = Mock()
        self.engine._reconcile_child_meta = Mock(
            return_value={
                "path": "/SharedRenamed/note.md",
                "type": "file",
                "parent_path": "/SharedRenamed",
                "remote_drivewsid": "file-2",
                "remote_docwsid": "doc-2",
                "remote_etag": "etag-2",
                "remote_zone": "zone-1",
                "remote_shareid": shareid,
                "remote_itemid": "item-2",
                "size": 11,
                "mtime": 123,
            }
        )
        calls = []
        self.engine._upload_file_to_parent = Mock(
            side_effect=lambda parent, stream: calls.append(("upload", parent, stream.name))
        )
        self.engine._delete_remote_node = Mock(side_effect=lambda node: calls.append(("delete", node)))

        self.engine._sync_file(self.state.get_entry("/SharedRenamed/note.md"))

        self.assertEqual([call[0] for call in calls], ["upload", "delete"])
        self.engine._sync_move_or_rename.assert_not_called()

    def test_create_remote_directory_includes_shareid_for_shared_parent(self):
        shareid = {"share-zone": "abc"}
        parent_node = Mock()
        parent_node.data = {"drivewsid": "shared-root", "shareID": shareid}
        response = Mock()
        response.json.return_value = {
            "folders": [
                {
                    "drivewsid": "folder-1",
                    "docwsid": "documents",
                    "etag": "etag-1",
                    "zone": "zone-1",
                    "name": "child",
                    "type": "FOLDER",
                }
            ]
        }
        self.engine.api.drive.session.post = Mock(return_value=response)
        self.engine.api.drive._raise_if_error = Mock()

        node = self.engine._create_remote_directory(parent_node, "child")

        request_json = self.engine.api.drive.session.post.call_args.kwargs["json"]
        self.assertEqual(request_json["shareID"], shareid)
        self.assertEqual(request_json["destinationDrivewsId"], "shared-root")
        self.assertEqual(
            self.engine.api.drive.session.post.call_args.kwargs["timeout"],
            (10, 60),
        )
        self.assertEqual(node.data["drivewsid"], "folder-1")
        self.assertEqual(node.data["shareID"], shareid)

    def test_move_remote_nodes_includes_shareid_for_shared_destination(self):
        shareid = {"share-zone": "abc"}
        node = Mock()
        node.data = {"drivewsid": "file-1", "etag": "etag-1"}
        destination = Mock()
        destination.data = {"drivewsid": "shared-root", "shareID": shareid}
        response = Mock()
        response.json.return_value = {"items": []}
        self.engine.api.drive.session.post = Mock(return_value=response)
        self.engine.api.drive._raise_if_error = Mock()

        self.engine._move_remote_nodes([node], destination)

        request_json = self.engine.api.drive.session.post.call_args.kwargs["json"]
        self.assertEqual(request_json["shareID"], shareid)
        self.assertEqual(request_json["destinationDrivewsId"], "shared-root")
        self.assertEqual(request_json["items"][0]["drivewsid"], "file-1")

    def test_upload_file_to_parent_stages_shared_uploads_via_root(self):
        shareid = {"share-zone": "abc"}
        parent_node = Mock()
        parent_node.data = {"drivewsid": "shared-root", "shareID": shareid}
        root_node = Mock()
        staging_node = Mock()
        staging_node.name = ".icloud-linux-stage-test"
        staged_child = Mock()
        staged_child.name = "note.md"
        staged_child.data = {"drivewsid": "file-1", "etag": "etag-1"}
        staging_node.get_children.side_effect = [[staged_child], []]
        self.engine.api.drive.root = root_node
        self.engine._create_remote_directory = Mock(return_value=staging_node)
        self.engine._move_remote_nodes = Mock()
        stream = io.BytesIO(b"hello")
        stream.name = "note.md"

        self.engine._upload_file_to_parent(parent_node, stream)

        create_args = self.engine._create_remote_directory.call_args[0]
        self.assertIs(create_args[0], root_node)
        self.assertTrue(create_args[1].startswith(".icloud-linux-stage-"))
        staging_node.upload.assert_called_once_with(stream)
        self.engine._move_remote_nodes.assert_called_once_with([staged_child], parent_node)
        staging_node.delete.assert_called_once()

    def test_delete_remote_node_uses_document_item_trash_for_shared_nodes(self):
        node = Mock()
        node.name = "shared-folder"
        node.data = {
            "drivewsid": "folder-1",
            "shareID": {"share-zone": "abc"},
            "item_id": "item-1",
        }
        response = Mock()
        response.json.return_value = {"item_id": "item-1"}
        self.engine.api.drive.session.put = Mock(return_value=response)
        self.engine.api.drive._raise_if_error = Mock()

        self.engine._delete_remote_node(node)

        call = self.engine.api.drive.session.put.call_args
        self.assertTrue(call.args[0].endswith("/v1/item/item-1"))
        self.assertEqual(call.kwargs["headers"]["Content-Type"], "text/plain")
        self.assertEqual(call.kwargs["timeout"], (10, 60))
        self.assertEqual(
            json.loads(call.kwargs["data"]),
            {"info_to_update": {"parent_item_id": "trash"}},
        )

    def test_sync_shared_directory_recreates_remote_folder_and_trashes_old_one(self):
        shareid = {"share-zone": "abc"}
        self.state.upsert_entry(
            {
                "path": "/Shared/renamed",
                "type": "folder",
                "parent_path": "/Shared",
                "remote_drivewsid": "folder-old",
                "remote_docwsid": "doc-old",
                "remote_etag": "etag-old",
                "remote_zone": "zone-1",
                "remote_shareid": shareid,
                "remote_itemid": "item-old",
                "hydrated": True,
                "dirty": True,
                "tombstone": False,
                "synced_path": "/Shared/original",
            }
        )
        entry = self.state.get_entry("/Shared/renamed")
        parent_node = Mock()
        parent_node.data = {"drivewsid": "shared-root", "shareID": shareid}
        old_node = Mock()
        old_node.data = {"drivewsid": "folder-old", "shareID": shareid, "item_id": "item-old"}
        child = Mock()
        old_node.get_children.return_value = [child]
        new_node = Mock()
        new_node.data = {
            "type": "FOLDER",
            "drivewsid": "folder-new",
            "docwsid": "doc-new",
            "etag": "etag-new",
            "zone": "zone-1",
            "shareID": shareid,
            "item_id": "item-new",
            "name": "renamed",
            "dateModified": "2026-04-06T00:00:00Z",
        }
        self.engine._node_from_entry = Mock(return_value=old_node)
        self.engine._create_remote_directory = Mock(return_value=new_node)
        self.engine._move_remote_nodes = Mock()
        self.engine._delete_remote_node = Mock()

        self.engine._sync_shared_directory(entry, parent_node)

        self.engine._move_remote_nodes.assert_called_once_with([child], new_node)
        self.engine._delete_remote_node.assert_called_once_with(old_node)
        updated = self.state.get_entry("/Shared/renamed")
        self.assertEqual(updated["remote_drivewsid"], "folder-new")
        self.assertEqual(updated["remote_itemid"], "item-new")
        self.assertEqual(updated["synced_path"], "/Shared/renamed")

    def test_reconcile_child_meta_falls_back_to_remote_snapshot_for_shared_parent(self):
        shareid = {"share-zone": "abc"}
        self.state.upsert_entry(
            {
                "path": "/Shared",
                "type": "folder",
                "parent_path": "/",
                "remote_drivewsid": "shared-root",
                "remote_shareid": shareid,
                "hydrated": True,
                "dirty": False,
                "tombstone": False,
                "synced_path": "/Shared",
            }
        )
        self.engine._refresh_child_meta = Mock(side_effect=KeyError("Missing child child under /Shared"))
        self.engine._crawl_remote_snapshot = Mock(
            return_value={
                "folder-1": {
                    "path": "/Shared/child",
                    "type": "folder",
                    "parent_path": "/Shared",
                    "remote_drivewsid": "folder-1",
                    "remote_docwsid": "documents",
                    "remote_etag": "etag-folder",
                    "remote_zone": "zone-1",
                    "remote_shareid": shareid,
                    "size": 0,
                    "mtime": 123,
                }
            }
        )

        def apply_snapshot(snapshot):
            self.state.upsert_entry(
                {
                    **snapshot["folder-1"],
                    "hydrated": True,
                    "dirty": False,
                    "tombstone": False,
                    "synced_path": "/Shared/child",
                }
            )

        self.engine._apply_remote_snapshot = Mock(side_effect=apply_snapshot)

        meta = self.engine._reconcile_child_meta("/Shared", "child")

        self.assertEqual(meta["remote_drivewsid"], "folder-1")
        self.assertEqual(meta["remote_shareid"], shareid)
        self.engine._crawl_remote_snapshot.assert_called_once()

    def test_crawl_remote_snapshot_propagates_shareid_to_descendants(self):
        shareid = {"share-zone": "abc"}
        note = Mock()
        note.name = "note.md"
        note.data = {
            "type": "FILE",
            "drivewsid": "file-1",
            "docwsid": "doc-1",
            "etag": "etag-1",
            "zone": "zone-1",
            "size": 12,
            "dateModified": "2026-04-06T00:00:00Z",
        }
        child_folder = Mock()
        child_folder.name = "child"
        child_folder.data = {
            "type": "FOLDER",
            "drivewsid": "folder-2",
            "docwsid": "documents",
            "etag": "etag-child",
            "zone": "zone-1",
            "dateModified": "2026-04-06T00:00:00Z",
        }
        child_folder.get_children.return_value = [note]
        shared_root = Mock()
        shared_root.name = "Shared"
        shared_root.data = {
            "type": "FOLDER",
            "drivewsid": "shared-root",
            "docwsid": "documents",
            "etag": "etag-root",
            "zone": "zone-1",
            "shareID": shareid,
            "dateModified": "2026-04-06T00:00:00Z",
        }
        shared_root.get_children.return_value = [child_folder]
        root = Mock()
        root.data = {"drivewsid": "root"}
        root.get_children.return_value = [shared_root]
        self.engine.api.drive.root = root

        snapshot = self.engine._crawl_remote_snapshot()

        self.assertEqual(snapshot["folder-2"]["remote_shareid"], shareid)
        self.assertEqual(snapshot["file-1"]["remote_shareid"], shareid)

    def test_reconcile_persistent_cache_normalizes_existing_modes(self):
        self.state.upsert_entry(
            {
                "path": "/docs/a.txt",
                "type": "file",
                "parent_path": "/docs",
                "size": 5,
                "mtime": 123,
                "hydrated": True,
                "dirty": False,
                "tombstone": False,
                "synced_path": "/docs/a.txt",
            }
        )
        self.mirror.write("/docs/a.txt", b"hello", 0)
        os.chmod(self.mirror.local_path("/docs"), 0o700)
        os.chmod(self.mirror.local_path("/docs/a.txt"), 0o600)

        self.engine._reconcile_persistent_cache = ICloudSyncEngine._reconcile_persistent_cache.__get__(self.engine)
        self.engine._reconcile_persistent_cache()

        note_mode = stat.S_IMODE(self.mirror.stat_local("/docs/a.txt").st_mode)
        dir_mode = stat.S_IMODE(self.mirror.stat_local("/docs").st_mode)

        self.assertEqual(note_mode, 0o644)
        self.assertEqual(dir_mode, 0o755)


class FuseOptionTests(unittest.TestCase):
    def test_apply_fuse_options_adds_enabled_options(self):
        fs = ICloudFS(version="%prog " + fuse.__version__, usage="%prog [options] mountpoint", dash_s_do="setsingle")

        fs.apply_fuse_options(
            {
                "allow_other": True,
                "nonempty": True,
                "ro": False,
                "fsname": "icloud-linux",
            }
        )

        self.assertIn("allow_other", fs.fuse_args.optlist)
        self.assertIn("nonempty", fs.fuse_args.optlist)
        self.assertNotIn("ro", fs.fuse_args.optlist)
        self.assertEqual(fs.fuse_args.optdict["fsname"], "icloud-linux")

    def test_apply_fuse_options_rejects_non_mapping(self):
        fs = ICloudFS(version="%prog " + fuse.__version__, usage="%prog [options] mountpoint", dash_s_do="setsingle")

        with self.assertRaises(ValueError):
            fs.apply_fuse_options(["allow_other"])


class PermissionConfigTests(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.mkdtemp(prefix="icloud-linux-test-")
        self.mirror = LocalMirror(self.root)
        self.state = SyncState(os.path.join(self.root, "state.sqlite3"))
        self.fs = ICloudFS(version="%prog " + fuse.__version__, usage="%prog [options] mountpoint", dash_s_do="setsingle")
        self.fs.mirror = self.mirror
        self.fs.state = self.state

    def tearDown(self):
        self.state.close()
        shutil.rmtree(self.root)

    def test_apply_permissions_config_controls_presented_attrs(self):
        self.fs.apply_permissions_config(
            {
                "uid": 2001,
                "gid": 3001,
                "file_mode": "0666",
                "dir_mode": "0777",
            }
        )
        self.fs.mirror = LocalMirror(self.root, file_mode=self.fs.file_mode, dir_mode=self.fs.dir_mode)
        self.fs.mirror.ensure_dir("/docs")
        self.fs.mirror.create_file("/docs/a.txt")

        dir_attrs = self.fs.getattr("/docs")
        file_attrs = self.fs.getattr("/docs/a.txt")

        self.assertEqual(stat.S_IMODE(dir_attrs.st_mode), 0o777)
        self.assertEqual(stat.S_IMODE(file_attrs.st_mode), 0o666)
        self.assertEqual(dir_attrs.st_uid, 2001)
        self.assertEqual(dir_attrs.st_gid, 3001)
        self.assertEqual(file_attrs.st_uid, 2001)
        self.assertEqual(file_attrs.st_gid, 3001)

    def test_resolve_permissions_config_uses_defaults(self):
        resolved = resolve_permissions_config(None)

        self.assertEqual(resolved["file_mode"], 0o644)
        self.assertEqual(resolved["dir_mode"], 0o755)
        self.assertEqual(resolved["uid"], os.getuid())
        self.assertEqual(resolved["gid"], os.getgid())

    def test_resolve_permissions_config_rejects_invalid_mode(self):
        with self.assertRaises(ValueError):
            resolve_permissions_config({"dir_mode": "not-a-mode"})


if __name__ == "__main__":
    unittest.main()


if __name__ == "__main__":
    unittest.main()
