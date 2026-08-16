import contextlib
import io
import os
import shutil
import sqlite3
import tempfile
import time
import unittest

import queue_diagnostic


ENTRY_SCHEMA = """
CREATE TABLE entries (
    path TEXT PRIMARY KEY,
    type TEXT,
    parent_path TEXT NOT NULL,
    hydrated INTEGER NOT NULL DEFAULT 0,
    dirty INTEGER NOT NULL DEFAULT 0,
    tombstone INTEGER NOT NULL DEFAULT 0,
    last_synced_at INTEGER
)
"""


class QueueDiagnosticTests(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.mkdtemp(prefix="icloud-linux-queue-test-")
        self.cache_dir = os.path.join(self.root, "cache")
        self.mirror_dir = os.path.join(self.cache_dir, "mirror")
        self.db_path = os.path.join(self.cache_dir, "state.sqlite3")
        self.config_path = os.path.join(self.root, "config.yaml")
        os.makedirs(self.mirror_dir)
        with open(self.config_path, "w", encoding="utf-8") as handle:
            handle.write(f"cache_dir: {self.cache_dir}\n")

    def tearDown(self):
        shutil.rmtree(self.root)

    def create_db(self):
        conn = sqlite3.connect(self.db_path)
        conn.execute(ENTRY_SCHEMA)
        return conn

    def add_entry(
        self,
        conn,
        path,
        entry_type="file",
        hydrated=1,
        dirty=0,
        tombstone=0,
        last_synced_at=None,
    ):
        conn.execute(
            """
            INSERT INTO entries (
                path, type, parent_path, hydrated, dirty, tombstone, last_synced_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                path,
                entry_type,
                os.path.dirname(path) or "/",
                hydrated,
                dirty,
                tombstone,
                last_synced_at,
            ),
        )

    def run_queue(self):
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            result = queue_diagnostic.main(["--config", self.config_path])
        return result, output.getvalue()

    def test_reports_clean_database(self):
        conn = self.create_db()
        self.add_entry(conn, "/documents", entry_type="folder")
        self.add_entry(conn, "/documents/kept.txt", dirty=1)
        conn.commit()
        conn.close()
        os.makedirs(os.path.join(self.mirror_dir, "documents"))
        open(os.path.join(self.mirror_dir, "documents", "kept.txt"), "wb").close()

        result, output = self.run_queue()

        self.assertEqual(result, 0)
        self.assertIn("Total entries: 2", output)
        self.assertIn("file: 1", output)
        self.assertIn("folder: 1", output)
        self.assertIn("Dirty entries: 1", output)
        self.assertIn("At-risk dirty files with missing mirror files: 0", output)

    def test_reports_dirty_files_missing_from_mirror(self):
        conn = self.create_db()
        oldest = int(time.time()) - 7200
        self.add_entry(
            conn,
            "/missing.txt",
            hydrated=0,
            dirty=1,
            last_synced_at=oldest,
        )
        self.add_entry(conn, "/also-missing.txt", dirty=1)
        self.add_entry(conn, "/already-deleted.txt", dirty=1, tombstone=1)
        conn.commit()
        conn.close()

        result, output = self.run_queue()

        self.assertEqual(result, 0)
        self.assertIn("At-risk dirty files with missing mirror files: 2", output)
        self.assertIn("/also-missing.txt", output)
        self.assertIn("/missing.txt", output)
        self.assertIn("Dirty and unhydrated: 1", output)
        self.assertIn("Oldest pending item:", output)

    def test_distinguishes_pending_retries_from_quarantined_entries(self):
        conn = self.create_db()
        conn.execute(
            "ALTER TABLE entries ADD COLUMN failed INTEGER NOT NULL DEFAULT 0"
        )
        conn.execute(
            "ALTER TABLE entries ADD COLUMN sync_attempt_count INTEGER NOT NULL DEFAULT 0"
        )
        conn.execute("ALTER TABLE entries ADD COLUMN sync_last_error TEXT")
        self.add_entry(conn, "/retry.txt", dirty=1)
        self.add_entry(conn, "/quarantined.txt", dirty=1)
        conn.execute(
            """
            UPDATE entries
            SET sync_attempt_count = 1, sync_last_error = ?
            WHERE path = ?
            """,
            ("temporary outage", "/retry.txt"),
        )
        conn.execute(
            """
            UPDATE entries
            SET failed = 1, sync_attempt_count = 8, sync_last_error = ?
            WHERE path = ?
            """,
            ("Mirror file is missing; restore it or delete through FUSE.", "/quarantined.txt"),
        )
        conn.commit()
        conn.close()

        result, output = self.run_queue()

        self.assertEqual(result, 0)
        self.assertIn("Pending retries: 1", output)
        self.assertIn(
            "/retry.txt (failed=0): temporary outage",
            output,
        )
        self.assertIn("Quarantined entries: 1", output)
        self.assertIn(
            "/quarantined.txt (failed=1): Mirror file is missing",
            output,
        )

    def test_reports_missing_app_library_as_at_risk(self):
        conn = self.create_db()
        self.add_entry(conn, "/library", entry_type="app_library", dirty=1)
        conn.commit()
        conn.close()

        result, output = self.run_queue()

        self.assertEqual(result, 0)
        self.assertIn("At-risk dirty files with missing mirror files: 1", output)
        self.assertIn("/library", output)

    def test_does_not_report_missing_folder_as_at_risk(self):
        conn = self.create_db()
        self.add_entry(conn, "/folder", entry_type="folder", dirty=1)
        conn.commit()
        conn.close()

        result, output = self.run_queue()

        self.assertEqual(result, 0)
        self.assertIn("At-risk dirty files with missing mirror files: 0", output)
        self.assertNotIn("  /folder", output)

    def test_reports_missing_null_typed_entry_as_at_risk(self):
        conn = self.create_db()
        self.add_entry(conn, "/unknown", entry_type=None, dirty=1)
        conn.commit()
        conn.close()

        result, output = self.run_queue()

        self.assertEqual(result, 0)
        self.assertIn("At-risk dirty files with missing mirror files: 1", output)
        self.assertIn("/unknown", output)

    def test_reports_missing_database_without_failure(self):
        result, output = self.run_queue()

        self.assertEqual(result, 0)
        self.assertIn("state DB not found", output)

    def test_read_only_connection_cannot_write(self):
        conn = self.create_db()
        conn.commit()
        conn.close()

        readonly = queue_diagnostic.open_read_only(self.db_path)
        try:
            with self.assertRaises(sqlite3.OperationalError):
                readonly.execute(
                    "INSERT INTO entries (path, type, parent_path) VALUES ('/x', 'file', '/')"
                )
        finally:
            readonly.close()

    def test_unexpected_schema_is_reported_without_crashing(self):
        conn = sqlite3.connect(self.db_path)
        conn.execute("CREATE TABLE unrelated (id INTEGER)")
        conn.commit()
        conn.close()

        result, output = self.run_queue()

        self.assertEqual(result, 0)
        self.assertIn("entries table is missing", output)


if __name__ == "__main__":
    unittest.main()
