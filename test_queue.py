import contextlib
import io
import json
import os
import shutil
import sqlite3
import tempfile
import time
import unittest

import queue_diagnostic
import queue_recovery


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

    def run_queue_summary(self):
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            result = queue_diagnostic.main(
                ["--config", self.config_path, "--summary"]
            )
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
        self.assertIn(
            "Queue recovery: no pending retries, quarantined sync entries, "
            "exhausted hydration, or remote deletions.",
            output,
        )

    def test_reports_unrecorded_fallback_failure_and_summary_count(self):
        conn = self.create_db()
        conn.execute(
            "ALTER TABLE entries ADD COLUMN failed INTEGER NOT NULL DEFAULT 0"
        )
        conn.execute(
            "ALTER TABLE entries ADD COLUMN sync_attempt_count INTEGER NOT NULL DEFAULT 0"
        )
        conn.execute("ALTER TABLE entries ADD COLUMN sync_next_attempt_at INTEGER")
        conn.execute("ALTER TABLE entries ADD COLUMN sync_last_error TEXT")
        conn.execute(
            "ALTER TABLE entries ADD COLUMN hydrate_attempt_count INTEGER NOT NULL DEFAULT 0"
        )
        conn.execute("ALTER TABLE entries ADD COLUMN hydrate_next_attempt_at INTEGER")
        conn.execute("ALTER TABLE entries ADD COLUMN hydrate_last_error TEXT")
        conn.commit()
        conn.close()
        with open(
            os.path.join(self.cache_dir, "unrecorded_failures.log"),
            "w",
            encoding="utf-8",
        ) as handle:
            handle.write(
                '{"timestamp": 1, "path": "/only-in-marker.txt", '
                '"operation": "open", "error": "offline", '
                '"record_error": "database is locked"}\n'
            )

        result, output = self.run_queue()
        summary_result, summary = self.run_queue_summary()

        self.assertEqual(result, 0)
        self.assertEqual(summary_result, 0)
        self.assertIn("Unrecorded foreground hydration failures (1):", output)
        self.assertIn("/only-in-marker.txt", output)
        self.assertIn("queue may be INCOMPLETE", output)
        self.assertIn("1 unrecorded-failures", summary)

    def test_summary_is_unchanged_without_unrecorded_failure_marker(self):
        conn = self.create_db()
        conn.execute(
            "ALTER TABLE entries ADD COLUMN failed INTEGER NOT NULL DEFAULT 0"
        )
        conn.execute(
            "ALTER TABLE entries ADD COLUMN sync_attempt_count INTEGER NOT NULL DEFAULT 0"
        )
        conn.execute("ALTER TABLE entries ADD COLUMN sync_next_attempt_at INTEGER")
        conn.execute("ALTER TABLE entries ADD COLUMN sync_last_error TEXT")
        conn.execute(
            "ALTER TABLE entries ADD COLUMN hydrate_attempt_count INTEGER NOT NULL DEFAULT 0"
        )
        conn.execute("ALTER TABLE entries ADD COLUMN hydrate_next_attempt_at INTEGER")
        conn.execute("ALTER TABLE entries ADD COLUMN hydrate_last_error TEXT")
        conn.commit()
        conn.close()

        report = queue_diagnostic.inspect_queue(self.db_path, self.mirror_dir)
        result, output = self.run_queue_summary()

        self.assertIn("unrecorded_failures", report)
        self.assertEqual(report["unrecorded_failures"], [])
        self.assertEqual(result, 0)
        self.assertEqual(
            output,
            "Queue: 0 sync-ready, 0 sync-pending, 0 hydrate-pending, "
            "0 authentication-blocked, 0 quarantined, 0 hydrate-failed\n",
        )

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

    def test_reports_retry_quarantine_hydration_and_tombstone_details(self):
        conn = self.create_db()
        conn.execute(
            "ALTER TABLE entries ADD COLUMN failed INTEGER NOT NULL DEFAULT 0"
        )
        conn.execute(
            "ALTER TABLE entries ADD COLUMN sync_attempt_count INTEGER NOT NULL DEFAULT 0"
        )
        conn.execute("ALTER TABLE entries ADD COLUMN sync_next_attempt_at INTEGER")
        conn.execute("ALTER TABLE entries ADD COLUMN sync_last_error TEXT")
        conn.execute(
            "ALTER TABLE entries ADD COLUMN hydrate_attempt_count INTEGER NOT NULL DEFAULT 0"
        )
        conn.execute("ALTER TABLE entries ADD COLUMN hydrate_next_attempt_at INTEGER")
        conn.execute("ALTER TABLE entries ADD COLUMN hydrate_last_error TEXT")
        self.add_entry(conn, "/retry.txt", dirty=1)
        self.add_entry(conn, "/ready.txt", dirty=1)
        self.add_entry(conn, "/quarantined.txt", dirty=1)
        self.add_entry(conn, "/hydrate.txt")
        self.add_entry(conn, "/deleted.txt", dirty=1, tombstone=1)
        conn.execute(
            """
            UPDATE entries
            SET sync_attempt_count = 2, sync_next_attempt_at = ?,
                sync_last_error = ?
            WHERE path = ?
            """,
            (int(time.time()) + 60, "temporary outage", "/retry.txt"),
        )
        conn.execute(
            """
            UPDATE entries
            SET failed = 1, sync_attempt_count = 8, sync_last_error = ?
            WHERE path = ?
            """,
            ("Mirror file is missing; restore it or delete through FUSE.", "/quarantined.txt"),
        )
        conn.execute(
            """
            UPDATE entries
            SET hydrate_attempt_count = 8, hydrate_last_error = ?
            WHERE path = ?
            """,
            ("download retry budget exhausted", "/hydrate.txt"),
        )
        conn.commit()
        conn.close()

        result, output = self.run_queue()

        self.assertEqual(result, 0)
        self.assertIn("Pending retries (1):", output)
        self.assertIn(
            "/retry.txt (attempt 2; due ",
            output,
        )
        self.assertIn("temporary outage", output)
        self.assertIn("Pending sync entries (1):", output)
        self.assertIn("/ready.txt", output)
        self.assertIn("Sync quarantine — manual action required (1):", output)
        self.assertIn(
            "/quarantined.txt (attempt 8): Mirror file is missing",
            output,
        )
        self.assertIn("./icloudctl retry '/quarantined.txt'", output)
        self.assertIn("Hydration exhausted (1):", output)
        self.assertIn(
            "/hydrate.txt (attempt 8): download retry budget exhausted",
            output,
        )
        self.assertIn("Tombstones awaiting remote deletion (1):", output)
        self.assertIn("/deleted.txt", output)

    def test_reports_auth_blocked_and_pending_hydration_entries(self):
        conn = self.create_db()
        conn.execute(
            "ALTER TABLE entries ADD COLUMN failed INTEGER NOT NULL DEFAULT 0"
        )
        conn.execute(
            "ALTER TABLE entries ADD COLUMN sync_attempt_count INTEGER NOT NULL DEFAULT 0"
        )
        conn.execute("ALTER TABLE entries ADD COLUMN sync_next_attempt_at INTEGER")
        conn.execute("ALTER TABLE entries ADD COLUMN sync_last_error TEXT")
        conn.execute(
            "ALTER TABLE entries ADD COLUMN hydrate_attempt_count INTEGER NOT NULL DEFAULT 0"
        )
        conn.execute("ALTER TABLE entries ADD COLUMN hydrate_next_attempt_at INTEGER")
        conn.execute("ALTER TABLE entries ADD COLUMN hydrate_last_error TEXT")
        self.add_entry(conn, "/auth-upload.txt", dirty=1)
        self.add_entry(conn, "/auth-download.txt", hydrated=0)
        self.add_entry(conn, "/download-retry.txt", hydrated=0)
        next_attempt = int(time.time()) + 60
        conn.execute(
            """
            UPDATE entries
            SET sync_next_attempt_at = ?, sync_last_error = ?
            WHERE path = ?
            """,
            (next_attempt, "expired session", "/auth-upload.txt"),
        )
        conn.execute(
            """
            UPDATE entries
            SET hydrate_next_attempt_at = ?, hydrate_last_error = ?
            WHERE path = ?
            """,
            (next_attempt, "expired session", "/auth-download.txt"),
        )
        conn.execute(
            """
            UPDATE entries
            SET hydrate_attempt_count = 2, hydrate_next_attempt_at = ?,
                hydrate_last_error = ?
            WHERE path = ?
            """,
            (next_attempt, "temporary download outage", "/download-retry.txt"),
        )
        conn.commit()
        conn.close()

        result, output = self.run_queue()

        self.assertEqual(result, 0)
        self.assertIn("Waiting for authentication (2):", output)
        self.assertIn("/auth-upload.txt (sync): expired session", output)
        self.assertIn("/auth-download.txt (hydration): expired session", output)
        self.assertIn("Remedy: run './icloudctl auth'", output)
        self.assertIn("Pending hydration retries (1):", output)
        self.assertIn(
            "/download-retry.txt (attempt 2; due ",
            output,
        )
        self.assertIn("temporary download outage", output)

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

    def test_recovery_sets_generous_busy_timeout(self):
        conn = self.create_db()
        conn.execute(
            "ALTER TABLE entries ADD COLUMN failed INTEGER NOT NULL DEFAULT 0"
        )
        conn.execute(
            "ALTER TABLE entries ADD COLUMN sync_attempt_count INTEGER NOT NULL DEFAULT 0"
        )
        conn.execute("ALTER TABLE entries ADD COLUMN sync_next_attempt_at INTEGER")
        conn.execute("ALTER TABLE entries ADD COLUMN sync_last_error TEXT")
        conn.execute(
            "ALTER TABLE entries ADD COLUMN hydrate_attempt_count INTEGER NOT NULL DEFAULT 0"
        )
        conn.execute("ALTER TABLE entries ADD COLUMN hydrate_next_attempt_at INTEGER")
        conn.execute("ALTER TABLE entries ADD COLUMN hydrate_last_error TEXT")
        conn.commit()
        conn.close()
        connection = sqlite3.connect(self.db_path)
        wrapped_connection = unittest.mock.Mock(wraps=connection)

        with unittest.mock.patch(
            "queue_recovery.sqlite3.connect",
            return_value=wrapped_connection,
        ):
            queue_recovery.clear_failures(self.db_path)

        self.assertIn(
            unittest.mock.call("PRAGMA busy_timeout = 30000"),
            wrapped_connection.execute.call_args_list,
        )
    def test_retry_clears_marker_when_no_queue_entry_exists(self):
        """Markers exist because the DB write failed, so retry must not
        depend on a matching row to clear them."""
        conn = self.create_db()
        conn.commit()
        conn.close()
        marker_path = os.path.join(
            os.path.dirname(self.db_path),
            queue_recovery.UNRECORDED_FAILURES_FILENAME,
        )
        with open(marker_path, "w", encoding="utf-8") as handle:
            handle.write(
                json.dumps({"path": "/ghost.txt", "operation": "open"}) + "\n"
            )
            handle.write(
                json.dumps({"path": "/keep.txt", "operation": "read"}) + "\n"
            )

        cleared, path, markers_cleared = queue_recovery.clear_failures(
            self.db_path, "/ghost.txt"
        )

        self.assertEqual(cleared, 0)
        self.assertEqual(path, "/ghost.txt")
        self.assertEqual(markers_cleared, 1)
        with open(marker_path, encoding="utf-8") as handle:
            remaining = [json.loads(line)["path"] for line in handle if line.strip()]
        self.assertEqual(remaining, ["/keep.txt"])
    def test_corrupt_marker_line_is_surfaced_not_skipped(self):
        """The fallback log is the last-resort visibility mechanism, so a
        damaged record must never be silently dropped."""
        marker_path = os.path.join(
            os.path.dirname(self.db_path),
            queue_diagnostic.UNRECORDED_FAILURES_FILENAME,
        )
        with open(marker_path, "w", encoding="utf-8") as handle:
            handle.write(json.dumps({"path": "/good.txt"}) + "\n")
            handle.write('{"path": "/trunc.txt", "operat\n')

        failures = queue_diagnostic.read_unrecorded_failures(
            os.path.dirname(self.db_path)
        )

        self.assertEqual(len(failures), 2)
        damaged = [f for f in failures if f.get("damaged")]
        self.assertEqual(len(damaged), 1)
        self.assertIsNone(damaged[0]["path"])

    def test_unreadable_marker_file_is_not_reported_as_none(self):
        marker_path = os.path.join(
            os.path.dirname(self.db_path),
            queue_diagnostic.UNRECORDED_FAILURES_FILENAME,
        )
        with open(marker_path, "w", encoding="utf-8") as handle:
            handle.write(json.dumps({"path": "/x.txt"}) + "\n")

        with unittest.mock.patch(
            "builtins.open", side_effect=OSError("permission denied")
        ):
            failures = queue_diagnostic.read_unrecorded_failures(
                os.path.dirname(self.db_path)
            )

        self.assertEqual(len(failures), 1)
        self.assertTrue(failures[0]["damaged"])

    def test_only_global_retry_discards_an_unattributable_marker(self):
        conn = self.create_db()
        conn.execute(
            "ALTER TABLE entries ADD COLUMN failed INTEGER NOT NULL DEFAULT 0"
        )
        conn.execute(
            "ALTER TABLE entries ADD COLUMN sync_attempt_count INTEGER NOT NULL DEFAULT 0"
        )
        conn.execute("ALTER TABLE entries ADD COLUMN sync_next_attempt_at INTEGER")
        conn.execute("ALTER TABLE entries ADD COLUMN sync_last_error TEXT")
        conn.execute(
            "ALTER TABLE entries ADD COLUMN hydrate_attempt_count INTEGER NOT NULL DEFAULT 0"
        )
        conn.execute("ALTER TABLE entries ADD COLUMN hydrate_next_attempt_at INTEGER")
        conn.execute("ALTER TABLE entries ADD COLUMN hydrate_last_error TEXT")
        conn.commit()
        conn.close()
        marker_path = os.path.join(
            os.path.dirname(self.db_path),
            queue_recovery.UNRECORDED_FAILURES_FILENAME,
        )
        corrupt = '{"path": "/trunc.txt", "operat\n'
        with open(marker_path, "w", encoding="utf-8") as handle:
            handle.write(corrupt)

        # A targeted retry cannot attribute the record, so it must keep it.
        _, _, removed = queue_recovery.clear_failures(self.db_path, "/trunc.txt")
        self.assertEqual(removed, 0)
        with open(marker_path, encoding="utf-8") as handle:
            self.assertEqual(handle.read(), corrupt)

        # A global retry is an explicit reset, so it may clear it.
        _, _, removed = queue_recovery.clear_failures(self.db_path)
        self.assertEqual(removed, 1)
        self.assertFalse(os.path.exists(marker_path))


if __name__ == "__main__":
    unittest.main()
