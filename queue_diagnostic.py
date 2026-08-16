#!/usr/bin/env python3
"""Report the local iCloud sync queue without changing its state."""

import argparse
import os
from pathlib import Path
import sqlite3
import sys
import time

import yaml


MISSING_PATH_LIMIT = 50


def load_config(config_path):
    with open(config_path, "r", encoding="utf-8") as handle:
        return yaml.safe_load(handle) or {}


def open_read_only(db_path):
    """Open an existing SQLite database without write access."""
    uri = Path(db_path).resolve().as_uri() + "?mode=ro"
    conn = sqlite3.connect(uri, uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def mirror_path(mirror_root, path):
    """Map an iCloud path to the local path used by LocalMirror."""
    normalized = os.path.normpath(path)
    if normalized == ".":
        normalized = "/"
    if not normalized.startswith("/"):
        normalized = "/" + normalized
    local = os.path.abspath(os.path.join(mirror_root, normalized.lstrip("/")))
    root = os.path.abspath(mirror_root)
    if local != root and not local.startswith(root + os.sep):
        raise ValueError(f"Path escapes mirror root: {path}")
    return local


def _count(conn, query):
    return conn.execute(query).fetchone()[0]


def inspect_queue(db_path, mirror_root, now=None):
    """Collect queue diagnostics from an already-existing read-only database."""
    now = time.time() if now is None else now
    conn = open_read_only(db_path)
    try:
        tables = {
            row["name"]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            )
        }
        if "entries" not in tables:
            return {"schema_error": "entries table is missing"}

        columns = {
            row["name"] for row in conn.execute("PRAGMA table_info(entries)")
        }
        report = {"columns": columns, "total": _count(conn, "SELECT COUNT(*) FROM entries")}

        if "type" in columns:
            report["by_type"] = {
                row["type"]: row["count"]
                for row in conn.execute(
                    "SELECT type, COUNT(*) AS count FROM entries GROUP BY type ORDER BY type"
                )
            }
        else:
            report["by_type"] = None

        report["dirty"] = (
            _count(conn, "SELECT COUNT(*) FROM entries WHERE dirty = 1")
            if "dirty" in columns
            else None
        )
        report["tombstone"] = (
            _count(conn, "SELECT COUNT(*) FROM entries WHERE tombstone = 1")
            if "tombstone" in columns
            else None
        )

        required_for_missing = {"path", "type", "dirty", "tombstone"}
        if required_for_missing <= columns:
            candidates = conn.execute(
                """
                SELECT path FROM entries
                WHERE COALESCE(type, '') != 'folder'
                  AND dirty = 1
                  AND tombstone = 0
                ORDER BY path
                """
            )
            report["missing_mirror_files"] = [
                row["path"]
                for row in candidates
                if not os.path.exists(mirror_path(mirror_root, row["path"]))
            ]
        else:
            report["missing_mirror_files"] = None

        if {"dirty", "hydrated"} <= columns:
            report["dirty_unhydrated"] = _count(
                conn,
                "SELECT COUNT(*) FROM entries WHERE dirty = 1 AND hydrated = 0",
            )
        else:
            report["dirty_unhydrated"] = None

        pending_columns = {"dirty", "tombstone", "last_synced_at"}
        if pending_columns <= columns:
            timestamps = [
                row["last_synced_at"]
                for row in conn.execute(
                    """
                    SELECT last_synced_at FROM entries
                    WHERE (dirty = 1 OR tombstone = 1)
                      AND last_synced_at IS NOT NULL
                    """
                )
            ]
            numeric_timestamps = [
                timestamp
                for timestamp in timestamps
                if isinstance(timestamp, (int, float))
            ]
            report["oldest_pending_age"] = (
                max(0, now - min(numeric_timestamps))
                if numeric_timestamps
                else None
            )
            report["pending_without_timestamp"] = _count(
                conn,
                """
                SELECT COUNT(*) FROM entries
                WHERE (dirty = 1 OR tombstone = 1)
                  AND last_synced_at IS NULL
                """,
            )
        else:
            report["oldest_pending_age"] = None
            report["pending_without_timestamp"] = None

        return report
    finally:
        conn.close()


def format_age(seconds):
    seconds = int(seconds)
    days, seconds = divmod(seconds, 24 * 60 * 60)
    hours, seconds = divmod(seconds, 60 * 60)
    minutes, seconds = divmod(seconds, 60)
    if days:
        return f"{days}d {hours}h"
    if hours:
        return f"{hours}h {minutes}m"
    if minutes:
        return f"{minutes}m {seconds}s"
    return f"{seconds}s"


def _value_or_unavailable(value):
    return "unavailable" if value is None else str(value)


def print_report(config_path, cache_dir, db_path, mirror_root, report):
    print("icloud-linux queue")
    print(f"  Config: {config_path}")
    print(f"  Cache:  {cache_dir}")
    print(f"  DB:     {db_path}")
    print(f"  Mirror: {mirror_root}")
    print()

    if "schema_error" in report:
        print(f"WARNING: {report['schema_error']}; no queue statistics available.")
        return

    print("Entries:")
    print(f"  Total entries: {report['total']}")
    if report["by_type"] is None:
        print("  By type: unavailable (type column is missing)")
    else:
        for entry_type, count in report["by_type"].items():
            print(f"  {entry_type}: {count}")
    print()

    print("Pending state:")
    print(f"  Dirty entries: {_value_or_unavailable(report['dirty'])}")
    print(f"  Tombstone entries: {_value_or_unavailable(report['tombstone'])}")
    print(
        "  Dirty and unhydrated: "
        f"{_value_or_unavailable(report['dirty_unhydrated'])}"
    )
    if report["oldest_pending_age"] is None:
        print("  Oldest pending item: unavailable (no last_synced_at value)")
    else:
        print(
            "  Oldest pending item: "
            f"{format_age(report['oldest_pending_age'])} since last sync"
        )
    if report["pending_without_timestamp"] is not None:
        print(
            "  Pending without last_synced_at: "
            f"{report['pending_without_timestamp']}"
        )
    print()

    missing = report["missing_mirror_files"]
    if missing is None:
        print("At-risk dirty files with missing mirror files: unavailable")
        return

    print(
        "At-risk dirty files with missing mirror files: "
        f"{len(missing)}"
    )
    if not missing:
        return

    print("  These dirty files would be marked tombstone on a sync pass:")
    for path in missing[:MISSING_PATH_LIMIT]:
        print(f"  {path}")
    remaining = len(missing) - MISSING_PATH_LIMIT
    if remaining > 0:
        print(f"  ... and {remaining} more")


def main(argv=None):
    parser = argparse.ArgumentParser(
        description="Report local sync queue state without modifying it."
    )
    parser.add_argument(
        "--config",
        default=os.path.expanduser("~/.config/icloud-linux/config.yaml"),
        help="Path to config.yaml (default: ~/.config/icloud-linux/config.yaml)",
    )
    args = parser.parse_args(argv)

    if not os.path.exists(args.config):
        print(f"ERROR: config not found: {args.config}", file=sys.stderr)
        return 1

    try:
        config = load_config(args.config)
    except (OSError, yaml.YAMLError) as exc:
        print(f"ERROR: cannot read config: {exc}", file=sys.stderr)
        return 1

    cache_dir = os.path.expanduser(
        config.get("cache_dir", "~/.cache/icloud-linux")
    )
    db_path = os.path.join(cache_dir, "state.sqlite3")
    mirror_root = os.path.join(cache_dir, "mirror")

    if not os.path.exists(db_path):
        print(f"state DB not found: {db_path}")
        print("No queue state exists yet. Start the service to create it.")
        return 0

    try:
        report = inspect_queue(db_path, mirror_root)
    except (OSError, sqlite3.Error, ValueError) as exc:
        print(f"ERROR: cannot read state DB: {db_path}: {exc}", file=sys.stderr)
        return 1

    print_report(args.config, cache_dir, db_path, mirror_root, report)
    return 0


if __name__ == "__main__":
    sys.exit(main())
