#!/usr/bin/env python3
"""Clear durable queue retry state through a bounded SQLite write."""

import argparse
import os
from pathlib import Path
import sqlite3
import sys

import yaml


WRITE_TIMEOUT_SECONDS = 10


def normalize_path(path):
    normalized = os.path.normpath("/" + path.lstrip("/"))
    return "/" if normalized == "." else normalized


def load_config(config_path):
    with open(config_path, "r", encoding="utf-8") as handle:
        return yaml.safe_load(handle) or {}


def subtree_where(path):
    if path is None or path == "/":
        return "1 = 1", ()
    escaped_path = (
        path.replace("\\", "\\\\")
        .replace("%", "\\%")
        .replace("_", "\\_")
    )
    return "path = ? OR path LIKE ? ESCAPE '\\'", (path, escaped_path + "/%")


def clear_failures(db_path, path=None):
    """Clear sync and hydration retry state for a path subtree in one update."""
    if path is not None:
        path = normalize_path(path)
    if not os.path.exists(db_path):
        raise FileNotFoundError(db_path)

    conn = sqlite3.connect(db_path, timeout=WRITE_TIMEOUT_SECONDS)
    try:
        conn.execute(f"PRAGMA busy_timeout = {WRITE_TIMEOUT_SECONDS * 1000}")
        conn.execute("BEGIN IMMEDIATE")
        where_clause, parameters = subtree_where(path)
        if path is not None:
            exists = conn.execute(
                "SELECT 1 FROM entries WHERE " + where_clause + " LIMIT 1",
                parameters,
            ).fetchone()
            if exists is None:
                conn.rollback()
                return 0, path
        cursor = conn.execute(
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
        conn.commit()
        return cursor.rowcount, path
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def main(argv=None):
    parser = argparse.ArgumentParser(
        description="Clear durable queue retry and quarantine state."
    )
    parser.add_argument(
        "--config",
        default=os.path.expanduser("~/.config/icloud-linux/config.yaml"),
        help="Path to config.yaml (default: ~/.config/icloud-linux/config.yaml)",
    )
    parser.add_argument(
        "path",
        nargs="?",
        help="iCloud path to clear recursively; omit to clear all entries",
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
    try:
        cleared, path = clear_failures(db_path, args.path)
    except FileNotFoundError:
        print(f"ERROR: state DB not found: {db_path}", file=sys.stderr)
        return 1
    except sqlite3.Error as exc:
        print(
            "ERROR: queue state is busy after "
            f"{WRITE_TIMEOUT_SECONDS}s; retry the command: {exc}",
            file=sys.stderr,
        )
        return 1

    if args.path is not None and not cleared:
        print(f"ERROR: queue path not found: {path}", file=sys.stderr)
        return 1
    if path is None:
        print(f"Cleared retry and quarantine state for {cleared} entries.")
    else:
        print(
            f"Cleared retry and quarantine state for {cleared} "
            f"entries under {path}."
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())
