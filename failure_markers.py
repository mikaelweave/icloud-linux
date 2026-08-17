"""Cross-process locking for the fallback hydration failure log."""

import fcntl
import os
from contextlib import contextmanager


UNRECORDED_FAILURES_FILENAME = "unrecorded_failures.log"
UNRECORDED_FAILURES_LOCK_FILENAME = "unrecorded_failures.lock"


@contextmanager
def exclusive_failure_marker_lock(cache_dir):
    os.makedirs(cache_dir, exist_ok=True)
    lock_path = os.path.join(cache_dir, UNRECORDED_FAILURES_LOCK_FILENAME)
    with open(lock_path, "a", encoding="utf-8") as lock_handle:
        fcntl.flock(lock_handle.fileno(), fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(lock_handle.fileno(), fcntl.LOCK_UN)
