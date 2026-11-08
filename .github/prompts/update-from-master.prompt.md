---
name: Update from master
description: Synchronize the fork's master with upstream and merge requested updates into main.
agent: agent
---

# Update from master

This fork keeps its fixes on `main`. Its `master` is a clean mirror of
upstream's `master`. Merge upstream updates into `main` only when requested.
Do not substitute `upstream/main` for `upstream/master`.

1. Inspect `git status --short --branch` and stop if tracked files have
   uncommitted changes. Preserve unrelated untracked files.
2. Fetch both remotes: `git fetch --prune upstream` and
   `git fetch --prune origin`.
3. Update the fork's `master` from `upstream/master` with a fast-forward only
   operation. Push the result to `origin/master`.
4. Compare `main` with the updated `master`. Prefer upstream's
   implementation when it supersedes local work; retain and port only behavior
   with no upstream equivalent.
5. Merge `master` into `main`. Do not rebase published `main` history.
   Resolve conflicts in favor of the upstream design unless the local
   behavior is deliberately retained.
6. Run the smallest relevant validation and push the updated `main`.

Do not force-push `master`, reset published branches, or treat remote-tracking
references as a replacement for fetching first.
