"""Fault injection — disk / permission / corruption failures.

Silent failure is the worst-case outcome for a persistence library:
a queue that drops jobs without an error, or a listener that
freezes on an unwritable WAL. These tests prove that each failure
mode raises a clear, propagated error that a caller can handle.

Cross-platform tests (corruption, readonly, nonexistent path) run
everywhere. The ENOSPC test needs a size-capped filesystem (tmpfs
on Linux) and is marked `@pytest.mark.linux_only`.
"""

import os
import stat
import subprocess
import sys

import pytest

import honker


def test_corrupted_db_file_raises_on_first_use(tmp_path):
    """A .db file with a trashed SQLite header must NOT silently
    behave like an empty DB. The first query (or first operation
    that actually reads) must raise with a clear message.

    Writes garbage to a *fresh* path (no WAL/SHM siblings), so
    SQLite can't reconstruct state from the journal.
    """
    path = tmp_path / "corrupt.db"
    # 4096 bytes of garbage — larger than a page header, small enough
    # to be a plausible corrupt file.
    path.write_bytes(b"NOT_AN_SQLITE_FILE" + b"\x00" * 4078)

    # Either `honker.open` or the first query must raise. The
    # exact point depends on when SQLite validates the header —
    # usually on first PRAGMA after open.
    with pytest.raises(RuntimeError, match="not a database|corrupt|malformed"):
        db = honker.open(str(path))
        db.query("SELECT 1")


def test_readonly_directory_raises_clear_error(tmp_path):
    """Opening a DB in a read-only directory must raise — not
    silently fall back to a read-only connection that would then
    fail mysteriously on the first database update."""
    ro_dir = tmp_path / "ro"
    ro_dir.mkdir()
    os.chmod(str(ro_dir), stat.S_IRUSR | stat.S_IXUSR)  # r-x only
    try:
        path = str(ro_dir / "t.db")
        with pytest.raises(RuntimeError, match="unable to open"):
            honker.open(path)
    finally:
        os.chmod(str(ro_dir), 0o755)  # let the tmp-cleanup remove it


def test_readonly_db_file_raises_on_write(tmp_path):
    """Existing DB, .db file + directory both chmod'd to readonly.
    Opening may succeed (depending on SQLite's detection cadence),
    but the first write must raise a clear "readonly database"
    error. Users MUST see a loud failure, never a silent drop."""
    path = tmp_path / "t.db"
    db = honker.open(str(path))
    db.queue("x").enqueue({"i": 1})
    del db

    # Chmod everything readonly — including the dir so SQLite can't
    # create a fresh -wal file.
    os.chmod(str(path), 0o444)
    os.chmod(str(tmp_path), stat.S_IRUSR | stat.S_IXUSR)
    try:
        with pytest.raises(RuntimeError, match="readonly|unable to open"):
            db2 = honker.open(str(path))
            db2.queue("x").enqueue({"i": 2})
    finally:
        os.chmod(str(tmp_path), 0o755)
        os.chmod(str(path), 0o644)


def test_nonexistent_parent_dir_raises(tmp_path):
    """Opening a .db whose parent dir doesn't exist must raise, not
    create the dir silently and certainly not hang."""
    path = str(tmp_path / "no" / "such" / "dir" / "t.db")
    with pytest.raises(RuntimeError, match="unable to open"):
        honker.open(path)


@pytest.mark.linux_only
def test_enqueue_on_full_filesystem_raises_disk_full(tmp_path):
    """Disk-full during a write must raise SQLITE_FULL (or
    equivalent), not hang or silently drop the enqueue.

    Strategy: mount a 1 MiB tmpfs at `tmp_path/full`, place the DB
    there, fill it with a big BLOB until the next write fails.
    Requires sudo-less tmpfs mount — works inside Linux CI
    containers; skipped elsewhere by the `linux_only` mark.
    """
    if sys.platform != "linux":
        pytest.skip("linux_only mark was not filtered")

    mount_dir = tmp_path / "full"
    mount_dir.mkdir()

    # mount a 1MB tmpfs; requires CAP_SYS_ADMIN in the test env.
    try:
        subprocess.run(
            ["mount", "-t", "tmpfs", "-o", "size=1m", "tmpfs", str(mount_dir)],
            check=True,
        )
    except (FileNotFoundError, subprocess.CalledProcessError) as e:
        pytest.skip(f"can't mount tmpfs for ENOSPC test: {e}")

    try:
        path = str(mount_dir / "t.db")
        db = honker.open(path)
        q = db.queue("x")
        # Keep enqueueing big payloads until the tmpfs is full. The
        # failing call must raise — NOT return without an error.
        raised = False
        try:
            for i in range(100):
                q.enqueue({"i": i, "blob": "A" * 50_000})
        except RuntimeError as e:
            msg = str(e).lower()
            # SQLite surfaces disk-full as either "database or disk
            # is full" or the lower-level SQLITE_FULL.
            assert "full" in msg or "disk" in msg, (
                f"disk-full should raise a recognizable error; got: {e}"
            )
            raised = True
        assert raised, (
            "expected SQLITE_FULL (or equivalent) before 100 enqueues; "
            "the tmpfs may not actually be capped, or enqueue is "
            "silently dropping on ENOSPC."
        )
    finally:
        subprocess.run(["umount", str(mount_dir)], check=False)
