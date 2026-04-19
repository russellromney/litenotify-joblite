"""SIGKILL mid-transaction crash recovery.

The "my worker box got rebooted" story. We spawn a real Python subprocess,
let it open the DB and hold a `BEGIN IMMEDIATE` with pending writes, then
`os.kill(pid, SIGKILL)` before COMMIT. A fresh process then opens the DB
and verifies:

  * the file is not corrupt (`PRAGMA integrity_check == 'ok'`),
  * the in-flight write did not land,
  * subsequent enqueue/claim/ack works end to end,
  * for a honk-bearing killed transaction, a listener (opened after the
    kill) sees zero notifications from the killed tx, and a fresh honk
    still flows.

Notes:
  * litenotify's commit-hook runs in-process — each process has its own
    Notifier, so cross-process NOTIFY is not a feature of the library.
    That means the "pre-subscribed listener" for the honk case is placed
    in the test process (which is what a second-process listener would
    see anyway: nothing from a different process). The real invariant
    under test is: an SIGKILLed honk-bearing tx leaves no stale state —
    not in the DB file, not in any in-process buffer we can reopen.
  * Each test uses its own tmp_path DB, so everything parallelizes under
    pytest -n auto.
"""

import asyncio
import os
import signal
import sqlite3
import subprocess
import sys
import textwrap
import time

import pytest


REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _spawn(script: str) -> subprocess.Popen:
    return subprocess.Popen(
        [sys.executable, "-c", script],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        bufsize=1,
    )


def _wait_for_line(
    proc: subprocess.Popen, expected: str, timeout: float = 5.0
) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if proc.poll() is not None:
            out = proc.stdout.read() if proc.stdout else ""
            err = proc.stderr.read() if proc.stderr else ""
            raise AssertionError(
                f"subprocess exited before emitting {expected!r}: "
                f"rc={proc.returncode}, stdout={out!r}, stderr={err!r}"
            )
        line = proc.stdout.readline() if proc.stdout else ""
        if not line:
            time.sleep(0.01)
            continue
        if line.strip() == expected:
            return
    raise AssertionError(f"timed out waiting for {expected!r} from subprocess")


def _sigkill_and_reap(proc: subprocess.Popen) -> None:
    os.kill(proc.pid, signal.SIGKILL)
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(timeout=2)


def _integrity_check(db_path: str) -> str:
    """Run PRAGMA integrity_check via raw sqlite3 so we don't rely on the
    library under test to report its own corruption."""
    conn = sqlite3.connect(db_path, timeout=5.0)
    try:
        row = conn.execute("PRAGMA integrity_check").fetchone()
        return row[0] if row else ""
    finally:
        conn.close()


def test_sigkill_mid_enqueue_tx_leaves_db_clean(tmp_path):
    """Subprocess holds a BEGIN IMMEDIATE with a pending enqueue, then gets
    SIGKILLed. Fresh process must find: integrity ok, zero jobs present,
    and a full enqueue+claim+ack round-trip still works."""
    db_path = str(tmp_path / "crash.db")

    # Pre-create the schema from a normal process so the subprocess can
    # open and hold the tx quickly.
    import joblite

    seed = joblite.open(db_path)
    seed.queue("q")
    del seed

    script = textwrap.dedent(
        f"""
        import sys, time
        sys.path.insert(0, {REPO_ROOT!r})
        import joblite

        db = joblite.open({db_path!r})
        q = db.queue('q')

        # BEGIN IMMEDIATE via db.transaction(); insert a row; signal; sleep.
        with db.transaction() as tx:
            q.enqueue({{"i": 999}}, tx=tx)
            print("READY", flush=True)
            time.sleep(60)  # parent SIGKILLs us here
        """
    )
    proc = _spawn(script)
    try:
        _wait_for_line(proc, "READY")
        _sigkill_and_reap(proc)
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait()

    # File must still be intact.
    assert _integrity_check(db_path) == "ok"

    # Fresh library process: killed write did not land.
    db = joblite.open(db_path)
    q = db.queue("q")
    existing = db.query("SELECT COUNT(*) AS c FROM _joblite_jobs")
    assert existing[0]["c"] == 0, f"killed tx leaked rows: {existing}"

    # End-to-end round-trip works post-crash.
    q.enqueue({"i": 1})
    job = q.claim_one("recovery-worker")
    assert job is not None, "post-crash claim returned None"
    assert job.payload == {"i": 1}
    assert job.ack() is True


def test_sigkill_mid_enqueue_followed_by_concurrent_writer(tmp_path):
    """After the SIGKILL, the DB must not be stuck in a write-locked state.
    A concurrent writer opening the same file must be able to immediately
    acquire the write lock. This catches the failure mode where a killed
    BEGIN IMMEDIATE left stale reserved locks (WAL mode recovers these
    automatically, but we verify it, since it's load-bearing)."""
    db_path = str(tmp_path / "crash-lock.db")

    import joblite

    seed = joblite.open(db_path)
    seed.queue("q")
    del seed

    script = textwrap.dedent(
        f"""
        import sys, time
        sys.path.insert(0, {REPO_ROOT!r})
        import joblite

        db = joblite.open({db_path!r})
        q = db.queue('q')
        with db.transaction() as tx:
            q.enqueue({{"i": 1}}, tx=tx)
            print("READY", flush=True)
            time.sleep(60)
        """
    )
    proc = _spawn(script)
    try:
        _wait_for_line(proc, "READY")
        _sigkill_and_reap(proc)
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait()

    start = time.time()
    db = joblite.open(db_path)
    q = db.queue("q")
    with db.transaction() as tx:
        q.enqueue({"i": 2}, tx=tx)
    elapsed = time.time() - start
    # Should be ~instant — if a stale lock were held we'd hit
    # busy_timeout (5s). Allow generous slack for slow CI but much less
    # than busy_timeout.
    assert elapsed < 2.0, (
        f"writer blocked {elapsed:.2f}s on opening post-crash DB — "
        f"stale lock?"
    )
    rows = db.query("SELECT COUNT(*) AS c FROM _joblite_jobs")
    assert rows[0]["c"] == 1


def test_sigkill_mid_honk_tx_delivers_no_notification(tmp_path):
    """A subprocess holds a honk-bearing BEGIN IMMEDIATE tx then gets
    SIGKILLed before COMMIT. The test process opens the DB afterwards,
    attaches a fresh listener on the same channel, and confirms:

      * no phantom notification appears from the killed tx,
      * the channel is still healthy: a subsequent committed honk from
        the test process is received normally,
      * the DB file is intact.
    """
    db_path = str(tmp_path / "crash-honk.db")
    channel = f"crash-channel-{os.getpid()}-{time.time_ns()}"

    # Create the file so subprocess can open immediately.
    import joblite as litenotify

    seed = litenotify.open(db_path)
    del seed

    writer_script = textwrap.dedent(
        f"""
        import sys, time
        sys.path.insert(0, {REPO_ROOT!r})
        import joblite as litenotify

        db = litenotify.open({db_path!r})
        with db.transaction() as tx:
            tx.notify({channel!r}, {{"killed": True}})
            print("READY", flush=True)
            time.sleep(60)
        """
    )
    proc = _spawn(writer_script)
    try:
        _wait_for_line(proc, "READY")
        _sigkill_and_reap(proc)
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait()

    assert _integrity_check(db_path) == "ok"

    # Attach a fresh listener and verify it sees nothing from the killed tx
    # and that a subsequent committed honk still flows.
    db = litenotify.open(db_path)
    listener = db.listen(channel)

    async def drain_and_replay():
        # First pull should time out — killed tx produced no notification.
        try:
            leaked = await asyncio.wait_for(listener.__anext__(), timeout=0.5)
            raise AssertionError(
                f"listener saw notification from killed tx: {leaked!r}"
            )
        except asyncio.TimeoutError:
            pass

        # A committed honk should flow end to end.
        with db.transaction() as tx:
            tx.notify(channel, {"alive": True})
        got = await asyncio.wait_for(listener.__anext__(), timeout=2.0)
        return got

    result = asyncio.run(drain_and_replay())
    import json as _json
    assert result.payload == {"alive": True}


def test_sigkill_while_listener_preattached_sees_no_leak(tmp_path):
    """Pre-attached listener in the test process: we open the DB and
    subscribe BEFORE the subprocess writer starts its killed tx, so any
    subscriber-buffering bug would latch a phantom notification in the
    in-process broadcast ring. Verify none appears.

    (Cross-process notify isn't a feature — a different-process writer
    can't reach this process's notifier. So the goal here is to make
    extra sure that no persisted/queued state in the SQLite file leaks
    into this process's listener when it later sees its own writes.)
    """
    db_path = str(tmp_path / "crash-preattached.db")
    channel = f"pre-{os.getpid()}-{time.time_ns()}"

    import joblite as litenotify

    db = litenotify.open(db_path)
    listener = db.listen(channel)  # pre-attached in the test process

    writer_script = textwrap.dedent(
        f"""
        import sys, time
        sys.path.insert(0, {REPO_ROOT!r})
        import joblite as litenotify

        db = litenotify.open({db_path!r})
        with db.transaction() as tx:
            tx.notify({channel!r}, {{"killed": True}})
            print("READY", flush=True)
            time.sleep(60)
        """
    )
    proc = _spawn(writer_script)
    try:
        _wait_for_line(proc, "READY")
        _sigkill_and_reap(proc)
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait()

    async def assert_no_leak_then_live_honk_works():
        try:
            leaked = await asyncio.wait_for(listener.__anext__(), timeout=0.5)
            raise AssertionError(
                f"pre-attached listener saw leaked notification: {leaked!r}"
            )
        except asyncio.TimeoutError:
            pass
        with db.transaction() as tx:
            tx.notify(channel, {"alive": True})
        got = await asyncio.wait_for(listener.__anext__(), timeout=2.0)
        return got

    result = asyncio.run(assert_no_leak_then_live_honk_works())
    import json as _json
    assert result.payload == {"alive": True}
    assert _integrity_check(db_path) == "ok"
