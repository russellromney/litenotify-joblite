"""Cross-process end-to-end proof for the experimental watcher backends.

Three concurrent layers of proof, each exercised against every backend:

1. **Cross-process delivery** — a writer subprocess fires N committed
   notifications; a parent listener (this test, with the chosen backend)
   must observe every one of them. Same shape as
   `test_cross_process_wake_latency.py`, but parameterized over backend
   and asserting *no missed notifications* rather than tail latency.

2. **Reliable delivery under burst load** — many small notifications in
   tight sequence. Each backend must surface every distinct
   `_honker_notifications` row without dropping or merging the visible
   count of `notify()` rows.

3. **Survival across the writer process restart** — the writer dies
   mid-flight, the listener must resume detecting commits when a fresh
   writer subprocess opens the same db file. Catches the case where a
   backend latches onto a transient file handle (-wal inode, etc.) and
   never re-attaches after a checkpoint cycle.

These are the hardest of the proof claims in
`intent/ecosystems/honker/phases/003-optional-kernel-watcher/plan.md`
and `004-optional-shm-fast-path/plan.md`: that the experimental backends
behave the same as the polling baseline on the public Python wake/listen
surface, in real cross-process use, not just from in-process Rust unit
tests.
"""

import asyncio
import os
import subprocess
import sys
import time

import pytest

import honker

pytestmark = pytest.mark.asyncio

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PACKAGES_ROOT = os.path.join(REPO_ROOT, "packages")

# Wheels built without the experimental Cargo features silently fall
# back to polling for `"kernel"` and `"shm"` — that's the documented
# behavior. The cross-process suite still runs them so a regression in
# the fallback path also surfaces here, not only in the Rust tests.
BACKENDS = [
    None,        # default polling — control
    "kernel",    # Phase 003
    "shm",       # Phase 004
]


_WRITER_BURST_SCRIPT = r"""
import sys
import time

sys.path.insert(0, {packages!r})
import honker

db = honker.open({db_path!r})
print("READY", flush=True)
sys.stdin.readline()  # parent gates the burst until listener is armed

for i in range({n}):
    with db.transaction() as tx:
        tx.notify({channel!r}, str(i))
    if {spacing_ms} > 0:
        time.sleep({spacing_ms} / 1000.0)
print("DONE", flush=True)
sys.stdin.readline()  # keep stdin open until parent says exit
"""


def _spawn_writer(db_path: str, channel: str, n: int, spacing_ms: int):
    """Spawn the writer subprocess. Caller owns lifecycle."""
    script = _WRITER_BURST_SCRIPT.format(
        packages=PACKAGES_ROOT,
        db_path=db_path,
        n=n,
        channel=channel,
        spacing_ms=spacing_ms,
    )
    proc = subprocess.Popen(
        [sys.executable, "-c", script],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        bufsize=1,  # line-buffered
    )
    ready = proc.stdout.readline()
    assert ready.strip() == "READY", (
        f"writer subprocess did not READY: {ready!r}; "
        f"stderr={proc.stderr.read()!r}"
    )
    return proc


def _stop_writer(proc):
    try:
        if proc.stdin and not proc.stdin.closed:
            try:
                proc.stdin.write("\n")
                proc.stdin.flush()
            except (BrokenPipeError, OSError):
                pass
            proc.stdin.close()
        proc.wait(timeout=3.0)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait()


# ----------------------------------------------------------------------
# Test 1: cross-process correctness — writer in subprocess, listener in
#         parent under each backend. Every commit must be detected.
# ----------------------------------------------------------------------


async def _drain_n_notifications(listener, n: int, timeout_s: float):
    """Drain at most `n` notifications from an existing `listener`.

    The listener MUST be constructed before the publisher is released —
    `Listener.__init__` snapshots `MAX(id)` and subscribes to the update
    watcher in that order. Constructing the listener after the writer
    has already committed loses every notification with id ≤ snapshot.
    See `test_subscribe_race.py`.
    """
    seen = []

    async def consume():
        async for note in listener:
            seen.append(note.payload)
            if len(seen) >= n:
                return

    try:
        await asyncio.wait_for(consume(), timeout=timeout_s)
    except asyncio.TimeoutError:
        pass
    return seen


@pytest.mark.parametrize("backend", BACKENDS)
async def test_cross_process_listener_detects_every_commit(db_path, backend):
    # Listener (this process, under `backend`) opens FIRST so it has the
    # watcher running before the writer starts emitting.
    db = honker.open(db_path, watcher_backend=backend)

    # The pre-warm transaction also forces the WAL to exist so the
    # kernel-watcher's per-file -wal watch attaches at startup.
    with db.transaction() as tx:
        tx.execute("CREATE TABLE _warm (i INTEGER)")
    await asyncio.sleep(0.05)

    n = 10
    spacing_ms = 15
    proc = _spawn_writer(db_path, "orders", n=n, spacing_ms=spacing_ms)
    # Subscribe BEFORE releasing the writer so the listener's MAX(id)
    # snapshot precedes the first commit.
    listener = db.listen("orders")

    try:
        proc.stdin.write("\n")
        proc.stdin.flush()

        # Wait long enough for: n × spacing_ms (writer pace), plus
        # the slowest backend's safety net (500 ms), plus generous
        # event-delivery slack.
        timeout_s = (n * spacing_ms / 1000.0) + 2.0
        seen = await _drain_n_notifications(listener, n, timeout_s)
    finally:
        _stop_writer(proc)

    payloads = sorted([int(p) for p in seen])
    assert payloads == list(range(n)), (
        f"backend={backend!r}: cross-process listener missed notifications. "
        f"Saw {payloads}, expected {list(range(n))}."
    )


# ----------------------------------------------------------------------
# Test 2: burst load — N committed notifications in a tight loop. The
#         persisted notifications table is the source of truth; the
#         listener must surface every row through the wake path.
# ----------------------------------------------------------------------


@pytest.mark.parametrize("backend", BACKENDS)
async def test_cross_process_burst_no_missed_notifications(db_path, backend):
    db = honker.open(db_path, watcher_backend=backend)
    with db.transaction() as tx:
        tx.execute("CREATE TABLE _warm (i INTEGER)")
    await asyncio.sleep(0.05)

    n = 50
    proc = _spawn_writer(db_path, "burst", n=n, spacing_ms=0)
    # Subscribe before the writer commits anything.
    listener = db.listen("burst")
    try:
        proc.stdin.write("\n")
        proc.stdin.flush()

        # Wait for writer to finish. Persisted-row check below is the
        # real assertion; this just bounds the wait.
        deadline = time.perf_counter() + 5.0
        line = ""
        while time.perf_counter() < deadline:
            try:
                line = proc.stdout.readline()
                if line.strip() == "DONE":
                    break
            except Exception:
                break
        assert line.strip() == "DONE", (
            f"backend={backend!r}: writer never reported DONE within 5s"
        )

        # Listener should observe every distinct payload row. Use a
        # tight per-message timeout so a stuck wake fails fast.
        seen = await _drain_n_notifications(listener, n, timeout_s=3.0)
    finally:
        _stop_writer(proc)

    persisted_rows = db.query(
        "SELECT payload FROM _honker_notifications "
        "WHERE channel = 'burst' ORDER BY id"
    )
    assert len(persisted_rows) == n, (
        f"backend={backend!r}: expected {n} persisted notifications, "
        f"saw {len(persisted_rows)} (writer subprocess didn't commit them all)"
    )
    assert len(seen) == n, (
        f"backend={backend!r}: listener observed {len(seen)} of {n} "
        f"notifications under burst load. Missed: "
        f"{set(str(i) for i in range(n)) - set(seen)}"
    )


# ----------------------------------------------------------------------
# Test 3: writer-process restart — kills the writer mid-flight, spawns a
#         fresh writer pointed at the same db file, listener must resume
#         seeing commits. Catches backends that latched onto a transient
#         file handle (-wal inode, mmap'd -shm) and never re-attached.
# ----------------------------------------------------------------------


@pytest.mark.parametrize("backend", BACKENDS)
async def test_cross_process_listener_survives_writer_restart(db_path, backend):
    db = honker.open(db_path, watcher_backend=backend)
    with db.transaction() as tx:
        tx.execute("CREATE TABLE _warm (i INTEGER)")
    await asyncio.sleep(0.05)

    # Listener subscribes once; both writer epochs must surface through it.
    # Constructing here, before any writer commits, snapshots MAX(id)=0.
    listener = db.listen("resilience")

    # Writer 1: emits 5 notifications, then we kill it.
    proc1 = _spawn_writer(db_path, "resilience", n=5, spacing_ms=20)
    try:
        proc1.stdin.write("\n")
        proc1.stdin.flush()
        # Drain the first batch BEFORE killing. With backends like the
        # kernel watcher whose -wal watch may need re-attach after
        # kill, we want to confirm batch 1 was delivered before tearing
        # down the writer.
        first_seen = await _drain_n_notifications(listener, 5, 2.0)
        proc1.kill()
        proc1.wait()
    finally:
        if proc1.poll() is None:
            proc1.kill()

    assert len(first_seen) == 5, (
        f"backend={backend!r}: listener saw {len(first_seen)} of 5 "
        f"notifications from writer #1 before kill"
    )

    # Writer 2: same db, fresh process. Listener must keep working.
    proc2 = _spawn_writer(db_path, "resilience", n=5, spacing_ms=20)
    try:
        proc2.stdin.write("\n")
        proc2.stdin.flush()
        # Listener already has the watcher attached; wakes from writer #2
        # must come through too.
        second_seen = await _drain_n_notifications(listener, 5, 2.5)
    finally:
        _stop_writer(proc2)

    assert len(second_seen) == 5, (
        f"backend={backend!r}: listener saw {len(second_seen)} of 5 "
        f"notifications from writer #2 (after writer #1 was killed). "
        f"Backend may have latched onto a stale file handle."
    )

    # Total: 10 distinct payloads (each writer emitted 0..4).
    persisted = db.query(
        "SELECT payload FROM _honker_notifications "
        "WHERE channel = 'resilience' ORDER BY id"
    )
    assert len(persisted) == 10, (
        f"backend={backend!r}: persisted {len(persisted)} of 10 "
        f"notifications across two writer lifetimes"
    )


# ----------------------------------------------------------------------
# Test 4: dead-man's switch propagates to the listener.
#         Atomically replace the db file mid-flight; the listener must
#         raise within ~1 s rather than hang on an empty queue.
# ----------------------------------------------------------------------


@pytest.mark.parametrize("backend", BACKENDS)
async def test_listener_raises_when_watcher_dies(db_path, backend, tmp_path):
    db = honker.open(db_path, watcher_backend=backend)
    with db.transaction() as tx:
        tx.execute("CREATE TABLE _warm (i INTEGER)")
        tx.notify("dead", "warmup")  # one row so listener has something to subscribe past
    await asyncio.sleep(0.1)

    # Subscribe; the watcher's per-backend dead-man's switch fires on
    # inode change. We use `update_events()` directly to avoid masking
    # the death by listen()'s SQL re-query path.
    events = db.update_events()
    aiter = events.__aiter__()
    # update_events() blocks until the watcher thread has captured its
    # baseline (initial inode), so os.replace can run immediately
    # without racing the snapshot.

    # Atomic replace of the db file — fresh inode on the same path.
    replacement = tmp_path / "replacement.db"
    replacement.write_bytes(b"")
    os.replace(replacement, db_path)

    # File replacement can produce a few "conservative wake" None ticks
    # (transient I/O errors before the dead-man's switch identity check
    # fires). Drain Nones until the death exception arrives, bounded by
    # a total deadline.
    async def drain_until_death(deadline_s: float):
        end = time.perf_counter() + deadline_s
        while time.perf_counter() < end:
            remaining = end - time.perf_counter()
            value = await asyncio.wait_for(aiter.__anext__(), timeout=remaining)
            assert value is None, f"unexpected non-None wake value: {value!r}"
        raise AssertionError("deadline reached without watcher death")

    with pytest.raises(Exception) as excinfo:
        await drain_until_death(deadline_s=3.0)
    msg = str(excinfo.value)
    assert "watcher" in msg.lower() or "replaced" in msg.lower() or "dead" in msg.lower(), (
        f"backend={backend!r}: expected a watcher-died-style error, "
        f"got {msg!r}"
    )
