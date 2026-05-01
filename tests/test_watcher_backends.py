"""Cross-language proof for the experimental watcher backends.

These tests prove that the optional kernel-watcher and shm-fast-path
backends behave the same as the default polling backend on the public
Python wake/listen surface. They run against the real Python binding
(the maturin-built `_honker_native` cdylib), not Rust unit tests.

Wheels built without the corresponding Cargo features silently fall back
to polling, so a missing feature shows up as "looks like polling" rather
than as a failed import.

A control assertion runs the same workload against the default polling
backend so a regression in the experimental path stands out from
test-environment flakiness.
"""

import asyncio
import time

import pytest

import honker

# All listen/update flows are async — match the rest of the suite.
pytestmark = pytest.mark.asyncio


async def _drive_commits_and_count_wakes(db, n: int, spacing_ms: int) -> int:
    """Subscribe to update_events, fire `n` commits, count wakes."""
    counted = 0
    done = asyncio.Event()
    events_seen = asyncio.Event()

    async def consume():
        nonlocal counted
        async for _ in db.update_events():
            counted += 1
            events_seen.set()
            if counted >= n:
                done.set()
                return

    consumer_task = asyncio.create_task(consume())
    # Give the watcher thread a moment to start before issuing commits.
    await asyncio.sleep(0.05)

    with db.transaction() as tx:
        tx.execute("CREATE TABLE IF NOT EXISTS t (x INT)")
    await asyncio.sleep(spacing_ms / 1000.0)

    for i in range(n):
        with db.transaction() as tx:
            tx.execute("INSERT INTO t VALUES (?)", [i])
        await asyncio.sleep(spacing_ms / 1000.0)

    # Wait long enough for the slowest backend's safety net (500 ms)
    # plus event delivery latency.
    try:
        await asyncio.wait_for(done.wait(), timeout=2.5)
    except asyncio.TimeoutError:
        pass

    consumer_task.cancel()
    try:
        await consumer_task
    except asyncio.CancelledError:
        pass
    return counted


@pytest.mark.parametrize(
    "backend",
    [
        None,        # default polling — control
        "kernel",    # Phase 003
        "shm",       # Phase 004
    ],
)
async def test_watcher_backend_detects_commits(db_path, backend):
    db = honker.open(db_path, watcher_backend=backend)
    # Each commit spaced 30 ms apart — well above polling (1 ms),
    # shm fast path (100 µs), and kernel-watcher event-delivery latency.
    n = 4
    counted = await _drive_commits_and_count_wakes(db, n=n, spacing_ms=30)
    # Conftest's gc.collect() releases the underlying SQLite handles.

    # `update_events()` fires once per observed commit. The first wake
    # may be from the CREATE TABLE; we tolerate >= n (each insert) and
    # bound generously to surface a runaway watcher.
    assert counted >= n, (
        f"watcher_backend={backend!r}: only {counted} wakes for {n} commits"
    )
    assert counted <= n + 2, (
        f"watcher_backend={backend!r}: {counted} wakes for {n} commits "
        "exceeds reasonable upper bound — runaway watcher?"
    )


async def test_unknown_watcher_backend_raises(db_path):
    with pytest.raises(ValueError):
        honker.open(db_path, watcher_backend="bogus")


async def test_shm_backend_works_for_real_db(db_path):
    """Sanity: the probe at honker.open() time succeeds for a normal db
    (WAL mode + open writer connection). The failure paths are
    exercised by the Rust unit test `watcher_backend_probe_fails_for_*`."""
    honker.open(db_path, watcher_backend="shm")  # must not raise
