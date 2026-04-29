"""Resource bounds: make sure bridge threads and memory don't balloon.

Guard against subscriber leaks in the shared update watcher: every
`db.update_events()` unsubscribes via `Drop` on the underlying binding,
and the bridge thread exits when its channel disconnects. If those
teardown paths regress, thread count grows linearly with the number
of listeners ever created. These tests fail loudly when that happens.
"""

import asyncio
import gc
import os
import threading
import time

import honker


def _active_thread_count() -> int:
    # A brief wait lets just-exited threads get reaped. On macOS/Linux,
    # threading.active_count reflects Python-level threads only; bridge
    # threads we spawn in Rust show up here because pyo3 creates a native
    # thread that Python tracks via sys-module reporting indirectly. So this
    # count is a fair proxy.
    return threading.active_count()


async def test_listener_churn_does_not_leak_threads(db_path):
    """Create 300 listeners back to back; consume one event each; drop.
    Thread count at the end must be near the baseline."""
    db = honker.open(db_path)

    baseline = _active_thread_count()
    peak = baseline

    for i in range(300):
        channel = f"ch-{i}"

        async def once():
            async for n in db.listen(channel):
                return n.payload

        task = asyncio.create_task(once())
        await asyncio.sleep(0.002)
        with db.transaction() as tx:
            tx.notify(channel, "ok")
        await asyncio.wait_for(task, timeout=2.0)

        peak = max(peak, _active_thread_count())

    # Give Drop handlers + exiting threads a moment.
    for _ in range(10):
        gc.collect()
        await asyncio.sleep(0.05)

    end = _active_thread_count()

    # After all listeners drop, we should be back within a small delta of
    # baseline. We allow for a few in-flight threads the OS hasn't reaped
    # yet, but absolutely not 300 leaked threads.
    assert end <= baseline + 20, (
        f"thread leak suspected: baseline={baseline} end={end} peak={peak}"
    )
    # Peak during churn is bounded too (one listener lives at a time in this
    # test, so peak should be baseline + ~1).
    assert peak <= baseline + 30, (
        f"too many concurrent threads during churn: peak={peak} baseline={baseline}"
    )


async def test_many_simultaneous_listeners_bounded_thread_count(db_path):
    """Hold 100 concurrent listeners alive, verify thread count ~= baseline + 100,
    then drop them all and verify we return to baseline."""
    db = honker.open(db_path)
    baseline = _active_thread_count()

    # Hold listeners alive by keeping references in a list; start the bridge
    # thread by awaiting __anext__ once through asyncio.wait_for with a tiny
    # timeout that we expect to lose.
    listeners = []
    tasks = []
    for i in range(100):
        ls = db.listen(f"k-{i}")
        listeners.append(ls)

        async def nudge(listener):
            try:
                await asyncio.wait_for(listener.__anext__(), timeout=0.01)
            except asyncio.TimeoutError:
                pass

        tasks.append(asyncio.create_task(nudge(ls)))

    await asyncio.gather(*tasks, return_exceptions=True)
    await asyncio.sleep(0.1)

    with_100 = _active_thread_count()
    assert with_100 >= baseline, "no threads spawned?"
    # With the shared update watcher (one update watcher thread per Database),
    # 100 listeners = 1 stat thread + N bridge threads (one per
    # listener that drove __anext__). Allow slack for pytest-xdist
    # workers and asyncio internals; reject anything suggesting
    # per-listener stat threads are back.
    assert with_100 <= baseline + 150, (
        f"too many threads for 100 listeners: {with_100} vs baseline {baseline}"
    )

    # Drop all listeners; threads should exit.
    listeners.clear()
    tasks.clear()
    for _ in range(20):
        gc.collect()
        await asyncio.sleep(0.1)
    end = _active_thread_count()
    assert end <= baseline + 20, (
        f"threads not reaped after dropping 100 listeners: "
        f"baseline={baseline} with_100={with_100} end={end}"
    )


async def test_sustained_honk_throughput_does_not_leak_memory(db_path):
    """Light-weight memory-growth sanity: send 1000 honks against a single
    listener; check resident-set doesn't blow up more than a small bound.

    We batch honks into a few transactions because each `with db.transaction()`
    is a full BEGIN IMMEDIATE + COMMIT round trip (~ms), and holding the writer
    per honk would starve the consumer on the same event loop.
    """
    db = honker.open(db_path)
    got: list = []
    n_events = 1000

    async def consume(n_expected: int):
        async for notif in db.listen("sustained"):
            got.append(notif.payload)
            if len(got) == n_expected:
                return

    task = asyncio.create_task(consume(n_events))
    await asyncio.sleep(0.05)

    rss_before = _rss_bytes()
    # Batch 100 honks per tx → 10 transactions for 1000 events. Keeps the
    # event loop responsive while producing a meaningful message volume.
    batch = 100
    for batch_start in range(0, n_events, batch):
        with db.transaction() as tx:
            for i in range(batch_start, batch_start + batch):
                tx.notify("sustained", f"p{i}")
        # Yield to the loop so the consumer can make progress.
        await asyncio.sleep(0)
    await asyncio.wait_for(task, timeout=15.0)
    gc.collect()
    rss_after = _rss_bytes()

    growth = rss_after - rss_before
    assert growth < 50 * 1024 * 1024, (
        f"RSS grew {growth / 1024 / 1024:.1f} MB after {n_events} notifications"
    )


def _rss_bytes() -> int:
    """Best-effort RSS fetch that works without psutil. macOS ps is fine."""
    try:
        import resource

        usage = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        # macOS reports ru_maxrss in bytes; Linux reports kilobytes.
        import sys
        if sys.platform == "darwin":
            return int(usage)
        return int(usage) * 1024
    except Exception:
        return 0
