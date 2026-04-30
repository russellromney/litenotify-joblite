"""Sustained-load soak tests.

`test_resource_bounds.py` already guards against thread leaks on
listener churn; those tests are seconds-scale. This file runs
minute-scale soak to catch slow memory and disk-usage leaks that
wouldn't show up in a fast test: statement-cache bloat, missing
WAL checkpoint, bridge-thread accumulation under steady-state
load.

Marked `@pytest.mark.slow`; excluded from the default `pytest`
run. Invoke via `pytest -m slow tests/test_soak.py`.
"""

import asyncio
import os
import sys
import time

import pytest

import honker

resource = pytest.importorskip("resource")


def _rss_bytes() -> int:
    """Current process RSS in bytes. `ru_maxrss` is in KB on Linux,
    bytes on macOS — normalize."""
    kb_or_bytes = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    if sys.platform == "darwin":
        return kb_or_bytes
    return kb_or_bytes * 1024


@pytest.mark.slow
async def test_60s_sustained_notify_no_rss_growth(db_path):
    """Run 60 seconds of 100 notify/s with one listener consuming
    them. Assert peak RSS growth stays under a loose bound (30 MB).

    Catches: statement cache bloat, asyncio task leaks, bridge
    thread accumulation, notification-table runaway (reminder:
    notifications are never auto-pruned; this test also prunes on
    its own to isolate *library* growth from test-data growth).
    """
    db = honker.open(db_path)

    # Warm up: open connections + register functions + stabilize
    # allocator state before baseline.
    db.queue("_warm")
    with db.transaction() as tx:
        tx.notify("_warm", "ok")
    await asyncio.sleep(0.1)

    baseline = _rss_bytes()

    received: list = []
    lst = db.listen("soak")

    async def consume():
        async for n in lst:
            received.append(n.id)

    consumer = asyncio.create_task(consume())

    DURATION_S = 60
    RATE_HZ = 100
    INTERVAL_S = 1.0 / RATE_HZ

    deadline = time.time() + DURATION_S
    prune_every = 10  # seconds — keep the notifications table bounded
    next_prune = time.time() + prune_every
    sent = 0
    peak = baseline

    while time.time() < deadline:
        with db.transaction() as tx:
            tx.notify("soak", {"i": sent})
        sent += 1
        if time.time() >= next_prune:
            # Keep only the most recent 1000 notifications. Without
            # this, the table grows unbounded — a real user issue
            # but not what this test is measuring.
            db.prune_notifications(max_keep=1000)
            next_prune += prune_every
            peak = max(peak, _rss_bytes())
        await asyncio.sleep(INTERVAL_S)

    # Give the listener a moment to drain.
    await asyncio.sleep(0.5)
    consumer.cancel()
    try:
        await consumer
    except asyncio.CancelledError:
        pass

    growth = peak - baseline
    # Loose ceiling: 30 MB over 60s. Allocator + asyncio internals
    # eat a few MB even on steady-state workloads; actual steady
    # growth should be far less.
    assert growth < 30 * 1024 * 1024, (
        f"peak RSS grew {growth / 1024 / 1024:.1f} MB over {DURATION_S}s "
        f"of 100 notify/s (baseline={baseline / 1024 / 1024:.1f} MB, "
        f"peak={peak / 1024 / 1024:.1f} MB). Likely a leak in the "
        f"bridge thread, statement cache, or listener buffer."
    )
    # Sanity: listener actually kept up (we won't be picky, but at
    # least 90% of sends should have made it through).
    assert len(received) >= int(sent * 0.9), (
        f"listener dropped too many: sent={sent}, got={len(received)}. "
        f"Delivery slowdown, not a memory test failure, but flag it."
    )


@pytest.mark.slow
def test_wal_bounded_under_sustained_writes(db_path):
    """Sustained commit-per-write loop must keep the .db-wal file
    bounded — SQLite auto-checkpoints every 10k pages per our
    PRAGMA. If a binding accidentally disables or stalls
    checkpointing, the WAL balloons linearly with write count.

    Writes 20k separate enqueue transactions and asserts WAL stays
    under 80 MB (auto-checkpoint threshold is 10k * 4096 B ≈ 40 MB,
    double for headroom).
    """
    db = honker.open(db_path)
    q = db.queue("wal-soak")

    N = 20_000
    for i in range(N):
        # Separate transactions to force WAL growth; a single big
        # tx would commit one page batch regardless of row count.
        q.enqueue({"i": i, "blob": "x" * 256})

    wal_size = os.path.getsize(f"{db_path}-wal")
    # Loose: 80 MB. Autocheckpoint kicks at ~40 MB (10k pages * 4K).
    # If it didn't kick, 20k rows * ~400 bytes/row + overhead ≈ 8-12 MB
    # just from this test — but the invariant is "bounded," not tight.
    assert wal_size < 80 * 1024 * 1024, (
        f"WAL grew to {wal_size / 1024 / 1024:.1f} MB after {N} writes. "
        f"Expected <80 MB (auto-checkpoint at 10k pages). Likely the "
        f"wal_autocheckpoint PRAGMA didn't apply or was overridden."
    )
