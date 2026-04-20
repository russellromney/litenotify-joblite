"""Performance floor tests.

These pin a loose throughput floor for hot paths so a 10x+ regression
(unindexed query, lost `prepare_cached`, extra JSON round-trip per
row) trips CI instead of shipping silently.

Thresholds are set ~3-5x below measured throughput on an M-series
laptop so they don't flake on slower CI hardware, but tight enough
that real regressions show up:

  Path                     measured (M-series)    floor
  enqueue 10k in one tx    ~21k/s                  3.3k/s
  claim_batch 10k (100/ea) ~44k/s                  3.3k/s
  100 notifies → listener  ~27k/s                  100/s

The aim is not to benchmark; run `bench/wake_latency_bench.py`
for that. These catch order-of-magnitude regressions only.
"""

import asyncio
import time

import pytest

import joblite


def test_enqueue_throughput_floor_one_tx(db_path):
    """10,000 enqueues inside one transaction must finish in under
    3 seconds. Measured ~0.5s on M-series. A 6x slowdown trips."""
    db = joblite.open(db_path)
    q = db.queue("perf-enqueue")
    t0 = time.perf_counter()
    with db.transaction() as tx:
        for i in range(10_000):
            q.enqueue({"i": i}, tx=tx)
    elapsed = time.perf_counter() - t0
    assert elapsed < 3.0, (
        f"enqueue 10k in one tx took {elapsed:.3f}s (floor: 3.0s). "
        f"Likely regression in honker_enqueue or the PyO3 param-marshaling."
    )
    # Sanity: rows actually landed.
    rows = db.query(
        "SELECT COUNT(*) AS c FROM _joblite_live WHERE queue='perf-enqueue'"
    )
    assert rows[0]["c"] == 10_000


def test_claim_batch_throughput_floor(db_path):
    """Seed 10k jobs, drain in batches of 100. Must finish in under
    3 seconds. Measured ~0.23s on M-series. A 13x slowdown trips.
    The claim path touches the partial index on every batch; if the
    index gets dropped or the planner picks a table scan, this
    floor trips."""
    db = joblite.open(db_path)
    q = db.queue("perf-claim", visibility_timeout_s=300)
    with db.transaction() as tx:
        for i in range(10_000):
            q.enqueue({"i": i}, tx=tx)

    t0 = time.perf_counter()
    claimed = 0
    while True:
        jobs = q.claim_batch("w1", 100)
        if not jobs:
            break
        claimed += len(jobs)
    elapsed = time.perf_counter() - t0
    assert claimed == 10_000
    assert elapsed < 3.0, (
        f"claim_batch 10k took {elapsed:.3f}s (floor: 3.0s). "
        f"Likely regression in the _joblite_live_claim partial index "
        f"or honker_claim_batch."
    )


async def test_notify_listener_receive_floor(db_path):
    """100 notifies delivered to a listener must be observed within
    1 second end-to-end. Measured ~4ms on M-series. A 250x slowdown
    trips. Catches regressions in the listener buffer, WAL watcher
    fanout, or the cross-thread asyncio.Queue bridge."""
    db = joblite.open(db_path)

    received: list = []
    lst = db.listen("perf-notify")

    async def consume():
        async for n in lst:
            received.append(n)
            if len(received) == 100:
                return

    task = asyncio.create_task(consume())
    # Give the listener a moment to attach + read MAX(id).
    await asyncio.sleep(0.05)

    t0 = time.perf_counter()
    with db.transaction() as tx:
        for i in range(100):
            tx.notify("perf-notify", {"i": i})
    await asyncio.wait_for(task, timeout=5.0)
    elapsed = time.perf_counter() - t0

    assert elapsed < 1.0, (
        f"100 notify → listener receive took {elapsed:.3f}s "
        f"(floor: 1.0s). Likely regression in listener polling, "
        f"WAL watcher fanout, or the asyncio bridge."
    )
    assert len(received) == 100
