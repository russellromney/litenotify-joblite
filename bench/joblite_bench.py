"""Throughput + latency benchmark for joblite.

Measures:
- enqueue ops/s (producer side)
- claim+ack ops/s (single consumer)
- end-to-end enqueue -> handler p50/p99 latency (async worker)

Usage:
    uv run python bench/joblite_bench.py [--n 10000] [--workers 1]
"""

import argparse
import asyncio
import os
import statistics
import sys
import tempfile
import time

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "packages"))
import joblite  # noqa: E402


def bench_enqueue(db: joblite.Database, n: int) -> dict:
    q = db.queue("bench")
    start = time.perf_counter()
    for i in range(n):
        q.enqueue({"i": i})
    elapsed = time.perf_counter() - start
    return {"n": n, "elapsed_s": elapsed, "ops_per_s": n / elapsed}


def bench_claim_ack(db: joblite.Database, n: int) -> dict:
    q = db.queue("bench")
    start = time.perf_counter()
    acked = 0
    while acked < n:
        job = q.claim_one("bench-w")
        if job is None:
            break
        job.ack()
        acked += 1
    elapsed = time.perf_counter() - start
    return {"n": acked, "elapsed_s": elapsed, "ops_per_s": acked / elapsed}


def bench_claim_batch_ack_batch(db: joblite.Database, n: int, batch: int = 32) -> dict:
    """One claim_batch + one ack_batch per batch. 2 tx per `batch` jobs, not
    2 tx per job."""
    q = db.queue("bench-batched")
    for i in range(n):
        q.enqueue({"i": i})
    start = time.perf_counter()
    acked = 0
    while acked < n:
        jobs = q.claim_batch("bench-w", batch)
        if not jobs:
            break
        ids = [j.id for j in jobs]
        worker_id = jobs[0].worker_id
        q.ack_batch(ids, worker_id)
        acked += len(jobs)
    elapsed = time.perf_counter() - start
    return {
        "n": acked,
        "batch": batch,
        "elapsed_s": elapsed,
        "ops_per_s": acked / elapsed,
    }


async def bench_end_to_end(db: joblite.Database, n: int) -> dict:
    q = db.queue("bench-e2e")
    latencies: list = []
    done = asyncio.Event()
    processed = 0

    async def worker():
        nonlocal processed
        async for job in q.claim("e2e-w"):
            latencies.append(time.perf_counter() - job.payload["t"])
            job.ack()
            processed += 1
            if processed >= n:
                done.set()
                return

    wtask = asyncio.create_task(worker())
    start = time.perf_counter()
    for i in range(n):
        q.enqueue({"i": i, "t": time.perf_counter()})
    try:
        await asyncio.wait_for(done.wait(), timeout=120.0)
    finally:
        if not wtask.done():
            wtask.cancel()
            try:
                await wtask
            except asyncio.CancelledError:
                pass
    elapsed = time.perf_counter() - start
    return {
        "n": len(latencies),
        "throughput_per_s": len(latencies) / elapsed,
        "p50_ms": statistics.median(latencies) * 1000,
        "p99_ms": (
            statistics.quantiles(latencies, n=100)[-1] * 1000
            if len(latencies) >= 100
            else max(latencies) * 1000
        ),
        "max_ms": max(latencies) * 1000,
    }


async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--n", type=int, default=5000)
    args = ap.parse_args()

    d = tempfile.mkdtemp()
    path = os.path.join(d, "bench.db")
    db = joblite.open(path)

    enq = bench_enqueue(db, args.n)
    print(
        f"enqueue:   {enq['ops_per_s']:>10.1f} ops/s  "
        f"({enq['n']} jobs in {enq['elapsed_s']:.3f}s)"
    )

    claim = bench_claim_ack(db, args.n)
    print(
        f"claim+ack: {claim['ops_per_s']:>10.1f} ops/s  "
        f"({claim['n']} jobs in {claim['elapsed_s']:.3f}s)"
    )

    for batch in (8, 32, 128):
        r = bench_claim_batch_ack_batch(db, args.n, batch)
        print(
            f"claim_batch+ack_batch (batch={batch:>3}): "
            f"{r['ops_per_s']:>10.1f} ops/s  "
            f"({r['n']} jobs in {r['elapsed_s']:.3f}s)"
        )

    e2e = await bench_end_to_end(db, args.n)
    print(
        f"end-to-end: {e2e['throughput_per_s']:>9.1f} ops/s  "
        f"p50={e2e['p50_ms']:.2f}ms  p99={e2e['p99_ms']:.2f}ms  "
        f"max={e2e['max_ms']:.2f}ms  (n={e2e['n']})"
    )


if __name__ == "__main__":
    asyncio.run(main())
