"""Stream publish + subscribe throughput + latency."""

import argparse
import asyncio
import os
import statistics
import sys
import tempfile
import time

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "packages"))
import joblite  # noqa: E402


async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--n", type=int, default=5000)
    args = ap.parse_args()

    d = tempfile.mkdtemp()
    db = joblite.open(os.path.join(d, "stream.db"))
    s = db.stream("bench")

    # Publish throughput
    start = time.perf_counter()
    for i in range(args.n):
        s.publish({"i": i, "t": time.perf_counter()})
    pub_elapsed = time.perf_counter() - start
    print(
        f"publish:   {args.n / pub_elapsed:>10.1f} events/s "
        f"({args.n} in {pub_elapsed:.3f}s)"
    )

    # Subscribe replay throughput (pre-seeded)
    got = []
    start = time.perf_counter()
    async for event in s.subscribe(from_offset=0):
        got.append(event)
        if len(got) == args.n:
            break
    sub_elapsed = time.perf_counter() - start
    print(
        f"replay:    {args.n / sub_elapsed:>10.1f} events/s "
        f"({args.n} in {sub_elapsed:.3f}s)"
    )

    # End-to-end publish -> delivered
    latencies: list = []
    done = asyncio.Event()
    target = min(args.n, 1000)

    async def consume():
        async for event in db.stream("live").subscribe(from_offset=0):
            latencies.append(time.perf_counter() - event.payload["t"])
            if len(latencies) >= target:
                done.set()
                return

    task = asyncio.create_task(consume())
    await asyncio.sleep(0.1)
    # Yield between publishes so the consumer can interleave.
    # Without this, publishes hog the event loop and p50 ends up being
    # approximately half the publish-loop duration (a harness artifact,
    # not a library latency).
    for i in range(target):
        db.stream("live").publish({"i": i, "t": time.perf_counter()})
        await asyncio.sleep(0)
    try:
        await asyncio.wait_for(done.wait(), timeout=30.0)
    finally:
        if not task.done():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

    print(
        f"e2e:       p50={statistics.median(latencies) * 1000:.2f}ms  "
        f"p99={statistics.quantiles(latencies, n=100)[-1] * 1000:.2f}ms  "
        f"max={max(latencies) * 1000:.2f}ms  (n={len(latencies)})"
    )


if __name__ == "__main__":
    asyncio.run(main())
