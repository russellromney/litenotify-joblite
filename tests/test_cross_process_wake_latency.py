"""Cross-process wake-latency regression test.

The README's pitch is 'sub-millisecond to low-single-digit-ms wake
latency, bounded by the 1 ms stat-poll cadence, for commits in OTHER
processes.' This test pins that story in CI so it can't silently
regress.

Strategy: parent spawns a subprocess that opens the same .db file
and registers a listener. Parent waits for READY, commits one
notify(), measures time-to-wake. Repeats `SAMPLES` times and asserts
p99 < 100 ms (loose ceiling — real p99 is ~2-30 ms on M-series,
kernel-dependent).

Kept as a test because the claim is load-bearing and the bench
(`bench/wake_latency_bench.py`) is run-it-yourself, not CI-enforced.
"""

import os
import subprocess
import sys
import time

import pytest


REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

SAMPLES = 30  # small enough to keep CI fast; p99 stable at 30+ samples


_LISTENER_SCRIPT = r"""
import asyncio
import sys

sys.path.insert(0, {repo!r})
import joblite

db = joblite.open({db_path!r})


async def main():
    listener = db.listen("wake")
    print("READY", flush=True)
    async for _ in listener:
        print("WAKE", flush=True)
        return


asyncio.run(main())
"""


def _run_sample(db_path: str) -> float:
    """One wake-latency sample, in milliseconds. Returns latency from
    the parent's `tx.notify()` commit to the parent observing the
    subprocess's WAKE line."""
    import joblite

    script = _LISTENER_SCRIPT.format(repo=REPO_ROOT, db_path=db_path)
    proc = subprocess.Popen(
        [sys.executable, "-c", script],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        ready = proc.stdout.readline()
        assert ready.strip() == "READY", f"listener: {ready!r}"

        db = joblite.open(db_path)
        t0 = time.perf_counter()
        with db.transaction() as tx:
            tx.notify("wake", "ping")
        wake = proc.stdout.readline()
        t1 = time.perf_counter()
        assert wake.strip() == "WAKE", f"listener: {wake!r}"
        return (t1 - t0) * 1000.0
    finally:
        try:
            proc.wait(timeout=3.0)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()


def test_cross_process_wake_latency_p99_under_bound(tmp_path):
    db_path = str(tmp_path / "wake.db")

    # Pre-create the WAL so first-sample latency doesn't include
    # journal bootstrap.
    import joblite
    db = joblite.open(db_path)
    with db.transaction() as tx:
        tx.execute("CREATE TABLE _warmup (i INTEGER)")
    del db

    times_ms = [_run_sample(db_path) for _ in range(SAMPLES)]
    times_ms.sort()

    p50 = times_ms[len(times_ms) // 2]
    p99 = times_ms[-1]  # max of the sample set
    median_bound = 25.0  # generous; real p50 ~= 1-2 ms on M-series
    p99_bound = 100.0    # loose; real p99 rarely exceeds 30 ms

    assert p50 < median_bound, (
        f"cross-process wake p50 = {p50:.2f} ms exceeds {median_bound} ms; "
        f"samples (sorted) = {times_ms}"
    )
    assert p99 < p99_bound, (
        f"cross-process wake p99 = {p99:.2f} ms exceeds {p99_bound} ms; "
        f"samples (sorted) = {times_ms}"
    )
