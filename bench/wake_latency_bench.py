"""Cross-process wake-latency microbench.

Measures what the README headline actually claims: the time from a
`tx.notify()` commit in one process to the corresponding wake in an
idle listener in another process. NOT throughput under saturation —
`real_bench.py` covers that.

Design: parent opens a DB, pre-seeds by commit-touching the WAL so
the watcher has a stable baseline. Then repeats:

  1. Parent spawns a short-lived Python subprocess that opens the
     same file, begins a listener, and waits to see one notification.
     The subprocess prints its READY marker once the listener is
     blocked on wal_events.
  2. Parent waits until READY, records `t0`, commits one
     `tx.notify('wake', ...)`, then reads a single line from the
     subprocess which contains the wake timestamp in ms-since-epoch.
  3. Parent records `dt = wake_ts - t0`.

Repeat N times, report p50 / p90 / p99. On M-series darwin expect
~1–2 ms p50 (bounded by the 1 ms stat-poll cadence); Linux tends to
run faster.

Run:
    python bench/wake_latency_bench.py --samples 500
"""

from __future__ import annotations

import argparse
import json
import os
import statistics
import subprocess
import sys
import tempfile
import time


REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# Let `python bench/wake_latency_bench.py` find the in-repo joblite
# package without needing `pip install -e`.
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)


_LISTENER_SCRIPT = r"""
import asyncio
import json
import sys
import time

sys.path.insert(0, {repo!r})
import joblite

db = joblite.open({db_path!r})


async def main():
    listener = db.listen("wake")
    # Record that we're armed; the parent can commit after this.
    print("READY", flush=True)
    async for n in listener:
        wake_ns = time.perf_counter_ns()
        # Print a line the parent can correlate. We emit
        # perf_counter_ns on the listener's clock — the parent also
        # uses perf_counter_ns, but different processes' monotonic
        # clocks may differ in origin. The parent's own "round-trip"
        # calculation is what's load-bearing; this print just tells
        # the parent the wake happened.
        print("WAKE:" + str(wake_ns), flush=True)
        return


asyncio.run(main())
"""


def run_sample(db_path: str, timeout_s: float) -> float:
    """One wake-latency sample, in seconds."""
    script = _LISTENER_SCRIPT.format(repo=REPO_ROOT, db_path=db_path)
    proc = subprocess.Popen(
        [sys.executable, "-c", script],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        # Wait for READY.
        line = proc.stdout.readline()
        if line.strip() != "READY":
            raise RuntimeError(f"listener never printed READY; got {line!r}")

        # Commit the notify. Import here so each sample's parent
        # handle is fresh (no shared writer slot across samples).
        import joblite  # noqa: E402

        db = joblite.open(db_path)
        t0 = time.perf_counter()
        with db.transaction() as tx:
            tx.notify("wake", {"ts": t0})
        # Wait for the WAKE line from the subprocess.
        wake_line = proc.stdout.readline()
        t1 = time.perf_counter()
        if not wake_line.startswith("WAKE:"):
            raise RuntimeError(f"unexpected listener output: {wake_line!r}")
        return t1 - t0
    finally:
        try:
            proc.wait(timeout=timeout_s)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument(
        "--samples", type=int, default=200,
        help="Number of wake samples to collect (default 200).",
    )
    ap.add_argument(
        "--timeout-s", type=float, default=5.0,
        help="Per-sample subprocess timeout (default 5s).",
    )
    args = ap.parse_args()

    with tempfile.TemporaryDirectory(prefix="wake-bench-") as d:
        db_path = os.path.join(d, "wake.db")
        # Touch the WAL so first-sample latency doesn't swallow
        # journal-mode bootstrap.
        import joblite
        db = joblite.open(db_path)
        with db.transaction() as tx:
            tx.execute("CREATE TABLE _warmup (i INTEGER)")
        del db

        print(
            f"running {args.samples} cross-process wake samples "
            f"against {db_path}",
            file=sys.stderr,
        )

        times_ms: list[float] = []
        for i in range(args.samples):
            dt_s = run_sample(db_path, timeout_s=args.timeout_s)
            times_ms.append(dt_s * 1000.0)
            if (i + 1) % 25 == 0:
                print(f"  {i + 1}/{args.samples}", file=sys.stderr)

    times_ms.sort()
    n = len(times_ms)
    p = lambda q: times_ms[min(n - 1, int(q * n))]  # noqa: E731
    print(json.dumps({
        "samples": n,
        "min_ms": times_ms[0],
        "p50_ms": p(0.50),
        "p90_ms": p(0.90),
        "p99_ms": p(0.99),
        "max_ms": times_ms[-1],
        "mean_ms": statistics.fmean(times_ms),
    }, indent=2))


if __name__ == "__main__":
    main()
