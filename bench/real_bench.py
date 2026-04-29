"""Realistic concurrent bench: N workers + M enqueuers, sustained load.

The fresh-DB single-threaded benchmarks don't catch what actually matters
in production:

  - multiple workers contending for the write lock (WAL serializes)
  - enqueue hitting the DB while workers are draining
  - the DB file larger than the page cache so actual I/O matters
  - sustained load long enough to exit transient/warmup effects

This bench seeds the DB with a configurable amount of dead-row history
(to grow the file past the page cache), launches N worker subprocesses
and M enqueuer subprocesses, and measures:

  - sustained claim+ack throughput across all workers (jobs/sec)
  - p50 / p99 / p99.9 end-to-end latency (enqueue -> ack)

Use it like:

    python bench/real_bench.py --workers 4 --enqueuers 2 --seconds 30 \\
                                --dead-rows 500000

By default runs with 4 workers, 2 enqueuers, 20s, and 100k dead rows.
"""

import argparse
import asyncio
import json
import os
import signal
import statistics
import subprocess
import sys
import tempfile
import textwrap
import time

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PACKAGES_ROOT = os.path.join(REPO_ROOT, "packages")


def seed_pending(db_path: str, queue: str, n: int) -> None:
    """Pre-seed `n` claimable jobs so workers can drain without live enqueue
    contention. Bypasses the honker Python layer for speed."""
    if n <= 0:
        return
    import sqlite3
    import time as _t

    conn = sqlite3.connect(db_path)
    conn.execute("BEGIN")
    t = _t.perf_counter()
    conn.executemany(
        """
        INSERT INTO _honker_live (queue, payload, state)
        VALUES (?, ?, 'pending')
        """,
        (
            (queue, f'{{"i": {i}, "t": {t}}}')
            for i in range(n)
        ),
    )
    conn.commit()
    conn.close()


def seed_dead(db_path: str, n: int) -> None:
    """Insert `n` fake dead rows directly via sqlite3 to stand in for
    historical job volume. Grows the DB file without going through the
    honker Python layer."""
    import sqlite3
    import honker

    # Let honker init the schema first.
    db = honker.open(db_path)
    db.queue("hist")
    del db

    if n <= 0:
        return

    conn = sqlite3.connect(db_path)
    conn.execute("BEGIN")
    conn.executemany(
        """
        INSERT INTO _honker_dead
          (id, queue, payload, priority, run_at, max_attempts, attempts,
           last_error, created_at, died_at)
        VALUES (?, 'hist', ?, 0, 0, 3, 3, 'fake', 0, 0)
        """,
        (
            (10_000_000 + i, f'{{"hist": {i}}}')
            for i in range(n)
        ),
    )
    conn.commit()
    conn.close()


def worker_script(
    db_path: str,
    worker_id: str,
    queue: str,
    idle_poll_s: float,
    lat_file: str,
) -> str:
    # Latencies are binary-appended (double per sample) to lat_file so we
    # don't contend on stdout pipes (subprocess.PIPE blocks once the
    # kernel buffer fills if the parent isn't draining). Reading the
    # file after the run is much simpler than a stdout drainer thread.
    return textwrap.dedent(
        f"""
        import asyncio, struct, sys, time
        sys.path.insert(0, {PACKAGES_ROOT!r})
        import honker

        async def main():
            db = honker.open({db_path!r})
            q = db.queue({queue!r})
            processed = 0
            buf = bytearray()
            FLUSH_EVERY = 4096  # bytes (512 samples)
            with open({lat_file!r}, "ab", buffering=0) as f:
                async for job in q.claim(
                    {worker_id!r},
                    idle_poll_s={idle_poll_s},
                ):
                    payload = job.payload
                    t_enq = payload.get("t", 0.0) if payload else 0.0
                    lat = time.perf_counter() - t_enq
                    buf.extend(struct.pack("<d", lat))
                    if len(buf) >= FLUSH_EVERY:
                        f.write(buf); buf.clear()
                    job.ack()
                    processed += 1
                if buf:
                    f.write(buf)
            print("DONE:" + str(processed), flush=True)

        try:
            asyncio.run(main())
        except KeyboardInterrupt:
            print("INT", flush=True)
        """
    )


def enqueuer_script(db_path: str, queue: str, rate_per_sec: int) -> str:
    # If rate_per_sec <= 0, enqueue as fast as possible.
    rate = max(0, int(rate_per_sec))
    return textwrap.dedent(
        f"""
        import sys, time
        sys.path.insert(0, {PACKAGES_ROOT!r})
        import honker

        db = honker.open({db_path!r})
        q = db.queue({queue!r})
        rate = {rate}
        interval = 1.0 / rate if rate > 0 else 0.0
        i = 0
        next_send = time.perf_counter()
        try:
            while True:
                if interval > 0:
                    now = time.perf_counter()
                    if now < next_send:
                        time.sleep(max(0, next_send - now))
                    next_send += interval
                q.enqueue({{"i": i, "t": time.perf_counter()}})
                i += 1
                if i % 1000 == 0:
                    print("ENQ:" + str(i), flush=True)
        except KeyboardInterrupt:
            print("INT:" + str(i), flush=True)
        """
    )


def spawn(script: str) -> subprocess.Popen:
    return subprocess.Popen(
        [sys.executable, "-c", script],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        bufsize=1,
    )


def run_bench(
    db_path: str,
    n_workers: int,
    n_enqueuers: int,
    duration_s: float,
    rate_per_enqueuer: int,
    queue_name: str,
    idle_poll_s: float,
) -> dict:
    # One latency file per worker. Binary-packed doubles -- workers
    # append, parent reads at the end. Avoids pipe buffer stalls.
    db_dir = os.path.dirname(os.path.abspath(db_path))
    lat_files = [
        os.path.join(db_dir, f"lat-{i}.bin") for i in range(n_workers)
    ]
    # Truncate any stale files from a previous run.
    for lf in lat_files:
        try:
            os.unlink(lf)
        except FileNotFoundError:
            pass
    workers = [
        spawn(
            worker_script(
                db_path,
                f"worker-{i}",
                queue_name,
                idle_poll_s,
                lat_files[i],
            )
        )
        for i in range(n_workers)
    ]
    enqueuers = [
        spawn(enqueuer_script(db_path, queue_name, rate_per_enqueuer))
        for _ in range(n_enqueuers)
    ]

    try:
        time.sleep(duration_s)
    finally:
        # SIGINT enqueuers first so nothing new arrives.
        for p in enqueuers:
            if p.poll() is None:
                p.send_signal(signal.SIGINT)
        for p in enqueuers:
            try:
                p.wait(timeout=5)
            except subprocess.TimeoutExpired:
                p.kill()
                p.wait()
        # Give workers a moment to drain, then SIGINT.
        time.sleep(0.5)
        for p in workers:
            if p.poll() is None:
                p.send_signal(signal.SIGINT)
        for p in workers:
            try:
                p.wait(timeout=5)
            except subprocess.TimeoutExpired:
                p.kill()
                p.wait()

    # Collect latencies from per-worker files (binary doubles).
    import struct
    latencies: list = []
    processed = 0
    for lf in lat_files:
        if os.path.exists(lf):
            with open(lf, "rb") as f:
                data = f.read()
            for i in range(0, len(data), 8):
                (val,) = struct.unpack_from("<d", data, i)
                latencies.append(val)
    for p in workers:
        out = p.stdout.read() if p.stdout else ""
        for line in out.splitlines():
            if line.startswith("DONE:"):
                processed += int(line[5:])

    enqueued = 0
    for p in enqueuers:
        out = p.stdout.read() if p.stdout else ""
        last = 0
        for line in out.splitlines():
            if line.startswith("ENQ:"):
                last = int(line[4:])
            elif line.startswith("INT:"):
                last = int(line[4:])
        enqueued += last

    latencies.sort()
    return {
        "enqueued": enqueued,
        "processed": processed if processed else len(latencies),
        "throughput": (len(latencies) / duration_s) if duration_s > 0 else 0,
        "lat_p50_ms": statistics.median(latencies) * 1000 if latencies else 0,
        "lat_p99_ms": (
            statistics.quantiles(latencies, n=100)[-1] * 1000
            if len(latencies) >= 100
            else (max(latencies) * 1000 if latencies else 0)
        ),
        "lat_p999_ms": (
            statistics.quantiles(latencies, n=1000)[-1] * 1000
            if len(latencies) >= 1000
            else (max(latencies) * 1000 if latencies else 0)
        ),
        "lat_max_ms": (max(latencies) * 1000) if latencies else 0,
        "duration_s": duration_s,
    }


def fmt(r: dict) -> str:
    return (
        f"  throughput: {r['throughput']:>8.0f} ops/s   "
        f"p50={r['lat_p50_ms']:>6.1f}ms   "
        f"p99={r['lat_p99_ms']:>7.1f}ms   "
        f"p999={r['lat_p999_ms']:>7.1f}ms   "
        f"max={r['lat_max_ms']:>7.1f}ms   "
        f"(processed {r['processed']}, enqueued {r['enqueued']})"
    )


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--workers", type=int, default=4)
    ap.add_argument("--enqueuers", type=int, default=2)
    ap.add_argument("--seconds", type=float, default=20.0)
    ap.add_argument(
        "--rate-per-enqueuer",
        type=int,
        default=0,
        help="Jobs per second per enqueuer (0 = as fast as possible)",
    )
    ap.add_argument(
        "--idle-poll-s",
        type=float,
        default=0.1,
        help="Worker poll interval when the queue is empty. Default 0.1s "
        "(10 checks/sec). Paranoia fallback only — the update watcher wakes "
        "workers on every commit from any process, so idle-poll rarely fires."
    )
    ap.add_argument("--queue", default="bench")
    ap.add_argument(
        "--dead-rows",
        type=int,
        default=100_000,
        help="Rows to insert into _honker_dead up front, simulating history",
    )
    ap.add_argument(
        "--preseed-jobs",
        type=int,
        default=0,
        help="Pre-seed this many pending jobs before bench starts (stress "
        "the drain path without live-enqueue contention).",
    )
    ap.add_argument("--runs", type=int, default=1)
    args = ap.parse_args()

    sys.path.insert(0, os.path.join(REPO_ROOT, "packages"))

    print(f"Config: {args.workers} workers, {args.enqueuers} enqueuers, "
          f"{args.seconds}s, "
          f"rate/enq={args.rate_per_enqueuer or 'unlimited'}")
    print()

    for dead in (0, args.dead_rows):
        for run in range(args.runs):
            with tempfile.TemporaryDirectory() as d:
                db_path = os.path.join(d, "bench.db")
                print(f"-- dead_rows={dead}  run={run+1}/{args.runs}")
                t0 = time.time()
                seed_dead(db_path, dead)
                print(f"   (seeded {dead} dead rows in {time.time()-t0:.1f}s)")
                if args.preseed_jobs:
                    t0 = time.time()
                    seed_pending(db_path, args.queue, args.preseed_jobs)
                    print(
                        f"   (pre-seeded {args.preseed_jobs} pending jobs "
                        f"in {time.time()-t0:.1f}s)"
                    )
                size_mb = os.path.getsize(db_path) / 1_000_000
                print(f"   DB file: {size_mb:.1f}MB")
                result = run_bench(
                    db_path=db_path,
                    n_workers=args.workers,
                    n_enqueuers=args.enqueuers,
                    duration_s=args.seconds,
                    rate_per_enqueuer=args.rate_per_enqueuer,
                    queue_name=args.queue,
                    idle_poll_s=args.idle_poll_s,
                )
                print(fmt(result))
                print()


if __name__ == "__main__":
    main()
