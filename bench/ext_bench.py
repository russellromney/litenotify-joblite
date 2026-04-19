"""Bench the loadable extension from raw Python sqlite3.

Compares:
  1. Raw sqlite3 + extension (single pattern per iteration):
       SELECT jl_claim_batch(...);  SELECT jl_ack_batch(...);
  2. Same pattern but with a batched claim+ack per tx cycle.
  3. Same but using batch=128.

Reference: how much of our Python joblite 3.4k/s ceiling is SQL+Rust
engine work vs PyO3/Python-loop overhead? Extension bench is the
same SQL work via sqlite3's C extension -- much less Python overhead.
"""
import json
import os
import sqlite3
import sys
import tempfile
import time

EXT = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "target",
    "release",
    "liblitenotify_ext",
)


def setup(path: str, n: int) -> sqlite3.Connection:
    conn = sqlite3.connect(path, isolation_level=None)
    conn.enable_load_extension(True)
    conn.load_extension(EXT)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA cache_size=-32000")
    conn.execute("PRAGMA temp_store=MEMORY")
    conn.execute("PRAGMA wal_autocheckpoint=10000")
    conn.execute("SELECT jl_bootstrap()")
    for i in range(n):
        conn.execute(
            "INSERT INTO _joblite_live (queue, payload) VALUES ('bench', ?)",
            (f'{{"i":{i}}}',),
        )
    return conn


def bench_single(n: int) -> float:
    with tempfile.TemporaryDirectory() as d:
        conn = setup(os.path.join(d, "t.db"), n)
        start = time.perf_counter()
        processed = 0
        while processed < n:
            row = conn.execute(
                "SELECT jl_claim_batch('bench', 'w', 1, 300)"
            ).fetchone()[0]
            jobs = json.loads(row)
            if not jobs:
                break
            ids_json = json.dumps([j["id"] for j in jobs])
            conn.execute("SELECT jl_ack_batch(?, 'w')", (ids_json,)).fetchone()
            processed += len(jobs)
        dt = time.perf_counter() - start
    return processed / dt


def bench_batched(n: int, batch: int) -> float:
    with tempfile.TemporaryDirectory() as d:
        conn = setup(os.path.join(d, "t.db"), n)
        start = time.perf_counter()
        processed = 0
        while processed < n:
            row = conn.execute(
                "SELECT jl_claim_batch('bench', 'w', ?, 300)",
                (batch,),
            ).fetchone()[0]
            jobs = json.loads(row)
            if not jobs:
                break
            ids_json = json.dumps([j["id"] for j in jobs])
            conn.execute("SELECT jl_ack_batch(?, 'w')", (ids_json,)).fetchone()
            processed += len(jobs)
        dt = time.perf_counter() - start
    return processed / dt


def main():
    N = 5000
    print(f"Extension benchmarked at n={N} jobs")
    print()
    for i in range(3):
        r_single = bench_single(N)
        r_batch_8 = bench_batched(N, 8)
        r_batch_32 = bench_batched(N, 32)
        r_batch_128 = bench_batched(N, 128)
        print(
            f"  run {i+1}:  "
            f"single={r_single:>7.0f}/s   "
            f"batch8={r_batch_8:>7.0f}/s   "
            f"batch32={r_batch_32:>7.0f}/s   "
            f"batch128={r_batch_128:>7.0f}/s"
        )


if __name__ == "__main__":
    main()
