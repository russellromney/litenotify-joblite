"""Multi-process durability tests.

These run actual Python subprocesses that open the same .db file, which is
the real deployment story for WSGI/ASGI apps with multiple workers. Every
single-process test we have relies on in-process lock cooperation; these
tests prove the disk-level story (BEGIN IMMEDIATE across connections in
WAL mode).
"""

import json
import os
import subprocess
import sys
import tempfile
import textwrap

import pytest


def _run_worker_script(db_path: str, worker_id: str, n: int) -> list:
    """Spawn a subprocess that runs a worker claim loop until the queue
    drains. Returns the list of payload "i" values it actually processed.
    """
    script = textwrap.dedent(
        f"""
        import asyncio
        import json
        import sys
        sys.path.insert(0, {os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "packages")!r})

        import honker

        async def main():
            db = honker.open({db_path!r})
            q = db.queue('shared', visibility_timeout_s=30)
            processed = []
            import time
            idle_start = None
            while True:
                job = q.claim_one({worker_id!r})
                if job is not None:
                    processed.append(job.payload['i'])
                    job.ack()
                    idle_start = None
                    # Yield between claims so SQLite's busy-timeout
                    # has a chance to hand the write lock to the other
                    # process. Without this, whichever process opens
                    # first drains serially while the other blocks on
                    # BEGIN IMMEDIATE — artifact of SQLite's non-FIFO
                    # busy retry, not a fairness bug worth testing.
                    await asyncio.sleep(0.002)
                    continue
                if idle_start is None:
                    idle_start = time.time()
                elif time.time() - idle_start > 0.5:
                    break
                await asyncio.sleep(0.01)
            print(json.dumps(processed))

        asyncio.run(main())
        """
    )
    return subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
        timeout=30,
    )


def test_two_processes_claim_exclusively(tmp_path):
    """Two worker subprocesses on the same .db must split the work with
    zero overlap. This is the disk-level claim exclusivity proof."""
    db_path = str(tmp_path / "shared.db")

    # Seed from a third process so both workers start cold on an existing DB.
    import honker

    db = honker.open(db_path)
    q = db.queue("shared")
    n = 200
    for i in range(n):
        q.enqueue({"i": i})
    del db  # close the seeding process's handle

    import concurrent.futures

    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as ex:
        f1 = ex.submit(_run_worker_script, db_path, "proc-a", n)
        f2 = ex.submit(_run_worker_script, db_path, "proc-b", n)
        r1 = f1.result()
        r2 = f2.result()

    assert r1.returncode == 0, f"proc-a failed: {r1.stderr}"
    assert r2.returncode == 0, f"proc-b failed: {r2.stderr}"

    a_processed = json.loads(r1.stdout.strip())
    b_processed = json.loads(r2.stdout.strip())

    combined = a_processed + b_processed
    # Every job handled exactly once.
    assert sorted(combined) == list(range(n))
    # Zero overlap between workers (this is the whole point).
    assert set(a_processed).isdisjoint(set(b_processed))
    # Each worker got a meaningful share (sanity check that we didn't just
    # serialize through one worker). Permissive lower bound because scheduler
    # quirks are real — we mainly care about "both workers participated".
    assert len(a_processed) > 0
    assert len(b_processed) > 0


def test_seeder_and_worker_in_separate_processes(tmp_path):
    """Simulate a web request process enqueuing while a worker process
    drains, just like a real FastAPI/Django deployment."""
    db_path = str(tmp_path / "shared.db")

    import honker

    db = honker.open(db_path)
    q = db.queue("shared")
    q.enqueue({"i": 1})
    q.enqueue({"i": 2})
    del db

    result = _run_worker_script(db_path, "w1", 2)
    assert result.returncode == 0
    processed = json.loads(result.stdout.strip())
    assert sorted(processed) == [1, 2]


def test_live_enqueuer_while_worker_drains(tmp_path):
    """A third process enqueues continuously while two worker processes
    drain. All produced jobs must eventually be processed exactly once."""
    db_path = str(tmp_path / "shared.db")

    # Pre-seed a few so workers find something immediately on start.
    import honker

    db = honker.open(db_path)
    q = db.queue("shared")
    for i in range(10):
        q.enqueue({"i": i})
    del db

    enqueuer_script = textwrap.dedent(
        f"""
        import sys, time
        sys.path.insert(0, {os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "packages")!r})
        import honker
        db = honker.open({db_path!r})
        q = db.queue('shared')
        for i in range(10, 60):
            q.enqueue({{'i': i}})
            time.sleep(0.005)
        """
    )

    import concurrent.futures

    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as ex:
        e = ex.submit(
            subprocess.run,
            [sys.executable, "-c", enqueuer_script],
            capture_output=True,
            text=True,
            timeout=10,
        )
        # Tiny stagger so workers come up alongside the enqueuer.
        import time
        time.sleep(0.05)
        w1 = ex.submit(_run_worker_script, db_path, "w1", 60)
        w2 = ex.submit(_run_worker_script, db_path, "w2", 60)
        e_res = e.result()
        w1_res = w1.result()
        w2_res = w2.result()

    assert e_res.returncode == 0, f"enqueuer failed: {e_res.stderr}"
    assert w1_res.returncode == 0, f"w1 failed: {w1_res.stderr}"
    assert w2_res.returncode == 0, f"w2 failed: {w2_res.stderr}"

    a = json.loads(w1_res.stdout.strip())
    b = json.loads(w2_res.stdout.strip())
    combined = a + b
    assert sorted(combined) == list(range(60))
    assert set(a).isdisjoint(set(b))


def _run_pressure_producer_script(
    db_path: str,
    producer_id: int,
    count: int,
) -> subprocess.CompletedProcess:
    script = textwrap.dedent(
        f"""
        import json
        import os
        import sys
        import time

        sys.path.insert(0, {os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "packages")!r})
        import honker

        db = honker.open({db_path!r})
        q = db.queue("pressure")
        keys = []
        for i in range({count}):
            key = f"p{producer_id}-{{i:03d}}"
            q.enqueue({{
                "producer": {producer_id},
                "seq": i,
                "key": key,
            }}, priority=i % 7)
            keys.append(key)
            if i % 11 == 0:
                time.sleep(0.001)
        print(json.dumps(keys))
        """
    )
    return subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
        timeout=30,
    )


def _run_pressure_worker_script(
    db_path: str,
    worker_id: str,
    idle_after_s: float = 1.0,
) -> subprocess.CompletedProcess:
    script = textwrap.dedent(
        f"""
        import json
        import os
        import sys
        import time

        sys.path.insert(0, {os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "packages")!r})
        import honker

        db = honker.open({db_path!r})
        q = db.queue("pressure", visibility_timeout_s=30)
        processed = []
        idle_since = None
        while True:
            jobs = q.claim_batch({worker_id!r}, 7)
            if jobs:
                ids = []
                for job in jobs:
                    ids.append(job.id)
                    processed.append({{
                        "id": job.id,
                        "key": job.payload["key"],
                        "producer": job.payload["producer"],
                        "seq": job.payload["seq"],
                    }})
                acked = q.ack_batch(ids, {worker_id!r})
                if acked != len(ids):
                    raise RuntimeError(f"acked {{acked}} of {{len(ids)}} jobs")
                idle_since = None
                # Small yield so the single SQLite writer lock rotates
                # across workers instead of one process draining alone.
                time.sleep(0.002)
                continue

            now = time.time()
            if idle_since is None:
                idle_since = now
            elif now - idle_since >= {idle_after_s!r}:
                break
            time.sleep(0.01)

        print(json.dumps(processed))
        """
    )
    return subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
        timeout=45,
    )


def test_many_processes_enqueue_claim_and_ack_exactly_once(tmp_path):
    """Pressure proof for the queue claim path.

    This is the real scary shape:

    - several independent producer processes all write to one SQLite file
    - several independent worker processes claim in batches
    - every worker acks through the shared SQL function
    - the parent proves every produced logical key was processed once

    This catches bugs that single-process tests miss: writer-lock
    serialization mistakes, claim UPDATE overlap, duplicate batch
    returns, acking the wrong worker's claim, and dropped rows under
    concurrent producer/consumer pressure.
    """
    db_path = str(tmp_path / "pressure.db")
    producer_count = 4
    jobs_per_producer = 75
    worker_count = 6
    total_jobs = producer_count * jobs_per_producer

    import honker

    # Bootstrap before subprocesses race to open the file.
    db = honker.open(db_path)
    del db

    import concurrent.futures

    with concurrent.futures.ThreadPoolExecutor(
        max_workers=producer_count + worker_count
    ) as ex:
        producer_futs = [
            ex.submit(_run_pressure_producer_script, db_path, p, jobs_per_producer)
            for p in range(producer_count)
        ]
        # Start workers while producers are still writing, not after the
        # queue is already full.
        import time

        time.sleep(0.03)
        worker_futs = [
            ex.submit(_run_pressure_worker_script, db_path, f"worker-{w}")
            for w in range(worker_count)
        ]
        producer_results = [f.result() for f in producer_futs]
        worker_results = [f.result() for f in worker_futs]

    produced = []
    for i, res in enumerate(producer_results):
        assert res.returncode == 0, f"producer {i} failed: {res.stderr}"
        produced.extend(json.loads(res.stdout.strip()))

    processed = []
    workers_that_helped = 0
    for i, res in enumerate(worker_results):
        assert res.returncode == 0, f"worker {i} failed: {res.stderr}"
        rows = json.loads(res.stdout.strip())
        if rows:
            workers_that_helped += 1
        processed.extend(rows)

    produced_keys = sorted(produced)
    processed_keys = sorted(row["key"] for row in processed)
    assert len(produced_keys) == total_jobs
    assert len(processed_keys) == total_jobs
    assert processed_keys == produced_keys
    assert len(set(processed_keys)) == total_jobs
    assert len({row["id"] for row in processed}) == total_jobs
    assert workers_that_helped >= 2

    db = honker.open(db_path)
    live = db.query("SELECT COUNT(*) AS c FROM _honker_live WHERE queue='pressure'")
    dead = db.query("SELECT COUNT(*) AS c FROM _honker_dead WHERE queue='pressure'")
    assert live[0]["c"] == 0
    assert dead[0]["c"] == 0
