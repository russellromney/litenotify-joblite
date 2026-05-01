"""Tests for the honker Python package.

Covers the six must-pass tests from PLAN.md plus ordering, delayed run_at,
max_attempts→dead, rollback-drops-enqueue, and worker-wakes-on-NOTIFY.
"""

import asyncio
import json
import os
import queue as queue_mod
import subprocess
import sys
import threading
import time

import pytest

import honker


REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PACKAGES_ROOT = os.path.join(REPO_ROOT, "packages")
HONKER_PYTHON_ROOT = os.path.join(PACKAGES_ROOT, "honker", "python")

_CLAIM_ONCE_SCRIPT = r"""
import asyncio
import json
import sys
import time

sys.path.insert(0, {honker_python!r})
sys.path.insert(0, {packages!r})
import honker

db = honker.open({db_path!r})
q = db.queue({queue_name!r}{queue_args})


async def main():
    it = q.claim({worker_id!r}, idle_poll_s={idle_poll_s})
    print("READY", flush=True)
    job = await asyncio.wait_for(it.__anext__(), timeout={timeout_s})
    claimed_at = time.time()
    payload = job.payload
    attempts = job.attempts
    job_id = job.id
    job.ack()
    print(json.dumps({{
        "claimed_at": claimed_at,
        "payload": payload,
        "attempts": attempts,
        "job_id": job_id,
    }}), flush=True)


asyncio.run(main())
"""


def _spawn_claim_once(
    db_path: str,
    queue_name: str,
    worker_id: str,
    *,
    idle_poll_s: float = 30.0,
    timeout_s: float = 20.0,
    queue_kwargs_sql: str = "",
) -> subprocess.Popen:
    script = _CLAIM_ONCE_SCRIPT.format(
        honker_python=HONKER_PYTHON_ROOT,
        packages=PACKAGES_ROOT,
        db_path=db_path,
        queue_name=queue_name,
        worker_id=worker_id,
        idle_poll_s=idle_poll_s,
        timeout_s=timeout_s,
        queue_args=queue_kwargs_sql,
    )
    return subprocess.Popen(
        [sys.executable, "-c", script],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        bufsize=1,
    )


def _wait_for_child_line(proc: subprocess.Popen, timeout: float = 5.0) -> str:
    lines: queue_mod.Queue[str] = queue_mod.Queue()

    def reader():
        if proc.stdout is None:
            return
        line = proc.stdout.readline()
        if line:
            lines.put(line.strip())

    threading.Thread(target=reader, daemon=True).start()
    deadline = time.time() + timeout
    while time.time() < deadline:
        if proc.poll() is not None:
            out = proc.stdout.read() if proc.stdout else ""
            err = proc.stderr.read() if proc.stderr else ""
            raise AssertionError(
                "child exited early: "
                f"rc={proc.returncode}, stdout={out!r}, stderr={err!r}"
            )
        try:
            return lines.get(timeout=0.1)
        except queue_mod.Empty:
            pass
    raise AssertionError("timed out waiting for child output")


def _drain_state(db):
    rows = db.query("SELECT id, state, attempts FROM _honker_jobs ORDER BY id")
    return {r["id"]: r for r in rows}


def test_enqueue_and_claim_round_trip(db_path):
    db = honker.open(db_path)
    q = db.queue("work")
    q.enqueue({"k": 1})
    job = q.claim_one("w1")
    assert job is not None
    assert job.payload == {"k": 1}
    assert job.state == "processing"
    assert job.attempts == 1
    assert job.ack() is True
    # ack deletes the processing row; no 'done' state exists as a table.
    # The inspection view now returns zero rows for the acked job.
    rows = db.query("SELECT state FROM _honker_jobs WHERE id=?", [job.id])
    assert rows == []


def test_two_workers_claim_returns_exactly_one_winner(db_path):
    """PLAN test #3: no double-claim under races."""
    db = honker.open(db_path)
    q = db.queue("work")
    q.enqueue({"n": 1})

    results: list = []
    barrier = threading.Barrier(8)

    def attempt(tag):
        barrier.wait()
        j = q.claim_one(f"w-{tag}")
        if j is not None:
            results.append(j)

    threads = [threading.Thread(target=attempt, args=(i,)) for i in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert len(results) == 1, f"expected one winner, got {len(results)}"
    assert results[0].attempts == 1


def test_expired_claim_cannot_ack_and_is_reclaimable(db_path):
    """PLAN test #4: a worker whose claim expired loses its ack."""
    db = honker.open(db_path)
    q = db.queue("work", visibility_timeout_s=1)
    q.enqueue({"n": 1})

    job = q.claim_one("slow")
    assert job is not None
    # Simulate claim expiry by advancing claim_expires_at into the past.
    with db.transaction() as tx:
        tx.execute(
            "UPDATE _honker_live SET claim_expires_at = unixepoch() - 1 WHERE id=?",
            [job.id],
        )

    # Another worker can now reclaim because the expired-processing branch
    # of the claim predicate kicks in.
    job2 = q.claim_one("fast")
    assert job2 is not None
    assert job2.id == job.id
    assert job2.attempts == 2

    # The slow worker's ack fails loudly (returns False).
    assert job.ack() is False
    # The fast worker's ack succeeds.
    assert job2.ack() is True


def test_heartbeat_only_extends_matching_worker(db_path):
    """PLAN test #5: heartbeat rejects a mismatched worker_id."""
    db = honker.open(db_path)
    q = db.queue("work", visibility_timeout_s=2)
    q.enqueue({"n": 1})
    job = q.claim_one("owner")
    assert job is not None

    # Mismatched worker_id → no extension.
    assert q.heartbeat(job.id, "imposter", extend_s=60) is False

    # Correct worker_id → extension applied.
    before = db.query(
        "SELECT claim_expires_at FROM _honker_jobs WHERE id=?", [job.id]
    )[0]["claim_expires_at"]
    time.sleep(0.05)
    assert q.heartbeat(job.id, "owner", extend_s=600) is True
    after = db.query(
        "SELECT claim_expires_at FROM _honker_jobs WHERE id=?", [job.id]
    )[0]["claim_expires_at"]
    assert after > before


def test_begin_immediate_under_concurrent_readers(db_path):
    """PLAN test #6: BEGIN IMMEDIATE does not deadlock under reader pressure."""
    db = honker.open(db_path, max_readers=4)
    q = db.queue("work")
    stop = threading.Event()

    def reader():
        while not stop.is_set():
            _ = db.query("SELECT COUNT(*) AS c FROM _honker_jobs")

    readers = [threading.Thread(target=reader) for _ in range(4)]
    for t in readers:
        t.start()

    try:
        for i in range(200):
            q.enqueue({"i": i})
    finally:
        stop.set()
        for t in readers:
            t.join(timeout=5.0)

    rows = db.query("SELECT COUNT(*) AS c FROM _honker_jobs")
    assert rows[0]["c"] == 200


def test_rollback_of_business_tx_drops_enqueue(db_path):
    """The whole pitch: business write + enqueue are atomic."""
    db = honker.open(db_path)
    q = db.queue("work")

    with pytest.raises(RuntimeError):
        with db.transaction() as tx:
            tx.execute("CREATE TABLE orders (id INTEGER)")
            tx.execute("INSERT INTO orders (id) VALUES (?)", [1])
            q.enqueue({"order": 1}, tx=tx)
            raise RuntimeError("business error")

    # The CREATE TABLE itself got rolled back; orders does not exist.
    with pytest.raises(RuntimeError):
        db.query("SELECT * FROM orders")

    # And no job.
    rows = db.query("SELECT COUNT(*) AS c FROM _honker_jobs")
    assert rows[0]["c"] == 0


def test_enqueue_in_request_tx_commits_atomically(db_path):
    db = honker.open(db_path)
    q = db.queue("work")
    with db.transaction() as tx:
        tx.execute("CREATE TABLE orders (id INTEGER)")
        tx.execute("INSERT INTO orders (id) VALUES (?)", [1])
        q.enqueue({"order": 1}, tx=tx)
    orders = db.query("SELECT id FROM orders")
    assert orders[0]["id"] == 1
    jobs = db.query("SELECT payload FROM _honker_jobs")
    assert len(jobs) == 1


def test_priority_ordering(db_path):
    db = honker.open(db_path)
    q = db.queue("work")
    q.enqueue({"n": 1}, priority=0)
    q.enqueue({"n": 2}, priority=10)
    q.enqueue({"n": 3}, priority=5)

    got = []
    for _ in range(3):
        j = q.claim_one("w")
        got.append(j.payload["n"])
    assert got == [2, 3, 1]


def test_delayed_run_at_not_claimed_until_due(db_path):
    db = honker.open(db_path)
    q = db.queue("work")
    future = int(time.time()) + 3600
    q.enqueue({"n": 1}, run_at=future)
    assert q.claim_one("w") is None

    # Rewrite run_at to the past via direct SQL to avoid real sleep.
    # The pending job lives in _honker_live; UPDATE there.
    with db.transaction() as tx:
        tx.execute(
            "UPDATE _honker_live SET run_at=unixepoch() - 1 WHERE payload=?",
            ['{"n": 1}'],
        )
    j = q.claim_one("w")
    assert j is not None
    assert j.payload == {"n": 1}


def test_max_attempts_transitions_to_dead(db_path):
    db = honker.open(db_path)
    q = db.queue("work", max_attempts=2)
    q.enqueue({"n": 1})

    j1 = q.claim_one("w")
    assert j1.attempts == 1
    assert j1.retry(delay_s=0, error="try1") is True

    # Rewrite run_at so it's immediately claimable again.
    # After retry the row is back in _honker_live.
    with db.transaction() as tx:
        tx.execute("UPDATE _honker_live SET run_at=unixepoch() - 1")

    j2 = q.claim_one("w")
    assert j2.attempts == 2
    # With attempts=2 and max_attempts=2, retry should mark it dead.
    assert j2.retry(delay_s=0, error="try2") is True

    rows = db.query("SELECT state, last_error FROM _honker_jobs")
    assert rows[0]["state"] == "dead"
    assert rows[0]["last_error"] == "try2"


def test_fail_goes_straight_to_dead(db_path):
    db = honker.open(db_path)
    q = db.queue("work", max_attempts=10)
    q.enqueue({"n": 1})
    j = q.claim_one("w")
    assert j.fail("bad payload") is True
    rows = db.query("SELECT state FROM _honker_jobs")
    assert rows[0]["state"] == "dead"


async def test_worker_wakes_on_notify_fast_path(db_path):
    """An idle worker loop returns as soon as a job is enqueued, well under
    the 5s polling fallback.
    """
    db = honker.open(db_path)
    q = db.queue("work")

    loop = asyncio.get_running_loop()

    async def consume():
        async for job in q.claim("w"):
            job.ack()
            return loop.time()

    task = asyncio.create_task(consume())
    await asyncio.sleep(0.1)  # let worker register its listener
    t0 = loop.time()

    def enqueue_later():
        q.enqueue({"n": 1})

    threading.Thread(target=enqueue_later).start()

    got_at = await asyncio.wait_for(task, timeout=3.0)
    assert got_at - t0 < 2.0, f"worker woke after {got_at - t0:.2f}s; NOTIFY path broken"


async def test_worker_wakes_on_delayed_job_deadline(db_path):
    """A sleeping worker should wake when `run_at` arrives even if no
    new commit lands at the due moment.
    """
    db = honker.open(db_path)
    q = db.queue("work")
    q.enqueue({"n": 1}, run_at=int(time.time()) + 2)
    assert q.claim_one("precheck") is None

    loop = asyncio.get_running_loop()

    async def consume():
        async for job in q.claim("w", idle_poll_s=30.0):
            job.ack()
            return loop.time(), job.payload

    task = asyncio.create_task(consume())
    await asyncio.sleep(0.1)
    t0 = loop.time()
    got_at, payload = await asyncio.wait_for(task, timeout=5.0)
    assert payload == {"n": 1}
    assert got_at - t0 < 4.0, (
        f"worker woke after {got_at - t0:.2f}s; delayed-job deadline path broken"
    )


async def test_worker_wakes_on_reclaim_deadline(db_path):
    """A sleeping worker should wake when another worker's claim
    expires, without waiting for idle_poll_s.
    """
    db = honker.open(db_path)
    q = db.queue("work", visibility_timeout_s=1)
    q.enqueue({"n": 1})
    first = q.claim_one("owner")
    assert first is not None

    loop = asyncio.get_running_loop()

    async def consume():
        async for job in q.claim("rescuer", idle_poll_s=30.0):
            job.ack()
            return loop.time(), job.id, job.attempts

    task = asyncio.create_task(consume())
    await asyncio.sleep(0.1)
    t0 = loop.time()
    got_at, job_id, attempts = await asyncio.wait_for(task, timeout=5.0)
    assert job_id == first.id
    assert attempts == 2
    assert got_at - t0 < 4.0, (
        f"worker woke after {got_at - t0:.2f}s; reclaim deadline path broken"
    )


def test_worker_wakes_on_delayed_job_deadline_cross_process(db_path):
    """A real worker process should wake around `run_at` even when no
    new commit lands at the due moment.

    Parent process:
      1. starts an idle worker subprocess with `idle_poll_s=30`;
      2. waits for READY;
      3. enqueues one delayed job due in ~2s; and
      4. does nothing else.

    If the worker secretly depends on fallback polling, this would take
    ~30s instead of a few seconds.
    """
    db = honker.open(db_path)
    q = db.queue("work")
    run_at = int(time.time()) + 4
    q.enqueue({"n": 1}, run_at=run_at)
    proc = _spawn_claim_once(db_path, "work", "remote-worker")
    try:
        assert _wait_for_child_line(proc) == "READY"

        payload_line = _wait_for_child_line(proc, timeout=15.0)
        got = json.loads(payload_line)

        assert got["payload"] == {"n": 1}
        assert got["claimed_at"] >= run_at - 0.05, (
            f"claimed before delayed boundary: claimed_at={got['claimed_at']}, "
            f"run_at={run_at}"
        )
        assert got["claimed_at"] <= run_at + 3.0, (
            f"claim lagged too far past delayed boundary: "
            f"claimed_at={got['claimed_at']}, run_at={run_at}"
        )
    finally:
        try:
            proc.wait(timeout=3.0)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()


def test_worker_wakes_on_reclaim_deadline_cross_process(db_path):
    """A real worker process should wake when another worker's claim
    expires, without any new commit arriving at reclaim time.
    """
    db = honker.open(db_path)
    q = db.queue("work")
    q.enqueue({"n": 1})
    first = q.claim_one("owner")
    assert first is not None
    reclaim_at = int(time.time()) + 4
    expected_claimable_at = reclaim_at + 1
    with db.transaction() as tx:
        tx.execute(
            "UPDATE _honker_live SET claim_expires_at = ? WHERE id = ?",
            [reclaim_at, first.id],
        )

    proc = _spawn_claim_once(
        db_path,
        "work",
        "rescuer",
    )
    try:
        assert _wait_for_child_line(proc) == "READY"

        payload_line = _wait_for_child_line(proc, timeout=15.0)
        got = json.loads(payload_line)

        assert got["job_id"] == first.id
        assert got["attempts"] == 2
        assert got["payload"] == {"n": 1}
        assert got["claimed_at"] >= expected_claimable_at - 0.05, (
            f"reclaimed before claim expiry window opened: "
            f"claimed_at={got['claimed_at']}, "
            f"expected_claimable_at={expected_claimable_at}"
        )
        assert got["claimed_at"] <= expected_claimable_at + 3.0, (
            f"reclaim lagged too far past expiry: claimed_at={got['claimed_at']}, "
            f"expected_claimable_at={expected_claimable_at}"
        )
    finally:
        try:
            proc.wait(timeout=3.0)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()


async def test_worker_processes_multiple_jobs(db_path):
    db = honker.open(db_path)
    q = db.queue("work")
    processed = []

    async def worker():
        async for job in q.claim("w"):
            processed.append(job.payload["n"])
            job.ack()
            if len(processed) == 5:
                return

    task = asyncio.create_task(worker())
    await asyncio.sleep(0.05)
    for i in range(5):
        q.enqueue({"n": i})
    await asyncio.wait_for(task, timeout=3.0)
    assert processed == [0, 1, 2, 3, 4]


def test_queue_instance_is_memoized(db_path):
    db = honker.open(db_path)
    q1 = db.queue("work")
    q2 = db.queue("work")
    assert q1 is q2


def test_retry_resets_worker_and_claim(db_path):
    db = honker.open(db_path)
    q = db.queue("work")
    q.enqueue({"n": 1})
    j = q.claim_one("w")
    assert j.retry(delay_s=0, error="e") is True
    row = db.query(
        "SELECT worker_id, claim_expires_at, state FROM _honker_jobs"
    )[0]
    assert row["worker_id"] is None
    assert row["claim_expires_at"] is None
    assert row["state"] == "pending"
