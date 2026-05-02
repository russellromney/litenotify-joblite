"""Real user-shaped end-to-end scenarios.

These tests use separate Python processes that share one SQLite file.
That is the deployment shape Honker is trying to make boring: an app
process commits data, sleeping workers/listeners wake, and all state is
in the same database file.
"""

from __future__ import annotations

import json
import os
import sqlite3
import subprocess
import sys
import textwrap
import time
from concurrent.futures import ThreadPoolExecutor

import pytest

import honker


REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PACKAGES_ROOT = os.path.join(REPO_ROOT, "packages")
HONKER_PYTHON_ROOT = os.path.join(PACKAGES_ROOT, "honker", "python")
IDLE_POLL_S = 30.0

EXT_CANDIDATES = [
    os.path.join(REPO_ROOT, "target", "release", "libhonker_ext.dylib"),
    os.path.join(REPO_ROOT, "target", "release", "libhonker_ext.so"),
    os.path.join(REPO_ROOT, "target", "release", "honker_ext.dll"),
]
EXT_PATH = next((p for p in EXT_CANDIDATES if os.path.exists(p)), None)
HAS_LOAD_EXTENSION = hasattr(sqlite3.connect(":memory:"), "enable_load_extension")


CHILD = r"""
import asyncio
import json
import os
import sqlite3
import sys
import time

sys.path.insert(0, {honker_python!r})
sys.path.insert(0, {packages!r})
import honker

IDLE_POLL_S = {idle_poll_s!r}


def close_db(db):
    close = getattr(getattr(db, "_inner", None), "close", None)
    if callable(close):
        close()


async def claim_and_ack(db_path, queue, worker):
    db = honker.open(db_path)
    try:
        q = db.queue(queue)
        it = q.claim(worker, idle_poll_s=IDLE_POLL_S)
        print("READY", flush=True)
        async for job in it:
            payload = job.payload
            claimed_at = time.time()
            assert job.ack()
            print("RESULT " + json.dumps({{
                "payload": payload,
                "claimed_at": claimed_at,
                "job_id": job.id,
            }}, sort_keys=True), flush=True)
            return
    finally:
        close_db(db)


def crash_after_claim(db_path, queue, worker):
    db = honker.open(db_path)
    q = db.queue(queue, visibility_timeout_s=2)
    job = q.claim_one(worker)
    assert job is not None
    print("CLAIMED " + json.dumps(job.payload, sort_keys=True), flush=True)
    os._exit(0)


async def retry_claim(db_path, queue, worker, delay_s):
    db = honker.open(db_path)
    try:
        q = db.queue(queue, max_attempts=2)
        it = q.claim(worker, idle_poll_s=IDLE_POLL_S)
        print("READY", flush=True)
        async for job in it:
            assert job.retry(delay_s=int(delay_s), error=f"retry by {{worker}}")
            print("RETRIED " + json.dumps({{
                "attempts": job.attempts,
                "payload": job.payload,
                "at": time.time(),
            }}, sort_keys=True), flush=True)
            return
    finally:
        close_db(db)


async def save_result_worker(db_path, queue):
    db = honker.open(db_path)
    try:
        q = db.queue(queue)
        it = q.claim("result-worker", idle_poll_s=IDLE_POLL_S)
        print("READY", flush=True)
        async for job in it:
            value = {{"sum": job.payload["x"] + job.payload["y"]}}
            q.save_result(job.id, value, ttl=60)
            assert job.ack()
            print("SAVED " + json.dumps(value, sort_keys=True), flush=True)
            return
    finally:
        close_db(db)


async def wait_result(db_path, queue, job_id):
    db = honker.open(db_path)
    try:
        q = db.queue(queue)
        waiter = asyncio.create_task(q.wait_result(int(job_id), timeout=IDLE_POLL_S))
        await asyncio.sleep(0.05)
        print("READY", flush=True)
        value = await waiter
        print("RESULT " + json.dumps(value, sort_keys=True), flush=True)
    finally:
        close_db(db)


def rate_limit_once(db_path, name, limit, per):
    db = honker.open(db_path)
    try:
        ok = db.try_rate_limit(name, limit=int(limit), per=int(per))
        print("RESULT " + json.dumps({{"ok": ok}}), flush=True)
    finally:
        close_db(db)


def lock_holder_crash(db_path, name, ttl):
    db = honker.open(db_path)
    lock = db.lock(name, ttl=int(ttl), owner="holder")
    lock.__enter__()
    print("HELD", flush=True)
    os._exit(0)


async def lock_waiter(db_path, name, ttl):
    db = honker.open(db_path)
    try:
        try:
            with db.lock(name, ttl=int(ttl), owner="waiter"):
                print("UNEXPECTED", flush=True)
                return
        except honker.LockHeld:
            print("BLOCKED", flush=True)
        await asyncio.sleep(int(ttl) + 1.2)
        with db.lock(name, ttl=int(ttl), owner="waiter"):
            print("ACQUIRED", flush=True)
    finally:
        close_db(db)


async def stream_read(db_path, stream_name, consumer, count):
    db = honker.open(db_path)
    try:
        stream = db.stream(stream_name)
        it = stream.subscribe(
            consumer=consumer,
            save_every_n=0,
            save_every_s=0,
        )
        print("READY", flush=True)
        out = []
        for _ in range(int(count)):
            event = await asyncio.wait_for(it.__anext__(), timeout=IDLE_POLL_S)
            out.append({{"offset": event.offset, "payload": event.payload}})
            stream.save_offset(consumer, event.offset)
        print("RESULT " + json.dumps(out, sort_keys=True), flush=True)
    finally:
        close_db(db)


def raw_sql_enqueue(db_path, ext_path, commit):
    conn = sqlite3.connect(db_path)
    conn.enable_load_extension(True)
    conn.load_extension(ext_path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("SELECT honker_bootstrap()")
    conn.execute("CREATE TABLE IF NOT EXISTS orders (id INTEGER PRIMARY KEY, email TEXT)")
    conn.execute("BEGIN IMMEDIATE")
    conn.execute("INSERT INTO orders (id, email) VALUES (1, 'alice@example.com')")
    conn.execute(
        "SELECT honker_enqueue(?, ?, NULL, NULL, 0, 3, NULL)",
        ("emails", json.dumps({{"to": "alice@example.com", "order_id": 1}})),
    )
    if commit == "commit":
        conn.commit()
        print("COMMIT", flush=True)
    else:
        conn.rollback()
        print("ROLLBACK", flush=True)
    conn.close()


async def main():
    role = sys.argv[1]
    if role == "claim":
        await claim_and_ack(sys.argv[2], sys.argv[3], sys.argv[4])
    elif role == "crash-after-claim":
        crash_after_claim(sys.argv[2], sys.argv[3], sys.argv[4])
    elif role == "retry-claim":
        await retry_claim(sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5])
    elif role == "save-result-worker":
        await save_result_worker(sys.argv[2], sys.argv[3])
    elif role == "wait-result":
        await wait_result(sys.argv[2], sys.argv[3], sys.argv[4])
    elif role == "rate-limit":
        rate_limit_once(sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5])
    elif role == "lock-holder-crash":
        lock_holder_crash(sys.argv[2], sys.argv[3], sys.argv[4])
    elif role == "lock-waiter":
        await lock_waiter(sys.argv[2], sys.argv[3], sys.argv[4])
    elif role == "stream-read":
        await stream_read(sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5])
    elif role == "raw-sql-enqueue":
        raw_sql_enqueue(sys.argv[2], sys.argv[3], sys.argv[4])
    else:
        raise SystemExit(f"unknown role: {{role}}")


asyncio.run(main())
""".format(
    honker_python=HONKER_PYTHON_ROOT,
    packages=PACKAGES_ROOT,
    idle_poll_s=IDLE_POLL_S,
)


def _spawn(*args: str) -> subprocess.Popen:
    return subprocess.Popen(
        [sys.executable, "-c", CHILD, *args],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )


def _run(*args: str, timeout: float = 20.0) -> subprocess.CompletedProcess:
    return subprocess.run(
        [sys.executable, "-c", CHILD, *args],
        capture_output=True,
        text=True,
        timeout=timeout,
    )


def _line(proc: subprocess.Popen, timeout: float = 12.0) -> str:
    assert proc.stdout is not None
    deadline = time.time() + timeout
    while time.time() < deadline:
        line = proc.stdout.readline()
        if line:
            return line.strip()
        if proc.poll() is not None:
            break
    stderr = ""
    if proc.stderr is not None:
        stderr = proc.stderr.read()
    raise AssertionError(f"child produced no line; rc={proc.poll()} stderr={stderr}")


def _json_line(line: str, prefix: str = "RESULT ") -> dict | list:
    assert line.startswith(prefix), line
    return json.loads(line.removeprefix(prefix))


def _finish(proc: subprocess.Popen, timeout: float = 5.0) -> None:
    try:
        proc.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(timeout=5.0)
    assert proc.returncode == 0, (
        f"child exited {proc.returncode}; stderr="
        f"{proc.stderr.read() if proc.stderr else ''}"
    )


def test_delayed_job_wakes_sleeping_worker_process(db_path):
    db = honker.open(db_path)
    q = db.queue("delayed")
    worker = _spawn("claim", db_path, "delayed", "delayed-worker")
    try:
        assert _line(worker) == "READY"
        run_at = int(time.time()) + 3
        q.enqueue({"kind": "delayed"}, run_at=run_at)

        got = _json_line(_line(worker, timeout=8.0))
        assert got["payload"] == {"kind": "delayed"}
        assert got["claimed_at"] >= run_at - 0.05
        assert got["claimed_at"] <= run_at + 3.0
        _finish(worker)
    finally:
        if worker.poll() is None:
            worker.kill()


def test_crashed_worker_claim_is_reclaimed_by_sleeping_worker(db_path):
    db = honker.open(db_path)
    q = db.queue("reclaim", visibility_timeout_s=2)
    q.enqueue({"kind": "recover"})

    crashed = _spawn("crash-after-claim", db_path, "reclaim", "crashy")
    assert _json_line(_line(crashed), prefix="CLAIMED ") == {"kind": "recover"}
    crashed.wait(timeout=5.0)
    assert crashed.returncode == 0

    rescuer = _spawn("claim", db_path, "reclaim", "rescuer")
    try:
        assert _line(rescuer) == "READY"
        got = _json_line(_line(rescuer, timeout=8.0))
        assert got["payload"] == {"kind": "recover"}
        _finish(rescuer)
    finally:
        if rescuer.poll() is None:
            rescuer.kill()


def test_retry_backoff_wakes_then_exhausts_to_dead(db_path):
    db = honker.open(db_path)
    q = db.queue("retry", max_attempts=2)
    q.enqueue({"kind": "retry"})

    first = _spawn("retry-claim", db_path, "retry", "retry-a", "2")
    try:
        assert _line(first) == "READY"
        first_retry = _json_line(_line(first), prefix="RETRIED ")
        assert first_retry["payload"] == {"kind": "retry"}
        _finish(first)
    finally:
        if first.poll() is None:
            first.kill()

    second = _spawn("retry-claim", db_path, "retry", "retry-b", "0")
    try:
        assert _line(second) == "READY"
        second_retry = _json_line(_line(second, timeout=8.0), prefix="RETRIED ")
        assert second_retry["payload"] == {"kind": "retry"}
        _finish(second)
    finally:
        if second.poll() is None:
            second.kill()

    rows = db.query(
        "SELECT state, attempts, last_error FROM _honker_jobs WHERE queue='retry'"
    )
    assert rows == [
        {
            "state": "dead",
            "attempts": 2,
            "last_error": "retry by retry-b",
        }
    ]


async def test_wait_result_wakes_when_worker_process_saves_result(db_path):
    db = honker.open(db_path)
    q = db.queue("results")
    job_id = q.enqueue({"x": 2, "y": 5})

    waiter = _spawn("wait-result", db_path, "results", str(job_id))
    try:
        assert _line(waiter) == "READY"
        worker = _spawn("save-result-worker", db_path, "results")
        try:
            assert _line(worker) == "READY"
            assert _json_line(_line(worker), prefix="SAVED ") == {"sum": 7}
            _finish(worker)
        finally:
            if worker.poll() is None:
                worker.kill()

        assert _json_line(_line(waiter)) == {"sum": 7}
        _finish(waiter)
    finally:
        if waiter.poll() is None:
            waiter.kill()


def test_rate_limit_is_shared_across_processes(db_path):
    honker.open(db_path).try_rate_limit("warmup", limit=1, per=60)

    def run_one(_i: int) -> bool:
        res = _run("rate-limit", db_path, "api", "3", "60")
        assert res.returncode == 0, res.stderr
        return bool(_json_line(res.stdout.strip())["ok"])

    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(run_one, range(8)))

    assert results.count(True) == 3
    assert results.count(False) == 5


def test_named_lock_blocks_cross_process_until_crashed_holder_ttl(db_path):
    holder = _spawn("lock-holder-crash", db_path, "singleton", "2")
    assert _line(holder) == "HELD"
    holder.wait(timeout=5.0)
    assert holder.returncode == 0

    waiter = _spawn("lock-waiter", db_path, "singleton", "2")
    try:
        assert _line(waiter) == "BLOCKED"
        assert _line(waiter, timeout=6.0) == "ACQUIRED"
        _finish(waiter)
    finally:
        if waiter.poll() is None:
            waiter.kill()


def test_stream_consumer_replays_then_resumes_after_saved_offset(db_path):
    db = honker.open(db_path)
    stream = db.stream("orders")
    stream.publish({"n": 1})
    stream.publish({"n": 2})

    first = _spawn("stream-read", db_path, "orders", "dashboard", "1")
    try:
        assert _line(first) == "READY"
        got_first = _json_line(_line(first))
        assert [row["payload"] for row in got_first] == [{"n": 1}]
        _finish(first)
    finally:
        if first.poll() is None:
            first.kill()

    second = _spawn("stream-read", db_path, "orders", "dashboard", "1")
    try:
        assert _line(second) == "READY"
        got_second = _json_line(_line(second))
        assert [row["payload"] for row in got_second] == [{"n": 2}]
        _finish(second)
    finally:
        if second.poll() is None:
            second.kill()


@pytest.mark.skipif(
    EXT_PATH is None or not HAS_LOAD_EXTENSION,
    reason="loadable extension is unavailable in this Python/sqlite build",
)
def test_raw_sql_transaction_enqueue_wakes_python_worker(db_path):
    worker = _spawn("claim", db_path, "emails", "python-worker")
    try:
        assert _line(worker) == "READY"

        rolled_back = _run(
            "raw-sql-enqueue",
            db_path,
            EXT_PATH,
            "rollback",
        )
        assert rolled_back.returncode == 0, rolled_back.stderr
        assert rolled_back.stdout.strip() == "ROLLBACK"

        committed = _run(
            "raw-sql-enqueue",
            db_path,
            EXT_PATH,
            "commit",
        )
        assert committed.returncode == 0, committed.stderr
        assert committed.stdout.strip() == "COMMIT"

        got = _json_line(_line(worker, timeout=8.0))
        assert got["payload"] == {"order_id": 1, "to": "alice@example.com"}
        _finish(worker)

        db = honker.open(db_path)
        rows = db.query("SELECT COUNT(*) AS c FROM orders")
        assert rows[0]["c"] == 1
    finally:
        if worker.poll() is None:
            worker.kill()
