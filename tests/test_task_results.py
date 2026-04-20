"""Tests for task result storage: save / get / wait / sweep.

Paired with extension interop tests (`test_extension_interop.py`) that
verify `honker_result_save` / `honker_result_get` / `honker_result_sweep` operate
on the same `_joblite_results` table and produce the same behavior.
"""

import asyncio
import time

import pytest

import joblite
import joblite._worker


# ---------- Queue.enqueue now returns an id ----------


def test_enqueue_returns_id(db_path):
    db = joblite.open(db_path)
    q = db.queue("idq")
    id1 = q.enqueue({"a": 1})
    id2 = q.enqueue({"a": 2})
    assert isinstance(id1, int) and id1 > 0
    assert id2 > id1


def test_enqueue_in_tx_returns_id(db_path):
    db = joblite.open(db_path)
    q = db.queue("tx-id")
    with db.transaction() as tx:
        rid = q.enqueue({"a": 1}, tx=tx)
    assert isinstance(rid, int)
    # Row actually landed.
    rows = db.query(
        "SELECT id FROM _joblite_live WHERE queue='tx-id'"
    )
    assert rows[0]["id"] == rid


# ---------- save_result / get_result ----------


def test_save_and_get_result(db_path):
    db = joblite.open(db_path)
    q = db.queue("res")
    q.save_result(42, {"status": "ok"})

    found, value = q.get_result(42)
    assert found is True
    assert value == {"status": "ok"}


def test_get_result_missing(db_path):
    db = joblite.open(db_path)
    q = db.queue("missing")
    found, value = q.get_result(999)
    assert found is False
    assert value is None


def test_get_result_none_roundtrips(db_path):
    """A handler that returns None is indistinguishable from a
    handler that hasn't returned yet IF we return bare None. The
    (found, value) tuple disambiguates: (True, None) vs (False, None).
    """
    db = joblite.open(db_path)
    q = db.queue("none-result")
    q.save_result(7, None)
    found, value = q.get_result(7)
    assert found is True
    assert value is None


def test_save_result_replaces(db_path):
    """save_result is an UPSERT — calling twice for the same job_id
    replaces the first."""
    db = joblite.open(db_path)
    q = db.queue("replace")
    q.save_result(1, "first")
    q.save_result(1, "second")
    found, value = q.get_result(1)
    assert found and value == "second"


def test_save_result_in_tx(db_path):
    db = joblite.open(db_path)
    q = db.queue("in-tx")
    with db.transaction() as tx:
        q.save_result(99, {"k": "v"}, ttl=60, tx=tx)
    found, value = q.get_result(99)
    assert found and value == {"k": "v"}


def test_save_result_rolled_back(db_path):
    """Saving inside a user tx rolls back with the rest of the tx."""
    db = joblite.open(db_path)
    q = db.queue("rollback")
    with pytest.raises(RuntimeError):
        with db.transaction() as tx:
            q.save_result(50, {"v": 1}, tx=tx)
            raise RuntimeError("boom")
    found, _ = q.get_result(50)
    assert found is False


def test_expired_result_reads_as_missing(db_path):
    """A result whose `expires_at` has passed reads as (False, None).
    Row stays in the table until sweep; the query just filters it."""
    db = joblite.open(db_path)
    q = db.queue("ttl-expired")
    # Directly insert a row with an already-past expiry.
    with db.transaction() as tx:
        tx.execute(
            """
            INSERT INTO _joblite_results (job_id, value, expires_at)
            VALUES (?, ?, unixepoch() - 10)
            """,
            [1, '"stale"'],
        )
    found, value = q.get_result(1)
    assert found is False
    # Row is still there; sweep will clean it.
    rows = db.query("SELECT COUNT(*) AS c FROM _joblite_results")
    assert rows[0]["c"] == 1
    assert q.sweep_results() == 1
    rows = db.query("SELECT COUNT(*) AS c FROM _joblite_results")
    assert rows[0]["c"] == 0


def test_sweep_results_only_expired(db_path):
    """sweep_results touches only rows with a non-null expires_at
    that's elapsed. Rows with NULL expires_at (no TTL) are kept."""
    db = joblite.open(db_path)
    q = db.queue("sweep-mix")
    with db.transaction() as tx:
        tx.execute(
            "INSERT INTO _joblite_results (job_id, value, expires_at) "
            "VALUES (1, '\"stale\"', unixepoch() - 10)"
        )
        tx.execute(
            "INSERT INTO _joblite_results (job_id, value, expires_at) "
            "VALUES (2, '\"fresh\"', unixepoch() + 3600)"
        )
        tx.execute(
            "INSERT INTO _joblite_results (job_id, value, expires_at) "
            "VALUES (3, '\"forever\"', NULL)"
        )
    assert q.sweep_results() == 1
    rows = db.query(
        "SELECT job_id FROM _joblite_results ORDER BY job_id"
    )
    assert [r["job_id"] for r in rows] == [2, 3]


# ---------- wait_result ----------


async def test_wait_result_immediate(db_path):
    """If the result is already saved, wait_result returns it on the
    first check (before subscribing to WAL)."""
    db = joblite.open(db_path)
    q = db.queue("wait-immediate")
    q.save_result(1, {"answer": 42})

    value = await asyncio.wait_for(q.wait_result(1), timeout=1.0)
    assert value == {"answer": 42}


async def test_wait_result_blocks_until_saved(db_path):
    """wait_result blocks until another coroutine saves the result,
    using WAL wakes to re-check."""
    db = joblite.open(db_path)
    q = db.queue("wait-blocks")

    async def save_later():
        await asyncio.sleep(0.1)
        q.save_result(7, "done")

    save_task = asyncio.create_task(save_later())
    value = await asyncio.wait_for(q.wait_result(7), timeout=3.0)
    assert value == "done"
    await save_task


async def test_wait_result_timeout(db_path):
    """wait_result raises asyncio.TimeoutError if the result never
    arrives in the allotted time."""
    db = joblite.open(db_path)
    q = db.queue("wait-timeout")

    with pytest.raises(asyncio.TimeoutError):
        await q.wait_result(123, timeout=0.2)


# ---------- run_task + save_result integration ----------


async def test_run_task_save_result_persists_return_value(db_path):
    """With `save_result=True`, the handler's return value is saved
    under the job id, and a caller blocking on wait_result gets it
    once the worker finishes."""
    db = joblite.open(db_path)
    q = db.queue("run-save")
    job_id = q.enqueue({"n": 10})

    async def handler(payload):
        return payload["n"] * 2

    jobs = q.claim_batch("w1", 1)
    assert len(jobs) == 1
    await joblite._worker.run_task(
        jobs[0], handler, save_result=True, result_ttl=60
    )

    found, value = q.get_result(job_id)
    assert found and value == 20


async def test_run_task_save_result_disabled_by_default(db_path):
    """Without save_result=True, no result row is created even if
    the handler returns something."""
    db = joblite.open(db_path)
    q = db.queue("run-nosave")
    job_id = q.enqueue({"n": 10})

    async def handler(payload):
        return payload["n"] * 2

    jobs = q.claim_batch("w1", 1)
    await joblite._worker.run_task(jobs[0], handler)

    found, _ = q.get_result(job_id)
    assert found is False


async def test_run_task_failed_handler_does_not_save_result(db_path):
    """A handler that raises doesn't save a result — the retry path
    is taken and nothing lands in _joblite_results. Callers treat
    missing-result as did-not-succeed."""
    db = joblite.open(db_path)
    q = db.queue("run-fail")
    job_id = q.enqueue({"n": 10})

    async def handler(payload):
        raise RuntimeError("boom")

    jobs = q.claim_batch("w1", 1)
    await joblite._worker.run_task(
        jobs[0], handler,
        save_result=True, retries=1, retry_delay=0,
    )

    found, _ = q.get_result(job_id)
    assert found is False


async def test_end_to_end_enqueue_worker_wait(db_path):
    """Full round trip: enqueue returns id, separate coroutine runs
    the worker (with save_result), caller awaits the result by id
    and gets the handler's return value. This is the 'task as RPC'
    pattern — works but intentionally not the library's main shape."""
    db = joblite.open(db_path)
    q = db.queue("e2e")

    async def handler(payload):
        return payload["x"] + payload["y"]

    job_id = q.enqueue({"x": 1, "y": 2})

    async def worker_once():
        # Poll once; this test races with the caller's wait_result but
        # the wait_result path is what we're exercising, so we give
        # the worker a brief head start.
        await asyncio.sleep(0.02)
        jobs = q.claim_batch("e2e-worker", 1)
        assert len(jobs) == 1
        await joblite._worker.run_task(
            jobs[0], handler, save_result=True, result_ttl=60
        )

    worker = asyncio.create_task(worker_once())
    result = await asyncio.wait_for(q.wait_result(job_id), timeout=3.0)
    assert result == 3
    await worker
