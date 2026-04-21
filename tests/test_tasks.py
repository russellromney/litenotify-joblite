"""Task decorator + worker loop tests.

Covers the Huey-style `@queue.task()` + `@queue.periodic_task()` API:
envelope round-trip, result storage, retry/timeout/fail paths,
worker dispatch of unknown tasks, and the `db.run_workers()` helper.
"""

import asyncio

import pytest

import honker


# The registry is process-global. Clear it between tests so task
# names don't leak across modules.
@pytest.fixture(autouse=True)
def _clear_registry():
    honker.registry()._by_name.clear()
    yield
    honker.registry()._by_name.clear()


# ---------- basic decorator + auto-naming ----------


def test_task_auto_name_is_module_dot_qualname(db_path):
    db = honker.open(db_path)
    q = db.queue("default")

    @q.task()
    def my_task(x):
        return x + 1

    assert my_task.name.endswith("my_task")
    # module.qualname shape
    assert "." in my_task.name
    # Registered in the global registry.
    assert honker.registry().get(my_task.name) is not None


def test_task_explicit_name_overrides(db_path):
    db = honker.open(db_path)
    q = db.queue("default")

    @q.task(name="send-email")
    def _send(to, subject):
        return "ok"

    assert _send.name == "send-email"


def test_calling_task_enqueues_does_not_run(db_path):
    """Core semantic: calling a @task-wrapped function enqueues.
    It doesn't run in the caller's process."""
    db = honker.open(db_path)
    q = db.queue("default")

    calls = []

    @q.task()
    def add(a, b):
        calls.append((a, b))
        return a + b

    result = add(2, 3)
    assert calls == [], "should not have run the function in-process"
    assert isinstance(result, honker.TaskResult)

    # Verify a row landed in the queue.
    rows = db.query(
        "SELECT payload FROM _honker_live WHERE queue='default'"
    )
    assert len(rows) == 1
    import json
    payload = json.loads(rows[0]["payload"])
    assert "__honker_task__" in payload
    assert payload["__honker_task__"]["args"] == [2, 3]


def test_call_local_bypasses_queue(db_path):
    """call_local runs the function synchronously — useful for tests."""
    db = honker.open(db_path)
    q = db.queue("default")

    @q.task()
    def double(x):
        return x * 2

    assert double.call_local(5) == 10
    # No queue row was written.
    rows = db.query("SELECT COUNT(*) AS c FROM _honker_live WHERE queue='default'")
    assert rows[0]["c"] == 0


# ---------- worker dispatch end-to-end ----------


async def test_worker_runs_task_and_stores_result(db_path):
    db = honker.open(db_path)
    q = db.queue("default")

    @q.task()
    def add(a, b):
        return a + b

    r = add(7, 11)

    # Run workers just long enough to drain.
    stop = asyncio.Event()
    run = asyncio.create_task(
        db.run_workers(queue="default", concurrency=1, stop_event=stop),
    )

    value = await asyncio.wait_for(r.aget(timeout=5.0), timeout=5.0)
    assert value == 18

    stop.set()
    await asyncio.wait_for(run, timeout=3.0)


async def test_worker_runs_async_task(db_path):
    db = honker.open(db_path)
    q = db.queue("default")

    @q.task()
    async def slow_add(a, b):
        await asyncio.sleep(0.01)
        return a + b

    r = slow_add(1, 2)
    stop = asyncio.Event()
    run = asyncio.create_task(
        db.run_workers(queue="default", concurrency=1, stop_event=stop),
    )

    value = await asyncio.wait_for(r.aget(timeout=5.0), timeout=5.0)
    assert value == 3

    stop.set()
    await asyncio.wait_for(run, timeout=3.0)


async def test_store_result_false_skips_save(db_path):
    db = honker.open(db_path)
    q = db.queue("default")

    @q.task(store_result=False)
    def fire_and_forget(x):
        return x

    r = fire_and_forget(42)

    stop = asyncio.Event()
    run = asyncio.create_task(
        db.run_workers(queue="default", concurrency=1, stop_event=stop),
    )

    # Let the worker run + ack; don't wait for a result we won't get.
    await asyncio.sleep(0.2)
    stop.set()
    await asyncio.wait_for(run, timeout=3.0)

    # No row in _honker_results for this job.
    rows = db.query(
        "SELECT COUNT(*) AS c FROM _honker_results WHERE job_id=?",
        [r.id],
    )
    assert rows[0]["c"] == 0
    # Job itself was ack'd (dropped from _honker_live).
    live = db.query(
        "SELECT COUNT(*) AS c FROM _honker_live WHERE queue='default'"
    )
    assert live[0]["c"] == 0


async def test_failed_task_retries_then_dead_letters(db_path):
    db = honker.open(db_path)
    q = db.queue("default", max_attempts=2)

    @q.task(retries=2, retry_delay_s=0)
    def always_fails():
        raise RuntimeError("nope")

    r = always_fails()

    stop = asyncio.Event()
    run = asyncio.create_task(
        db.run_workers(queue="default", concurrency=1, stop_event=stop),
    )

    # Give the worker time to drain 2 attempts.
    deadline = asyncio.get_event_loop().time() + 5.0
    while asyncio.get_event_loop().time() < deadline:
        rows = db.query(
            "SELECT COUNT(*) AS c FROM _honker_dead WHERE id=?", [r.id]
        )
        if rows[0]["c"] == 1:
            break
        await asyncio.sleep(0.05)

    stop.set()
    await asyncio.wait_for(run, timeout=3.0)

    dead = db.query(
        "SELECT last_error FROM _honker_dead WHERE id=?", [r.id]
    )
    assert len(dead) == 1
    assert "nope" in dead[0]["last_error"]


async def test_unknown_task_goes_to_dead_letter(db_path):
    """Worker sees a job with a task name it doesn't know about
    (e.g. someone deployed a rename without updating the worker).
    Honest failure: move to dead-letter with a clear error."""
    db = honker.open(db_path)
    q = db.queue("default")

    # Register a known task so the registry isn't empty.
    @q.task(name="known.keep")
    def _keep():
        return None

    # Enqueue an unknown task directly (bypassing the decorator).
    q.enqueue({
        "__honker_task__": {
            "task": "myapp.removed_function",
            "args": [],
            "kwargs": {},
        }
    })

    stop = asyncio.Event()
    run = asyncio.create_task(
        db.run_workers(queue="default", concurrency=1, stop_event=stop),
    )

    deadline = asyncio.get_event_loop().time() + 3.0
    while asyncio.get_event_loop().time() < deadline:
        rows = db.query("SELECT COUNT(*) AS c FROM _honker_dead")
        if rows[0]["c"] >= 1:
            break
        await asyncio.sleep(0.05)

    stop.set()
    await asyncio.wait_for(run, timeout=3.0)

    dead = db.query("SELECT last_error FROM _honker_dead")
    assert any("unknown task" in d["last_error"] for d in dead)


# ---------- duplicate registration ----------


def test_duplicate_task_name_raises(db_path):
    db = honker.open(db_path)
    q = db.queue("default")

    @q.task(name="dup")
    def one():
        return 1

    with pytest.raises(ValueError, match="duplicate task name"):
        @q.task(name="dup")
        def two():
            return 2


# ---------- Database-level shortcut ----------


def test_db_task_shortcut(db_path):
    db = honker.open(db_path)

    @db.task(queue="emails")
    def send(to):
        return f"sent to {to}"

    # Task landed in the emails queue registry.
    spec = honker.registry().get(send.name)
    assert spec is not None
    assert spec.queue_name == "emails"
