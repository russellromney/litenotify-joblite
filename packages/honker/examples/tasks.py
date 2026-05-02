"""Huey-style task decorators.

`@queue.task()` wraps a function so calling it enqueues a job instead
of running it in-process. A separate worker (or the same process via
`db.run_workers()`) picks it up, runs it, stores the return value.

    python -m honker worker packages/honker/examples/tasks:db

Or inline, all-in-one-process, as shown below.
"""

import asyncio
import os
import tempfile
import time

import honker


# Module-level db + queue so the CLI can target this file:
#   python -m honker worker packages/honker/examples/tasks:db
_tmp = tempfile.mkdtemp(prefix="honker-tasks-")
db = honker.open(os.path.join(_tmp, "app.db"))
q = db.queue("demo")


@q.task(retries=2, retry_delay_s=0)
def add(a: int, b: int) -> int:
    """Pure function. Return value is JSON-serialized into the result table."""
    return a + b


@q.task(store_result=False)
def log_event(event: str) -> None:
    """Fire-and-forget task. store_result=False skips the result INSERT."""
    print(f"  [worker] logged event: {event}")


@q.task(timeout_s=0.1, retries=1, retry_delay_s=0)
async def slow_thing() -> str:
    """Async task that times out; goes to dead-letter after retries exhaust."""
    await asyncio.sleep(5.0)
    return "never"


async def main():
    # Enqueue some work from the "web" side of the app.
    r1 = add(2, 3)
    r2 = add(100, 200)
    log_event("user signed up")
    log_event("email sent")
    _timeout_result = slow_thing()  # will dead-letter

    print(f"enqueued: add(2,3)={r1.id}, add(100,200)={r2.id}, slow_thing(), 2 log_events")

    # Run workers inline. In production you'd do
    #   python -m honker worker packages/honker/examples/tasks:db
    # from a separate process instead.
    stop = asyncio.Event()
    worker = asyncio.create_task(
        db.run_workers(queue="demo", concurrency=2, stop_event=stop),
    )

    # Wait for the add() results.
    v1 = await r1.aget(timeout=5.0)
    v2 = await r2.aget(timeout=5.0)
    print(f"add(2,3) = {v1}, add(100,200) = {v2}")

    # Give the timeout case time to retry + die.
    deadline = time.time() + 3.0
    while time.time() < deadline:
        rows = db.query("SELECT COUNT(*) AS c FROM _honker_dead WHERE queue='demo'")
        if rows[0]["c"] >= 1:
            break
        await asyncio.sleep(0.1)

    dead = db.query("SELECT last_error FROM _honker_dead WHERE queue='demo'")
    print(f"dead-letter: {len(dead)} job(s)")
    for d in dead:
        err_line = d["last_error"].splitlines()[0]
        print(f"  {err_line}")

    stop.set()
    await asyncio.wait_for(worker, timeout=5.0)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    finally:
        import shutil
        shutil.rmtree(_tmp, ignore_errors=True)
