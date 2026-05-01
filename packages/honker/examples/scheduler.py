"""Crontab-style periodic tasks with leader election.

Scheduler enqueues into a named queue on every cron boundary. Workers
consume those jobs like any other. Multiple scheduler processes can
run — only one holds the leader lock at a time, so nothing fires
twice.

This example uses the every-minute schedule (`* * * * *`) and shuts
down after the first fire so it finishes in under a minute.

    python examples/scheduler.py
"""

import asyncio
import os
import tempfile

import honker
from honker import Scheduler, crontab


async def run_for_one_fire(db: honker.Database):
    db.queue("health-checks")
    scheduler = Scheduler(db)
    scheduler.add(
        name="ping",
        queue="health-checks",
        schedule=crontab("* * * * *"),  # every minute
        payload={"target": "https://example.com/health"},
    )

    stop = asyncio.Event()
    run_task = asyncio.create_task(scheduler.run(stop))

    # Poll the queue until the first fire lands (or 70s, which covers
    # the worst case of starting 1s after a boundary just missed).
    deadline = asyncio.get_event_loop().time() + 70
    while asyncio.get_event_loop().time() < deadline:
        rows = db.query(
            "SELECT payload FROM _honker_live WHERE queue='health-checks'"
        )
        if rows:
            print(f"fired: {rows[0]['payload']}")
            break
        await asyncio.sleep(1)

    stop.set()
    await asyncio.wait_for(run_task, timeout=5.0)


async def main():
    with tempfile.TemporaryDirectory() as d:
        db = honker.open(os.path.join(d, "app.db"))
        print("waiting for next minute boundary...")
        await run_for_one_fire(db)
        print("scheduler fired cleanly and stopped")


if __name__ == "__main__":
    asyncio.run(main())
