"""Small real-app proof.

One process pretends to be a web request:

* insert an order
* enqueue an email
* publish a durable stream event
* notify a live listener

All four writes commit together. Then a worker claims the email, the
stream consumer reads the event, and a scheduler fires a cleanup job.

Run:

    python packages/honker/examples/real_app.py
"""

from __future__ import annotations

import asyncio
import os
import tempfile
from dataclasses import dataclass

import honker
from honker import Scheduler, every_s


@dataclass
class Proof:
    order_id: int
    email_sent_to: str
    stream_event: dict
    notification: dict
    scheduler_payload: dict


async def run(db_path: str) -> Proof:
    db = honker.open(db_path)
    emails = db.queue("emails")
    maintenance = db.queue("maintenance")
    events = db.stream("order-events")

    with db.transaction() as tx:
        tx.execute(
            "CREATE TABLE IF NOT EXISTS orders "
            "(id INTEGER PRIMARY KEY, email TEXT NOT NULL, total INTEGER NOT NULL)"
        )

    async def wait_for_order_notification():
        async for note in db.listen("orders"):
            return note.payload

    notification_task = asyncio.create_task(wait_for_order_notification())
    await asyncio.sleep(0.05)

    with db.transaction() as tx:
        tx.execute(
            "INSERT INTO orders (id, email, total) VALUES (?, ?, ?)",
            [1, "alice@example.com", 4999],
        )
        emails.enqueue({"order_id": 1, "to": "alice@example.com"}, tx=tx)
        events.publish({"order_id": 1, "kind": "created"}, tx=tx)
        tx.notify("orders", {"order_id": 1, "kind": "created"})

    notification = await asyncio.wait_for(notification_task, timeout=3.0)

    email_job = emails.claim_one("mailer-1")
    assert email_job is not None
    assert email_job.ack()

    stream_iter = events.subscribe(consumer="dashboard")
    stream_event = await asyncio.wait_for(stream_iter.__anext__(), timeout=3.0)
    events.save_offset("dashboard", stream_event.offset)

    scheduler = Scheduler(db)
    scheduler.add(
        name="cleanup",
        queue="maintenance",
        schedule=every_s(1),
        payload={"task": "prune_notifications"},
    )
    stop = asyncio.Event()
    scheduler_task = asyncio.create_task(scheduler.run(stop))
    try:
        deadline = asyncio.get_running_loop().time() + 3.0
        scheduled_job = None
        while asyncio.get_running_loop().time() < deadline:
            scheduled_job = maintenance.claim_one("maint-1")
            if scheduled_job is not None:
                break
            await asyncio.sleep(0.05)
        assert scheduled_job is not None
        assert scheduled_job.ack()
    finally:
        stop.set()
        await asyncio.wait_for(scheduler_task, timeout=3.0)

    return Proof(
        order_id=1,
        email_sent_to=email_job.payload["to"],
        stream_event=stream_event.payload,
        notification=notification,
        scheduler_payload=scheduled_job.payload,
    )


async def main() -> None:
    with tempfile.TemporaryDirectory() as d:
        proof = await run(os.path.join(d, "app.db"))
        print(f"order: {proof.order_id}")
        print(f"email: {proof.email_sent_to}")
        print(f"stream: {proof.stream_event}")
        print(f"notify: {proof.notification}")
        print(f"scheduler: {proof.scheduler_payload}")


if __name__ == "__main__":
    asyncio.run(main())
