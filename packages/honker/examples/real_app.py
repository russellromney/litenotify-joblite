"""Small real-app proof.

One process is a web request. A second process is already waiting like
a worker:

* insert an order
* enqueue an email
* publish a durable stream event
* notify a live listener

All four writes commit together. The worker process must wake, claim the
email, observe the notification, and read the stream event promptly. Then
a scheduler fires a cleanup job.

Run:

    python packages/honker/examples/real_app.py
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
import tempfile
import time
from dataclasses import dataclass

import honker
from honker import Scheduler, every_s


WORKER_IDLE_POLL_S = 30.0
WORKER_RESULT_TIMEOUT_S = 8.0


def _close_db(db) -> None:
    close = getattr(getattr(db, "_inner", None), "close", None)
    if callable(close):
        close()


@dataclass
class Proof:
    order_id: int
    email_sent_to: str
    stream_event: dict
    notification: dict
    scheduler_payload: dict
    worker_wake_ms: float


async def _worker_claim_email(db, worker_id: str) -> dict:
    emails = db.queue("emails")
    async for job in emails.claim(worker_id, idle_poll_s=WORKER_IDLE_POLL_S):
        payload = job.payload
        assert job.ack()
        return payload
    raise RuntimeError("email worker stopped before claiming a job")


async def _worker_listen_order(db) -> dict:
    async for note in db.listen("orders"):
        return note.payload
    raise RuntimeError("listener stopped before receiving an order notification")


async def _worker_read_stream(db) -> dict:
    events = db.stream("order-events")
    stream_iter = events.subscribe(
        consumer="dashboard-worker",
        save_every_n=0,
        save_every_s=0,
    )
    event = await stream_iter.__anext__()
    events.save_offset("dashboard-worker", event.offset)
    return event.payload


async def _worker_process(db_path: str) -> None:
    db = honker.open(db_path)

    try:
        email_task = asyncio.create_task(_worker_claim_email(db, "mailer-1"))
        notification_task = asyncio.create_task(_worker_listen_order(db))
        stream_task = asyncio.create_task(_worker_read_stream(db))

        # Let each async iterator subscribe before the parent writes. This is
        # the user shape we care about: worker asleep first, writer commits later.
        await asyncio.sleep(0.05)
        print("READY", flush=True)

        email_payload, notification, stream_event = await asyncio.wait_for(
            asyncio.gather(email_task, notification_task, stream_task),
            timeout=WORKER_RESULT_TIMEOUT_S,
        )
        print(
            "RESULT "
            + json.dumps(
                {
                    "email": email_payload,
                    "notification": notification,
                    "stream_event": stream_event,
                },
                sort_keys=True,
            ),
            flush=True,
        )
    finally:
        _close_db(db)


async def _read_child_line(proc, timeout_s: float) -> str:
    assert proc.stdout is not None
    line = await asyncio.wait_for(proc.stdout.readline(), timeout=timeout_s)
    if not line:
        stderr = ""
        if proc.stderr is not None:
            stderr = (await proc.stderr.read()).decode(errors="replace")
        raise RuntimeError(f"worker exited before writing a line: {stderr}")
    return line.decode().strip()


async def _start_worker(db_path: str):
    proc = await asyncio.create_subprocess_exec(
        sys.executable,
        os.path.abspath(__file__),
        "--worker",
        db_path,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        ready = await _read_child_line(proc, timeout_s=5.0)
        assert ready == "READY", f"worker did not become ready: {ready!r}"

        return proc
    except BaseException:
        proc.kill()
        await proc.wait()
        raise


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

    worker_proc = await _start_worker(db_path)

    try:
        with db.transaction() as tx:
            tx.execute(
                "INSERT INTO orders (id, email, total) VALUES (?, ?, ?)",
                [1, "alice@example.com", 4999],
            )
            emails.enqueue({"order_id": 1, "to": "alice@example.com"}, tx=tx)
            events.publish({"order_id": 1, "kind": "created"}, tx=tx)
            tx.notify("orders", {"order_id": 1, "kind": "created"})
        committed_at = time.perf_counter()

        result_line = await _read_child_line(
            worker_proc,
            timeout_s=WORKER_RESULT_TIMEOUT_S,
        )
        result_received_at = time.perf_counter()
        assert result_line.startswith("RESULT "), result_line
        worker_result = json.loads(result_line.removeprefix("RESULT "))

        code = await asyncio.wait_for(worker_proc.wait(), timeout=3.0)
        assert code == 0
    finally:
        if worker_proc.returncode is None:
            worker_proc.kill()
            await worker_proc.wait()

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

    proof = Proof(
        order_id=1,
        email_sent_to=worker_result["email"]["to"],
        stream_event=worker_result["stream_event"],
        notification=worker_result["notification"],
        scheduler_payload=scheduled_job.payload,
        worker_wake_ms=(result_received_at - committed_at) * 1000.0,
    )
    _close_db(db)
    return proof


async def main() -> None:
    with tempfile.TemporaryDirectory() as d:
        proof = await run(os.path.join(d, "app.db"))
        print(f"order: {proof.order_id}")
        print(f"email: {proof.email_sent_to}")
        print(f"stream: {proof.stream_event}")
        print(f"notify: {proof.notification}")
        print(f"scheduler: {proof.scheduler_payload}")
        print(f"worker wake: {proof.worker_wake_ms:.1f} ms")


if __name__ == "__main__":
    if len(sys.argv) == 3 and sys.argv[1] == "--worker":
        asyncio.run(_worker_process(sys.argv[2]))
    else:
        asyncio.run(main())
