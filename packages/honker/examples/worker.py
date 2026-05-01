"""End-to-end worker loop.

Enqueues a few jobs, runs an async worker loop that drains them via
the long-lived `claim()` iterator (wakes on every database update, no
polling), and exits when the queue is empty.

This is the idiomatic Python worker shape: one async iterator, one
try/except per job, retry on failure, ack on success.

    python examples/worker.py
"""

import asyncio
import os
import random
import tempfile

import honker


async def send_email_fake(payload: dict) -> None:
    """Stand-in for a real side effect. Fails ~20% of the time."""
    await asyncio.sleep(0.01)  # simulate work
    if random.random() < 0.2:
        raise RuntimeError("SMTP server had a moment")
    # print(f"  -> sent to {payload['to']}")


async def drain(db: honker.Database) -> int:
    emails = db.queue("emails", max_attempts=3)
    processed = 0
    async for job in emails.claim("worker-1"):
        try:
            await send_email_fake(job.payload)
            job.ack()
            processed += 1
        except Exception as e:
            # retry bumps attempts; after max_attempts, moved to dead.
            job.retry(delay_s=0, error=str(e))

        # Break after the queue drains + any retried-to-dead jobs settle.
        remaining = db.query(
            "SELECT COUNT(*) AS c FROM _honker_live WHERE queue='emails'"
        )[0]["c"]
        if remaining == 0:
            return processed
    return processed


async def main():
    with tempfile.TemporaryDirectory() as d:
        db = honker.open(os.path.join(d, "app.db"))
        emails = db.queue("emails", max_attempts=3)

        for i in range(20):
            emails.enqueue({"to": f"user-{i}@example.com"})
        print("enqueued 20 jobs")

        processed = await asyncio.wait_for(drain(db), timeout=10.0)

        dead = db.query(
            "SELECT COUNT(*) AS c FROM _honker_dead WHERE queue='emails'"
        )[0]["c"]
        print(f"processed {processed} successfully, {dead} moved to dead")
        assert processed + dead == 20


if __name__ == "__main__":
    asyncio.run(main())
