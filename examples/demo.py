"""Minimal honker demo: enqueue a job, claim it, ack it."""
import asyncio
import os

import honker


async def main():
    if os.path.exists("app.db"):
        os.remove("app.db")

    db = honker.open("app.db")
    emails = db.queue("emails")

    with db.transaction() as tx:
        tx.execute(
            "CREATE TABLE IF NOT EXISTS orders "
            "(id INTEGER PRIMARY KEY, user_id INTEGER, amount REAL)"
        )

    with db.transaction() as tx:
        tx.execute("INSERT INTO orders (user_id, amount) VALUES (?, ?)", [42, 19.99])
        emails.enqueue(
            {"to": "alice@example.com", "body": "Receipt for 19.99"}, tx=tx
        )

    async def worker():
        async for job in emails.claim("w1"):
            print(f"sending email to {job.payload['to']}: {job.payload['body']}")
            job.ack()
            return

    await asyncio.wait_for(worker(), timeout=2.0)
    print("done")


if __name__ == "__main__":
    asyncio.run(main())
