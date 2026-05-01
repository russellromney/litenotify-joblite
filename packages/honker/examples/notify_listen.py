"""Ephemeral pub/sub — pg_notify semantics on SQLite.

Starts a listener on the 'orders' channel in one asyncio task, fires
three notifications from another, and prints each received payload.
Wake latency is ~1 ms cross-process; same-process (this example) is
faster.

    python examples/notify_listen.py
"""

import asyncio
import os
import tempfile

import honker


async def consumer(db: honker.Database, received: list, stop: asyncio.Event):
    async for n in db.listen("orders"):
        received.append(n.payload)
        if len(received) == 3:
            stop.set()
            return


async def main():
    with tempfile.TemporaryDirectory() as d:
        db = honker.open(os.path.join(d, "app.db"))

        received: list = []
        stop = asyncio.Event()
        task = asyncio.create_task(consumer(db, received, stop))
        # Give the listener a moment to attach + snapshot MAX(id).
        await asyncio.sleep(0.05)

        for order_id in (1, 2, 3):
            with db.transaction() as tx:
                tx.notify("orders", {"id": order_id, "event": "placed"})

        await asyncio.wait_for(stop.wait(), timeout=5.0)
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

        print(f"received {len(received)} notifications:")
        for r in received:
            print(f"  {r}")
        assert len(received) == 3


if __name__ == "__main__":
    asyncio.run(main())
