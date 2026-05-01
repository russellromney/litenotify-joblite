"""Durable pub/sub with per-consumer offsets.

`db.stream(name)` gives you an append-only event log with per-consumer
offset tracking. Reconnect, resume. Publish atomically with a business
write (rollback drops both, just like queue.enqueue).

This example publishes 5 events, subscribes, processes 3, then
simulates a restart: reconnects with the same consumer name and
resumes from offset 3.

    python examples/stream.py
"""

import asyncio
import os
import tempfile

import honker


async def process_first_three(db: honker.Database):
    stream = db.stream("user-events")
    seen: list = []
    async for event in stream.subscribe(consumer="downstream"):
        seen.append(event)
        if len(seen) == 3:
            # Manually save our high-water mark since we're about to die.
            stream.save_offset("downstream", event.offset)
            return seen


async def resume(db: honker.Database):
    stream = db.stream("user-events")
    rest: list = []
    async for event in stream.subscribe(consumer="downstream"):
        rest.append(event)
        if len(rest) == 2:
            return rest


async def main():
    with tempfile.TemporaryDirectory() as d:
        db = honker.open(os.path.join(d, "app.db"))

        # Publish 5 events. Could be atomic with a business write (tx= arg).
        stream = db.stream("user-events")
        for i in range(5):
            stream.publish({"user_id": 100 + i, "event": f"signup-{i}"})

        # First run: process 3, save offset, "crash."
        first = await asyncio.wait_for(process_first_three(db), timeout=3.0)
        print(f"first run saw {len(first)} events (offsets "
              f"{[e.offset for e in first]})")

        # Second run: same consumer, resumes from saved offset.
        rest = await asyncio.wait_for(resume(db), timeout=3.0)
        print(f"resume saw {len(rest)} events (offsets "
              f"{[e.offset for e in rest]})")

        assert len(first) == 3
        assert len(rest) == 2
        assert first[-1].offset + 1 == rest[0].offset
        print("per-consumer offset tracking works")


if __name__ == "__main__":
    asyncio.run(main())
