import asyncio
import honker
import os


async def main():
    if os.path.exists("test.db"):
        os.remove("test.db")

    db = honker.open("test.db")

    with db.transaction() as tx:
        tx.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, total REAL)")

    async def listener():
        count = 0
        print("Listener started")
        async for notif in db.listen("orders"):
            print(f"got: {notif.channel} {notif.payload}")
            count += 1
            if count >= 2:
                break
        print("Listener finished")

    task = asyncio.create_task(listener())
    await asyncio.sleep(0.1)

    with db.transaction() as tx:
        tx.execute("INSERT INTO orders (id, total) VALUES (1, 99.99)")
        tx.notify("orders", '{"id": 1}')

    with db.transaction() as tx:
        tx.execute("INSERT INTO orders (id, total) VALUES (2, 14.50)")
        tx.notify("orders", '{"id": 2}')

    try:
        await asyncio.wait_for(task, timeout=1.0)
    except asyncio.TimeoutError:
        print("Timeout waiting for messages!")


if __name__ == "__main__":
    asyncio.run(main())
