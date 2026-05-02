"""Atomic business-write + enqueue in one transaction.

This is honker's killer feature vs Redis/Celery: the job enqueue
commits in the same SQLite transaction as your business INSERT.
Rollback drops both. No dual-write window, no outbox pattern needed.

    python examples/atomic.py
"""

import os
import tempfile

import honker


def main():
    with tempfile.TemporaryDirectory() as d:
        db = honker.open(os.path.join(d, "app.db"))

        # Set up a plain business table. Nothing honker-specific.
        with db.transaction() as tx:
            tx.execute(
                "CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER, total INTEGER)"
            )

        emails = db.queue("emails")

        # Success path: order INSERT and job enqueue commit together.
        with db.transaction() as tx:
            tx.execute(
                "INSERT INTO orders (user_id, total) VALUES (?, ?)",
                [42, 9900],
            )
            emails.enqueue({"to": "alice@example.com", "order_id": 42}, tx=tx)

        orders = db.query("SELECT id, user_id, total FROM orders")
        queued = db.query("SELECT payload FROM _honker_live WHERE queue='emails'")
        print(f"committed: {len(orders)} order(s), {len(queued)} job(s)")
        assert len(orders) == 1 and len(queued) == 1

        # Rollback path: raise inside the block, both writes disappear.
        try:
            with db.transaction() as tx:
                tx.execute(
                    "INSERT INTO orders (user_id, total) VALUES (?, ?)",
                    [43, 5000],
                )
                emails.enqueue({"to": "bob@example.com", "order_id": 43}, tx=tx)
                raise RuntimeError("boom — simulate a payment-processing failure")
        except RuntimeError as e:
            print(f"rolled back: {e}")

        orders = db.query("SELECT id FROM orders")
        queued = db.query("SELECT payload FROM _honker_live WHERE queue='emails'")
        print(f"after rollback: {len(orders)} order(s), {len(queued)} job(s)")
        # Still just the first order + first job; the rolled-back pair is gone.
        assert len(orders) == 1 and len(queued) == 1

        print("atomic enqueue + rollback both work as expected")


if __name__ == "__main__":
    main()
