//! Atomic business-write + enqueue in one transaction.
//!
//! `db.transaction()` pins the connection for its lifetime; any
//! INSERTs you run plus `q.enqueue_tx(&tx, ...)` commit together or
//! not at all. No dual-write, no outbox table.
//!
//!     cargo run --release --example atomic

use honker::{Database, EnqueueOpts, QueueOpts};
use rusqlite::params;
use serde_json::json;

fn main() -> honker::Result<()> {
    let tmp = tempfile::tempdir().expect("tmpdir");
    let db_path = tmp.path().join("app.db");
    let db = Database::open(&db_path)?;

    // Plain business table — nothing honker-specific.
    db.with_conn(|c| {
        c.execute(
            "CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER, total INTEGER)",
            [],
        )
        .map(|_| ())
    })
    .expect("create orders");

    let q = db.queue("emails", QueueOpts::default());

    // Success path: order INSERT and job enqueue commit together.
    {
        let tx = db.transaction()?;
        tx.execute(
            "INSERT INTO orders (user_id, total) VALUES (?1, ?2)",
            params![42, 9900],
        )?;
        q.enqueue_tx(
            &tx,
            &json!({"to": "alice@example.com", "order_id": 42}),
            EnqueueOpts::default(),
        )?;
        tx.commit()?;
    }

    let (orders, queued) = counts(&db);
    println!("committed: {} order(s), {} job(s)", orders, queued);
    assert_eq!(orders, 1);
    assert_eq!(queued, 1);

    // Rollback path: everything disappears.
    {
        let tx = db.transaction()?;
        tx.execute(
            "INSERT INTO orders (user_id, total) VALUES (?1, ?2)",
            params![43, 5000],
        )?;
        q.enqueue_tx(
            &tx,
            &json!({"to": "bob@example.com", "order_id": 43}),
            EnqueueOpts::default(),
        )?;
        tx.rollback()?;
    }
    println!("rolled back: simulated payment-processing failure");

    let (orders, queued) = counts(&db);
    println!("after rollback: {} order(s), {} job(s)", orders, queued);
    assert_eq!(orders, 1);
    assert_eq!(queued, 1);

    println!("atomic enqueue + rollback both work as expected");
    Ok(())
}

fn counts(db: &Database) -> (i64, i64) {
    db.with_conn(|c| {
        let orders: i64 = c
            .query_row("SELECT COUNT(*) FROM orders", [], |r| r.get(0))
            .unwrap();
        let queued: i64 = c
            .query_row(
                "SELECT COUNT(*) FROM _honker_live WHERE queue='emails'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        (orders, queued)
    })
}
