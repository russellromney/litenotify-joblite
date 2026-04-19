//! Cross-process NOTIFY/LISTEN for SQLite, backed by a table.
//!
//! `notify(channel, payload)` is a SQL scalar function: inside any
//! transaction, it does an INSERT into `_litenotify_notifications`.
//! The INSERT is part of the caller's transaction, so rollback drops
//! the notification atomically. On commit, the row becomes visible to
//! any reader in any process (via WAL).
//!
//! Listeners watch the `.db-wal` file for changes, then SELECT new
//! rows matching their channel. This is done by the Python side in
//! `litenotify.Listener`; this module only provides the server-side
//! SQL primitive.
//!
//! ## Retention
//!
//! The table is a short-term replay buffer, not a durable log. Every
//! 1000th INSERT prunes rows that are older than 10 seconds OR beyond
//! the most recent 10,000. For anything that needs longer replay or
//! per-consumer offsets, use `joblite.Stream`.
//!
//! ## IMPORTANT
//!
//! SQLite's `commit_hook` does NOT fire for `BEGIN DEFERRED`
//! transactions with no writes. Callers must use `BEGIN IMMEDIATE` (or
//! perform at least one write) for `notify()` to take effect — which
//! is automatic because notify() itself is a write.

use rusqlite::Connection;
use rusqlite::functions::FunctionFlags;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Database error: {0}")]
    Sqlite(#[from] rusqlite::Error),
}

/// Attach the notifications schema + notify() SQL function to a
/// connection. Idempotent; safe to call on every open_conn.
pub fn attach(conn: &Connection) -> Result<(), Error> {
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS _litenotify_notifications (
           id INTEGER PRIMARY KEY AUTOINCREMENT,
           channel TEXT NOT NULL,
           payload TEXT NOT NULL,
           created_at INTEGER NOT NULL DEFAULT (unixepoch())
         );
         CREATE INDEX IF NOT EXISTS _litenotify_notifications_recent
           ON _litenotify_notifications(channel, id);",
    )?;

    conn.create_scalar_function(
        "notify",
        2,
        FunctionFlags::SQLITE_UTF8,
        |ctx| {
            let channel: String = ctx.get(0)?;
            let payload: String = ctx.get(1)?;
            // The scalar function runs inside whatever transaction
            // the caller is in. INSERTing here just appends to that
            // transaction — rolled back on ROLLBACK, visible on COMMIT.
            let db = unsafe { ctx.get_connection() }?;
            let mut ins = db.prepare_cached(
                "INSERT INTO _litenotify_notifications (channel, payload) VALUES (?1, ?2)",
            )?;
            let id = ins.insert(rusqlite::params![channel, payload])?;

            // Opportunistic pruning. Keeps the table bounded in size
            // without needing a background task. `id % 1000 == 0`
            // gives ~0.1% of notify() calls a small DELETE to run.
            // Retention window: 10 seconds, OR most recent 10k rows,
            // whichever leaves fewer rows. Anything past that window
            // is someone else's problem — point them at
            // `joblite.Stream` for persistent replay.
            if id % 1000 == 0 {
                let mut del = db.prepare_cached(
                    "DELETE FROM _litenotify_notifications
                     WHERE created_at < unixepoch() - 10
                        OR id <= (SELECT MAX(id) - 10000 FROM _litenotify_notifications)",
                )?;
                let _ = del.execute([]);
            }

            Ok(id)
        },
    )?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn setup() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        attach(&conn).unwrap();
        conn
    }

    #[test]
    fn test_notify_inserts_row() {
        let conn = setup();
        conn.execute_batch("BEGIN IMMEDIATE;").unwrap();
        conn.query_row("SELECT notify('orders', 'new')", [], |_| Ok(()))
            .unwrap();
        conn.execute_batch("COMMIT;").unwrap();

        let n: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM _litenotify_notifications WHERE channel='orders'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(n, 1);
    }

    #[test]
    fn test_rollback_drops_notification() {
        let conn = setup();
        conn.execute_batch("BEGIN IMMEDIATE;").unwrap();
        conn.query_row("SELECT notify('x', 'y')", [], |_| Ok(()))
            .unwrap();
        conn.execute_batch("ROLLBACK;").unwrap();

        let n: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM _litenotify_notifications",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(n, 0);
    }

    #[test]
    fn test_multiple_notifies_preserve_order() {
        let conn = setup();
        conn.execute_batch("BEGIN IMMEDIATE;").unwrap();
        for i in 0..5 {
            conn.query_row(
                "SELECT notify('ch', ?1)",
                [format!("p{}", i)],
                |_| Ok(()),
            )
            .unwrap();
        }
        conn.execute_batch("COMMIT;").unwrap();

        let payloads: Vec<String> = conn
            .prepare(
                "SELECT payload FROM _litenotify_notifications
                 WHERE channel='ch' ORDER BY id",
            )
            .unwrap()
            .query_map([], |row| row.get::<_, String>(0))
            .unwrap()
            .filter_map(Result::ok)
            .collect();
        assert_eq!(payloads, vec!["p0", "p1", "p2", "p3", "p4"]);
    }

    #[test]
    fn test_unicode_and_large_payload() {
        let conn = setup();
        let big = "a".repeat(100_000);
        conn.execute_batch("BEGIN IMMEDIATE;").unwrap();
        conn.query_row("SELECT notify('ch', 'héllo 🦆')", [], |_| Ok(()))
            .unwrap();
        conn.query_row("SELECT notify('ch', ?1)", [&big], |_| Ok(()))
            .unwrap();
        conn.execute_batch("COMMIT;").unwrap();

        let payloads: Vec<String> = conn
            .prepare(
                "SELECT payload FROM _litenotify_notifications
                 WHERE channel='ch' ORDER BY id",
            )
            .unwrap()
            .query_map([], |row| row.get::<_, String>(0))
            .unwrap()
            .filter_map(Result::ok)
            .collect();
        assert_eq!(payloads[0], "héllo 🦆");
        assert_eq!(payloads[1].len(), 100_000);
    }
}
