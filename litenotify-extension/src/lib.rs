//! litenotify/joblite SQLite loadable extension.
//!
//! Provides SQL functions that do the claim/ack dance inside SQLite, via
//! Rust, without crossing a binding boundary. Point of this crate is to
//! measure the "true" engine throughput without PyO3 or Python-loop
//! overhead in the way.
//!
//!     SELECT load_extension('/path/to/liblitenotify');
//!     SELECT jl_bootstrap();                             -- create tables & indexes
//!     INSERT INTO _joblite_live (queue, payload)
//!     VALUES ('emails', '{"to": "alice"}');
//!     SELECT jl_claim_batch('emails', 'worker-1', 32, 300);
//!     -- returns JSON text: '[{"id":1,"queue":"emails",...}, ...]'
//!     SELECT jl_ack_batch('[1,2,3]', 'worker-1');        -- count ack'd
//!     SELECT notify('orders', 'new');                    -- in-tx buffered notify
//!
//! Subscriber state (for async `listen()`) lives in the language
//! bindings, not here. This crate is the SQL-side primitive set; the
//! Python wrapper layers `db.listen(channel)` on top.

use rusqlite::Connection;
use rusqlite::ffi;
use rusqlite::functions::FunctionFlags;
use std::os::raw::{c_char, c_int};

/// SQLite entry point. Name must match `sqlite3_<extname>_init`; SQLite
/// derives `<extname>` from the filename — stripping the `lib` prefix
/// and any non-alphabetic characters:
/// `liblitenotify_ext.dylib` -> `litenotify_ext` -> `litenotifyext`
/// -> `sqlite3_litenotifyext_init`.
///
/// Delegates to rusqlite's `extension_init2`, which binds the SQLite
/// API vtable and hands us a `Connection` wrapped around the incoming
/// raw handle.
///
/// # Safety
/// Called by SQLite. All pointers are SQLite-owned.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn sqlite3_litenotifyext_init(
    db: *mut ffi::sqlite3,
    pz_err_msg: *mut *mut c_char,
    p_api: *mut ffi::sqlite3_api_routines,
) -> c_int {
    unsafe {
        Connection::extension_init2(db, pz_err_msg, p_api, |conn| {
            install_functions(&conn)?;
            // Return true for "persistent" load: the extension stays
            // registered across connection close, so reopening the DB
            // in the same process doesn't re-load.
            Ok(true)
        })
    }
}

fn install_functions(conn: &Connection) -> rusqlite::Result<()> {
    // jl_bootstrap() -- idempotent schema setup.
    conn.create_scalar_function(
        "jl_bootstrap",
        0,
        FunctionFlags::SQLITE_UTF8,
        |ctx| {
            let db = unsafe { ctx.get_connection() }?;
            bootstrap_schema(&db).map_err(|e| {
                rusqlite::Error::UserFunctionError(Box::new(
                    std::io::Error::new(std::io::ErrorKind::Other, e.to_string()),
                ))
            })?;
            Ok(1i64)
        },
    )?;

    // jl_claim_batch(queue, worker_id, n, timeout_s) -> JSON array of claimed rows
    conn.create_scalar_function(
        "jl_claim_batch",
        4,
        FunctionFlags::SQLITE_UTF8,
        |ctx| {
            let queue: String = ctx.get(0)?;
            let worker_id: String = ctx.get(1)?;
            let n: i64 = ctx.get(2)?;
            let timeout_s: i64 = ctx.get(3)?;
            let db = unsafe { ctx.get_connection() }?;
            claim_batch(&db, &queue, &worker_id, n, timeout_s).map_err(|e| {
                rusqlite::Error::UserFunctionError(Box::new(
                    std::io::Error::new(std::io::ErrorKind::Other, e.to_string()),
                ))
            })
        },
    )?;

    // jl_ack_batch(ids_json, worker_id) -> count ack'd
    conn.create_scalar_function(
        "jl_ack_batch",
        2,
        FunctionFlags::SQLITE_UTF8,
        |ctx| {
            let ids_json: String = ctx.get(0)?;
            let worker_id: String = ctx.get(1)?;
            let db = unsafe { ctx.get_connection() }?;
            ack_batch(&db, &ids_json, &worker_id).map_err(|e| {
                rusqlite::Error::UserFunctionError(Box::new(
                    std::io::Error::new(std::io::ErrorKind::Other, e.to_string()),
                ))
            })
        },
    )?;

    Ok(())
}

fn bootstrap_schema(conn: &Connection) -> rusqlite::Result<()> {
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS _joblite_live (
           id INTEGER PRIMARY KEY AUTOINCREMENT,
           queue TEXT NOT NULL,
           payload TEXT NOT NULL,
           state TEXT NOT NULL DEFAULT 'pending',
           priority INTEGER NOT NULL DEFAULT 0,
           run_at INTEGER NOT NULL DEFAULT (unixepoch()),
           worker_id TEXT,
           claim_expires_at INTEGER,
           attempts INTEGER NOT NULL DEFAULT 0,
           max_attempts INTEGER NOT NULL DEFAULT 3,
           created_at INTEGER NOT NULL DEFAULT (unixepoch())
         );
         CREATE INDEX IF NOT EXISTS _joblite_live_claim
           ON _joblite_live(queue, priority DESC, run_at, id)
           WHERE state IN ('pending', 'processing');
         CREATE TABLE IF NOT EXISTS _joblite_dead (
           id INTEGER PRIMARY KEY,
           queue TEXT NOT NULL,
           payload TEXT NOT NULL,
           attempts INTEGER NOT NULL,
           last_error TEXT,
           died_at INTEGER NOT NULL DEFAULT (unixepoch())
         );",
    )
}

/// Returns JSON text: `[{"id":1,"queue":"...","payload":"...","worker_id":"...","attempts":N,"claim_expires_at":T}, ...]`
fn claim_batch(
    conn: &Connection,
    queue: &str,
    worker_id: &str,
    n: i64,
    timeout_s: i64,
) -> rusqlite::Result<String> {
    // One statement: UPDATE in place (state transitions pending -> processing
    // stay inside the partial-index WHERE, no B-tree reshuffle). RETURNING
    // pulls back just the hot fields so row materialization is cheap.
    let mut stmt = conn.prepare_cached(
        "UPDATE _joblite_live
         SET state = 'processing',
             worker_id = ?1,
             claim_expires_at = unixepoch() + ?4,
             attempts = attempts + 1
         WHERE id IN (
           SELECT id FROM _joblite_live
           WHERE queue = ?2
             AND state IN ('pending', 'processing')
             AND ((state = 'pending' AND run_at <= unixepoch())
               OR (state = 'processing' AND claim_expires_at < unixepoch()))
           ORDER BY priority DESC, run_at ASC, id ASC
           LIMIT ?3
         )
         RETURNING id, queue, payload, worker_id, attempts, claim_expires_at",
    )?;
    let rows = stmt.query_map(
        rusqlite::params![worker_id, queue, n, timeout_s],
        |row| {
            Ok(ClaimedRow {
                id: row.get(0)?,
                queue: row.get(1)?,
                payload: row.get(2)?,
                worker_id: row.get(3)?,
                attempts: row.get(4)?,
                claim_expires_at: row.get(5)?,
            })
        },
    )?;
    let mut out = String::from("[");
    let mut first = true;
    for row in rows {
        let r = row?;
        if !first {
            out.push(',');
        }
        first = false;
        // Hand-rolled JSON emit. Avoids pulling in serde_json just for
        // one use; the inputs are numeric ids / known-safe strings
        // (payload was already valid JSON when enqueued). We escape
        // conservatively.
        out.push_str(&format!(
            "{{\"id\":{},\"queue\":{},\"payload\":{},\"worker_id\":{},\"attempts\":{},\"claim_expires_at\":{}}}",
            r.id,
            json_str(&r.queue),
            json_str(&r.payload),
            json_str(&r.worker_id),
            r.attempts,
            r.claim_expires_at,
        ));
    }
    out.push(']');
    Ok(out)
}

fn ack_batch(conn: &Connection, ids_json: &str, worker_id: &str) -> rusqlite::Result<i64> {
    // One statement. SQLite parses the JSON array via json_each.
    let mut stmt = conn.prepare_cached(
        "UPDATE _joblite_live
         SET state = 'done'
         WHERE id IN (SELECT value FROM json_each(?1))
           AND worker_id = ?2
           AND claim_expires_at >= unixepoch()
         RETURNING id",
    )?;
    let mut rows = stmt.query(rusqlite::params![ids_json, worker_id])?;
    let mut count = 0;
    while rows.next()?.is_some() {
        count += 1;
    }
    Ok(count)
}

struct ClaimedRow {
    id: i64,
    queue: String,
    payload: String,
    worker_id: String,
    attempts: i64,
    claim_expires_at: i64,
}

fn json_str(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => {
                out.push_str(&format!("\\u{:04x}", c as u32));
            }
            c => out.push(c),
        }
    }
    out.push('"');
    out
}
