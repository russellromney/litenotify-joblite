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

/// Wrap any Displayable error into rusqlite::Error::UserFunctionError so
/// SQLite reports it back to the caller.
fn to_sql_err<E: std::fmt::Display>(e: E) -> rusqlite::Error {
    rusqlite::Error::UserFunctionError(Box::new(std::io::Error::new(
        std::io::ErrorKind::Other,
        e.to_string(),
    )))
}

fn install_functions(conn: &Connection) -> rusqlite::Result<()> {
    // notify() scalar + _litenotify_notifications table (shared with PyO3/Node).
    litenotify_core::attach_notify(conn).map_err(to_sql_err)?;

    // jl_bootstrap() -- idempotent schema setup.
    conn.create_scalar_function(
        "jl_bootstrap",
        0,
        FunctionFlags::SQLITE_UTF8,
        |ctx| {
            let db = unsafe { ctx.get_connection() }?;
            bootstrap_schema(&db).map_err(to_sql_err)?;
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
            claim_batch(&db, &queue, &worker_id, n, timeout_s).map_err(to_sql_err)
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
            ack_batch(&db, &ids_json, &worker_id).map_err(to_sql_err)
        },
    )?;

    // jl_sweep_expired(queue) -> count moved to _joblite_dead
    conn.create_scalar_function(
        "jl_sweep_expired",
        1,
        FunctionFlags::SQLITE_UTF8,
        |ctx| {
            let queue: String = ctx.get(0)?;
            let db = unsafe { ctx.get_connection() }?;
            sweep_expired(&db, &queue).map_err(to_sql_err)
        },
    )?;

    // jl_lock_acquire(name, owner, ttl_s) -> 1 if acquired, 0 if held by another
    conn.create_scalar_function(
        "jl_lock_acquire",
        3,
        FunctionFlags::SQLITE_UTF8,
        |ctx| {
            let name: String = ctx.get(0)?;
            let owner: String = ctx.get(1)?;
            let ttl: i64 = ctx.get(2)?;
            let db = unsafe { ctx.get_connection() }?;
            lock_acquire(&db, &name, &owner, ttl).map_err(to_sql_err)
        },
    )?;

    // jl_lock_release(name, owner) -> 1 if released, 0 if not our row
    conn.create_scalar_function(
        "jl_lock_release",
        2,
        FunctionFlags::SQLITE_UTF8,
        |ctx| {
            let name: String = ctx.get(0)?;
            let owner: String = ctx.get(1)?;
            let db = unsafe { ctx.get_connection() }?;
            lock_release(&db, &name, &owner).map_err(to_sql_err)
        },
    )?;

    // jl_rate_limit_try(name, limit, per) -> 1 if under limit, 0 if at/over
    conn.create_scalar_function(
        "jl_rate_limit_try",
        3,
        FunctionFlags::SQLITE_UTF8,
        |ctx| {
            let name: String = ctx.get(0)?;
            let limit: i64 = ctx.get(1)?;
            let per: i64 = ctx.get(2)?;
            let db = unsafe { ctx.get_connection() }?;
            rate_limit_try(&db, &name, limit, per).map_err(to_sql_err)
        },
    )?;

    // jl_rate_limit_sweep(older_than_s) -> count rows deleted
    conn.create_scalar_function(
        "jl_rate_limit_sweep",
        1,
        FunctionFlags::SQLITE_UTF8,
        |ctx| {
            let older_than_s: i64 = ctx.get(0)?;
            let db = unsafe { ctx.get_connection() }?;
            rate_limit_sweep(&db, older_than_s).map_err(to_sql_err)
        },
    )?;

    // jl_scheduler_record_fire(name, fire_at_unix) -> 0
    conn.create_scalar_function(
        "jl_scheduler_record_fire",
        2,
        FunctionFlags::SQLITE_UTF8,
        |ctx| {
            let name: String = ctx.get(0)?;
            let fire_at: i64 = ctx.get(1)?;
            let db = unsafe { ctx.get_connection() }?;
            scheduler_record_fire(&db, &name, fire_at).map_err(to_sql_err)
        },
    )?;

    // jl_scheduler_last_fire(name) -> unix_ts or 0 if never
    conn.create_scalar_function(
        "jl_scheduler_last_fire",
        1,
        FunctionFlags::SQLITE_UTF8,
        |ctx| {
            let name: String = ctx.get(0)?;
            let db = unsafe { ctx.get_connection() }?;
            scheduler_last_fire(&db, &name).map_err(to_sql_err)
        },
    )?;

    // jl_result_save(job_id, value_json, ttl_s) -> 1
    // ttl_s=0 means no expiration (NULL expires_at).
    conn.create_scalar_function(
        "jl_result_save",
        3,
        FunctionFlags::SQLITE_UTF8,
        |ctx| {
            let job_id: i64 = ctx.get(0)?;
            let value: String = ctx.get(1)?;
            let ttl_s: i64 = ctx.get(2)?;
            let db = unsafe { ctx.get_connection() }?;
            result_save(&db, job_id, &value, ttl_s).map_err(to_sql_err)
        },
    )?;

    // jl_result_get(job_id) -> value_json or NULL (row absent or expired)
    conn.create_scalar_function(
        "jl_result_get",
        1,
        FunctionFlags::SQLITE_UTF8,
        |ctx| {
            let job_id: i64 = ctx.get(0)?;
            let db = unsafe { ctx.get_connection() }?;
            result_get(&db, job_id).map_err(to_sql_err)
        },
    )?;

    // jl_result_sweep() -> count expired rows deleted
    conn.create_scalar_function(
        "jl_result_sweep",
        0,
        FunctionFlags::SQLITE_UTF8,
        |ctx| {
            let db = unsafe { ctx.get_connection() }?;
            result_sweep(&db).map_err(to_sql_err)
        },
    )?;

    Ok(())
}

fn bootstrap_schema(conn: &Connection) -> rusqlite::Result<()> {
    // Delegate to the shared core so the extension and the Python
    // binding can't drift on column counts. Pre-core, the extension
    // had a 6-column `_joblite_dead` and Python had a 10-column one —
    // silent divergence until a `.fail()` from Python tripped on the
    // missing `priority` column.
    litenotify_core::bootstrap_joblite_schema(conn).map_err(|e| {
        rusqlite::Error::UserFunctionError(Box::new(std::io::Error::new(
            std::io::ErrorKind::Other,
            e.to_string(),
        )))
    })
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
             AND (expires_at IS NULL OR expires_at > unixepoch())
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
    // Ack = DELETE. No `state='done'` row ever exists — matches the
    // Python `Queue.ack` path. Industry default (Sidekiq, Dramatiq,
    // graphile-worker, pgmq): delete on ack, keep audit separate.
    // Previous UPDATE-SET-state='done' left rows in _joblite_live
    // forever, unbounded growth + inspection-view divergence vs Python.
    let mut stmt = conn.prepare_cached(
        "DELETE FROM _joblite_live
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

/// Move expired-pending rows from _joblite_live to _joblite_dead with
/// last_error='expired'. Returns the count moved. Mirrors Python's
/// `Queue.sweep_expired`.
fn sweep_expired(conn: &Connection, queue: &str) -> rusqlite::Result<i64> {
    let mut select = conn.prepare_cached(
        "DELETE FROM _joblite_live
         WHERE queue = ?1
           AND state = 'pending'
           AND expires_at IS NOT NULL
           AND expires_at <= unixepoch()
         RETURNING id, queue, payload, priority, run_at, max_attempts,
                   attempts, created_at",
    )?;
    #[allow(clippy::type_complexity)]
    let rows: Vec<(i64, String, String, i64, i64, i64, i64, i64)> = select
        .query_map(rusqlite::params![queue], |r| {
            Ok((
                r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?,
                r.get(4)?, r.get(5)?, r.get(6)?, r.get(7)?,
            ))
        })?
        .collect::<Result<Vec<_>, _>>()?;
    if rows.is_empty() {
        return Ok(0);
    }
    let mut insert = conn.prepare_cached(
        "INSERT INTO _joblite_dead
           (id, queue, payload, priority, run_at, max_attempts,
            attempts, last_error, created_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, 'expired', ?8)",
    )?;
    let count = rows.len() as i64;
    for r in rows {
        insert.execute(rusqlite::params![
            r.0, r.1, r.2, r.3, r.4, r.5, r.6, r.7
        ])?;
    }
    Ok(count)
}

/// Try to acquire a named lock. Returns 1 if we got it, 0 if held by
/// someone else (and the TTL hasn't elapsed). Stale rows (expired
/// TTL) are pruned opportunistically before the attempt — a crashed
/// holder can't block others past the TTL.
fn lock_acquire(
    conn: &Connection,
    name: &str,
    owner: &str,
    ttl_s: i64,
) -> rusqlite::Result<i64> {
    conn.execute(
        "DELETE FROM _joblite_locks
         WHERE name = ?1 AND expires_at <= unixepoch()",
        rusqlite::params![name],
    )?;
    conn.execute(
        "INSERT OR IGNORE INTO _joblite_locks (name, owner, expires_at)
         VALUES (?1, ?2, unixepoch() + ?3)",
        rusqlite::params![name, owner, ttl_s],
    )?;
    let current: Option<String> = conn
        .query_row(
            "SELECT owner FROM _joblite_locks WHERE name = ?1",
            rusqlite::params![name],
            |r| r.get(0),
        )
        .ok();
    Ok(if current.as_deref() == Some(owner) { 1 } else { 0 })
}

/// Release a named lock we hold. Returns 1 if we actually owned it
/// (and the row was deleted), 0 otherwise (TTL elapsed and another
/// holder took over — safe no-op).
fn lock_release(conn: &Connection, name: &str, owner: &str) -> rusqlite::Result<i64> {
    let deleted = conn.execute(
        "DELETE FROM _joblite_locks WHERE name = ?1 AND owner = ?2",
        rusqlite::params![name, owner],
    )?;
    Ok(deleted as i64)
}

/// Fixed-window rate limit. Returns 1 if under `limit` in the current
/// `per`-second window (and records this invocation); 0 if at the
/// limit (no side effect, hot-loop safe). Mirrors Python's
/// `Database.try_rate_limit`.
fn rate_limit_try(
    conn: &Connection,
    name: &str,
    limit: i64,
    per: i64,
) -> rusqlite::Result<i64> {
    if limit <= 0 || per <= 0 {
        return Err(to_sql_err("limit and per must be positive"));
    }
    let window_start: i64 = conn.query_row(
        "SELECT (unixepoch() / ?1) * ?1",
        rusqlite::params![per],
        |r| r.get(0),
    )?;
    let current: i64 = conn
        .query_row(
            "SELECT COALESCE(MAX(count), 0) FROM _joblite_rate_limits
             WHERE name = ?1 AND window_start = ?2",
            rusqlite::params![name, window_start],
            |r| r.get(0),
        )
        .unwrap_or(0);
    if current >= limit {
        return Ok(0);
    }
    conn.execute(
        "INSERT INTO _joblite_rate_limits (name, window_start, count)
         VALUES (?1, ?2, 1)
         ON CONFLICT(name, window_start) DO UPDATE SET count = count + 1",
        rusqlite::params![name, window_start],
    )?;
    Ok(1)
}

/// Delete rate-limit rows older than `older_than_s` seconds. Stale
/// windows are never consulted; this is a disk-space reclaim only.
fn rate_limit_sweep(conn: &Connection, older_than_s: i64) -> rusqlite::Result<i64> {
    let deleted = conn.execute(
        "DELETE FROM _joblite_rate_limits
         WHERE window_start < unixepoch() - ?1",
        rusqlite::params![older_than_s],
    )?;
    Ok(deleted as i64)
}

/// Record a scheduler fire. UPSERT into _joblite_scheduler_state.
fn scheduler_record_fire(
    conn: &Connection,
    name: &str,
    fire_at_unix: i64,
) -> rusqlite::Result<i64> {
    conn.execute(
        "INSERT INTO _joblite_scheduler_state (name, last_fire_at)
         VALUES (?1, ?2)
         ON CONFLICT(name) DO UPDATE
           SET last_fire_at = excluded.last_fire_at",
        rusqlite::params![name, fire_at_unix],
    )?;
    Ok(0)
}

/// Look up the most recent fire time for a scheduled task. Returns
/// the unix epoch or 0 if the task has never fired.
fn scheduler_last_fire(conn: &Connection, name: &str) -> rusqlite::Result<i64> {
    Ok(conn
        .query_row(
            "SELECT last_fire_at FROM _joblite_scheduler_state WHERE name = ?1",
            rusqlite::params![name],
            |r| r.get(0),
        )
        .unwrap_or(0))
}

/// Save a task's return value keyed by its job id. `ttl_s=0` or
/// negative means no expiration (expires_at = NULL). UPSERTs, so
/// a worker saving twice for the same job_id replaces the first.
fn result_save(
    conn: &Connection,
    job_id: i64,
    value: &str,
    ttl_s: i64,
) -> rusqlite::Result<i64> {
    let expires_at: Option<i64> = if ttl_s > 0 {
        Some(ttl_s)  // treated as relative, turned absolute in SQL below
    } else {
        None
    };
    match expires_at {
        Some(rel) => conn.execute(
            "INSERT INTO _joblite_results (job_id, value, expires_at)
             VALUES (?1, ?2, unixepoch() + ?3)
             ON CONFLICT(job_id) DO UPDATE
               SET value = excluded.value,
                   expires_at = excluded.expires_at",
            rusqlite::params![job_id, value, rel],
        )?,
        None => conn.execute(
            "INSERT INTO _joblite_results (job_id, value, expires_at)
             VALUES (?1, ?2, NULL)
             ON CONFLICT(job_id) DO UPDATE
               SET value = excluded.value,
                   expires_at = NULL",
            rusqlite::params![job_id, value],
        )?,
    };
    Ok(1)
}

/// Fetch a saved result. Returns the stored value text, or NULL
/// (Option::None) if the row doesn't exist OR has expired. Callers
/// can't distinguish "result was stored as null-string" from "no
/// result" from this function alone — store values as JSON and
/// interpret null appropriately.
fn result_get(conn: &Connection, job_id: i64) -> rusqlite::Result<Option<String>> {
    let row: Option<(Option<String>, Option<i64>)> = conn
        .query_row(
            "SELECT value, expires_at FROM _joblite_results WHERE job_id = ?1",
            rusqlite::params![job_id],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .ok();
    match row {
        None => Ok(None),
        Some((_, Some(exp))) if exp <= now_unix(conn)? => Ok(None),
        Some((value, _)) => Ok(value),
    }
}

fn now_unix(conn: &Connection) -> rusqlite::Result<i64> {
    conn.query_row("SELECT unixepoch()", [], |r| r.get(0))
}

/// Delete expired result rows. Returns count deleted. Call on a
/// schedule; result storage is disk-space only so this isn't
/// correctness-critical.
fn result_sweep(conn: &Connection) -> rusqlite::Result<i64> {
    let deleted = conn.execute(
        "DELETE FROM _joblite_results
         WHERE expires_at IS NOT NULL AND expires_at <= unixepoch()",
        [],
    )?;
    Ok(deleted as i64)
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
