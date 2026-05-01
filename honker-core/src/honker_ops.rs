//! Rust implementations of the `honker_*` SQL scalar functions, plus a
//! single `attach_honker_functions` helper that registers them on a
//! [`rusqlite::Connection`].
//!
//! Consumers:
//!   * `honker-extension` — the loadable SQLite extension. Calls
//!     `attach_honker_functions` so `.load ./libhonker_ext` in any
//!     SQLite client exposes the full function set.
//!   * `packages/honker` — the PyO3 binding. Calls
//!     `attach_honker_functions` on its writer connection so Python
//!     can invoke `SELECT honker_*(...)` inside its own transactions
//!     without loading the `.dylib` at runtime.
//!   * Future bindings (Go, Ruby, napi-rs) — load the extension via
//!     SQLite's `sqlite3_load_extension` and get the same functions
//!     for free.
//!
//! Rationale: each per-language binding would otherwise re-implement
//! this SQL. Moving it here gives us one source of truth that's
//! tested once and inherited by every consumer.

use rusqlite::Connection;
use rusqlite::functions::FunctionFlags;

/// Wrap a Displayable error for SQLite scalar-function returns.
fn to_sql_err<E: std::fmt::Display>(e: E) -> rusqlite::Error {
    rusqlite::Error::UserFunctionError(Box::new(std::io::Error::new(
        std::io::ErrorKind::Other,
        e.to_string(),
    )))
}

/// Register all `honker_*` honker scalar functions on `conn`. Idempotent
/// per-connection: creating the same function twice is a rusqlite
/// error, so call exactly once per connection.
pub fn attach_honker_functions(conn: &Connection) -> rusqlite::Result<()> {
    conn.create_scalar_function("honker_bootstrap", 0, FunctionFlags::SQLITE_UTF8, |ctx| {
        let db = unsafe { ctx.get_connection() }?;
        super::bootstrap_honker_schema(&db).map_err(to_sql_err)?;
        Ok(1i64)
    })?;

    conn.create_scalar_function("honker_claim_batch", 4, FunctionFlags::SQLITE_UTF8, |ctx| {
        let queue: String = ctx.get(0)?;
        let worker_id: String = ctx.get(1)?;
        let n: i64 = ctx.get(2)?;
        let timeout_s: i64 = ctx.get(3)?;
        let db = unsafe { ctx.get_connection() }?;
        claim_batch(&db, &queue, &worker_id, n, timeout_s).map_err(to_sql_err)
    })?;

    conn.create_scalar_function("honker_ack_batch", 2, FunctionFlags::SQLITE_UTF8, |ctx| {
        let ids_json: String = ctx.get(0)?;
        let worker_id: String = ctx.get(1)?;
        let db = unsafe { ctx.get_connection() }?;
        ack_batch(&db, &ids_json, &worker_id).map_err(to_sql_err)
    })?;

    conn.create_scalar_function(
        "honker_sweep_expired",
        1,
        FunctionFlags::SQLITE_UTF8,
        |ctx| {
            let queue: String = ctx.get(0)?;
            let db = unsafe { ctx.get_connection() }?;
            sweep_expired(&db, &queue).map_err(to_sql_err)
        },
    )?;

    conn.create_scalar_function(
        "honker_lock_acquire",
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

    conn.create_scalar_function(
        "honker_lock_release",
        2,
        FunctionFlags::SQLITE_UTF8,
        |ctx| {
            let name: String = ctx.get(0)?;
            let owner: String = ctx.get(1)?;
            let db = unsafe { ctx.get_connection() }?;
            lock_release(&db, &name, &owner).map_err(to_sql_err)
        },
    )?;

    conn.create_scalar_function(
        "honker_rate_limit_try",
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

    conn.create_scalar_function(
        "honker_rate_limit_sweep",
        1,
        FunctionFlags::SQLITE_UTF8,
        |ctx| {
            let older_than_s: i64 = ctx.get(0)?;
            let db = unsafe { ctx.get_connection() }?;
            rate_limit_sweep(&db, older_than_s).map_err(to_sql_err)
        },
    )?;

    // honker_scheduler_register(name, queue, cron_expr, payload_json,
    //                       priority, expires_s_or_null, max_runs_or_null) -> 1.
    // Upserts the task row. `next_fire_at` is recomputed as the next
    // cron boundary strictly after `unixepoch()`. Calling twice with
    // the same name replaces the first registration entirely.
    // `max_runs` limits how many times the task fires before being
    // automatically unregistered; NULL means unlimited.
    conn.create_scalar_function(
        "honker_scheduler_register",
        7,
        FunctionFlags::SQLITE_UTF8,
        |ctx| {
            let name: String = ctx.get(0)?;
            let queue: String = ctx.get(1)?;
            let cron_expr: String = ctx.get(2)?;
            let payload: String = ctx.get(3)?;
            let priority: i64 = ctx.get(4)?;
            let expires_s: Option<i64> = ctx.get(5)?;
            let max_runs: Option<i64> = ctx.get(6)?;
            let db = unsafe { ctx.get_connection() }?;
            scheduler_register(
                &db, &name, &queue, &cron_expr, &payload, priority, expires_s, max_runs,
            )
            .map_err(to_sql_err)
        },
    )?;

    // honker_scheduler_unregister(name) -> rows deleted (0 or 1).
    conn.create_scalar_function(
        "honker_scheduler_unregister",
        1,
        FunctionFlags::SQLITE_UTF8,
        |ctx| {
            let name: String = ctx.get(0)?;
            let db = unsafe { ctx.get_connection() }?;
            scheduler_unregister(&db, &name).map_err(to_sql_err)
        },
    )?;

    // honker_scheduler_tick(now_unix) -> JSON array of fires. For each
    // registered task whose `next_fire_at <= now`, enqueues the
    // payload into the task's queue, advances `next_fire_at` to the
    // next cron boundary, and appends `{name, queue, fire_at,
    // job_id}` to the output array. Caller typically holds
    // `_honker_locks` entry 'honker-scheduler' for mutual
    // exclusion across scheduler processes.
    conn.create_scalar_function(
        "honker_scheduler_tick",
        1,
        FunctionFlags::SQLITE_UTF8,
        |ctx| {
            let now_unix: i64 = ctx.get(0)?;
            let db = unsafe { ctx.get_connection() }?;
            scheduler_tick(&db, now_unix).map_err(to_sql_err)
        },
    )?;

    // honker_scheduler_soonest() -> unix ts of the earliest next_fire_at
    // across all registered tasks, or 0 if no tasks. Scheduler main
    // loop uses this to compute its sleep duration.
    conn.create_scalar_function(
        "honker_scheduler_soonest",
        0,
        FunctionFlags::SQLITE_UTF8,
        |ctx| {
            let db = unsafe { ctx.get_connection() }?;
            scheduler_soonest(&db).map_err(to_sql_err)
        },
    )?;

    conn.create_scalar_function("honker_result_save", 3, FunctionFlags::SQLITE_UTF8, |ctx| {
        let job_id: i64 = ctx.get(0)?;
        let value: String = ctx.get(1)?;
        let ttl_s: i64 = ctx.get(2)?;
        let db = unsafe { ctx.get_connection() }?;
        result_save(&db, job_id, &value, ttl_s).map_err(to_sql_err)
    })?;

    conn.create_scalar_function("honker_result_get", 1, FunctionFlags::SQLITE_UTF8, |ctx| {
        let job_id: i64 = ctx.get(0)?;
        let db = unsafe { ctx.get_connection() }?;
        result_get(&db, job_id).map_err(to_sql_err)
    })?;

    conn.create_scalar_function(
        "honker_result_sweep",
        0,
        FunctionFlags::SQLITE_UTF8,
        |ctx| {
            let db = unsafe { ctx.get_connection() }?;
            result_sweep(&db).map_err(to_sql_err)
        },
    )?;

    // honker_enqueue(queue, payload, run_at_or_null, delay_or_null,
    //            priority, max_attempts, expires_or_null) -> inserted id.
    // Precedence: if `delay` is not NULL, use `unixepoch() + delay`;
    // else if `run_at` is not NULL, use that literal; else use
    // `unixepoch()`. `expires` is `unixepoch() + expires` if non-NULL,
    // else NULL (never expires).
    conn.create_scalar_function("honker_enqueue", 7, FunctionFlags::SQLITE_UTF8, |ctx| {
        let queue: String = ctx.get(0)?;
        let payload: String = ctx.get(1)?;
        let run_at: Option<i64> = ctx.get(2)?;
        let delay: Option<i64> = ctx.get(3)?;
        let priority: i64 = ctx.get(4)?;
        let max_attempts: i64 = ctx.get(5)?;
        let expires: Option<i64> = ctx.get(6)?;
        let db = unsafe { ctx.get_connection() }?;
        enqueue(
            &db,
            &queue,
            &payload,
            run_at,
            delay,
            priority,
            max_attempts,
            expires,
        )
        .map_err(to_sql_err)
    })?;

    // honker_ack(job_id, worker_id) -> 1 if ack'd, 0 if claim expired /
    // not ours.
    conn.create_scalar_function("honker_ack", 2, FunctionFlags::SQLITE_UTF8, |ctx| {
        let job_id: i64 = ctx.get(0)?;
        let worker_id: String = ctx.get(1)?;
        let db = unsafe { ctx.get_connection() }?;
        ack(&db, job_id, &worker_id).map_err(to_sql_err)
    })?;

    // honker_retry(job_id, worker_id, delay_s, error) -> 1 if retried /
    // moved to dead, 0 if not our claim. If attempts >= max_attempts,
    // moves the row to `_honker_dead` instead of flipping it back
    // to pending. Fires a notify on the queue's channel on successful
    // pending-flip (so waiting workers wake).
    conn.create_scalar_function("honker_retry", 4, FunctionFlags::SQLITE_UTF8, |ctx| {
        let job_id: i64 = ctx.get(0)?;
        let worker_id: String = ctx.get(1)?;
        let delay_s: i64 = ctx.get(2)?;
        let error: String = ctx.get(3)?;
        let db = unsafe { ctx.get_connection() }?;
        retry(&db, job_id, &worker_id, delay_s, &error).map_err(to_sql_err)
    })?;

    // honker_fail(job_id, worker_id, error) -> 1 if failed-to-dead, 0 if
    // not our claim.
    conn.create_scalar_function("honker_fail", 3, FunctionFlags::SQLITE_UTF8, |ctx| {
        let job_id: i64 = ctx.get(0)?;
        let worker_id: String = ctx.get(1)?;
        let error: String = ctx.get(2)?;
        let db = unsafe { ctx.get_connection() }?;
        fail(&db, job_id, &worker_id, &error).map_err(to_sql_err)
    })?;

    // honker_heartbeat(job_id, worker_id, extend_s) -> 1 if extended, 0
    // if not our claim.
    conn.create_scalar_function("honker_heartbeat", 3, FunctionFlags::SQLITE_UTF8, |ctx| {
        let job_id: i64 = ctx.get(0)?;
        let worker_id: String = ctx.get(1)?;
        let extend_s: i64 = ctx.get(2)?;
        let db = unsafe { ctx.get_connection() }?;
        heartbeat(&db, job_id, &worker_id, extend_s).map_err(to_sql_err)
    })?;

    // honker_cron_next_after(expr, from_unix) -> unix_ts of next boundary
    // strictly after `from_unix`, minute precision, system local time.
    // Same 5-field grammar as standard Unix cron. Deterministic +
    // pure; marked DETERMINISTIC to let SQLite optimize inside joins.
    conn.create_scalar_function(
        "honker_cron_next_after",
        2,
        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
        |ctx| {
            let expr: String = ctx.get(0)?;
            let from_unix: i64 = ctx.get(1)?;
            super::cron::next_after_unix(&expr, from_unix).map_err(to_sql_err)
        },
    )?;

    // Stream functions. One impl for every binding; _honker_stream +
    // _honker_stream_consumers are the shared on-disk layout.

    // honker_stream_publish(topic, key_or_null, payload_json) -> offset.
    // INSERTs one event and fires a wake on honker:stream:<topic>.
    conn.create_scalar_function(
        "honker_stream_publish",
        3,
        FunctionFlags::SQLITE_UTF8,
        |ctx| {
            let topic: String = ctx.get(0)?;
            let key: Option<String> = ctx.get(1)?;
            let payload: String = ctx.get(2)?;
            let db = unsafe { ctx.get_connection() }?;
            stream_publish(&db, &topic, key.as_deref(), &payload).map_err(to_sql_err)
        },
    )?;

    // honker_stream_read_since(topic, offset, limit) -> JSON array of
    // {offset, topic, key, payload, created_at}.
    conn.create_scalar_function(
        "honker_stream_read_since",
        3,
        FunctionFlags::SQLITE_UTF8,
        |ctx| {
            let topic: String = ctx.get(0)?;
            let offset: i64 = ctx.get(1)?;
            let limit: i64 = ctx.get(2)?;
            let db = unsafe { ctx.get_connection() }?;
            stream_read_since(&db, &topic, offset, limit).map_err(to_sql_err)
        },
    )?;

    // honker_stream_save_offset(consumer, topic, offset) -> 1 if row
    // advanced (new row or higher offset), 0 if the saved offset is
    // already >= `offset`. Monotonic: never rewinds on duplicate
    // deliveries.
    conn.create_scalar_function(
        "honker_stream_save_offset",
        3,
        FunctionFlags::SQLITE_UTF8,
        |ctx| {
            let consumer: String = ctx.get(0)?;
            let topic: String = ctx.get(1)?;
            let offset: i64 = ctx.get(2)?;
            let db = unsafe { ctx.get_connection() }?;
            stream_save_offset(&db, &consumer, &topic, offset).map_err(to_sql_err)
        },
    )?;

    // honker_stream_get_offset(consumer, topic) -> offset or 0.
    conn.create_scalar_function(
        "honker_stream_get_offset",
        2,
        FunctionFlags::SQLITE_UTF8,
        |ctx| {
            let consumer: String = ctx.get(0)?;
            let topic: String = ctx.get(1)?;
            let db = unsafe { ctx.get_connection() }?;
            stream_get_offset(&db, &consumer, &topic).map_err(to_sql_err)
        },
    )?;

    Ok(())
}

// ---------------------------------------------------------------------
// Claim / ack
// ---------------------------------------------------------------------

/// Returns JSON text: `[{"id":1,"queue":"...","payload":"...","worker_id":"...","attempts":N,"claim_expires_at":T}, ...]`
pub fn claim_batch(
    conn: &Connection,
    queue: &str,
    worker_id: &str,
    n: i64,
    timeout_s: i64,
) -> rusqlite::Result<String> {
    let mut stmt = conn.prepare_cached(
        "UPDATE _honker_live
         SET state = 'processing',
             worker_id = ?1,
             claim_expires_at = unixepoch() + ?4,
             attempts = attempts + 1
         WHERE id IN (
           SELECT id FROM _honker_live
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
    let rows = stmt.query_map(rusqlite::params![worker_id, queue, n, timeout_s], |row| {
        Ok((
            row.get::<_, i64>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, String>(2)?,
            row.get::<_, String>(3)?,
            row.get::<_, i64>(4)?,
            row.get::<_, i64>(5)?,
        ))
    })?;
    let mut out = String::from("[");
    let mut first = true;
    for row in rows {
        let (id, q, payload, w, attempts, claim_expires_at) = row?;
        if !first {
            out.push(',');
        }
        first = false;
        out.push_str(&format!(
            "{{\"id\":{},\"queue\":{},\"payload\":{},\"worker_id\":{},\"attempts\":{},\"claim_expires_at\":{}}}",
            id, json_str(&q), json_str(&payload), json_str(&w),
            attempts, claim_expires_at,
        ));
    }
    out.push(']');
    Ok(out)
}

pub fn ack_batch(conn: &Connection, ids_json: &str, worker_id: &str) -> rusqlite::Result<i64> {
    let mut stmt = conn.prepare_cached(
        "DELETE FROM _honker_live
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

// ---------------------------------------------------------------------
// Enqueue / single-job ack / retry / fail / heartbeat
// ---------------------------------------------------------------------

/// INSERT a job. Returns the new row's id.
///
/// Scheduling (lowest-to-highest precedence):
///   - no run_at, no delay → `unixepoch()` (claimable immediately)
///   - run_at set           → that literal unix timestamp
///   - delay set            → `unixepoch() + delay` (wins over run_at)
///
/// Expiration: NULL = never; `Some(s)` = `unixepoch() + s`.
pub fn enqueue(
    conn: &Connection,
    queue: &str,
    payload: &str,
    run_at: Option<i64>,
    delay: Option<i64>,
    priority: i64,
    max_attempts: i64,
    expires: Option<i64>,
) -> rusqlite::Result<i64> {
    let now: i64 = conn.query_row("SELECT unixepoch()", [], |r| r.get(0))?;
    let run_at_val: i64 = match (delay, run_at) {
        (Some(d), _) => now + d,
        (None, Some(r)) => r,
        (None, None) => now,
    };
    let expires_at: Option<i64> = expires.map(|e| now + e);
    let channel = format!("honker:{}", queue);

    let id: i64 = conn.query_row(
        "INSERT INTO _honker_live
           (queue, payload, run_at, priority, max_attempts, expires_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6)
         RETURNING id",
        rusqlite::params![
            queue,
            payload,
            run_at_val,
            priority,
            max_attempts,
            expires_at
        ],
        |r| r.get(0),
    )?;
    // Fire a wake so workers parked on this queue's channel re-poll.
    conn.execute(
        "INSERT INTO _honker_notifications (channel, payload)
         VALUES (?1, 'new')",
        rusqlite::params![channel],
    )?;
    Ok(id)
}

/// Single-job ack. DELETEs the row if the caller's claim is still
/// valid. Returns 1 on success, 0 if the claim expired or the row
/// isn't ours.
pub fn ack(conn: &Connection, job_id: i64, worker_id: &str) -> rusqlite::Result<i64> {
    let deleted = conn.execute(
        "DELETE FROM _honker_live
         WHERE id = ?1 AND worker_id = ?2 AND claim_expires_at >= unixepoch()",
        rusqlite::params![job_id, worker_id],
    )?;
    Ok(deleted as i64)
}

/// Retry or fail based on `attempts` vs `max_attempts`. If another
/// attempt is allowed, flips the row back to `'pending'` with
/// `run_at = unixepoch() + delay_s` and fires a wake. Otherwise
/// DELETEs from `_honker_live` and INSERTs into `_honker_dead`
/// with `last_error=error`.
///
/// Returns 1 if either branch ran, 0 if the claim is no longer valid
/// (expired / not our worker / row moved on).
pub fn retry(
    conn: &Connection,
    job_id: i64,
    worker_id: &str,
    delay_s: i64,
    error: &str,
) -> rusqlite::Result<i64> {
    #[allow(clippy::type_complexity)]
    let row: Option<(i64, String, String, i64, i64, i64, i64, i64)> = conn
        .query_row(
            "SELECT id, queue, payload, priority, run_at, max_attempts,
                    attempts, created_at
             FROM _honker_live
             WHERE id = ?1 AND worker_id = ?2
               AND claim_expires_at >= unixepoch()
               AND state = 'processing'",
            rusqlite::params![job_id, worker_id],
            |r| {
                Ok((
                    r.get(0)?,
                    r.get(1)?,
                    r.get(2)?,
                    r.get(3)?,
                    r.get(4)?,
                    r.get(5)?,
                    r.get(6)?,
                    r.get(7)?,
                ))
            },
        )
        .ok();
    let Some((id, queue, payload, priority, run_at, max_attempts, attempts, created_at)) = row
    else {
        return Ok(0);
    };
    if attempts >= max_attempts {
        conn.execute(
            "DELETE FROM _honker_live WHERE id = ?1",
            rusqlite::params![id],
        )?;
        conn.execute(
            "INSERT INTO _honker_dead
               (id, queue, payload, priority, run_at, max_attempts,
                attempts, last_error, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            rusqlite::params![
                id,
                queue,
                payload,
                priority,
                run_at,
                max_attempts,
                attempts,
                error,
                created_at
            ],
        )?;
    } else {
        conn.execute(
            "UPDATE _honker_live
             SET state = 'pending',
                 run_at = unixepoch() + ?2,
                 worker_id = NULL,
                 claim_expires_at = NULL
             WHERE id = ?1",
            rusqlite::params![id, delay_s],
        )?;
        // Fire a wake — the row is now claimable again (after the
        // delay), and waiting workers should re-poll.
        let channel = format!("honker:{}", queue);
        conn.execute(
            "INSERT INTO _honker_notifications (channel, payload)
         VALUES (?1, 'new')",
            rusqlite::params![channel],
        )?;
    }
    Ok(1)
}

/// Unconditionally move the claim to `_honker_dead` with the given
/// error. Returns 1 if moved, 0 if not our claim.
pub fn fail(conn: &Connection, job_id: i64, worker_id: &str, error: &str) -> rusqlite::Result<i64> {
    #[allow(clippy::type_complexity)]
    let row: Option<(i64, String, String, i64, i64, i64, i64, i64)> = conn
        .query_row(
            "DELETE FROM _honker_live
             WHERE id = ?1 AND worker_id = ?2
               AND claim_expires_at >= unixepoch()
             RETURNING id, queue, payload, priority, run_at, max_attempts,
                       attempts, created_at",
            rusqlite::params![job_id, worker_id],
            |r| {
                Ok((
                    r.get(0)?,
                    r.get(1)?,
                    r.get(2)?,
                    r.get(3)?,
                    r.get(4)?,
                    r.get(5)?,
                    r.get(6)?,
                    r.get(7)?,
                ))
            },
        )
        .ok();
    let Some((id, queue, payload, priority, run_at, max_attempts, attempts, created_at)) = row
    else {
        return Ok(0);
    };
    conn.execute(
        "INSERT INTO _honker_dead
           (id, queue, payload, priority, run_at, max_attempts,
            attempts, last_error, created_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
        rusqlite::params![
            id,
            queue,
            payload,
            priority,
            run_at,
            max_attempts,
            attempts,
            error,
            created_at
        ],
    )?;
    Ok(1)
}

/// Extend the current claim by `extend_s` seconds. Returns 1 if the
/// heartbeat landed, 0 if we're not the holder (either the row is
/// in a different state or worker_id doesn't match).
pub fn heartbeat(
    conn: &Connection,
    job_id: i64,
    worker_id: &str,
    extend_s: i64,
) -> rusqlite::Result<i64> {
    let updated = conn.execute(
        "UPDATE _honker_live
         SET claim_expires_at = unixepoch() + ?3
         WHERE id = ?1 AND worker_id = ?2 AND state = 'processing'",
        rusqlite::params![job_id, worker_id, extend_s],
    )?;
    Ok(updated as i64)
}

// ---------------------------------------------------------------------
// Task expiration
// ---------------------------------------------------------------------

/// Move expired-pending rows from `_honker_live` to `_honker_dead`
/// with `last_error='expired'`. Returns count moved.
pub fn sweep_expired(conn: &Connection, queue: &str) -> rusqlite::Result<i64> {
    let mut select = conn.prepare_cached(
        "DELETE FROM _honker_live
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
                r.get(0)?,
                r.get(1)?,
                r.get(2)?,
                r.get(3)?,
                r.get(4)?,
                r.get(5)?,
                r.get(6)?,
                r.get(7)?,
            ))
        })?
        .collect::<Result<Vec<_>, _>>()?;
    if rows.is_empty() {
        return Ok(0);
    }
    let mut insert = conn.prepare_cached(
        "INSERT INTO _honker_dead
           (id, queue, payload, priority, run_at, max_attempts,
            attempts, last_error, created_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, 'expired', ?8)",
    )?;
    let count = rows.len() as i64;
    for r in rows {
        insert.execute(rusqlite::params![r.0, r.1, r.2, r.3, r.4, r.5, r.6, r.7])?;
    }
    Ok(count)
}

// ---------------------------------------------------------------------
// Named locks
// ---------------------------------------------------------------------

pub fn lock_acquire(
    conn: &Connection,
    name: &str,
    owner: &str,
    ttl_s: i64,
) -> rusqlite::Result<i64> {
    conn.execute(
        "DELETE FROM _honker_locks
         WHERE name = ?1 AND expires_at <= unixepoch()",
        rusqlite::params![name],
    )?;
    conn.execute(
        "INSERT OR IGNORE INTO _honker_locks (name, owner, expires_at)
         VALUES (?1, ?2, unixepoch() + ?3)",
        rusqlite::params![name, owner, ttl_s],
    )?;
    let current: Option<String> = conn
        .query_row(
            "SELECT owner FROM _honker_locks WHERE name = ?1",
            rusqlite::params![name],
            |r| r.get(0),
        )
        .ok();
    Ok(if current.as_deref() == Some(owner) {
        1
    } else {
        0
    })
}

pub fn lock_release(conn: &Connection, name: &str, owner: &str) -> rusqlite::Result<i64> {
    let deleted = conn.execute(
        "DELETE FROM _honker_locks WHERE name = ?1 AND owner = ?2",
        rusqlite::params![name, owner],
    )?;
    Ok(deleted as i64)
}

// ---------------------------------------------------------------------
// Rate limiting
// ---------------------------------------------------------------------

pub fn rate_limit_try(
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
            "SELECT COALESCE(MAX(count), 0) FROM _honker_rate_limits
             WHERE name = ?1 AND window_start = ?2",
            rusqlite::params![name, window_start],
            |r| r.get(0),
        )
        .unwrap_or(0);
    if current >= limit {
        return Ok(0);
    }
    conn.execute(
        "INSERT INTO _honker_rate_limits (name, window_start, count)
         VALUES (?1, ?2, 1)
         ON CONFLICT(name, window_start) DO UPDATE SET count = count + 1",
        rusqlite::params![name, window_start],
    )?;
    Ok(1)
}

pub fn rate_limit_sweep(conn: &Connection, older_than_s: i64) -> rusqlite::Result<i64> {
    let deleted = conn.execute(
        "DELETE FROM _honker_rate_limits
         WHERE window_start < unixepoch() - ?1",
        rusqlite::params![older_than_s],
    )?;
    Ok(deleted as i64)
}

// ---------------------------------------------------------------------
// Scheduler state
// ---------------------------------------------------------------------

/// Register (or re-register) a periodic task. `next_fire_at` is
/// computed as the next cron boundary strictly after
/// `unixepoch()`. Calling twice with the same name replaces the
/// first registration entirely. `max_runs` limits total fires before
/// automatic unregistration; `None` means unlimited.
pub fn scheduler_register(
    conn: &Connection,
    name: &str,
    queue: &str,
    cron_expr: &str,
    payload: &str,
    priority: i64,
    expires_s: Option<i64>,
    max_runs: Option<i64>,
) -> rusqlite::Result<i64> {
    let now = now_unix(conn)?;
    let next_fire_at = super::cron::next_after_unix(cron_expr, now).map_err(to_sql_err)?;
    conn.execute(
        "INSERT INTO _honker_scheduler_tasks
           (name, queue, cron_expr, payload, priority, expires_s, next_fire_at, max_runs, run_count)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, 0)
         ON CONFLICT(name) DO UPDATE SET
           queue = excluded.queue,
           cron_expr = excluded.cron_expr,
           payload = excluded.payload,
           priority = excluded.priority,
           expires_s = excluded.expires_s,
           next_fire_at = excluded.next_fire_at,
           max_runs = excluded.max_runs,
           run_count = 0",
        rusqlite::params![
            name,
            queue,
            cron_expr,
            payload,
            priority,
            expires_s,
            next_fire_at,
            max_runs,
        ],
    )?;
    // Wake any sleeping scheduler leader so it re-computes
    // honker_scheduler_soonest() against the new task set. Without
    // this, a leader that went to sleep for an hour before a newly-
    // registered 1-minute-from-now task existed would oversleep past
    // its first fire.
    scheduler_wake(conn)?;
    Ok(1)
}

pub fn scheduler_unregister(conn: &Connection, name: &str) -> rusqlite::Result<i64> {
    let n = conn.execute(
        "DELETE FROM _honker_scheduler_tasks WHERE name = ?1",
        rusqlite::params![name],
    )?;
    if n > 0 {
        // Unregister can only make the "soonest" later, so a sleeping
        // leader wouldn't miss anything by oversleeping. But waking it
        // lets the loop observe the removal and notice if the table is
        // now empty (soonest() returns 0 → leader exits cleanly).
        scheduler_wake(conn)?;
    }
    Ok(n as i64)
}

/// INSERT a row on channel `honker:scheduler` so a sleeping scheduler
/// leader sitting on `update_events()` wakes and re-evaluates. Payload
/// is opaque — the leader doesn't read it, only the update tick matters.
fn scheduler_wake(conn: &Connection) -> rusqlite::Result<()> {
    conn.execute(
        "INSERT INTO _honker_notifications (channel, payload)
         VALUES ('honker:scheduler', 'wake')",
        [],
    )?;
    Ok(())
}

/// For each registered task whose `next_fire_at <= now_unix`,
/// enqueue the payload into its queue and advance `next_fire_at`
/// to the next boundary. Keeps advancing within one tick while
/// boundaries remain in the past (catches up after a scheduler
/// outage) — same semantics as the previous Python `_fire_due`.
/// Returns a JSON array of `{name, queue, fire_at, job_id}` fires.
pub fn scheduler_tick(conn: &Connection, now_unix: i64) -> rusqlite::Result<String> {
    #[allow(clippy::type_complexity)]
    let tasks: Vec<(String, String, String, String, i64, Option<i64>, i64, Option<i64>, i64)> = {
        let mut stmt = conn.prepare_cached(
            "SELECT name, queue, cron_expr, payload, priority, expires_s, next_fire_at,
                    max_runs, run_count
             FROM _honker_scheduler_tasks
             WHERE next_fire_at <= ?1",
        )?;
        stmt.query_map(rusqlite::params![now_unix], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, String>(2)?,
                r.get::<_, String>(3)?,
                r.get::<_, i64>(4)?,
                r.get::<_, Option<i64>>(5)?,
                r.get::<_, i64>(6)?,
                r.get::<_, Option<i64>>(7)?,
                r.get::<_, i64>(8)?,
            ))
        })?
        .collect::<Result<Vec<_>, _>>()?
    };
    let mut out = String::from("[");
    let mut first = true;
    for (name, queue, cron_expr, payload, priority, expires_s, mut next_fire_at, max_runs, run_count) in tasks {
        let mut local_run_count = run_count;
        while next_fire_at <= now_unix {
            // Stop if this task has hit its run limit.
            if let Some(limit) = max_runs {
                if local_run_count >= limit {
                    break;
                }
            }
            // Enqueue at this boundary. `run_at` is NULL (claimable
            // immediately); `expires` is the task's expires_s if set.
            let job_id = enqueue(
                conn, &queue, &payload, None, None, priority, 3, /* max_attempts default */
                expires_s,
            )?;
            local_run_count += 1;
            if !first {
                out.push(',');
            }
            first = false;
            out.push_str(&format!(
                "{{\"name\":{},\"queue\":{},\"fire_at\":{},\"job_id\":{}}}",
                json_str(&name),
                json_str(&queue),
                next_fire_at,
                job_id,
            ));
            // Advance to the next boundary strictly after this one.
            next_fire_at =
                super::cron::next_after_unix(&cron_expr, next_fire_at).map_err(to_sql_err)?;
        }
        // If the run limit is exhausted, unregister the task entirely.
        // Otherwise persist the advanced next_fire_at and updated run_count.
        if max_runs.is_some_and(|limit| local_run_count >= limit) {
            conn.execute(
                "DELETE FROM _honker_scheduler_tasks WHERE name = ?1",
                rusqlite::params![name],
            )?;
        } else {
            conn.execute(
                "UPDATE _honker_scheduler_tasks
                 SET next_fire_at = ?2, run_count = ?3 WHERE name = ?1",
                rusqlite::params![name, next_fire_at, local_run_count],
            )?;
        }
    }
    out.push(']');
    Ok(out)
}

pub fn scheduler_soonest(conn: &Connection) -> rusqlite::Result<i64> {
    Ok(conn
        .query_row(
            "SELECT COALESCE(MIN(next_fire_at), 0) FROM _honker_scheduler_tasks",
            [],
            |r| r.get(0),
        )
        .unwrap_or(0))
}

// ---------------------------------------------------------------------
// Task result storage
// ---------------------------------------------------------------------

pub fn result_save(
    conn: &Connection,
    job_id: i64,
    value: &str,
    ttl_s: i64,
) -> rusqlite::Result<i64> {
    if ttl_s > 0 {
        conn.execute(
            "INSERT INTO _honker_results (job_id, value, expires_at)
             VALUES (?1, ?2, unixepoch() + ?3)
             ON CONFLICT(job_id) DO UPDATE
               SET value = excluded.value,
                   expires_at = excluded.expires_at",
            rusqlite::params![job_id, value, ttl_s],
        )?;
    } else {
        conn.execute(
            "INSERT INTO _honker_results (job_id, value, expires_at)
             VALUES (?1, ?2, NULL)
             ON CONFLICT(job_id) DO UPDATE
               SET value = excluded.value,
                   expires_at = NULL",
            rusqlite::params![job_id, value],
        )?;
    }
    Ok(1)
}

pub fn result_get(conn: &Connection, job_id: i64) -> rusqlite::Result<Option<String>> {
    let row: Option<(Option<String>, Option<i64>)> = conn
        .query_row(
            "SELECT value, expires_at FROM _honker_results WHERE job_id = ?1",
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

pub fn result_sweep(conn: &Connection) -> rusqlite::Result<i64> {
    let deleted = conn.execute(
        "DELETE FROM _honker_results
         WHERE expires_at IS NOT NULL AND expires_at <= unixepoch()",
        [],
    )?;
    Ok(deleted as i64)
}

// ---------------------------------------------------------------------
// Streams
// ---------------------------------------------------------------------

pub fn stream_publish(
    conn: &Connection,
    topic: &str,
    key: Option<&str>,
    payload: &str,
) -> rusqlite::Result<i64> {
    let offset: i64 = conn.query_row(
        "INSERT INTO _honker_stream (topic, key, payload)
         VALUES (?1, ?2, ?3)
         RETURNING offset",
        rusqlite::params![topic, key, payload],
        |r| r.get(0),
    )?;
    let channel = format!("honker:stream:{}", topic);
    conn.execute(
        "INSERT INTO _honker_notifications (channel, payload)
         VALUES (?1, 'new')",
        rusqlite::params![channel],
    )?;
    Ok(offset)
}

/// Returns JSON: `[{"offset":N,"topic":"t","key":"k_or_null","payload":"...","created_at":T}, ...]`.
/// `key` is a raw JSON token — `null` for SQL NULL, otherwise a JSON
/// string literal.
pub fn stream_read_since(
    conn: &Connection,
    topic: &str,
    offset: i64,
    limit: i64,
) -> rusqlite::Result<String> {
    let mut stmt = conn.prepare_cached(
        "SELECT offset, topic, key, payload, created_at
         FROM _honker_stream
         WHERE topic = ?1 AND offset > ?2
         ORDER BY offset ASC
         LIMIT ?3",
    )?;
    let rows = stmt.query_map(rusqlite::params![topic, offset, limit], |r| {
        Ok((
            r.get::<_, i64>(0)?,
            r.get::<_, String>(1)?,
            r.get::<_, Option<String>>(2)?,
            r.get::<_, String>(3)?,
            r.get::<_, i64>(4)?,
        ))
    })?;
    let mut out = String::from("[");
    let mut first = true;
    for row in rows {
        let (off, top, key, payload, created_at) = row?;
        if !first {
            out.push(',');
        }
        first = false;
        let key_tok = match key {
            Some(s) => json_str(&s),
            None => "null".to_string(),
        };
        out.push_str(&format!(
            "{{\"offset\":{},\"topic\":{},\"key\":{},\"payload\":{},\"created_at\":{}}}",
            off,
            json_str(&top),
            key_tok,
            json_str(&payload),
            created_at,
        ));
    }
    out.push(']');
    Ok(out)
}

pub fn stream_save_offset(
    conn: &Connection,
    consumer: &str,
    topic: &str,
    offset: i64,
) -> rusqlite::Result<i64> {
    // Monotonic upsert: WHERE excluded.offset > existing. The CHANGES
    // pragma reports affected rows, which we translate to 1/0.
    let changed = conn.execute(
        "INSERT INTO _honker_stream_consumers (name, topic, offset)
         VALUES (?1, ?2, ?3)
         ON CONFLICT(name, topic) DO UPDATE SET offset = excluded.offset
           WHERE excluded.offset > _honker_stream_consumers.offset",
        rusqlite::params![consumer, topic, offset],
    )?;
    Ok(if changed > 0 { 1 } else { 0 })
}

pub fn stream_get_offset(conn: &Connection, consumer: &str, topic: &str) -> rusqlite::Result<i64> {
    Ok(conn
        .query_row(
            "SELECT offset FROM _honker_stream_consumers
             WHERE name = ?1 AND topic = ?2",
            rusqlite::params![consumer, topic],
            |r| r.get(0),
        )
        .unwrap_or(0))
}

// ---------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------

fn now_unix(conn: &Connection) -> rusqlite::Result<i64> {
    conn.query_row("SELECT unixepoch()", [], |r| r.get(0))
}

/// Escape a string for inclusion as a JSON string literal. Used by
/// `claim_batch` to build its JSON array return value without
/// pulling in serde_json just for one site.
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
