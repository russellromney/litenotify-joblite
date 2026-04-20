//! litenotify/joblite SQLite loadable extension.
//!
//! Thin wrapper around `litenotify-core`. Registers:
//!
//!   * `notify()` SQL scalar function + `_litenotify_notifications`
//!     table — via `litenotify_core::attach_notify`.
//!   * Every `jl_*` queue / lock / rate-limit / scheduler / result
//!     function — via `litenotify_core::attach_joblite_functions`.
//!
//!     .load ./liblitenotify_ext
//!     SELECT jl_bootstrap();
//!     INSERT INTO _joblite_live (queue, payload)
//!     VALUES ('emails', '{"to": "alice"}');
//!     SELECT jl_claim_batch('emails', 'worker-1', 32, 300);
//!     SELECT jl_ack_batch('[1,2,3]', 'worker-1');
//!     SELECT notify('orders', '{"id": 42}');
//!
//! Actual SQL implementations live in `litenotify_core::joblite_ops`
//! so the Python (PyO3) and Node (napi-rs) bindings can register the
//! same functions on their own connections without loading this
//! `.dylib`. One source of truth for the SQL.

use rusqlite::Connection;
use rusqlite::ffi;
use std::os::raw::{c_char, c_int};

/// SQLite entry point. Name must match `sqlite3_<extname>_init`; SQLite
/// derives `<extname>` from the filename — stripping the `lib` prefix
/// and any non-alphabetic characters:
/// `liblitenotify_ext.dylib` -> `litenotify_ext` -> `litenotifyext`
/// -> `sqlite3_litenotifyext_init`.
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
            // notify() + _litenotify_notifications.
            litenotify_core::attach_notify(&conn).map_err(|e| {
                rusqlite::Error::UserFunctionError(Box::new(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    e.to_string(),
                )))
            })?;
            // jl_bootstrap, jl_claim_batch, jl_ack_batch, jl_sweep_expired,
            // jl_lock_acquire, jl_lock_release, jl_rate_limit_try,
            // jl_rate_limit_sweep, jl_cron_next_after,
            // jl_scheduler_register, jl_scheduler_unregister,
            // jl_scheduler_tick, jl_scheduler_soonest, jl_result_save,
            // jl_result_get, jl_result_sweep, jl_enqueue, jl_ack,
            // jl_retry, jl_fail, jl_heartbeat.
            litenotify_core::attach_joblite_functions(&conn)?;
            // Persistent load: extension stays registered across
            // connection close in the same process.
            Ok(true)
        })
    }
}
