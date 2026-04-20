//! honker SQLite loadable extension.
//!
//! Thin wrapper around `honker-core`. Registers:
//!
//!   * `notify()` SQL scalar function + `_litenotify_notifications`
//!     table — via `honker_core::attach_notify`.
//!   * Every `honker_*` queue / lock / rate-limit / scheduler / result
//!     function — via `honker_core::attach_honker_functions`.
//!
//!     .load ./libhonker_ext
//!     SELECT honker_bootstrap();
//!     INSERT INTO _honker_live (queue, payload)
//!     VALUES ('emails', '{"to": "alice"}');
//!     SELECT honker_claim_batch('emails', 'worker-1', 32, 300);
//!     SELECT honker_ack_batch('[1,2,3]', 'worker-1');
//!     SELECT notify('orders', '{"id": 42}');
//!
//! Actual SQL implementations live in `honker_core::honker_ops`
//! so the Python (PyO3) and Node (napi-rs) bindings can register the
//! same functions on their own connections without loading this
//! `.dylib`. One source of truth for the SQL.

use rusqlite::Connection;
use rusqlite::ffi;
use std::os::raw::{c_char, c_int};

/// SQLite entry point. Name must match `sqlite3_<extname>_init`; SQLite
/// derives `<extname>` from the filename — stripping the `lib` prefix
/// and any non-alphabetic characters:
/// `libhonker_ext.dylib` -> `honker_ext` -> `honkerext`
/// -> `sqlite3_honkerext_init`.
///
/// # Safety
/// Called by SQLite. All pointers are SQLite-owned.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn sqlite3_honkerext_init(
    db: *mut ffi::sqlite3,
    pz_err_msg: *mut *mut c_char,
    p_api: *mut ffi::sqlite3_api_routines,
) -> c_int {
    unsafe {
        Connection::extension_init2(db, pz_err_msg, p_api, |conn| {
            // notify() + _litenotify_notifications.
            honker_core::attach_notify(&conn).map_err(|e| {
                rusqlite::Error::UserFunctionError(Box::new(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    e.to_string(),
                )))
            })?;
            // Queue + lifecycle: honker_bootstrap, honker_enqueue, honker_claim_batch,
            // honker_ack, honker_ack_batch, honker_retry, honker_fail, honker_heartbeat,
            // honker_sweep_expired. Locks: honker_lock_acquire, honker_lock_release.
            // Rate limiting: honker_rate_limit_try, honker_rate_limit_sweep.
            // Cron + scheduler: honker_cron_next_after, honker_scheduler_register,
            // honker_scheduler_unregister, honker_scheduler_tick,
            // honker_scheduler_soonest. Results: honker_result_save,
            // honker_result_get, honker_result_sweep. Streams: honker_stream_publish,
            // honker_stream_read_since, honker_stream_save_offset,
            // honker_stream_get_offset.
            honker_core::attach_honker_functions(&conn)?;
            // Persistent load: extension stays registered across
            // connection close in the same process.
            Ok(true)
        })
    }
}
