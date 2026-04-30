//! honker SQLite loadable extension.
//!
//! Thin wrapper around `honker-core`. Registers:
//!
//!   * `notify()` SQL scalar function + `_honker_notifications`
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

use rusqlite::ffi;
use rusqlite::{Connection, Error, Result};
use std::os::raw::{c_char, c_int};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::ptr;

fn panic_error(payload: Box<dyn std::any::Any + Send>) -> Error {
    let msg = if let Some(s) = payload.downcast_ref::<&str>() {
        *s
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.as_str()
    } else {
        "non-string panic payload"
    };
    Error::UserFunctionError(Box::new(std::io::Error::other(format!(
        "honker extension initialization panicked: {msg}"
    ))))
}

fn extension_init(conn: Connection) -> Result<bool> {
    match catch_unwind(AssertUnwindSafe(|| {
        honker_core::attach_notify(&conn).map_err(|e| {
            Error::UserFunctionError(Box::new(std::io::Error::other(e.to_string())))
        })?;
        honker_core::attach_honker_functions(&conn)?;
        Ok(true)
    })) {
        Ok(result) => result,
        Err(payload) => Err(panic_error(payload)),
    }
}

unsafe fn set_error_msg(
    pz_err_msg: *mut *mut c_char,
    p_api: *mut ffi::sqlite3_api_routines,
    message: &str,
) {
    if pz_err_msg.is_null() || p_api.is_null() {
        return;
    }
    let Some(malloc) = (unsafe { (*p_api).malloc }) else {
        return;
    };
    let len = match message.len().checked_add(1) {
        Some(len) if c_int::try_from(len).is_ok() => len,
        _ => return,
    };
    let ptr = unsafe { malloc(len as c_int) }.cast::<c_char>();
    if ptr.is_null() {
        return;
    }
    unsafe {
        ptr::copy_nonoverlapping(message.as_ptr().cast::<c_char>(), ptr, message.len());
        *ptr.add(message.len()) = 0;
        *pz_err_msg = ptr;
    }
}

unsafe fn extension_init2(
    db: *mut ffi::sqlite3,
    pz_err_msg: *mut *mut c_char,
    p_api: *mut ffi::sqlite3_api_routines,
) -> c_int {
    if p_api.is_null() {
        return ffi::SQLITE_ERROR;
    }
    let result = unsafe { ffi::rusqlite_extension_init2(p_api) }
        .map_err(Error::from)
        .and_then(|()| unsafe { Connection::from_handle(db) })
        .and_then(extension_init);
    match result {
        Ok(true) => ffi::SQLITE_OK_LOAD_PERMANENTLY,
        Ok(false) => ffi::SQLITE_OK,
        Err(err) => {
            unsafe { set_error_msg(pz_err_msg, p_api, &err.to_string()) };
            ffi::SQLITE_ERROR
        }
    }
}

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
    match catch_unwind(AssertUnwindSafe(|| unsafe {
        extension_init2(db, pz_err_msg, p_api)
    })) {
        Ok(code) => code,
        Err(payload) => {
            let err = panic_error(payload);
            unsafe { set_error_msg(pz_err_msg, p_api, &err.to_string()) };
            ffi::SQLITE_ERROR
        }
    }
}
