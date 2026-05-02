//! Node binding for honker.
//!
//! Mirrors the Python API, with types tuned for JavaScript:
//!
//!     const lit = require('@honker/node');
//!     const db = lit.open('app.db');
//!     const tx = db.transaction();
//!     tx.execute('INSERT INTO orders (id) VALUES (?)', [42]);
//!     tx.notify('orders', JSON.stringify({id: 42}));
//!     tx.commit();
//!
//!     const ev = db.updateEvents();
//!     while (running) {
//!       await ev.next();
//!       const rows = db.query(
//!         'SELECT * FROM _honker_notifications WHERE id > ?',
//!         [lastSeen]);
//!       // ...
//!     }
//!
//! Rows and parameter values are serialized via JSON at the boundary —
//! keeps the binding small and avoids nominal-value-type matching between
//! JS and SQLite. Users can pass numbers, strings, booleans, null,
//! arrays, and objects; objects/arrays get JSON-stringified.
//!
//! Writer pool, reader pool, connection open, notify() attach, and the
//! update watcher all come from the shared `honker-core` rlib so the
//! PyO3, SQLite-extension, and Node bindings can't drift apart.

use honker_core::{Readers, SharedUpdateWatcher, WatcherConfig, Writer, open_conn};
use napi::Result;
use napi_derive::napi;
use parking_lot::Mutex;
use rusqlite::Connection;
use rusqlite::types::{Value as SqlValue, ValueRef};
use serde_json::{Map, Value as JsonValue};
use std::path::PathBuf;
use std::sync::Arc;

fn napi_err(e: impl std::fmt::Display) -> napi::Error {
    napi::Error::new(napi::Status::GenericFailure, e.to_string())
}

fn parse_watcher_backend(backend: Option<String>) -> Result<WatcherConfig> {
    honker_core::WatcherBackend::parse(backend.as_deref())
        .map(|backend| WatcherConfig { backend })
        .map_err(|other| {
            napi_err(format!(
                "unknown watcherBackend {other:?}; valid: null, 'polling', 'kernel', 'shm'"
            ))
        })
}

// ---------- JSON <-> SQL param conversion ----------

fn json_to_sql(v: &JsonValue) -> SqlValue {
    match v {
        JsonValue::Null => SqlValue::Null,
        JsonValue::Bool(b) => SqlValue::Integer(if *b { 1 } else { 0 }),
        JsonValue::Number(n) => {
            if let Some(i) = n.as_i64() {
                SqlValue::Integer(i)
            } else if let Some(f) = n.as_f64() {
                SqlValue::Real(f)
            } else {
                SqlValue::Text(n.to_string())
            }
        }
        JsonValue::String(s) => SqlValue::Text(s.clone()),
        // Objects/arrays are SQL-serialized as JSON text, consistent with
        // how honker.Queue.enqueue(payload) treats dicts/lists.
        JsonValue::Array(_) | JsonValue::Object(_) => SqlValue::Text(v.to_string()),
    }
}

fn row_to_json(columns: &[String], row: &rusqlite::Row) -> rusqlite::Result<JsonValue> {
    let mut obj = Map::new();
    for (i, name) in columns.iter().enumerate() {
        let vref = row.get_ref(i)?;
        let v = match vref {
            ValueRef::Null => JsonValue::Null,
            ValueRef::Integer(n) => JsonValue::from(n),
            ValueRef::Real(f) => JsonValue::from(f),
            ValueRef::Text(t) => JsonValue::from(std::str::from_utf8(t).unwrap_or("")),
            ValueRef::Blob(b) => {
                let hex: String = b.iter().map(|byte| format!("{:02x}", byte)).collect();
                JsonValue::from(hex)
            }
        };
        obj.insert(name.clone(), v);
    }
    Ok(JsonValue::Object(obj))
}

fn run_query(conn: &Connection, sql: &str, params: &[SqlValue]) -> Result<JsonValue> {
    let mut stmt = conn.prepare_cached(sql).map_err(napi_err)?;
    let columns: Vec<String> = stmt.column_names().iter().map(|s| s.to_string()).collect();
    let mut rows = stmt
        .query(rusqlite::params_from_iter(params.iter()))
        .map_err(napi_err)?;
    let mut arr: Vec<JsonValue> = Vec::new();
    while let Some(row) = rows.next().map_err(napi_err)? {
        arr.push(row_to_json(&columns, row).map_err(napi_err)?);
    }
    Ok(JsonValue::Array(arr))
}

fn run_execute(conn: &Connection, sql: &str, params: &[SqlValue]) -> Result<u32> {
    let mut stmt = conn.prepare_cached(sql).map_err(napi_err)?;
    let n = stmt
        .execute(rusqlite::params_from_iter(params.iter()))
        .map_err(napi_err)?;
    Ok(n as u32)
}

fn sql_params_from_json(arr: Option<Vec<JsonValue>>) -> Vec<SqlValue> {
    arr.unwrap_or_default().iter().map(json_to_sql).collect()
}

// ---------- napi-rs classes ----------

#[napi]
pub struct Database {
    writer: Arc<Writer>,
    readers: Arc<Readers>,
    db_path: PathBuf,
    /// Lazy-initialized shared update watcher — one PRAGMA-poll thread per
    /// Database regardless of how many `updateEvents()` subscribers. See
    /// honker-core::SharedUpdateWatcher.
    shared_watcher: Arc<Mutex<Option<Arc<SharedUpdateWatcher>>>>,
    /// Watcher backend chosen at `open()` time. Stored so the lazy
    /// `updateEvents()` initialization picks up the right backend.
    watcher_config: WatcherConfig,
}

#[napi]
impl Database {
    /// Begin a write transaction. Must `.commit()` or `.rollback()`.
    /// Dropping without either rolls back.
    #[napi]
    pub fn transaction(&self) -> Result<Transaction> {
        // Fast path: uncontended slot, no condvar wait. Matches the
        // PyO3 binding's optimization (CHANGELOG: ~5 μs/tx saved).
        let conn = self
            .writer
            .try_acquire()
            .or_else(|| self.writer.acquire())
            .ok_or_else(|| napi_err("Database is closed"))?;
        match conn.execute_batch("BEGIN IMMEDIATE") {
            Ok(()) => Ok(Transaction {
                inner: Arc::new(Mutex::new(TxState {
                    conn: Some(conn),
                    writer: self.writer.clone(),
                    started: true,
                    finished: false,
                })),
            }),
            Err(e) => {
                self.writer.release(conn);
                Err(napi_err(e))
            }
        }
    }

    /// SELECT returns `Array<Object>` (each row is a plain object).
    #[napi(ts_return_type = "Array<Record<string, any>>")]
    pub fn query(&self, sql: String, params: Option<Vec<JsonValue>>) -> Result<JsonValue> {
        let params = sql_params_from_json(params);
        let conn = self.readers.acquire().map_err(napi_err)?;
        let result = run_query(&conn, &sql, &params);
        self.readers.release(conn);
        result
    }

    /// Update watcher for this database. `await ev.next()` resolves
    /// on every commit to the database (any process, any writer).
    ///
    /// N subscribers share a single background poll thread via the
    /// core SharedUpdateWatcher.
    #[napi]
    pub fn update_events(&self) -> Result<UpdateEvents> {
        let shared = {
            let mut guard = self.shared_watcher.lock();
            if let Some(existing) = guard.as_ref() {
                existing.clone()
            } else {
                let w = Arc::new(SharedUpdateWatcher::new_with_config(
                    self.db_path.clone(),
                    self.watcher_config.clone(),
                ));
                *guard = Some(w.clone());
                w
            }
        };
        let (sub_id, rx) = shared.subscribe();
        Ok(UpdateEvents {
            shared,
            sub_id,
            rx: Arc::new(Mutex::new(rx)),
        })
    }

    /// Delete notifications older than a duration, or beyond a count.
    /// Returns number of rows removed.
    #[napi]
    pub fn prune_notifications(
        &self,
        older_than_s: Option<i64>,
        max_keep: Option<i64>,
    ) -> Result<u32> {
        let mut conditions: Vec<&str> = Vec::new();
        let mut params: Vec<SqlValue> = Vec::new();
        if let Some(secs) = older_than_s {
            conditions.push("created_at < unixepoch() - ?");
            params.push(SqlValue::Integer(secs));
        }
        if let Some(k) = max_keep {
            conditions.push("id <= (SELECT MAX(id) - ? FROM _honker_notifications)");
            params.push(SqlValue::Integer(k));
        }
        if conditions.is_empty() {
            return Ok(0);
        }
        let sql = format!(
            "DELETE FROM _honker_notifications WHERE {}",
            conditions.join(" OR ")
        );
        let conn = self
            .writer
            .acquire()
            .ok_or_else(|| napi_err("Database is closed"))?;
        let result = (|| -> rusqlite::Result<u32> {
            conn.execute_batch("BEGIN IMMEDIATE")?;
            let mut stmt = conn.prepare_cached(&sql)?;
            let n = stmt.execute(rusqlite::params_from_iter(params.iter()))?;
            conn.execute_batch("COMMIT")?;
            Ok(n as u32)
        })();
        let final_result = match result {
            Ok(n) => Ok(n),
            Err(e) => {
                let _ = conn.execute_batch("ROLLBACK");
                Err(napi_err(e))
            }
        };
        self.writer.release(conn);
        final_result
    }

    /// Release the underlying SQLite handles and the watcher poll
    /// thread so the OS can unlink the database file. After `close()`,
    /// any further `transaction()` / `query()` / `updateEvents()` /
    /// `pruneNotifications()` calls return an error.
    ///
    /// Idempotent. Safe to call from `finally`/`afterEach` blocks even
    /// if the test never used the database.
    ///
    /// Why this matters on Windows: SQLite holds the `.db` file open
    /// for the lifetime of every Connection. On Linux/macOS, `unlink`
    /// on a still-open file works (the inode survives until the last
    /// fd closes). On Windows, it does not — `EBUSY` until every
    /// handle is dropped. Without explicit close, the writer/reader
    /// connections live until the JavaScript GC drops every
    /// Transaction/UpdateEvents object that ever cloned the
    /// `Arc<Writer>` / `Arc<Readers>`, which a test-cleanup block
    /// can't force.
    #[napi]
    pub fn close(&self) {
        // Drop the watcher's read-only connection (joins the poll
        // thread synchronously). Doing this first guarantees no
        // background reader is racing the writer/reader closes below.
        if let Some(shared) = self.shared_watcher.lock().take() {
            let _ = shared.close();
        }
        // These reach inside the Arc<Writer> / Arc<Readers> and drop
        // the underlying Connection regardless of how many Arc clones
        // exist (Transactions still alive in JS, etc.).
        self.writer.close();
        self.readers.close();
    }
}

struct TxState {
    conn: Option<Connection>,
    writer: Arc<Writer>,
    started: bool,
    finished: bool,
}

impl Drop for TxState {
    fn drop(&mut self) {
        if let Some(conn) = self.conn.take() {
            if self.started && !self.finished {
                let _ = conn.execute_batch("ROLLBACK");
            }
            self.writer.release(conn);
        }
    }
}

#[napi]
pub struct Transaction {
    inner: Arc<Mutex<TxState>>,
}

#[napi]
impl Transaction {
    #[napi]
    pub fn execute(&self, sql: String, params: Option<Vec<JsonValue>>) -> Result<u32> {
        let params = sql_params_from_json(params);
        let state = self.inner.lock();
        let conn = state
            .conn
            .as_ref()
            .ok_or_else(|| napi_err("transaction already finished"))?;
        run_execute(conn, &sql, &params)
    }

    #[napi(ts_return_type = "Array<Record<string, any>>")]
    pub fn query(&self, sql: String, params: Option<Vec<JsonValue>>) -> Result<JsonValue> {
        let params = sql_params_from_json(params);
        let state = self.inner.lock();
        let conn = state
            .conn
            .as_ref()
            .ok_or_else(|| napi_err("transaction already finished"))?;
        run_query(conn, &sql, &params)
    }

    /// Publish a cross-process notification. `payload` is any
    /// JSON-serializable value; it's `JSON.stringify`'d on the way in
    /// and `JSON.parse`'d by consumers on the way out. Matches the
    /// Python binding's unconditional `json.dumps` contract so payloads
    /// round-trip identically across bindings.
    #[napi]
    pub fn notify(&self, channel: String, payload: JsonValue) -> Result<i64> {
        let state = self.inner.lock();
        let conn = state
            .conn
            .as_ref()
            .ok_or_else(|| napi_err("transaction already finished"))?;
        let payload_str = payload.to_string();
        let id: i64 = conn
            .query_row(
                "SELECT notify(?1, ?2)",
                rusqlite::params![channel, payload_str],
                |r| r.get(0),
            )
            .map_err(napi_err)?;
        Ok(id)
    }

    #[napi]
    pub fn commit(&self) -> Result<()> {
        let mut state = self.inner.lock();
        if state.finished {
            return Ok(());
        }
        let conn = state
            .conn
            .take()
            .ok_or_else(|| napi_err("transaction already finished"))?;
        let result = match conn.execute_batch("COMMIT") {
            Ok(()) => Ok(()),
            Err(e) => {
                let _ = conn.execute_batch("ROLLBACK");
                Err(napi_err(e))
            }
        };
        state.writer.release(conn);
        state.finished = true;
        state.started = false;
        result
    }

    #[napi]
    pub fn rollback(&self) -> Result<()> {
        let mut state = self.inner.lock();
        if state.finished {
            return Ok(());
        }
        let conn = state
            .conn
            .take()
            .ok_or_else(|| napi_err("transaction already finished"))?;
        let _ = conn.execute_batch("ROLLBACK");
        state.writer.release(conn);
        state.finished = true;
        state.started = false;
        Ok(())
    }
}

#[napi]
pub struct UpdateEvents {
    /// Keep the shared watcher alive as long as this subscription
    /// exists. `Drop` on UpdateEvents unsubscribes our channel — the
    /// `rx.recv()` inside `next()` then returns Err and the await
    /// resolves, letting JS see a clean end.
    shared: Arc<SharedUpdateWatcher>,
    sub_id: u64,
    rx: Arc<Mutex<std::sync::mpsc::Receiver<()>>>,
}

impl Drop for UpdateEvents {
    fn drop(&mut self) {
        self.shared.unsubscribe(self.sub_id);
    }
}

#[napi]
impl UpdateEvents {
    /// Await the next database update. Resolves on every DB commit.
    #[napi]
    pub async fn next(&self) -> Result<()> {
        let rx = self.rx.clone();
        tokio::task::spawn_blocking(move || {
            let r = rx.lock();
            r.recv().map_err(napi_err)
        })
        .await
        .map_err(napi_err)??;
        Ok(())
    }

    /// Stop this subscription eagerly. Idempotent.
    #[napi]
    pub fn close(&self) {
        self.shared.unsubscribe(self.sub_id);
    }
}

// ---------- module entry ----------

/// Open a Honker database at `path`.
///
/// `watcherBackend` selects the update-detection strategy:
/// - omitted / `"polling"` — default 1 ms PRAGMA polling
/// - `"kernel"` — kernel filesystem notifications (experimental)
/// - `"shm"` — mmap `-shm` fast path (experimental)
///
/// Experimental backends require the corresponding Cargo feature; if a
/// build doesn't include them, requesting one logs a warning and falls
/// back to polling.
#[napi]
pub fn open(
    path: String,
    max_readers: Option<u32>,
    watcher_backend: Option<String>,
) -> Result<Database> {
    let max_readers = max_readers.unwrap_or(8).max(1) as usize;
    let watcher_config = parse_watcher_backend(watcher_backend)?;
    let writer_conn = open_conn(&path, true).map_err(napi_err)?;
    // Register every honker_* SQL scalar on the writer connection so
    // Node callers get the full queue / scheduler / stream surface
    // via db.transaction().query("SELECT honker_enqueue(...)").
    // Matches the Python binding's open() — parity across languages.
    honker_core::attach_honker_functions(&writer_conn).map_err(napi_err)?;
    honker_core::bootstrap_honker_schema(&writer_conn).map_err(napi_err)?;
    // Probe the chosen backend now (after the writer connection has
    // created -wal / -shm in WAL mode). Failures throw from open() so a
    // backend that can't run never silently produces no wakes.
    let db_path: PathBuf = path.into();
    if let Err(reason) = watcher_config.backend.probe(&db_path) {
        return Err(napi_err(format!(
            "watcherBackend probe failed: {reason}"
        )));
    }
    Ok(Database {
        writer: Arc::new(Writer::new(writer_conn)),
        readers: Arc::new(Readers::new(db_path.to_string_lossy().into_owned(), max_readers)),
        db_path,
        shared_watcher: Arc::new(Mutex::new(None)),
        watcher_config,
    })
}
