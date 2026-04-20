//! PyO3 Python binding for litenotify.
//!
//! This crate is thin: it owns Python-flavored types (Database,
//! Transaction, WalEvents pyclasses), marshals Python dict/list/bytes
//! to rusqlite params, and materializes rows into Python dicts. All
//! the SQLite plumbing — connection opening, PRAGMAs, the notify()
//! SQL function + notifications table, the writer/readers pools, the
//! WAL-file watcher thread — lives in [`litenotify_core`] and is
//! shared with the other bindings (cdylib extension, napi-rs Node).

use honker_core::{Readers, SharedWalWatcher, Writer, open_conn};
use parking_lot::Mutex;
use pyo3::exceptions::{PyRuntimeError, PyTypeError};
use pyo3::prelude::*;
use pyo3::types::{PyAny, PyBool, PyBytes, PyDict, PyList};
use rusqlite::Connection;
use rusqlite::types::{Value, ValueRef};
use std::sync::Arc;

fn core_err<E: std::fmt::Display>(e: E) -> PyErr {
    PyRuntimeError::new_err(e.to_string())
}

// ---------------------------------------------------------------------
// Python param marshaling + row materialization
// ---------------------------------------------------------------------

fn py_to_value(item: &Bound<'_, PyAny>) -> PyResult<Value> {
    if item.is_none() {
        return Ok(Value::Null);
    }
    if let Ok(b) = item.cast::<PyBool>() {
        return Ok(Value::Integer(if b.is_true() { 1 } else { 0 }));
    }
    if let Ok(b) = item.cast::<PyBytes>() {
        return Ok(Value::Blob(b.as_bytes().to_vec()));
    }
    if let Ok(i) = item.extract::<i64>() {
        return Ok(Value::Integer(i));
    }
    if let Ok(f) = item.extract::<f64>() {
        return Ok(Value::Real(f));
    }
    if let Ok(s) = item.extract::<String>() {
        return Ok(Value::Text(s));
    }
    let tname = item
        .get_type()
        .name()
        .map(|s| s.to_string())
        .unwrap_or_else(|_| "<unknown>".to_string());
    Err(PyTypeError::new_err(format!(
        "unsupported SQL parameter type: {}",
        tname
    )))
}

fn build_params(params: Option<&Bound<'_, PyList>>) -> PyResult<Vec<Value>> {
    let mut out = Vec::new();
    if let Some(p) = params {
        for item in p.iter() {
            out.push(py_to_value(&item)?);
        }
    }
    Ok(out)
}

fn run_query<'py>(
    py: Python<'py>,
    conn: &Connection,
    sql: &str,
    params: Option<&Bound<'_, PyList>>,
) -> PyResult<Bound<'py, PyList>> {
    let values = build_params(params)?;
    // prepare_cached hits rusqlite's per-connection statement cache.
    // Without it, every execute re-parses the SQL (~4x overhead).
    let mut stmt = conn.prepare_cached(sql).map_err(core_err)?;
    let columns: Vec<String> = stmt.column_names().iter().map(|s| s.to_string()).collect();
    let mut rows = stmt
        .query(rusqlite::params_from_iter(values))
        .map_err(core_err)?;
    let out = PyList::empty(py);
    while let Some(row) = rows.next().map_err(core_err)? {
        let dict = PyDict::new(py);
        for (i, name) in columns.iter().enumerate() {
            let v = row.get_ref(i).map_err(core_err)?;
            match v {
                ValueRef::Null => dict.set_item(name, py.None())?,
                ValueRef::Integer(iv) => dict.set_item(name, iv)?,
                ValueRef::Real(fv) => dict.set_item(name, fv)?,
                ValueRef::Text(t) => {
                    let s = std::str::from_utf8(t).unwrap_or("");
                    dict.set_item(name, s)?
                }
                ValueRef::Blob(b) => dict.set_item(name, b)?,
            }
        }
        out.append(dict)?;
    }
    Ok(out)
}

fn run_execute(
    conn: &Connection,
    sql: &str,
    params: Option<&Bound<'_, PyList>>,
) -> PyResult<usize> {
    let values = build_params(params)?;
    let mut stmt = conn.prepare_cached(sql).map_err(core_err)?;
    stmt.execute(rusqlite::params_from_iter(values))
        .map_err(core_err)
}

/// Run a fixed SQL statement (no params) via the cached statement pool.
/// Used for BEGIN IMMEDIATE / COMMIT / ROLLBACK so we don't re-parse
/// every transaction.
fn run_cached_noparams(conn: &Connection, sql: &str) -> rusqlite::Result<()> {
    let mut stmt = conn.prepare_cached(sql)?;
    stmt.execute([])?;
    Ok(())
}

fn serialize_payload(py: Python<'_>, payload: &Bound<'_, PyAny>) -> PyResult<String> {
    // Unconditional json.dumps — matches Queue.enqueue and Stream.publish.
    // Previously strings stored raw and everything else json-encoded,
    // which round-tripped inconsistently: tx.notify("ch", "42") → int 42,
    // tx.notify("ch", "null") → None, tx.notify("ch", '"x"') → "x". Pre-1.0
    // normalization so all three primitives share one wire format.
    let json = py.import("json")?;
    let dumps = json.getattr("dumps")?;
    let result = dumps.call1((payload,))?;
    result.extract::<String>()
}

// ---------------------------------------------------------------------
// Database
// ---------------------------------------------------------------------

#[pyclass]
struct Database {
    writer: Arc<Writer>,
    readers: Arc<Readers>,
    wal_path: std::path::PathBuf,
    /// Lazy-initialized shared WAL watcher. One stat-poll thread per
    /// Database regardless of how many listeners subscribe. Previously
    /// every `wal_events()` spawned its own thread — 100 listeners
    /// meant 100 stat threads hammering the same file.
    shared_watcher: Mutex<Option<Arc<SharedWalWatcher>>>,
}

#[pymethods]
impl Database {
    #[new]
    #[pyo3(signature = (path, max_readers=8))]
    fn new(path: String, max_readers: usize) -> PyResult<Self> {
        // Writer conn registers the notify() SQL function + ensures
        // _litenotify_notifications exists. Readers just SELECT.
        let writer_conn = open_conn(&path, true).map_err(core_err)?;
        // Also register every `jl_*` SQL function on the writer
        // connection so Python can call `SELECT jl_foo(...)` inside
        // its own transactions — same implementations the loadable
        // extension registers, no `.dylib` load needed at runtime.
        honker_core::attach_honker_functions(&writer_conn)
            .map_err(core_err)?;
        let wal_path: std::path::PathBuf = format!("{}-wal", path).into();
        Ok(Self {
            writer: Arc::new(Writer::new(writer_conn)),
            readers: Arc::new(Readers::new(path, max_readers)),
            wal_path,
            shared_watcher: Mutex::new(None),
        })
    }

    fn transaction(&self) -> PyResult<Transaction> {
        Ok(Transaction {
            writer: self.writer.clone(),
            inner: Arc::new(Mutex::new(TxState::default())),
        })
    }

    /// Watcher on this database's `.db-wal` file. Returns an async
    /// iterator that yields `None` every time the WAL changes — i.e.
    /// every time any process committed a transaction to this file.
    ///
    /// N calls share a single background poll thread via the core
    /// [`SharedWalWatcher`]. Each subscriber gets its own bounded
    /// channel so a slow consumer can't block other listeners.
    fn wal_events(&self) -> PyResult<WalEvents> {
        let shared = {
            let mut guard = self.shared_watcher.lock();
            if let Some(existing) = guard.as_ref() {
                existing.clone()
            } else {
                let w = Arc::new(SharedWalWatcher::new(self.wal_path.clone()));
                *guard = Some(w.clone());
                w
            }
        };
        let (sub_id, rx) = shared.subscribe();
        Ok(WalEvents {
            wal_path: self.wal_path.clone(),
            shared,
            sub_id,
            inner: Arc::new(Mutex::new(WalWatchState {
                rx: Some(rx),
                queue: None,
            })),
        })
    }

    #[pyo3(signature = (sql, params=None))]
    fn query<'py>(
        &self,
        py: Python<'py>,
        sql: String,
        params: Option<Bound<'py, PyList>>,
    ) -> PyResult<Bound<'py, PyList>> {
        let conn = self.readers.acquire().map_err(core_err)?;
        let result = run_query(py, &conn, &sql, params.as_ref());
        self.readers.release(conn);
        result
    }
}

// ---------------------------------------------------------------------
// Transaction
// ---------------------------------------------------------------------

#[derive(Default)]
struct TxState {
    conn: Option<Connection>,
    started: bool,
    released: bool,
}

#[pyclass]
struct Transaction {
    writer: Arc<Writer>,
    inner: Arc<Mutex<TxState>>,
}

impl Drop for Transaction {
    fn drop(&mut self) {
        let mut state = self.inner.lock();
        if !state.released {
            if let Some(conn) = state.conn.take() {
                if state.started {
                    let _ = run_cached_noparams(&conn, "ROLLBACK");
                }
                self.writer.release(conn);
            }
            state.released = true;
        }
    }
}

#[pymethods]
impl Transaction {
    fn __enter__<'a>(slf: PyRef<'a, Self>, py: Python<'a>) -> PyResult<PyRef<'a, Self>> {
        let writer = slf.writer.clone();
        // Fast path: uncontended slot acquire doesn't release the GIL.
        // Slow path: GIL released while we wait on the writer condvar.
        let conn = match writer.try_acquire() {
            Some(c) => c,
            None => py.detach(|| writer.acquire()),
        };
        match run_cached_noparams(&conn, "BEGIN IMMEDIATE") {
            Ok(()) => {
                {
                    let mut state = slf.inner.lock();
                    state.conn = Some(conn);
                    state.started = true;
                    state.released = false;
                }
                Ok(slf)
            }
            Err(e) => {
                slf.writer.release(conn);
                Err(core_err(e))
            }
        }
    }

    fn __exit__(
        &self,
        _py: Python<'_>,
        exc_type: Option<&Bound<'_, PyAny>>,
        _exc_value: Option<&Bound<'_, PyAny>>,
        _tb: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<bool> {
        let mut state = self.inner.lock();
        if state.released || state.conn.is_none() {
            return Ok(false);
        }
        let conn = state.conn.take().unwrap();
        let raised = exc_type.map_or(false, |e| !e.is_none());
        let was_started = state.started;
        state.started = false;
        let err = if was_started {
            if raised {
                run_cached_noparams(&conn, "ROLLBACK").err()
            } else {
                match run_cached_noparams(&conn, "COMMIT") {
                    Ok(()) => None,
                    Err(e) => {
                        let _ = run_cached_noparams(&conn, "ROLLBACK");
                        Some(e)
                    }
                }
            }
        } else {
            None
        };
        self.writer.release(conn);
        state.released = true;
        if let Some(e) = err {
            return Err(core_err(e));
        }
        Ok(false)
    }

    #[pyo3(signature = (sql, params=None))]
    fn execute(
        &self,
        _py: Python<'_>,
        sql: String,
        params: Option<Bound<'_, PyList>>,
    ) -> PyResult<()> {
        let state = self.inner.lock();
        let conn = state
            .conn
            .as_ref()
            .ok_or_else(|| PyRuntimeError::new_err("Transaction not started"))?;
        run_execute(conn, &sql, params.as_ref())?;
        Ok(())
    }

    #[pyo3(signature = (sql, params=None))]
    fn query<'py>(
        &self,
        py: Python<'py>,
        sql: String,
        params: Option<Bound<'py, PyList>>,
    ) -> PyResult<Bound<'py, PyList>> {
        let state = self.inner.lock();
        let conn = state
            .conn
            .as_ref()
            .ok_or_else(|| PyRuntimeError::new_err("Transaction not started"))?;
        run_query(py, conn, &sql, params.as_ref())
    }

    fn notify(
        &self,
        py: Python<'_>,
        channel: String,
        payload: Bound<'_, PyAny>,
    ) -> PyResult<i64> {
        let state = self.inner.lock();
        let conn = state
            .conn
            .as_ref()
            .ok_or_else(|| PyRuntimeError::new_err("Transaction not started"))?;
        let payload_str = serialize_payload(py, &payload)?;
        let id: i64 = conn
            .query_row(
                "SELECT notify(?1, ?2)",
                rusqlite::params![channel, payload_str],
                |r| r.get(0),
            )
            .map_err(core_err)?;
        Ok(id)
    }

    /// Install the canonical joblite queue schema. Idempotent; the
    /// DDL lives in `litenotify-core` so the Python binding and the
    /// SQLite extension can't drift on column counts.
    fn bootstrap_joblite_schema(&self) -> PyResult<()> {
        let state = self.inner.lock();
        let conn = state
            .conn
            .as_ref()
            .ok_or_else(|| PyRuntimeError::new_err("Transaction not started"))?;
        honker_core::bootstrap_joblite_schema(conn).map_err(core_err)
    }
}

// ---------------------------------------------------------------------
// WalEvents (async iterator over a SharedWalWatcher subscription)
// ---------------------------------------------------------------------

struct WalWatchState {
    /// Per-subscriber receiver from the shared watcher. Moved into
    /// the bridge thread on first `__aiter__`; on subsequent
    /// `__aiter__` calls the queue has been created and we return
    /// it directly.
    rx: Option<std::sync::mpsc::Receiver<()>>,
    /// Python asyncio Queue, populated lazily on first __aiter__.
    queue: Option<Py<PyAny>>,
}

#[pyclass]
struct WalEvents {
    wal_path: std::path::PathBuf,
    /// Keep the shared watcher alive as long as this subscription
    /// exists. `Drop` calls `shared.unsubscribe(sub_id)` so the bridge
    /// thread's `rx.recv()` sees a disconnect and exits.
    shared: Arc<SharedWalWatcher>,
    sub_id: u64,
    inner: Arc<Mutex<WalWatchState>>,
}

impl Drop for WalEvents {
    fn drop(&mut self) {
        // Remove our sender from the shared watcher so the bridge
        // thread's rx.recv() returns Err and the thread exits. Without
        // this, the bridge thread + its Python-object references would
        // leak until the last Arc<SharedWalWatcher> is dropped.
        self.shared.unsubscribe(self.sub_id);
    }
}

impl WalEvents {
    fn ensure_started(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let mut state = self.inner.lock();
        if let Some(q) = &state.queue {
            return Ok(q.clone_ref(py));
        }
        let asyncio = py.import("asyncio")?;
        let queue = asyncio.call_method0("Queue")?;
        let loop_obj = asyncio.call_method0("get_running_loop")?;

        let queue_py: Py<PyAny> = queue.clone().unbind();
        let queue_py_for_thread = queue_py.clone_ref(py);
        let loop_py: Py<PyAny> = loop_obj.unbind();

        let rx = state.rx.take().expect("wal rx already taken");

        std::thread::Builder::new()
            .name("litenotify-wal-bridge".into())
            .spawn(move || {
                // Blocks on the subscriber channel. Exits when the
                // shared watcher's sender list prunes this subscriber
                // (happens when the Arc<SharedWalWatcher> inside
                // WalEvents is dropped, severing our end of the
                // channel — recv() then returns Err).
                while rx.recv().is_ok() {
                    Python::attach(|py| {
                        let put = match queue_py_for_thread.getattr(py, "put_nowait") {
                            Ok(v) => v,
                            Err(_) => return,
                        };
                        let _ = loop_py.call_method1(
                            py,
                            "call_soon_threadsafe",
                            (put, py.None()),
                        );
                    });
                }
            })
            .map_err(core_err)?;

        state.queue = Some(queue_py.clone_ref(py));
        Ok(queue_py)
    }
}

#[pymethods]
impl WalEvents {
    fn __aiter__<'a>(slf: PyRef<'a, Self>, py: Python<'a>) -> PyResult<PyRef<'a, Self>> {
        slf.ensure_started(py)?;
        Ok(slf)
    }

    fn __anext__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let queue = self.ensure_started(py)?;
        queue.bind(py).call_method0("get")
    }

    /// Path this watcher is monitoring. Useful in tests / debugging.
    #[getter]
    fn path(&self) -> String {
        self.wal_path.to_string_lossy().into_owned()
    }
}

// ---------------------------------------------------------------------
// Module entry
// ---------------------------------------------------------------------

#[pyfunction]
#[pyo3(signature = (path, max_readers=8))]
fn open(path: String, max_readers: usize) -> PyResult<Database> {
    Database::new(path, max_readers)
}

/// Compute the next unix timestamp strictly after `from_unix` that
/// matches `expr`, at minute precision, in the system local time zone.
/// Pure function — no database needed. Raises `ValueError` on a
/// malformed cron expression. Used by `joblite.CronSchedule` so the
/// parser + next-boundary math lives once in Rust.
#[pyfunction]
fn cron_next_after(expr: String, from_unix: i64) -> PyResult<i64> {
    honker_core::cron::next_after_unix(&expr, from_unix)
        .map_err(|e| pyo3::exceptions::PyValueError::new_err(e))
}

#[pymodule]
fn litenotify(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(open, m)?)?;
    m.add_function(wrap_pyfunction!(cron_next_after, m)?)?;
    m.add_class::<Database>()?;
    m.add_class::<Transaction>()?;
    m.add_class::<WalEvents>()?;
    Ok(())
}
