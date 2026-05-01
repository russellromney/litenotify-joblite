//! PyO3 Python binding for honker.
//!
//! This crate is thin: it owns Python-flavored types (Database,
//! Transaction, UpdateEvents pyclasses), marshals Python dict/list/bytes
//! to rusqlite params, and materializes rows into Python dicts. All
//! the SQLite plumbing — connection opening, PRAGMAs, the notify()
//! SQL function + notifications table, the writer/readers pools, the
//! update watcher thread — lives in [`honker_core`] and is
//! shared with the other bindings (cdylib extension, napi-rs Node).

use honker_core::{Readers, SharedUpdateWatcher, Writer, open_conn};
use parking_lot::Mutex;
use pyo3::exceptions::{PyRuntimeError, PyTypeError};
use pyo3::prelude::*;
use pyo3::types::{PyAny, PyBool, PyBytes, PyDict, PyList};
use rusqlite::Connection;
use rusqlite::types::{Value, ValueRef};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

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
    db_path: std::path::PathBuf,
    /// Lazy-initialized shared update watcher. One PRAGMA-poll thread per
    /// Database regardless of how many listeners subscribe.
    shared_watcher: Mutex<Option<Arc<SharedUpdateWatcher>>>,
}

#[pymethods]
impl Database {
    #[new]
    #[pyo3(signature = (path, max_readers=8))]
    fn new(path: String, max_readers: usize) -> PyResult<Self> {
        // Writer conn registers the notify() SQL function + ensures
        // _honker_notifications exists. Readers just SELECT.
        let writer_conn = open_conn(&path, true).map_err(core_err)?;
        // Also register every `honker_*` SQL function on the writer
        // connection so Python can call `SELECT honker_foo(...)` inside
        // its own transactions — same implementations the loadable
        // extension registers, no `.dylib` load needed at runtime.
        honker_core::attach_honker_functions(&writer_conn).map_err(core_err)?;
        Ok(Self {
            writer: Arc::new(Writer::new(writer_conn)),
            readers: Arc::new(Readers::new(path.clone(), max_readers)),
            db_path: path.into(),
            shared_watcher: Mutex::new(None),
        })
    }

    fn transaction(&self) -> PyResult<Transaction> {
        Ok(Transaction {
            writer: self.writer.clone(),
            inner: Arc::new(Mutex::new(TxState::default())),
        })
    }

    /// Watcher on this database's update stream. Returns an async
    /// iterator that yields `None` every time the database updates — i.e.
    /// every time any process committed a transaction to this file.
    ///
    /// N calls share a single background poll thread via the core
    /// [`SharedUpdateWatcher`]. Each subscriber gets its own bounded
    /// channel so a slow consumer can't block other listeners.
    fn update_events(&self) -> PyResult<UpdateEvents> {
        let shared = {
            let mut guard = self.shared_watcher.lock();
            if let Some(existing) = guard.as_ref() {
                existing.clone()
            } else {
                let w = Arc::new(SharedUpdateWatcher::new(self.db_path.clone()));
                *guard = Some(w.clone());
                w
            }
        };
        let (sub_id, rx) = shared.subscribe();
        Ok(UpdateEvents {
            db_path: self.db_path.clone(),
            shared,
            sub_id,
            inner: Arc::new(Mutex::new(UpdateWatchState {
                rx: Some(rx),
                queue: None,
                bridge_handle: None,
            })),
            shutdown: Arc::new(AtomicBool::new(false)),
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

    /// Release the underlying SQLite handles and the watcher poll
    /// thread so the OS can unlink the database file. After `close()`,
    /// any further `transaction()` / `query()` / `update_events()`
    /// calls return an error.
    ///
    /// Idempotent. Safe to call from `__exit__`/`finally` blocks even
    /// if the database was never used.
    ///
    /// Why this matters on Windows: SQLite holds the `.db` file open
    /// for the lifetime of every Connection, and Windows' mandatory
    /// locking blocks `unlink` while any handle is alive. Without
    /// explicit close, the writer/reader connections live until the
    /// Python GC drops every Transaction object that ever cloned the
    /// `Arc<Writer>` — which a `tempfile.TemporaryDirectory` cleanup
    /// can't force.
    fn close(&self) {
        // Drop the watcher's read-only connection (joins the poll
        // thread synchronously) first so no background reader is
        // racing the writer/reader closes below.
        if let Some(shared) = self.shared_watcher.lock().take() {
            let _ = shared.close();
        }
        // These reach inside the Arc<Writer> / Arc<Readers> and drop
        // the underlying Connection regardless of how many Arc clones
        // exist (Transactions still alive in Python, etc.).
        self.writer.close();
        self.readers.close();
    }

    fn __enter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __exit__(
        &self,
        _exc_type: Option<&Bound<'_, PyAny>>,
        _exc_val: Option<&Bound<'_, PyAny>>,
        _exc_tb: Option<&Bound<'_, PyAny>>,
    ) -> bool {
        self.close();
        false
    }
}

impl Drop for Database {
    /// Mirror what `close()` does so that `db = honker.open(...)`
    /// without an explicit close still releases SQLite handles when
    /// CPython refcounts the `Database` to zero (e.g. at end of test
    /// scope). Required for `tempfile.TemporaryDirectory` cleanup on
    /// Windows: mandatory locking blocks unlink while any connection
    /// is alive, and Python tests can't be relied on to call
    /// `close()` explicitly. Idempotent with `close()`.
    fn drop(&mut self) {
        if let Some(shared) = self.shared_watcher.lock().take() {
            let _ = shared.close();
        }
        self.writer.close();
        self.readers.close();
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
        // Either path returns None if the database has been closed.
        let conn = writer
            .try_acquire()
            .or_else(|| py.detach(|| writer.acquire()))
            .ok_or_else(|| {
                pyo3::exceptions::PyRuntimeError::new_err("Database is closed")
            })?;
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

    fn notify(&self, py: Python<'_>, channel: String, payload: Bound<'_, PyAny>) -> PyResult<i64> {
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

    /// Install the canonical honker queue schema. Idempotent; the
    /// DDL lives in `honker-core` so the Python binding and the
    /// SQLite extension can't drift on column counts.
    fn bootstrap_honker_schema(&self) -> PyResult<()> {
        let state = self.inner.lock();
        let conn = state
            .conn
            .as_ref()
            .ok_or_else(|| PyRuntimeError::new_err("Transaction not started"))?;
        honker_core::bootstrap_honker_schema(conn).map_err(core_err)
    }
}

// ---------------------------------------------------------------------
// UpdateEvents (async iterator over a SharedUpdateWatcher subscription)
// ---------------------------------------------------------------------

struct UpdateWatchState {
    /// Per-subscriber receiver from the shared watcher. Moved into
    /// the bridge thread on first `__aiter__`; on subsequent
    /// `__aiter__` calls the queue has been created and we return
    /// it directly.
    rx: Option<std::sync::mpsc::Receiver<()>>,
    /// Python asyncio Queue, populated lazily on first __aiter__.
    queue: Option<Py<PyAny>>,
    /// Bridge thread JoinHandle, populated on first `__aiter__`.
    /// `Drop` joins this synchronously so the bridge thread is
    /// guaranteed to have exited before this `UpdateEvents` goes
    /// away. Required to prevent the panic tracked in the parent
    /// repo's #16: without a join, the bridge thread can outlive
    /// Python interpreter teardown and panic on its next
    /// `Python::attach`.
    bridge_handle: Option<std::thread::JoinHandle<()>>,
}

#[pyclass]
struct UpdateEvents {
    db_path: std::path::PathBuf,
    /// Keep the shared watcher alive as long as this subscription
    /// exists. `Drop` calls `shared.unsubscribe(sub_id)` so the bridge
    /// thread's `rx.recv()` sees a disconnect and exits.
    shared: Arc<SharedUpdateWatcher>,
    sub_id: u64,
    inner: Arc<Mutex<UpdateWatchState>>,
    /// Shared shutdown flag with the bridge thread. `Drop` sets this
    /// before unsubscribing and joining. The bridge thread checks it
    /// AFTER `rx.recv()` returns and BEFORE entering `Python::attach`,
    /// so a tick already in-flight on the channel doesn't trigger a
    /// post-shutdown call into Python.
    shutdown: Arc<AtomicBool>,
}

impl Drop for UpdateEvents {
    fn drop(&mut self) {
        // Order matters here:
        //   1. Set shutdown FIRST so even an in-flight tick the bridge
        //      thread is about to recv() doesn't race a Python::attach
        //      after we start tearing down.
        //   2. Unsubscribe so any blocked rx.recv() returns Err
        //      immediately (sender dropped).
        //   3. Synchronously join the bridge thread so it's
        //      definitively gone before this Drop returns.
        //
        // Without (3), the bridge thread can keep running after the
        // Python interpreter finalizes — see #16. Watcher poll thread
        // (in honker-core) is already joined synchronously by
        // SharedUpdateWatcher::close(), but the bridge thread is the
        // one that calls back into Python and the one that panicked.
        self.shutdown.store(true, Ordering::Release);
        self.shared.unsubscribe(self.sub_id);

        let handle = self.inner.lock().bridge_handle.take();
        if let Some(handle) = handle {
            // CPython's deallocator calls Drop with the GIL held. The
            // bridge thread, if mid-`Python::attach`, is itself
            // blocked acquiring the GIL we hold — so a naive `join()`
            // would deadlock. Release the GIL via `py.detach` while
            // we wait so the bridge thread can finish its in-flight
            // attach, observe the cleared sender, and exit.
            //
            // `Python::attach` (0.28's rename of `with_gil`) is
            // reentrant: a no-op acquisition if we already hold it
            // (the typical pyclass-Drop case). `attach`-then-`detach`
            // works whether or not we entered with the GIL.
            //
            // If the interpreter is being finalized (rare; normally
            // pyclass dealloc runs before Py_FinalizeEx), `attach`
            // could fail. Guard with catch_unwind so a panic from
            // PyO3 internals doesn't propagate through Drop.
            let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                Python::attach(|py| {
                    py.detach(|| {
                        let _ = handle.join();
                    });
                });
            }));
        }
    }
}

impl UpdateEvents {
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

        let rx = state.rx.take().expect("update rx already taken");
        let shutdown = self.shutdown.clone();

        let handle = std::thread::Builder::new()
            .name("honker-update-bridge".into())
            .spawn(move || {
                // Blocks on the subscriber channel. Exits when:
                //   * the shared watcher's sender list prunes this
                //     subscriber (Drop -> unsubscribe -> sender dropped
                //     -> recv() returns Err), OR
                //   * the shutdown flag is set (Drop sets it before
                //     unsubscribing). The check sits BETWEEN recv() and
                //     `Python::attach` so a tick that landed before
                //     shutdown was set doesn't trigger a Python call
                //     after the interpreter is mid-teardown.
                //
                // Without the shutdown check + the synchronous join in
                // Drop, the bridge thread could outlive the Python
                // interpreter and panic on the next attach (#16 in the
                // honker repo).
                while rx.recv().is_ok() {
                    if shutdown.load(Ordering::Acquire) {
                        return;
                    }
                    Python::attach(|py| {
                        let put = match queue_py_for_thread.getattr(py, "put_nowait") {
                            Ok(v) => v,
                            Err(_) => return,
                        };
                        let _ = loop_py.call_method1(py, "call_soon_threadsafe", (put, py.None()));
                    });
                }
            })
            .map_err(core_err)?;

        state.queue = Some(queue_py.clone_ref(py));
        state.bridge_handle = Some(handle);
        Ok(queue_py)
    }
}

#[pymethods]
impl UpdateEvents {
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
        self.db_path.to_string_lossy().into_owned()
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
/// malformed cron expression. Used by `honker.CronSchedule` so the
/// parser + next-boundary math lives once in Rust.
#[pyfunction]
fn cron_next_after(expr: String, from_unix: i64) -> PyResult<i64> {
    honker_core::cron::next_after_unix(&expr, from_unix)
        .map_err(|e| pyo3::exceptions::PyValueError::new_err(e))
}

#[pymodule]
fn _honker_native(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(open, m)?)?;
    m.add_function(wrap_pyfunction!(cron_next_after, m)?)?;
    m.add_class::<Database>()?;
    m.add_class::<Transaction>()?;
    m.add_class::<UpdateEvents>()?;
    Ok(())
}
