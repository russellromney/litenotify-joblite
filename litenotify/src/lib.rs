mod notifier;

use notifier::{Notification, Notifier};
use parking_lot::{Condvar, Mutex};
use pyo3::exceptions::{PyRuntimeError, PyTypeError};
use pyo3::prelude::*;
use pyo3::types::{PyAny, PyBool, PyBytes, PyDict, PyList};
use rusqlite::types::{Value, ValueRef};
use rusqlite::{Connection, OpenFlags};
use std::sync::Arc;

fn open_conn(path: &str, attach: Option<&Notifier>) -> PyResult<Connection> {
    let conn = Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_WRITE
            | OpenFlags::SQLITE_OPEN_CREATE
            | OpenFlags::SQLITE_OPEN_URI,
    )
    .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
    // PRAGMA tuning notes:
    //   journal_mode=WAL        -> concurrent readers with a single writer
    //   synchronous=NORMAL      -> fsync WAL at checkpoint only (not per
    //                              commit); safe against app crashes, not
    //                              OS crashes / power loss
    //   busy_timeout=5000       -> wait up to 5s for the writer to
    //                              release the lock before returning
    //                              SQLITE_BUSY
    //   foreign_keys=ON         -> enforce FK constraints (off by default
    //                              in SQLite, a real footgun)
    //   cache_size=-32000       -> 32MB page cache (default was 2MB).
    //                              Pending/processing tables stay hot.
    //   temp_store=MEMORY       -> temporary B-trees for ORDER BY /
    //                              DISTINCT live in RAM, not the temp dir
    //   wal_autocheckpoint=10000 -> checkpoint (and fsync) every 10k WAL
    //                              pages rather than 1k. Reduces fsync
    //                              frequency 10x. Tradeoff: WAL grows
    //                              larger between checkpoints; crash
    //                              recovery has more to replay.
    conn.execute_batch(
        "PRAGMA journal_mode = WAL;\n         \
         PRAGMA synchronous = NORMAL;\n         \
         PRAGMA busy_timeout = 5000;\n         \
         PRAGMA foreign_keys = ON;\n         \
         PRAGMA cache_size = -32000;\n         \
         PRAGMA temp_store = MEMORY;\n         \
         PRAGMA wal_autocheckpoint = 10000;",
    )
    .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
    if let Some(n) = attach {
        n.attach(&conn)
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
    }
    Ok(conn)
}

struct Writer {
    slot: Mutex<Option<Connection>>,
    available: Condvar,
}

impl Writer {
    fn new(conn: Connection) -> Self {
        Self {
            slot: Mutex::new(Some(conn)),
            available: Condvar::new(),
        }
    }

    fn acquire(&self) -> Connection {
        let mut guard = self.slot.lock();
        while guard.is_none() {
            self.available.wait(&mut guard);
        }
        guard.take().unwrap()
    }

    /// Non-blocking take. Returns `Some(Connection)` if the slot is free at
    /// lock time, otherwise `None` — caller must fall back to `acquire` (and
    /// should drop the GIL around it so other Python threads can run while
    /// we wait). Used by the `__enter__` fast-path to skip `py.detach` on
    /// the uncontended single-writer case.
    fn try_acquire(&self) -> Option<Connection> {
        let mut guard = self.slot.lock();
        guard.take()
    }

    fn release(&self, conn: Connection) {
        let mut guard = self.slot.lock();
        *guard = Some(conn);
        self.available.notify_one();
    }
}

struct Readers {
    pool: Mutex<Vec<Connection>>,
    available: Condvar,
    outstanding: Mutex<usize>,
    max: usize,
    path: String,
}

impl Readers {
    fn new(path: String, max: usize) -> Self {
        Self {
            pool: Mutex::new(Vec::new()),
            available: Condvar::new(),
            outstanding: Mutex::new(0),
            max: max.max(1),
            path,
        }
    }

    fn acquire(&self) -> PyResult<Connection> {
        loop {
            let mut pool = self.pool.lock();
            if let Some(c) = pool.pop() {
                return Ok(c);
            }
            let mut out = self.outstanding.lock();
            if *out < self.max {
                *out += 1;
                drop(out);
                drop(pool);
                return open_conn(&self.path, None);
            }
            drop(out);
            self.available.wait(&mut pool);
        }
    }

    fn release(&self, conn: Connection) {
        let mut pool = self.pool.lock();
        pool.push(conn);
        self.available.notify_one();
    }
}

#[pyclass]
struct Database {
    notifier: Arc<Notifier>,
    writer: Arc<Writer>,
    readers: Arc<Readers>,
}

#[pymethods]
impl Database {
    #[new]
    #[pyo3(signature = (path, max_readers=8))]
    fn new(path: String, max_readers: usize) -> PyResult<Self> {
        let notifier = Arc::new(Notifier::new());
        let writer_conn = open_conn(&path, Some(&notifier))?;
        Ok(Self {
            notifier,
            writer: Arc::new(Writer::new(writer_conn)),
            readers: Arc::new(Readers::new(path, max_readers)),
        })
    }

    fn transaction(&self) -> PyResult<Transaction> {
        Ok(Transaction {
            writer: self.writer.clone(),
            inner: Arc::new(Mutex::new(TxState::default())),
        })
    }

    fn listen(&self, channel: String) -> PyResult<Listener> {
        let sub = self.notifier.subscribe(channel.clone());
        Ok(Listener {
            channel,
            subscription_id: sub.id,
            notifier: self.notifier.clone(),
            inner: Arc::new(Mutex::new(ListenerState {
                rx: Some(sub.rx),
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
        let conn = self.readers.acquire()?;
        let result = run_query(py, &conn, &sql, params.as_ref());
        self.readers.release(conn);
        result
    }
}

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
    // prepare_cached hits rusqlite's per-connection statement cache. Without
    // this, every execute re-parses the SQL and rebuilds the plan — which
    // measures at ~4x overhead vs. the underlying SQLite ceiling for hot
    // INSERT/UPDATE/SELECT loops. Same pattern applied in run_execute.
    let mut stmt = conn
        .prepare_cached(sql)
        .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
    let columns: Vec<String> = stmt.column_names().iter().map(|s| s.to_string()).collect();
    let mut rows = stmt
        .query(rusqlite::params_from_iter(values))
        .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
    let out = PyList::empty(py);
    while let Some(row) = rows
        .next()
        .map_err(|e| PyRuntimeError::new_err(e.to_string()))?
    {
        let dict = PyDict::new(py);
        for (i, name) in columns.iter().enumerate() {
            let v = row
                .get_ref(i)
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
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
    let mut stmt = conn
        .prepare_cached(sql)
        .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
    stmt.execute(rusqlite::params_from_iter(values))
        .map_err(|e| PyRuntimeError::new_err(e.to_string()))
}

/// Run a fixed SQL statement with no params via the cached statement pool.
/// Used for BEGIN IMMEDIATE / COMMIT / ROLLBACK so we don't re-parse every
/// transaction.
fn run_cached_noparams(conn: &Connection, sql: &str) -> rusqlite::Result<()> {
    let mut stmt = conn.prepare_cached(sql)?;
    stmt.execute([])?;
    Ok(())
}

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
        // Fast path: if the writer slot is immediately free, take it under
        // the parking_lot mutex without releasing the GIL. Uncontended
        // single-writer workloads (the bench, most web apps with a single
        // process, most workers) avoid py.detach's GIL release+reacquire
        // (~5us savings per tx, measurable at 5k+ ops/s).
        //
        // Slow path: slot is held — drop the GIL so other Python threads
        // can run while we block on the condvar.
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
                Err(PyRuntimeError::new_err(e.to_string()))
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
            return Err(PyRuntimeError::new_err(e.to_string()));
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
    ) -> PyResult<()> {
        let state = self.inner.lock();
        let conn = state
            .conn
            .as_ref()
            .ok_or_else(|| PyRuntimeError::new_err("Transaction not started"))?;
        let payload_str = serialize_payload(py, &payload)?;
        conn.query_row(
            "SELECT notify(?1, ?2)",
            rusqlite::params![channel, payload_str],
            |_| Ok(()),
        )
        .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
        Ok(())
    }
}

fn serialize_payload(py: Python<'_>, payload: &Bound<'_, PyAny>) -> PyResult<String> {
    if payload.is_none() {
        return Ok("null".to_string());
    }
    if let Ok(s) = payload.extract::<String>() {
        return Ok(s);
    }
    let json = py.import("json")?;
    let dumps = json.getattr("dumps")?;
    let result = dumps.call1((payload,))?;
    result.extract::<String>()
}

#[pyclass]
struct NotificationResult {
    #[pyo3(get)]
    channel: String,
    #[pyo3(get)]
    payload: String,
}

struct ListenerState {
    rx: Option<tokio::sync::broadcast::Receiver<Arc<Notification>>>,
    queue: Option<Py<PyAny>>,
}

#[pyclass]
struct Listener {
    channel: String,
    subscription_id: u64,
    notifier: Arc<Notifier>,
    inner: Arc<Mutex<ListenerState>>,
}

impl Drop for Listener {
    fn drop(&mut self) {
        // Remove the subscriber from the notifier so the registry entry
        // doesn't leak and the bridge thread's blocking_recv() returns
        // Closed instead of waiting forever.
        self.notifier.unsubscribe(self.subscription_id);
    }
}

impl Listener {
    /// Lazily start the bridge thread the first time this listener is
    /// iterated. The thread blocks on the tokio broadcast receiver and hands
    /// each notification to the asyncio loop that called __aiter__ via
    /// loop.call_soon_threadsafe(queue.put_nowait, ...). Works on any
    /// asyncio loop (TestClient portal, anyio, Jupyter, asyncio.run).
    ///
    /// No channel filter here — the notifier's per-channel registry means we only
    /// receive notifications for our own channel.
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
        let channel = self.channel.clone();
        let mut rx = state.rx.take().expect("rx already taken");

        std::thread::Builder::new()
            .name(format!("litenotify-listen-{}", channel))
            .spawn(move || {
                loop {
                    match rx.blocking_recv() {
                        Ok(n) => {
                            Python::attach(|py| {
                                let notif = match Py::new(
                                    py,
                                    NotificationResult {
                                        channel: n.channel.clone(),
                                        payload: n.payload.clone(),
                                    },
                                ) {
                                    Ok(v) => v,
                                    Err(_) => return,
                                };
                                let put = match queue_py_for_thread.getattr(py, "put_nowait") {
                                    Ok(v) => v,
                                    Err(_) => return,
                                };
                                let _ = loop_py.call_method1(
                                    py,
                                    "call_soon_threadsafe",
                                    (put, notif),
                                );
                            });
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                    }
                }
            })
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;

        state.queue = Some(queue_py.clone_ref(py));
        Ok(queue_py)
    }
}

#[pymethods]
impl Listener {
    fn __aiter__<'a>(slf: PyRef<'a, Self>, py: Python<'a>) -> PyResult<PyRef<'a, Self>> {
        slf.ensure_started(py)?;
        Ok(slf)
    }

    fn __anext__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let queue = self.ensure_started(py)?;
        queue.bind(py).call_method0("get")
    }
}

#[pyfunction]
#[pyo3(signature = (path, max_readers=8))]
fn open(path: String, max_readers: usize) -> PyResult<Database> {
    Database::new(path, max_readers)
}

#[pymodule]
fn litenotify(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(open, m)?)?;
    m.add_class::<Database>()?;
    m.add_class::<Transaction>()?;
    m.add_class::<Listener>()?;
    m.add_class::<NotificationResult>()?;
    Ok(())
}
