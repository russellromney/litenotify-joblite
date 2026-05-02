//! Shared Rust core for the honker bindings.
//!
//! This crate is NOT intended for direct use. It's the plain-Rust
//! foundation that three binding crates depend on:
//!
//!   * `honker`               — PyO3 Python extension
//!   * `honker-extension`    — SQLite loadable extension (cdylib)
//!   * `honker-node`         — napi-rs Node.js binding
//!
//! Moving this code here once avoids the three-copies-of-the-same-SQL
//! problem every binding would otherwise suffer. Behavioral drift
//! between the three bindings was a real risk — one would get a new
//! PRAGMA, one wouldn't, and silent inconsistencies would surface only
//! when a Python process and a Node process tried to share a `.db`
//! file.
//!
//! What's here:
//!
//!   - [`open_conn`] — open a SQLite connection with the library's
//!     PRAGMA defaults (WAL, synchronous=NORMAL, 32MB cache, etc.).
//!   - [`attach_notify`] — create `_honker_notifications` and
//!     register the `notify(channel, payload)` SQL scalar function.
//!   - [`Writer`] — single-connection write slot with blocking
//!     acquire, non-blocking try_acquire, and release.
//!   - [`Readers`] — bounded pool of reader connections that open
//!     lazily up to a max.
//!   - [`UpdateWatcher`] — 1 ms PRAGMA-polling thread that fires a
//!     callback on every database commit. Uses `PRAGMA data_version`
//!     for precise change detection, with a periodic stat identity check
//!     to detect file replacement. Bindings wrap this to surface wake
//!     events to their language's async primitive.
//!
//! Anything language-specific — PyO3 classes, napi classes, SQLite
//! entry-point symbols, row-materialization into Python dicts or JS
//! objects — stays in the respective binding crate.

pub mod cron;
mod honker_ops;

pub use honker_ops::attach_honker_functions;

use parking_lot::{Condvar, Mutex};
use rusqlite::functions::FunctionFlags;
use rusqlite::{Connection, OpenFlags};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{SyncSender, TrySendError};
use std::time::{Duration, Instant};

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Database error: {0}")]
    Sqlite(#[from] rusqlite::Error),
}

// ---------------------------------------------------------------------
// PRAGMAs
// ---------------------------------------------------------------------

/// Default PRAGMA block applied on every connection open. Rationale:
///
///   * `journal_mode=WAL`        — concurrent readers with one writer.
///   * `synchronous=NORMAL`      — fsync WAL at checkpoint, not every
///     commit. Safe against app crashes; OS crashes may lose the last
///     few unchecked-pointed transactions.
///   * `busy_timeout=5000`       — wait up to 5s for the writer lock
///     before returning SQLITE_BUSY.
///   * `foreign_keys=ON`         — enforce FK constraints (off by
///     default in SQLite, a real footgun).
///   * `cache_size=-32000`       — 32MB page cache (default was 2MB).
///   * `temp_store=MEMORY`       — temp B-trees in RAM, not disk.
///   * `wal_autocheckpoint=10000`— fsync every 10k WAL pages. Reduces
///     fsync frequency 10× vs the default of 1k.
pub const DEFAULT_PRAGMAS: &str = "PRAGMA journal_mode = WAL;
         PRAGMA synchronous = NORMAL;
         PRAGMA busy_timeout = 5000;
         PRAGMA foreign_keys = ON;
         PRAGMA cache_size = -32000;
         PRAGMA temp_store = MEMORY;
         PRAGMA wal_autocheckpoint = 10000;";

/// Apply the library's default PRAGMAs to an already-open connection.
/// Idempotent.
pub fn apply_default_pragmas(conn: &Connection) -> rusqlite::Result<()> {
    conn.execute_batch(DEFAULT_PRAGMAS)
}

// ---------------------------------------------------------------------
// notify() SQL function + notifications schema
// ---------------------------------------------------------------------

/// Install the `_honker_notifications` table and the
/// `notify(channel, payload)` SQL scalar function on `conn`. Idempotent.
///
/// `notify()` is the public cross-process primitive. Callers do:
///
/// ```sql
/// BEGIN IMMEDIATE;
/// INSERT INTO orders ...;
/// SELECT notify('orders', '{"id":42}');
/// COMMIT;
/// ```
///
/// The scalar function returns the INSERTed row id. Listeners watch
/// database updates and SELECT new rows by channel.
///
/// Pruning is NOT done here. Callers invoke
/// `Database.prune_notifications(older_than_s, max_keep)` when they want
/// to trim the table. No magic timer.
pub fn attach_notify(conn: &Connection) -> Result<(), Error> {
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS _honker_notifications (
           id INTEGER PRIMARY KEY AUTOINCREMENT,
           channel TEXT NOT NULL,
           payload TEXT NOT NULL,
           created_at INTEGER NOT NULL DEFAULT (unixepoch())
         );
         CREATE INDEX IF NOT EXISTS _honker_notifications_recent
           ON _honker_notifications(channel, id);",
    )?;

    conn.create_scalar_function("notify", 2, FunctionFlags::SQLITE_UTF8, |ctx| {
        let channel: String = ctx.get(0)?;
        let payload: String = ctx.get(1)?;
        let db = unsafe { ctx.get_connection() }?;
        let mut ins = db.prepare_cached(
            "INSERT INTO _honker_notifications (channel, payload) VALUES (?1, ?2)",
        )?;
        let id = ins.insert(rusqlite::params![channel, payload])?;
        Ok(id)
    })?;

    Ok(())
}

// ---------------------------------------------------------------------
// honker queue schema
// ---------------------------------------------------------------------

/// Canonical DDL for the honker queue schema. Shared source of truth
/// so the Python binding's `Queue._init_schema`, the SQLite loadable
/// extension's `honker_bootstrap()`, and any future binding can't drift.
///
/// Schema:
///
///   * `_honker_live`  — pending + processing jobs. Partial index
///     `_honker_live_claim` restricts to those two states so dead-row
///     history never slows down the claim hot path.
///   * `_honker_dead`  — terminal rows (retry-exhausted or explicitly
///     failed). Never scanned by the claim path; retention policy is
///     the user's problem.
///
/// Idempotent (`CREATE TABLE IF NOT EXISTS` / `CREATE INDEX IF NOT
/// EXISTS`). Views and schema-version cleanup live in the language
/// binding, not here — they're caller-specific.
pub const BOOTSTRAP_HONKER_SQL: &str = "
    CREATE TABLE IF NOT EXISTS _honker_live (
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
      created_at INTEGER NOT NULL DEFAULT (unixepoch()),
      expires_at INTEGER
    );
    CREATE INDEX IF NOT EXISTS _honker_live_claim
      ON _honker_live(queue, priority DESC, run_at, id)
      WHERE state IN ('pending', 'processing');
    CREATE INDEX IF NOT EXISTS _honker_live_pending_deadline
      ON _honker_live(queue, run_at)
      WHERE state = 'pending';
    CREATE INDEX IF NOT EXISTS _honker_live_processing_deadline
      ON _honker_live(queue, claim_expires_at)
      WHERE state = 'processing';
    CREATE TABLE IF NOT EXISTS _honker_dead (
      id INTEGER PRIMARY KEY,
      queue TEXT NOT NULL,
      payload TEXT NOT NULL,
      priority INTEGER NOT NULL DEFAULT 0,
      run_at INTEGER NOT NULL DEFAULT 0,
      attempts INTEGER NOT NULL DEFAULT 0,
      max_attempts INTEGER NOT NULL DEFAULT 0,
      last_error TEXT,
      created_at INTEGER NOT NULL DEFAULT (unixepoch()),
      died_at INTEGER NOT NULL DEFAULT (unixepoch())
    );
    CREATE TABLE IF NOT EXISTS _honker_locks (
      name TEXT PRIMARY KEY,
      owner TEXT NOT NULL,
      expires_at INTEGER NOT NULL
    );
    CREATE TABLE IF NOT EXISTS _honker_rate_limits (
      name TEXT NOT NULL,
      window_start INTEGER NOT NULL,
      count INTEGER NOT NULL DEFAULT 0,
      PRIMARY KEY (name, window_start)
    );
    CREATE TABLE IF NOT EXISTS _honker_scheduler_tasks (
      name TEXT PRIMARY KEY,
      queue TEXT NOT NULL,
      cron_expr TEXT NOT NULL,
      payload TEXT NOT NULL,
      priority INTEGER NOT NULL DEFAULT 0,
      expires_s INTEGER,
      next_fire_at INTEGER NOT NULL
    );
    CREATE TABLE IF NOT EXISTS _honker_results (
      job_id INTEGER PRIMARY KEY,
      value TEXT,
      created_at INTEGER NOT NULL DEFAULT (unixepoch()),
      expires_at INTEGER
    );
    CREATE TABLE IF NOT EXISTS _honker_stream (
      offset INTEGER PRIMARY KEY AUTOINCREMENT,
      topic TEXT NOT NULL,
      key TEXT,
      payload TEXT NOT NULL,
      created_at INTEGER NOT NULL DEFAULT (unixepoch())
    );
    CREATE INDEX IF NOT EXISTS _honker_stream_topic
      ON _honker_stream(topic, offset);
    CREATE TABLE IF NOT EXISTS _honker_stream_consumers (
      name TEXT NOT NULL,
      topic TEXT NOT NULL,
      offset INTEGER NOT NULL DEFAULT 0,
      PRIMARY KEY (name, topic)
    );
";

/// Install the honker queue schema on `conn`. Idempotent. See
/// [`BOOTSTRAP_HONKER_SQL`] for the DDL and rationale.
///
/// Works in any journal mode. WAL mode is still the recommended
/// default (concurrent readers, one writer, efficient fsync), but
/// callers who know what they're doing can run honker tables on a
/// DELETE-journal database. Cross-process wake is their responsibility.
pub fn bootstrap_honker_schema(conn: &Connection) -> Result<(), Error> {
    conn.execute_batch(BOOTSTRAP_HONKER_SQL)?;
    Ok(())
}

// ---------------------------------------------------------------------
// Opening connections
// ---------------------------------------------------------------------

/// Open a SQLite connection at `path` with the library's PRAGMA
/// defaults. If `install_notify` is true, also attach the notifications
/// table + `notify()` SQL function. Readers don't need it; only the
/// writer connection does.
pub fn open_conn(path: &str, install_notify: bool) -> Result<Connection, Error> {
    let conn = Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_WRITE
            | OpenFlags::SQLITE_OPEN_CREATE
            | OpenFlags::SQLITE_OPEN_URI,
    )?;
    apply_default_pragmas(&conn)?;
    if install_notify {
        attach_notify(&conn)?;
    }
    Ok(conn)
}

// ---------------------------------------------------------------------
// Writer slot
// ---------------------------------------------------------------------

/// Single-connection write slot. Writers serialize through one
/// rusqlite `Connection` because WAL mode allows only one writer at a
/// time anyway; doing it in user space avoids busy-timeout retries.
///
/// Provides explicit [`close`](Self::close) so bindings can release the
/// underlying SQLite handle independent of `Arc<Writer>` reference
/// count. Without this, an outstanding `Arc<Writer>` clone (kept alive
/// by a not-yet-GC'd Transaction object on the JS/Python side) would
/// keep the connection open and the `.db` file locked. On Windows that
/// blocks `rmdir`/`unlink` of the temp directory until GC runs; on
/// Linux/macOS the unlink succeeds but the file descriptor leaks.
pub struct Writer {
    slot: Mutex<Option<Connection>>,
    available: Condvar,
    closed: AtomicBool,
}

impl Writer {
    pub fn new(conn: Connection) -> Self {
        Self {
            slot: Mutex::new(Some(conn)),
            available: Condvar::new(),
            closed: AtomicBool::new(false),
        }
    }

    /// Blocking acquire. Waits on a condvar if the slot is held.
    /// Returns `None` if the writer has been [closed](Self::close).
    pub fn acquire(&self) -> Option<Connection> {
        let mut guard = self.slot.lock();
        loop {
            if self.closed.load(Ordering::Acquire) {
                return None;
            }
            if let Some(c) = guard.take() {
                return Some(c);
            }
            self.available.wait(&mut guard);
        }
    }

    /// Non-blocking. Returns `Some(conn)` if the slot was immediately
    /// free, else `None`. Bindings use this for a fast path that
    /// avoids GIL release (Python) or async thread-hops (Node) when
    /// the slot is uncontended. Also returns `None` if closed.
    pub fn try_acquire(&self) -> Option<Connection> {
        if self.closed.load(Ordering::Acquire) {
            return None;
        }
        self.slot.lock().take()
    }

    /// Return a connection to the slot. After [close](Self::close), the
    /// connection is dropped instead of being returned to the pool.
    pub fn release(&self, conn: Connection) {
        if self.closed.load(Ordering::Acquire) {
            // Drop conn instead of returning it to a closed pool.
            return;
        }
        let mut guard = self.slot.lock();
        *guard = Some(conn);
        self.available.notify_one();
    }

    /// Drop the underlying connection and refuse further acquisitions.
    /// Idempotent. Wakes any blocked `acquire()` callers; they observe
    /// the closed flag and return `None`.
    ///
    /// If a transaction is currently holding the connection (i.e. the
    /// slot is empty), it stays out — the transaction's eventual
    /// `release` will see `closed == true` and drop the connection
    /// itself. So the file handle is released either way; what
    /// matters is that no further writes happen after `close`.
    pub fn close(&self) {
        self.closed.store(true, Ordering::Release);
        let mut guard = self.slot.lock();
        guard.take(); // drops the connection if the slot is occupied
        self.available.notify_all();
    }
}

// ---------------------------------------------------------------------
// Reader pool
// ---------------------------------------------------------------------

/// Bounded pool of reader connections. Readers are cheap (one file
/// descriptor + a page cache) and WAL mode allows any number to run
/// concurrently with the writer.
///
/// Provides explicit [`close`](Self::close) for the same reason as
/// [`Writer::close`] — see that doc.
pub struct Readers {
    pool: Mutex<Vec<Connection>>,
    outstanding: Mutex<usize>,
    available: Condvar,
    path: String,
    max: usize,
    closed: AtomicBool,
}

impl Readers {
    pub fn new(path: String, max: usize) -> Self {
        Self {
            pool: Mutex::new(Vec::new()),
            outstanding: Mutex::new(0),
            available: Condvar::new(),
            path,
            max: max.max(1),
            closed: AtomicBool::new(false),
        }
    }

    /// Acquire a reader. Pops a pooled one if available; otherwise
    /// opens a new connection up to `max`. Above `max`, waits on the
    /// condvar. After [`close`](Self::close), returns
    /// `Err(rusqlite::Error::ExecuteReturnedResults)` as a sentinel —
    /// bindings should map this to "Database is closed".
    pub fn acquire(&self) -> Result<Connection, Error> {
        loop {
            if self.closed.load(Ordering::Acquire) {
                return Err(closed_err());
            }
            let mut pool = self.pool.lock();
            if let Some(c) = pool.pop() {
                return Ok(c);
            }
            let mut out = self.outstanding.lock();
            if *out < self.max {
                *out += 1;
                drop(out);
                drop(pool);
                let conn = open_conn(&self.path, false)?;
                // Re-check: if close() raced us, drop the brand-new
                // connection instead of handing it out.
                if self.closed.load(Ordering::Acquire) {
                    drop(conn);
                    return Err(closed_err());
                }
                return Ok(conn);
            }
            drop(out);
            self.available.wait(&mut pool);
        }
    }

    /// Return a connection to the pool. After [close](Self::close), the
    /// connection is dropped instead of pooled.
    pub fn release(&self, conn: Connection) {
        if self.closed.load(Ordering::Acquire) {
            return;
        }
        let mut pool = self.pool.lock();
        pool.push(conn);
        self.available.notify_one();
    }

    /// Drop all pooled connections and refuse further acquisitions.
    /// Idempotent. Wakes any blocked `acquire()` callers; they observe
    /// the closed flag and return the closed sentinel.
    pub fn close(&self) {
        self.closed.store(true, Ordering::Release);
        self.pool.lock().clear(); // drops pooled connections
        self.available.notify_all();
    }
}

/// Sentinel error for "pool closed". Bindings can match the inner
/// `rusqlite::Error::SqliteFailure` with code `SQLITE_MISUSE` and
/// message containing "Database is closed" to surface a clean error
/// to user code. `SQLITE_MISUSE` is appropriate here — calling
/// acquire on a closed pool is a misuse of the API.
fn closed_err() -> Error {
    Error::Sqlite(rusqlite::Error::SqliteFailure(
        rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_MISUSE),
        Some("Database is closed".to_string()),
    ))
}

// ---------------------------------------------------------------------
// Database file watcher
// ---------------------------------------------------------------------

/// Platform-specific file identity: `(dev, ino)` on Unix,
/// `(volume_serial, file_index)` on Windows. Used to detect when the
/// database file has been replaced underneath us (atomic rename,
/// litestream restore, volume remount).
///
/// Uses the `file-id` crate on unix and windows for stable Rust
/// support without nightly features. Falls back to `(0, 0)` on other
/// targets (WASI, Redox, illumos, etc.) — same behavior as the
/// pre-`file-id` `#[cfg(not(any(unix, windows)))]` branch. On those
/// targets the dead-man's switch is a no-op (every `stat_identity`
/// returns `(0, 0)` so the equality check never trips); replacement
/// detection is disabled but the watcher still functions. Nobody is
/// known to deploy honker there today.
#[cfg(any(unix, windows))]
fn stat_identity(path: &Path) -> std::io::Result<(u64, u64)> {
    let id = file_id::get_file_id(path)?;
    match id {
        file_id::FileId::Inode {
            device_id,
            inode_number,
        } => Ok((device_id, inode_number)),
        file_id::FileId::LowRes {
            volume_serial_number,
            file_index,
        } => Ok((volume_serial_number as u64, file_index)),
        file_id::FileId::HighRes {
            volume_serial_number,
            file_id,
        } => Ok(fold_high_res(volume_serial_number, file_id)),
    }
}

/// Fold a 128-bit ReFS / `FILE_ID_INFO` `file_id` into a 64-bit
/// identity that fits the `(u64, u64)` return type of
/// [`stat_identity`].
///
/// NTFS leaves the upper 64 bits at 0 so the result is just the lower
/// 64 bits — bit-for-bit equivalent to truncation. ReFS can populate
/// both halves; XOR-folding mixes the bits so we use both halves'
/// entropy for symmetry.
///
/// For the "did this file get atomically renamed?" detection that
/// `UpdateWatcher` uses, either truncation or XOR-fold works — ReFS
/// file_ids change wholesale on rename, so the lower 64 bits change
/// too. The practical collision probability is the same as
/// truncation (~2⁻⁶⁴) and acceptable for this use.
#[cfg(any(unix, windows))]
fn fold_high_res(volume_serial_number: u64, file_id: u128) -> (u64, u64) {
    let file_index = ((file_id >> 64) as u64) ^ (file_id as u64);
    (volume_serial_number, file_index)
}

#[cfg(not(any(unix, windows)))]
fn stat_identity(_path: &Path) -> std::io::Result<(u64, u64)> {
    Ok((0, 0))
}

/// Read the pager's `data_version` counter via `PRAGMA data_version`.
/// Returns a monotonic u32 incremented on every commit by any
/// connection (and on checkpoint). Empirically verified to detect
/// cross-connection database updates on all SQLite versions tested.
/// Cost: ~3.5 µs/call = ~3.5 ms/sec at 1 kHz.
fn poll_data_version(conn: &Connection) -> Result<u32, String> {
    conn.pragma_query_value(None, "data_version", |row| row.get(0))
        .map_err(|e| e.to_string())
}

/// Background thread that polls a SQLite database file for changes.
///
/// Three-layer defensive architecture:
///
/// 1. **Fast path (every 1 ms):** `PRAGMA data_version`. Compare the
///    integer to last seen value. Notify on change. (~3.5 µs/call.)
/// 2. **Error recovery (every 1 ms on failure):** If the query fails,
///    reconnect the SQLite connection and force one wake.
/// 3. **Identity check (about every 100 ms):** `stat(db_path)` to compare
///    `(dev, ino)`. If the file was replaced, panic with a clear
///    message — continuing would silently watch stale data.
///
/// Why PRAGMA data_version instead of stat on the WAL? It is a
/// counter, not a timestamp — immune to clock skew, WAL truncation,
/// and exact-size collisions. It also ignores rolled-back transactions
/// that touch the WAL but never commit.
pub struct UpdateWatcher {
    stop: Arc<AtomicBool>,
    handle: Option<std::thread::JoinHandle<()>>,
}

const UPDATE_WATCHER_IDENTITY_INTERVAL: Duration = Duration::from_millis(100);

impl UpdateWatcher {
    /// Spawn a watcher thread on `db_path`. `on_change` is called
    /// once per observed commit. The thread runs until [`UpdateWatcher`]
    /// is dropped or [`stop`](Self::stop) is called.
    pub fn spawn<F>(db_path: PathBuf, on_change: F) -> Self
    where
        F: Fn() + Send + 'static,
    {
        let stop = Arc::new(AtomicBool::new(false));
        let stop_t = stop.clone();
        let handle = std::thread::Builder::new()
            .name("honker-update-poll".into())
            .spawn(move || {
                let mut conn = match Connection::open_with_flags(
                    &db_path,
                    OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_NO_MUTEX,
                ) {
                    Ok(c) => Some(c),
                    Err(e) => {
                        eprintln!("honker: failed to open watcher connection: {e}");
                        None
                    }
                };
                let mut last_version = conn
                    .as_ref()
                    .and_then(|c| poll_data_version(c).ok())
                    .unwrap_or(0);
                let initial_identity = match stat_identity(&db_path) {
                    Ok(id) => id,
                    Err(e) => {
                        eprintln!("honker: failed to stat database for identity check: {e}");
                        (0, 0)
                    }
                };
                let mut next_identity_check = Instant::now() + UPDATE_WATCHER_IDENTITY_INTERVAL;

                while !stop_t.load(Ordering::Acquire) {
                    std::thread::sleep(Duration::from_millis(1));

                    // Path 1: PRAGMA data_version (fast path)
                    if let Some(ref c) = conn {
                        match poll_data_version(c) {
                            Ok(version) => {
                                if version != last_version {
                                    last_version = version;
                                    on_change();
                                }
                            }
                            Err(e) => {
                                eprintln!("honker: data_version poll failed: {e}");
                                conn = None;
                                on_change(); // conservative wake
                            }
                        }
                    } else {
                        // Path 2: reconnect after transient failure
                        match Connection::open_with_flags(
                            &db_path,
                            OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_NO_MUTEX,
                        ) {
                            Ok(c) => {
                                last_version = poll_data_version(&c).unwrap_or(0);
                                conn = Some(c);
                            }
                            Err(e) => {
                                eprintln!("honker: reconnect failed: {e}");
                            }
                        }
                    }

                    // Path 3: stat identity check (dead-man's switch).
                    //
                    // Triggers on Linux/macOS scenarios that swap the
                    // db file out from under us — atomic rename
                    // (litestream restore), volume remount, NFS
                    // server restart. On Windows the kernel rejects
                    // rename-over-open even with `FILE_SHARE_DELETE`,
                    // so the practical replacement scenarios that
                    // trigger this on unix don't happen in the same
                    // way; the dead-man's switch is effectively a
                    // no-op on Windows. Identity check still runs,
                    // it just rarely sees a difference.
                    let now = Instant::now();
                    if now >= next_identity_check {
                        next_identity_check = now + UPDATE_WATCHER_IDENTITY_INTERVAL;
                        match stat_identity(&db_path) {
                            Ok(current) => {
                                if current != initial_identity {
                                    panic!(
                                        "honker: database file replaced: \
                                         expected (dev={}, ino={}), \
                                         found (dev={}, ino={}) at {:?}. \
                                         Restart required.",
                                        initial_identity.0,
                                        initial_identity.1,
                                        current.0,
                                        current.1,
                                        db_path
                                    );
                                }
                            }
                            Err(e) => {
                                eprintln!("honker: stat identity check failed: {e}");
                                conn = None;
                                on_change();
                            }
                        }
                    }
                }
            })
            .expect("spawn update-poll thread");
        Self {
            stop,
            handle: Some(handle),
        }
    }

    /// Request the watcher thread to stop. Idempotent. Dropping the
    /// `UpdateWatcher` also stops the thread.
    pub fn stop(&self) {
        self.stop.store(true, Ordering::Release);
    }

    /// Stop the watcher and wait for the thread to exit. Returns the
    /// thread's result — `Ok(())` on clean shutdown, `Err(payload)`
    /// if the thread panicked (e.g. the dead-man's switch detected
    /// file replacement). Consumes `self` so the watcher can't be
    /// used after joining.
    pub fn join(mut self) -> std::thread::Result<()> {
        self.stop();
        match self.handle.take() {
            Some(h) => h.join(),
            None => Ok(()),
        }
    }
}

impl Drop for UpdateWatcher {
    fn drop(&mut self) {
        self.stop();
    }
}

// ---------------------------------------------------------------------
// Shared update watcher (one thread per Database, N subscribers)
// ---------------------------------------------------------------------

/// Shared database-file watcher: one PRAGMA-poll thread per database
/// path, N subscribers. Each [`subscribe`](Self::subscribe) returns a
/// fresh `Receiver<()>` that sees a tick on every observed commit.
///
/// Previously every call to `db.update_events()` spawned its own
/// update watcher thread, so N listeners in one process meant N threads
/// hammering `stat(2)` on the same file at 1 ms cadence. A web
/// process with 100 active SSE subscribers was doing ~200k stat
/// syscalls/sec against one file. Now a single shared thread
/// services all subscribers — 1 ms cadence, same kernel cost
/// regardless of N.
///
/// Subscriber channels are bounded; on overflow, additional ticks for
/// that subscriber are dropped. Wakes are idempotent signals — the
/// consumer re-reads state from SQLite on each wake — so dropping
/// during backpressure is safe. A disconnected subscriber (receiver
/// dropped) gets pruned on the next wake via `TrySendError::Disconnected`.
pub struct SharedUpdateWatcher {
    /// Hold the underlying poll thread alive. Dropping or
    /// [`close`](Self::close)ing this stops it. Wrapped in
    /// `Mutex<Option<...>>` so `close()` can take the watcher out and
    /// `join()` it synchronously — required to release the watcher's
    /// read-only `Connection` before a `db.close()` consumer tries to
    /// `unlink` the database file (Windows: `EBUSY` until every
    /// handle is dropped).
    watcher: Mutex<Option<UpdateWatcher>>,
    /// Shared with the watcher closure so it can fan out to every
    /// subscriber and prune disconnected ones opportunistically.
    senders: Arc<Mutex<HashMap<u64, SyncSender<()>>>>,
    next_id: AtomicU64,
}

impl SharedUpdateWatcher {
    /// Spawn the shared poll thread for `db_path`.
    pub fn new(db_path: PathBuf) -> Self {
        let senders: Arc<Mutex<HashMap<u64, SyncSender<()>>>> =
            Arc::new(Mutex::new(HashMap::new()));
        let senders_t = senders.clone();
        let watcher = UpdateWatcher::spawn(db_path, move || {
            let mut list = senders_t.lock();
            list.retain(|_id, s| match s.try_send(()) {
                Ok(()) | Err(TrySendError::Full(_)) => true,
                Err(TrySendError::Disconnected(_)) => false,
            });
        });
        Self {
            watcher: Mutex::new(Some(watcher)),
            senders,
            next_id: AtomicU64::new(0),
        }
    }

    /// Subscribe. Returns a subscriber id and a [`Receiver<()>`] that
    /// sees one tick per observed database update. Callers MUST
    /// [`unsubscribe`](Self::unsubscribe) the returned id when done —
    /// otherwise the sender stays in the map and a bridge thread
    /// blocking on `recv()` will never see a disconnect.
    ///
    /// Channel buffer is 1024; backpressure drops additional ticks
    /// for the slow subscriber without blocking the watcher.
    pub fn subscribe(&self) -> (u64, std::sync::mpsc::Receiver<()>) {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = std::sync::mpsc::sync_channel(1024);
        self.senders.lock().insert(id, tx);
        (id, rx)
    }

    /// Remove a subscriber. The corresponding receiver sees
    /// `Err(RecvError)` on its next blocking `recv()`, letting a
    /// bridge thread exit cleanly.
    pub fn unsubscribe(&self, id: u64) {
        self.senders.lock().remove(&id);
    }

    /// Current subscriber count. Test/introspection helper.
    pub fn subscriber_count(&self) -> usize {
        self.senders.lock().len()
    }

    /// Disconnect all subscribers and synchronously join the poll
    /// thread. The thread owns the watcher's read-only `Connection`;
    /// joining drops that connection and releases the file handle.
    /// Idempotent — safe to call more than once.
    pub fn close(&self) -> std::thread::Result<()> {
        self.senders.lock().clear();
        match self.watcher.lock().take() {
            Some(watcher) => watcher.join(),
            None => Ok(()),
        }
    }
}

impl Drop for SharedUpdateWatcher {
    fn drop(&mut self) {
        // Best-effort: signal stop. We don't synchronously join here
        // because Drop runs from arbitrary contexts (including async
        // executors) where blocking on a thread join is unsafe.
        // Bindings that need a synchronous release should call
        // `close()` explicitly.
        self.senders.lock().clear();
        if let Some(watcher) = self.watcher.get_mut().take() {
            // Dropping UpdateWatcher signals stop; the thread exits
            // shortly after and its Connection drops then.
            drop(watcher);
        }
    }
}

// ---------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::Connection;

    fn mem() -> Connection {
        Connection::open_in_memory().unwrap()
    }

    #[test]
    fn notify_inserts_row() {
        let conn = mem();
        attach_notify(&conn).unwrap();
        conn.execute_batch("BEGIN IMMEDIATE;").unwrap();
        conn.query_row("SELECT notify('orders', 'new')", [], |_| Ok(()))
            .unwrap();
        conn.execute_batch("COMMIT;").unwrap();

        let n: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM _honker_notifications WHERE channel='orders'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(n, 1);
    }

    #[test]
    fn rollback_drops_notification() {
        let conn = mem();
        attach_notify(&conn).unwrap();
        conn.execute_batch("BEGIN IMMEDIATE;").unwrap();
        conn.query_row("SELECT notify('x', 'y')", [], |_| Ok(()))
            .unwrap();
        conn.execute_batch("ROLLBACK;").unwrap();

        let n: i64 = conn
            .query_row("SELECT COUNT(*) FROM _honker_notifications", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(n, 0);
    }

    #[test]
    fn writer_try_acquire_returns_none_when_held() {
        let w = Writer::new(Connection::open_in_memory().unwrap());
        let conn = w.acquire().expect("acquire on fresh Writer");
        assert!(w.try_acquire().is_none());
        w.release(conn);
        assert!(w.try_acquire().is_some());
    }

    #[test]
    fn writer_close_drops_idle_connection() {
        let w = Writer::new(Connection::open_in_memory().unwrap());
        // Slot is currently occupied (Some(conn)).
        w.close();
        // After close, acquire and try_acquire return None even though
        // a slot was free at close time — the connection was dropped.
        assert!(w.acquire().is_none());
        assert!(w.try_acquire().is_none());
    }

    #[test]
    fn writer_close_drops_returned_connection() {
        let w = Writer::new(Connection::open_in_memory().unwrap());
        let conn = w.acquire().expect("acquire on fresh Writer");
        // Close while a transaction is "holding" the connection.
        w.close();
        // Releasing after close drops the connection (no-op into the
        // pool); acquire still returns None.
        w.release(conn);
        assert!(w.try_acquire().is_none());
    }

    #[test]
    fn readers_close_returns_closed_err() {
        let tmp = std::env::temp_dir().join(format!(
            "honker-readers-close-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_file(&tmp);
        // Create the file so open_conn succeeds.
        Connection::open(&tmp)
            .unwrap()
            .execute_batch("PRAGMA journal_mode=WAL;")
            .unwrap();

        let r = Readers::new(tmp.to_string_lossy().into_owned(), 4);
        // Acquire one to populate the pool indirectly via outstanding count.
        let c = r.acquire().expect("first acquire");
        r.release(c);
        r.close();
        // After close, acquire returns the closed sentinel.
        match r.acquire() {
            Err(Error::Sqlite(rusqlite::Error::SqliteFailure(_, Some(msg)))) => {
                assert!(msg.contains("Database is closed"));
            }
            other => panic!("expected closed err, got {other:?}"),
        }
        let _ = std::fs::remove_file(&tmp);
    }

    #[test]
    fn shared_update_watcher_fans_out_to_many_subscribers() {
        let tmp = std::env::temp_dir().join(format!("honker-shared-test-{}", std::process::id()));
        let _ = std::fs::remove_file(&tmp);
        // Create a real SQLite database in WAL mode so the watcher can
        // open a read-only connection and poll data_version.
        {
            let conn = Connection::open(&tmp).unwrap();
            conn.execute_batch("PRAGMA journal_mode = WAL;").unwrap();
        }

        let shared = SharedUpdateWatcher::new(tmp.clone());
        let subs: Vec<(u64, std::sync::mpsc::Receiver<()>)> =
            (0..50).map(|_| shared.subscribe()).collect();

        // Open a separate writer connection to trigger commits.
        let writer = Connection::open(&tmp).unwrap();
        writer
            .execute(
                "CREATE TABLE IF NOT EXISTS _test_trigger(id INTEGER PRIMARY KEY)",
                [],
            )
            .unwrap();
        for i in 0..5 {
            std::thread::sleep(Duration::from_millis(5));
            writer
                .execute("INSERT INTO _test_trigger(id) VALUES (?)", [i])
                .unwrap();
        }
        std::thread::sleep(Duration::from_millis(50));

        for (i, (_id, rx)) in subs.iter().enumerate() {
            let mut got_any = false;
            while rx.try_recv().is_ok() {
                got_any = true;
            }
            assert!(got_any, "subscriber {} saw no ticks", i);
        }

        let _ = std::fs::remove_file(&tmp);
    }

    #[test]
    fn shared_update_watcher_explicit_unsubscribe_disconnects_receiver() {
        let tmp = std::env::temp_dir().join(format!("honker-unsub-test-{}", std::process::id()));
        let _ = std::fs::remove_file(&tmp);
        {
            let conn = Connection::open(&tmp).unwrap();
            conn.execute_batch("PRAGMA journal_mode = WAL;").unwrap();
        }

        let shared = SharedUpdateWatcher::new(tmp.clone());
        let (id, rx) = shared.subscribe();
        assert_eq!(shared.subscriber_count(), 1);

        shared.unsubscribe(id);
        assert_eq!(shared.subscriber_count(), 0);

        // Receiver now sees Err on blocking recv — the contract that
        // lets a bridge thread exit cleanly when its UpdateEvents drops.
        assert!(rx.recv().is_err());

        let _ = std::fs::remove_file(&tmp);
    }

    #[test]
    fn shared_update_watcher_prunes_subscribers_when_receiver_dropped() {
        let tmp = std::env::temp_dir().join(format!("honker-prune-test-{}", std::process::id()));
        let _ = std::fs::remove_file(&tmp);
        {
            let conn = Connection::open(&tmp).unwrap();
            conn.execute_batch("PRAGMA journal_mode = WAL;").unwrap();
        }

        let shared = SharedUpdateWatcher::new(tmp.clone());
        {
            let _subs: Vec<_> = (0..10).map(|_| shared.subscribe()).collect();
            assert_eq!(shared.subscriber_count(), 10);
        }

        // Trigger commits so the watcher attempts to send on each
        // dropped receiver and prunes them.
        let writer = Connection::open(&tmp).unwrap();
        writer
            .execute(
                "CREATE TABLE IF NOT EXISTS _test_prune(id INTEGER PRIMARY KEY)",
                [],
            )
            .unwrap();
        // Poll for pruning instead of sleeping a fixed duration —
        // the 1 ms poll thread needs to notice the commit AND
        // attempt to send on each dropped receiver AND prune. Under
        // parallel test load, 30 ms is not enough.
        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        while shared.subscriber_count() != 0 && std::time::Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(5));
            writer
                .execute("INSERT INTO _test_prune(id) VALUES (random())", [])
                .unwrap();
        }
        assert_eq!(shared.subscriber_count(), 0);

        let _ = std::fs::remove_file(&tmp);
    }

    #[test]
    fn data_version_detects_commits_and_ignores_rollbacks() {
        let tmp = std::env::temp_dir().join(format!("honker-dv-test-{}", std::process::id()));
        let _ = std::fs::remove_file(&tmp);
        // PRAGMA data_version detects changes from OTHER connections.
        let watcher = Connection::open(&tmp).unwrap();
        watcher.execute_batch("PRAGMA journal_mode = WAL;").unwrap();
        let writer = Connection::open(&tmp).unwrap();

        let v0 = poll_data_version(&watcher).unwrap();

        // Commit increments data_version (observed by watcher).
        writer.execute("CREATE TABLE t(x INTEGER)", []).unwrap();
        let v1 = poll_data_version(&watcher).unwrap();
        assert!(v1 > v0, "commit should increment data_version");

        // Rollback does NOT increment data_version.
        writer.execute_batch("BEGIN IMMEDIATE;").unwrap();
        writer.execute("INSERT INTO t VALUES (1)", []).unwrap();
        writer.execute_batch("ROLLBACK;").unwrap();
        let v2 = poll_data_version(&watcher).unwrap();
        assert_eq!(v2, v1, "rollback should not increment data_version");

        let _ = std::fs::remove_file(&tmp);
    }

    #[test]
    fn data_version_survives_wal_checkpoint() {
        let tmp = std::env::temp_dir().join(format!("honker-dv-ckpt-test-{}", std::process::id()));
        let _ = std::fs::remove_file(&tmp);
        // Watcher connection — observe changes from the writer.
        let watcher = Connection::open(&tmp).unwrap();
        watcher.execute_batch("PRAGMA journal_mode = WAL;").unwrap();
        let w0 = poll_data_version(&watcher).unwrap();

        // Writer connection — make changes.
        let writer = Connection::open(&tmp).unwrap();
        writer.execute("CREATE TABLE t(x INTEGER)", []).unwrap();
        let w1 = poll_data_version(&watcher).unwrap();
        assert!(
            w1 > w0,
            "commit from other conn should increment data_version"
        );

        // Checkpoint truncates WAL; watcher should still see the change.
        writer
            .execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
            .unwrap();
        let w2 = poll_data_version(&watcher).unwrap();
        assert!(
            w2 > w1,
            "checkpoint from other conn should increment data_version"
        );

        // Post-checkpoint commit still detected.
        writer.execute("INSERT INTO t VALUES (1)", []).unwrap();
        let w3 = poll_data_version(&watcher).unwrap();
        assert!(
            w3 > w2,
            "post-checkpoint commit should increment data_version"
        );

        let _ = std::fs::remove_file(&tmp);
    }

    // Gate to platforms where stat_identity returns real values.
    // On other targets the function returns (0, 0) for every call,
    // so the assert_ne! below would fire.
    #[cfg(any(unix, windows))]
    #[test]
    fn stat_identity_detects_file_replacement() {
        let tmp = std::env::temp_dir().join(format!("honker-id-test-{}", std::process::id()));
        let tmp2 = std::env::temp_dir().join(format!("honker-id-test2-{}", std::process::id()));
        let _ = std::fs::remove_file(&tmp);
        let _ = std::fs::remove_file(&tmp2);

        // Create two distinct files.
        std::fs::write(&tmp, b"original").unwrap();
        std::fs::write(&tmp2, b"replacement").unwrap();

        let id1 = stat_identity(&tmp).unwrap();
        let id2 = stat_identity(&tmp2).unwrap();
        assert_ne!(id1, id2, "different files should have different identities");

        // After atomic rename, tmp now has tmp2's identity.
        std::fs::rename(&tmp2, &tmp).unwrap();
        let id3 = stat_identity(&tmp).unwrap();
        assert_eq!(
            id3, id2,
            "renamed file should carry the replacement's identity"
        );

        let _ = std::fs::remove_file(&tmp);
    }

    /// Direct test of the XOR-fold logic on synthetic 128-bit
    /// inputs. CI runners use NTFS, so the live `stat_identity` test
    /// above never exercises the `HighRes` arm with non-zero upper
    /// bits. This unit test does.
    #[cfg(any(unix, windows))]
    #[test]
    fn fold_high_res_uses_both_halves() {
        // NTFS-shaped: upper = 0, fold == lower.
        let (vsn, idx) = fold_high_res(0xAABB, 0x0000_0000_0000_0000_DEAD_BEEF_CAFE_F00D);
        assert_eq!(vsn, 0xAABB);
        assert_eq!(idx, 0xDEAD_BEEF_CAFE_F00D);

        // ReFS-shaped: both halves non-zero, fold == upper XOR lower.
        let upper = 0x1111_2222_3333_4444u64;
        let lower = 0x5555_6666_7777_8888u64;
        let file_id = ((upper as u128) << 64) | (lower as u128);
        let (vsn, idx) = fold_high_res(0xCCDD, file_id);
        assert_eq!(vsn, 0xCCDD);
        assert_eq!(idx, upper ^ lower);

        // Adversarial: upper == lower → fold == 0. This is the known
        // XOR weakness; documented and acceptable because ReFS
        // file_ids aren't constructed to satisfy this property.
        let same = 0xDEAD_BEEF_CAFE_F00Du64;
        let file_id = ((same as u128) << 64) | (same as u128);
        let (_, idx) = fold_high_res(0, file_id);
        assert_eq!(idx, 0);
    }

    /// Verifies the `UpdateWatcher` dead-man's-switch panics when the
    /// underlying database file is replaced under it. Joins the
    /// watcher thread and inspects the panic payload — no global
    /// panic-hook trickery needed because `UpdateWatcher::join()`
    /// returns the thread's `Result`.
    ///
    /// Unix-only. The test triggers replacement via `std::fs::rename`
    /// over the open SQLite file, which is a normal operation on
    /// Linux / macOS and the actual scenario the dead-man's switch
    /// defends against — litestream restore, atomic backup swap, NFS
    /// remount. On Windows the kernel rejects the rename with
    /// `ERROR_ACCESS_DENIED` even though SQLite opens with
    /// `FILE_SHARE_DELETE`, so the test can't trigger the scenario
    /// it's verifying. Atomic open-file replacement isn't a typical
    /// Windows pattern in practice; the Windows behavior of the
    /// dead-man's switch is intentionally untested.
    #[cfg(unix)]
    #[test]
    fn update_watcher_panics_on_file_replacement() {
        let tmp =
            std::env::temp_dir().join(format!("honker-watcher-replace-{}", std::process::id()));
        let _ = std::fs::remove_file(&tmp);

        // Create the DB so the watcher can open + stat it.
        {
            let conn = Connection::open(&tmp).unwrap();
            conn.execute_batch("PRAGMA journal_mode = WAL;").unwrap();
        }

        let watcher = UpdateWatcher::spawn(tmp.clone(), || {});

        // Give the watcher thread time to open and capture the initial
        // file identity before we replace the file.
        std::thread::sleep(Duration::from_millis(200));

        // Replace the file. Atomic-rename instead of delete+create so
        // it works even when SQLite has the destination open
        // (Windows allows replace-on-rename for files opened with
        // FILE_SHARE_DELETE, which SQLite uses).
        let tmp2 =
            std::env::temp_dir().join(format!("honker-watcher-replace-new-{}", std::process::id()));
        let _ = std::fs::remove_file(&tmp2);
        {
            let conn = Connection::open(&tmp2).unwrap();
            conn.execute_batch("PRAGMA journal_mode = WAL;").unwrap();
        }
        std::fs::rename(&tmp2, &tmp).unwrap();

        // Wait for the next time-based identity check to fire and
        // panic.
        std::thread::sleep(Duration::from_millis(500));

        // Stop and join. Should be Err because the thread panicked.
        let result = watcher.join();
        assert!(
            result.is_err(),
            "watcher should have panicked on file replacement, instead got Ok"
        );
        let payload = result.unwrap_err();
        let msg = if let Some(s) = payload.downcast_ref::<String>() {
            s.clone()
        } else if let Some(s) = payload.downcast_ref::<&str>() {
            (*s).to_string()
        } else {
            String::from("<panic payload not a string>")
        };
        assert!(
            msg.contains("database file replaced"),
            "panic message should mention replacement; got: {msg}"
        );

        let _ = std::fs::remove_file(&tmp);
    }

    /// Verify `poll_data_version` detects cross-connection commits in
    /// every supported journal mode. WAL was the only mode that had
    /// explicit coverage before; the bootstrap-without-database update in
    /// commit `c6716d5` made the watcher work in any mode but never
    /// added tests for the others. This closes that gap.
    fn poll_data_version_works_in_journal_mode(mode: &str) {
        let tmp = std::env::temp_dir().join(format!(
            "honker-jm-{}-{}",
            mode.to_ascii_lowercase(),
            std::process::id()
        ));
        let _ = std::fs::remove_file(&tmp);

        let watcher = Connection::open(&tmp).unwrap();
        watcher
            .execute_batch(&format!("PRAGMA journal_mode = {mode};"))
            .unwrap();

        // Verify the mode actually took effect. SQLite returns the
        // resulting mode from the PRAGMA, but `execute_batch`
        // discards the result — without this assertion, a silent
        // fallback (e.g., to `MEMORY` for `:memory:` databases, or
        // a sticky setting that won't change) would leave the test
        // green while exercising a different mode entirely.
        let actual: String = watcher
            .pragma_query_value(None, "journal_mode", |r| r.get(0))
            .unwrap();
        assert_eq!(
            actual.to_ascii_uppercase(),
            mode.to_ascii_uppercase(),
            "PRAGMA journal_mode = {mode} silently fell back to {actual}"
        );

        let writer = Connection::open(&tmp).unwrap();

        let v0 = poll_data_version(&watcher).unwrap();

        // Commit increments data_version (observed across connections).
        writer.execute("CREATE TABLE t(x INTEGER)", []).unwrap();
        let v1 = poll_data_version(&watcher).unwrap();
        assert!(
            v1 > v0,
            "journal_mode={mode}: cross-conn commit should bump \
             data_version; saw {v0} -> {v1}"
        );

        // Rollback should NOT increment data_version (still true in
        // non-WAL modes — the docs are journal-mode-agnostic on this).
        writer.execute_batch("BEGIN IMMEDIATE;").unwrap();
        writer.execute("INSERT INTO t VALUES (1)", []).unwrap();
        writer.execute_batch("ROLLBACK;").unwrap();
        let v2 = poll_data_version(&watcher).unwrap();
        assert_eq!(
            v2, v1,
            "journal_mode={mode}: rollback should not bump data_version"
        );

        let _ = std::fs::remove_file(&tmp);
        let _ = std::fs::remove_file(format!("{}-wal", tmp.display()));
        let _ = std::fs::remove_file(format!("{}-shm", tmp.display()));
        let _ = std::fs::remove_file(format!("{}-journal", tmp.display()));
    }

    #[test]
    fn poll_data_version_works_in_wal() {
        poll_data_version_works_in_journal_mode("WAL");
    }

    #[test]
    fn poll_data_version_works_in_delete() {
        poll_data_version_works_in_journal_mode("DELETE");
    }

    #[test]
    fn poll_data_version_works_in_truncate() {
        poll_data_version_works_in_journal_mode("TRUNCATE");
    }

    #[test]
    fn poll_data_version_works_in_persist() {
        poll_data_version_works_in_journal_mode("PERSIST");
    }

    // MEMORY journal mode is per-connection (the journal lives in
    // RAM, not a file), so cross-connection rollback semantics are
    // different. SQLite's docs are clear that MEMORY is intended for
    // single-process use. honker doesn't promise MEMORY support, so
    // we don't test it here — flagging in case it ever becomes a
    // user-visible question.

    /// Crash-recovery / durability test. Spawns a child writer
    /// process (python3, available on every CI runner) that hammers
    /// the DB with committed inserts. Hard-kills the child mid-flight
    /// (SIGKILL on unix, `TerminateProcess` via std on Windows),
    /// reopens the DB in the parent, asserts:
    ///
    ///   1. `PRAGMA integrity_check` returns "ok" (no corruption
    ///      from the crash-mid-WAL state).
    ///   2. Some rows committed before the kill are still present.
    ///   3. Re-opening the DB succeeds (WAL replay works after a
    ///      hard kill).
    ///
    /// Cross-platform: `Child::kill` is portable, `python3` and
    /// `sqlite3.connect` work the same on all three OSes. The
    /// scenario being tested — process dies with WAL writes
    /// outstanding — is universal, not unix-specific. Worth running
    /// on Windows specifically because Windows file-locking
    /// semantics differ; if `Child::kill` doesn't release the WAL
    /// + SHM file handles cleanly, the parent's reopen will fail
    /// and we'll know.
    #[test]
    fn writer_killed_mid_workload_leaves_db_consistent() {
        use std::process::{Command, Stdio};

        // Locate a Python interpreter. setup-python in CI puts both
        // `python` and `python3` on PATH on every OS; locally
        // either may be present. Try in order. If neither resolves,
        // skip with a clear message rather than leaving the test
        // mysteriously red on a stripped-down dev box.
        let python = ["python3", "python"]
            .iter()
            .find(|cmd| {
                Command::new(cmd)
                    .arg("--version")
                    .stdout(Stdio::null())
                    .stderr(Stdio::null())
                    .status()
                    .map(|s| s.success())
                    .unwrap_or(false)
            })
            .map(|s| *s);
        let Some(python) = python else {
            eprintln!(
                "writer_killed_mid_workload_leaves_db_consistent: \
                 no `python3` or `python` on PATH; skipping (set up Python \
                 to exercise the crash-recovery path)"
            );
            return;
        };

        let tmp = std::env::temp_dir().join(format!("honker-crash-{}", std::process::id()));
        let _ = std::fs::remove_file(&tmp);
        let _ = std::fs::remove_file(format!("{}-wal", tmp.display()));
        let _ = std::fs::remove_file(format!("{}-shm", tmp.display()));

        // Bootstrap schema + WAL mode in the parent.
        {
            let conn = Connection::open(&tmp).unwrap();
            conn.execute_batch(
                "PRAGMA journal_mode = WAL;
                 PRAGMA synchronous = NORMAL;
                 CREATE TABLE q(id INTEGER PRIMARY KEY AUTOINCREMENT, v INTEGER);",
            )
            .unwrap();
        }

        // Spawn a Python child that writes committed rows in a tight
        // loop. Open DB in WAL mode + synchronous=NORMAL to match
        // honker's default. Each iteration is its own auto-commit
        // transaction. The path is debug-formatted so quoting is
        // correct on every platform (Windows backslashes are
        // escaped, unix paths get safe quoting).
        let writer_script = format!(
            r#"
import sqlite3
conn = sqlite3.connect({path:?})
conn.execute("PRAGMA journal_mode = WAL")
conn.execute("PRAGMA synchronous = NORMAL")
i = 0
while True:
    conn.execute("INSERT INTO q(v) VALUES (?)", (i,))
    conn.commit()
    i += 1
"#,
            path = tmp.to_str().unwrap()
        );

        let mut child = Command::new(python)
            .arg("-c")
            .arg(&writer_script)
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap_or_else(|e| panic!("spawn {python} child writer: {e}"));

        // Poll the database from a separate connection until we see
        // at least one committed row. This turns a timing-fragile
        // "sleep N ms and hope" into a deterministic "kill once
        // we've observed a commit" — robust across slow-Python
        // startup on Windows, loaded CI runners, etc.
        let read_conn = Connection::open(&tmp).unwrap();
        let deadline = std::time::Instant::now() + Duration::from_secs(15);
        let mut high_water: i64 = 0;
        while std::time::Instant::now() < deadline {
            // Bail early if the child died — surface its stderr
            // rather than the downstream "got 0 rows" symptom.
            if let Ok(Some(status)) = child.try_wait() {
                let mut stderr = String::new();
                if let Some(mut s) = child.stderr.take() {
                    use std::io::Read;
                    let _ = s.read_to_string(&mut stderr);
                }
                panic!(
                    "python child exited before kill (status={status:?}); \
                     stderr: {stderr}"
                );
            }
            if let Ok(c) = read_conn.query_row("SELECT count(*) FROM q", [], |r| r.get::<_, i64>(0))
            {
                if c > 0 {
                    high_water = c;
                    break;
                }
            }
            std::thread::sleep(Duration::from_millis(50));
        }
        // Drop the read connection before kill so we don't hold any
        // shared lock when the child's process is reaped.
        drop(read_conn);

        // Let a few more commits accumulate so we test "lots of
        // committed transactions, then sudden death" rather than
        // "exactly one commit" — gives the WAL-replay path
        // something more interesting to recover.
        std::thread::sleep(Duration::from_millis(200));

        // Hard kill. `Child::kill` sends SIGKILL on unix and
        // `TerminateProcess` on Windows. No chance for graceful
        // close — file handles are released by the OS, and any
        // outstanding writes-since-last-fsync are lost.
        let _ = child.kill();
        let _ = child.wait();

        // Reopen and verify. On Windows the OS may take a moment
        // to fully release the killed process's file locks; a tight
        // retry loop on the open absorbs that without flaking.
        let conn = (0..20)
            .find_map(|i| match Connection::open(&tmp) {
                Ok(c) => Some(c),
                Err(_) => {
                    std::thread::sleep(Duration::from_millis(50 * (i + 1)));
                    None
                }
            })
            .unwrap_or_else(|| {
                Connection::open(&tmp).expect("reopen after retry budget exhausted")
            });
        let integrity: String = conn
            .query_row("PRAGMA integrity_check", [], |r| r.get(0))
            .unwrap();
        assert_eq!(
            integrity, "ok",
            "DB should be intact after writer hard-kill during WAL writes"
        );

        let count: i64 = conn
            .query_row("SELECT count(*) FROM q", [], |r| r.get(0))
            .unwrap();
        // Stronger durability assertion: at least the rows we
        // observed before kill must still be there. (Likely many
        // more committed in the +200ms window before the kill —
        // we're checking the floor, not the exact count.)
        assert!(
            count >= high_water,
            "lost committed rows: observed {high_water} before kill, \
             only {count} present after reopen"
        );
        assert!(
            count > 0,
            "expected the child to commit some rows in the 15s window \
             before timeout; got {count}"
        );

        // Drop the connection before cleanup — Windows can't unlink
        // open files. (Linux/macOS tolerate this either way.)
        drop(conn);

        let _ = std::fs::remove_file(&tmp);
        let _ = std::fs::remove_file(format!("{}-wal", tmp.display()));
        let _ = std::fs::remove_file(format!("{}-shm", tmp.display()));
    }

    /// Long-running soak. Spawns an `UpdateWatcher` plus a committer
    /// thread, lets them run for `HONKER_SOAK_DURATION_SECS` (default
    /// 3600 = 1 hour), then asserts:
    ///
    ///   * `PRAGMA integrity_check` returns "ok" — DB structurally
    ///     intact after a long stress.
    ///   * the queue table has exactly the writer's reported row
    ///     count — catches "writer silently lost rows" regressions.
    ///   * the watcher observed at least 10% of the expected wake
    ///     rate — catches "watcher silently stopped" regressions.
    ///
    /// What this **doesn't** verify (yet): memory / FD / thread
    /// leaks. Real leak detection would need to read
    /// `/proc/self/status` (Linux) or equivalent to track VmRSS, FD
    /// count, and thread count across the soak. Not currently
    /// implemented; running this manually under `valgrind` /
    /// `heaptrack` is the substitute. Tracked as a follow-up under
    /// issue #12.
    ///
    /// `#[ignore]`-d so it doesn't run in normal `cargo test` and
    /// doesn't run in CI at all. Invoke manually:
    ///
    /// ```sh
    /// HONKER_SOAK_DURATION_SECS=600 \
    ///     cargo test -p honker-core --release --lib \
    ///         soak_watcher_durability -- --ignored --nocapture
    /// ```
    ///
    /// Set `HONKER_SOAK_DURATION_SECS=10` for a quick local
    /// smoke-run before pushing soak-relevant changes.
    #[test]
    #[ignore]
    fn soak_watcher_durability() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicU64, Ordering};

        let duration_secs: u64 = std::env::var("HONKER_SOAK_DURATION_SECS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(3600);

        eprintln!("soak: running for {duration_secs} seconds");

        let tmp = std::env::temp_dir().join(format!("honker-soak-{}", std::process::id()));
        let _ = std::fs::remove_file(&tmp);
        let _ = std::fs::remove_file(format!("{}-wal", tmp.display()));
        let _ = std::fs::remove_file(format!("{}-shm", tmp.display()));

        {
            let conn = Connection::open(&tmp).unwrap();
            conn.execute_batch(
                "PRAGMA journal_mode = WAL;
                 PRAGMA synchronous = NORMAL;
                 CREATE TABLE q(id INTEGER PRIMARY KEY AUTOINCREMENT, v INTEGER);",
            )
            .unwrap();
        }

        let observed = Arc::new(AtomicU64::new(0));
        let observed_w = observed.clone();
        let watcher = UpdateWatcher::spawn(tmp.clone(), move || {
            observed_w.fetch_add(1, Ordering::Relaxed);
        });

        // Committer thread. Commits ~100/sec — pacing keeps WAL from
        // growing unboundedly between checkpoints and gives the
        // watcher time to actually observe each change.
        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let stop_w = stop.clone();
        let tmp_w = tmp.clone();
        let writer_handle = std::thread::Builder::new()
            .name("soak-writer".into())
            .spawn(move || {
                let conn = Connection::open(&tmp_w).unwrap();
                let mut i: i64 = 0;
                while !stop_w.load(Ordering::Acquire) {
                    conn.execute("INSERT INTO q(v) VALUES (?1)", [i]).unwrap();
                    i += 1;
                    std::thread::sleep(Duration::from_millis(10));
                }
                i
            })
            .unwrap();

        // Run the soak.
        std::thread::sleep(Duration::from_secs(duration_secs));

        // Stop the writer and join. join() returns Err if the
        // thread panicked; surface that explicitly rather than the
        // opaque `unwrap` panic-on-Err message.
        stop.store(true, Ordering::Release);
        let writer_result = writer_handle.join();
        assert!(
            writer_result.is_ok(),
            "writer thread panicked during soak: {writer_result:?}"
        );
        let writes = writer_result.unwrap();

        // Stop the watcher and join. join() returns Err if it
        // panicked; for a clean soak we expect Ok.
        let watcher_result = watcher.join();
        assert!(
            watcher_result.is_ok(),
            "watcher thread panicked during soak: {watcher_result:?}"
        );

        // Verify integrity, row count, and that the watcher observed
        // a reasonable fraction of the writes.
        let conn = Connection::open(&tmp).unwrap();
        let integrity: String = conn
            .query_row("PRAGMA integrity_check", [], |r| r.get(0))
            .unwrap();
        assert_eq!(integrity, "ok", "soak ended with corrupt DB");

        let count: i64 = conn
            .query_row("SELECT count(*) FROM q", [], |r| r.get(0))
            .unwrap();
        assert_eq!(
            count, writes,
            "row count {count} should match writer's reported {writes}"
        );

        let observed_count = observed.load(Ordering::Relaxed);
        // The committer commits every 10ms → ~100 wakes/sec
        // expected. Floor at 10% of that absorbs runner jitter,
        // merged ticks (multiple commits in one watcher poll), and
        // initial-warmup time. Anything below this floor means the
        // watcher silently stalled or fired far below the commit
        // rate — both real regressions worth catching.
        let expected = duration_secs * 100;
        let floor = expected / 10;
        assert!(
            observed_count >= floor,
            "watcher saw only {observed_count} wakes in {duration_secs}s; \
             expected ≥ {floor} (10% of theoretical {expected}; writer committed {writes})"
        );

        eprintln!(
            "soak: {duration_secs}s, {writes} writes, {observed_count} observed wakes, integrity ok"
        );

        let _ = std::fs::remove_file(&tmp);
        let _ = std::fs::remove_file(format!("{}-wal", tmp.display()));
        let _ = std::fs::remove_file(format!("{}-shm", tmp.display()));
    }

    #[test]
    fn bootstrap_honker_schema_creates_tables_and_index() {
        let conn = mem();
        bootstrap_honker_schema(&conn).unwrap();

        // Idempotent.
        bootstrap_honker_schema(&conn).unwrap();

        // _honker_live has the 12 columns we expect (Python binding
        // and the extension have historically disagreed on _honker_dead
        // column count; this pins both).
        let live_cols: Vec<String> = conn
            .prepare("SELECT name FROM pragma_table_info('_honker_live')")
            .unwrap()
            .query_map([], |r| r.get::<_, String>(0))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(live_cols.len(), 12);
        assert!(live_cols.contains(&"expires_at".to_string()));

        let dead_cols: Vec<String> = conn
            .prepare("SELECT name FROM pragma_table_info('_honker_dead')")
            .unwrap()
            .query_map([], |r| r.get::<_, String>(0))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(dead_cols.len(), 10);
        assert!(dead_cols.contains(&"priority".to_string()));
        assert!(dead_cols.contains(&"run_at".to_string()));
        assert!(dead_cols.contains(&"max_attempts".to_string()));
        assert!(dead_cols.contains(&"created_at".to_string()));

        // Partial index present.
        let idx: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master
                 WHERE type='index' AND name='_honker_live_claim'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(idx, 1);

        // _honker_locks table present for db.lock() support.
        let locks_cols: Vec<String> = conn
            .prepare("SELECT name FROM pragma_table_info('_honker_locks')")
            .unwrap()
            .query_map([], |r| r.get::<_, String>(0))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(locks_cols, vec!["name", "owner", "expires_at"]);

        // _honker_rate_limits table present for db.try_rate_limit().
        let rl_cols: Vec<String> = conn
            .prepare("SELECT name FROM pragma_table_info('_honker_rate_limits')")
            .unwrap()
            .query_map([], |r| r.get::<_, String>(0))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(rl_cols, vec!["name", "window_start", "count"]);

        // _honker_scheduler_tasks table present for Scheduler's
        // per-task registration + next_fire_at persistence.
        let sched_cols: Vec<String> = conn
            .prepare("SELECT name FROM pragma_table_info('_honker_scheduler_tasks')")
            .unwrap()
            .query_map([], |r| r.get::<_, String>(0))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(
            sched_cols,
            vec![
                "name",
                "queue",
                "cron_expr",
                "payload",
                "priority",
                "expires_s",
                "next_fire_at",
            ],
        );
        let res_cols: Vec<String> = conn
            .prepare("SELECT name FROM pragma_table_info('_honker_results')")
            .unwrap()
            .query_map([], |r| r.get::<_, String>(0))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(
            res_cols,
            vec!["job_id", "value", "created_at", "expires_at"]
        );

        // _honker_stream + _honker_stream_consumers tables for
        // durable pub/sub streams.
        let stream_cols: Vec<String> = conn
            .prepare("SELECT name FROM pragma_table_info('_honker_stream')")
            .unwrap()
            .query_map([], |r| r.get::<_, String>(0))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(
            stream_cols,
            vec!["offset", "topic", "key", "payload", "created_at"]
        );
        let sc_cols: Vec<String> = conn
            .prepare("SELECT name FROM pragma_table_info('_honker_stream_consumers')")
            .unwrap()
            .query_map([], |r| r.get::<_, String>(0))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(sc_cols, vec!["name", "topic", "offset"]);
    }
}
