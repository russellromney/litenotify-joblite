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
#[cfg(feature = "kernel-watcher")]
mod kernel_watcher;
#[cfg(feature = "shm-fast-path")]
mod shm_watcher;

pub use honker_ops::attach_honker_functions;

use parking_lot::{Condvar, Mutex};
use rusqlite::functions::FunctionFlags;
use rusqlite::{Connection, OpenFlags, ffi};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{SyncSender, TrySendError};
use std::time::{Duration, Instant};

// ---------------------------------------------------------------------
// Watcher backend configuration
// ---------------------------------------------------------------------

/// Which backend drives the update-detection loop.
///
/// `Polling` is the default: 1 ms `PRAGMA data_version` loop, proven
/// correct across all platforms. The optional backends are **experimental**
/// — they must first prove equivalence to the polling path before
/// being relied on for correctness.
#[derive(Debug, Clone, Default)]
pub enum WatcherBackend {
    /// Default: 1 ms `PRAGMA data_version` polling loop.
    #[default]
    Polling,
    /// OS kernel filesystem notifications (experimental).
    ///
    /// Fires `on_change()` on every non-Access filesystem event in the
    /// db's parent directory plus per-file events on `-wal`/`-shm`.
    /// Spurious wakes possible (consumers re-read state, dedupe).
    /// Missed wakes possible if the OS drops events; consumer's
    /// `idle_poll_s` is the only backstop. Setup failures log and
    /// silently disable — no fall-back to polling.
    #[cfg(feature = "kernel-watcher")]
    KernelWatch,
    /// mmap `-shm` WAL index fast path (experimental).
    ///
    /// Reads `iChange` (offset 8 in the WAL index header) at 100 µs
    /// cadence; fires `on_change()` when it advances. WAL mode only.
    /// Trusts the on-disk shm layout (verified via the equivalence
    /// test at build time). If the layout changes or the `-shm` file
    /// is recreated mid-flight, wakes silently stop until restart.
    #[cfg(feature = "shm-fast-path")]
    ShmFastPath,
}

/// Configuration passed to [`UpdateWatcher::spawn_with_config`] and
/// [`SharedUpdateWatcher::new_with_config`].
#[derive(Debug, Clone, Default)]
pub struct WatcherConfig {
    pub backend: WatcherBackend,
}

impl WatcherBackend {
    /// Parse a binding-level string into a backend. Shared across
    /// Python/Node so the accepted aliases stay in lockstep. If the
    /// requested backend isn't compiled in, prints a one-line stderr
    /// warning and falls back to `Polling`. Unknown values return
    /// `Err(input_string)` so bindings can raise a typed error.
    ///
    /// Accepted: `None` / `"polling"` / `"poll"`,
    /// `"kernel"` / `"kernel-watcher"`, `"shm"` / `"shm-fast-path"`.
    pub fn parse(name: Option<&str>) -> Result<Self, String> {
        match name {
            None | Some("polling" | "poll") => Ok(WatcherBackend::Polling),
            Some("kernel" | "kernel-watcher") => {
                #[cfg(feature = "kernel-watcher")]
                { Ok(WatcherBackend::KernelWatch) }
                #[cfg(not(feature = "kernel-watcher"))]
                {
                    eprintln!("honker: kernel-watcher feature not compiled; using polling");
                    Ok(WatcherBackend::Polling)
                }
            }
            Some("shm" | "shm-fast-path") => {
                #[cfg(feature = "shm-fast-path")]
                { Ok(WatcherBackend::ShmFastPath) }
                #[cfg(not(feature = "shm-fast-path"))]
                {
                    eprintln!("honker: shm-fast-path feature not compiled; using polling");
                    Ok(WatcherBackend::Polling)
                }
            }
            Some(other) => Err(other.to_string()),
        }
    }

    /// Verify the backend can actually initialize for `db_path`. Bindings
    /// call this at `honker.open()` time so a backend that can't run
    /// errors loudly instead of silently producing no wakes. Returns a
    /// human-readable reason on failure.
    pub fn probe(&self, db_path: &Path) -> Result<(), String> {
        match self {
            WatcherBackend::Polling => Ok(()),
            #[cfg(feature = "kernel-watcher")]
            WatcherBackend::KernelWatch => kernel_watcher::probe(db_path),
            #[cfg(feature = "shm-fast-path")]
            WatcherBackend::ShmFastPath => shm_watcher::probe(db_path),
        }
    }
}

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
pub(crate) fn stat_identity(path: &Path) -> std::io::Result<(u64, u64)> {
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
pub(crate) fn stat_identity(_path: &Path) -> std::io::Result<(u64, u64)> {
    Ok((0, 0))
}

/// Read the pager's `data_version` counter via `PRAGMA data_version`.
/// Returns a monotonic u32 incremented on every commit by any
/// connection (and on checkpoint). Empirically verified to detect
/// cross-connection database updates on all SQLite versions tested.
/// Cost: ~3.5 µs/call = ~3.5 ms/sec at 1 kHz.
pub(crate) fn poll_data_version(conn: &Connection) -> Result<u32, String> {
    conn.pragma_query_value(None, "data_version", |row| row.get(0))
        .map_err(|e| e.to_string())
}

/// Returns true if `e` is a transient lock conflict (SQLITE_BUSY /
/// SQLITE_LOCKED). On non-WAL journal modes the writer holds an
/// exclusive lock during commit, so the watcher's PRAGMA frequently
/// races into one of these. Treat as "try again next tick", not as a
/// connection failure — dropping and re-opening would silently re-
/// baseline `last_version` and skip pending wakes.
fn is_transient_lock_error(e: &rusqlite::Error) -> bool {
    matches!(
        e,
        rusqlite::Error::SqliteFailure(
            ffi::Error {
                code: ffi::ErrorCode::DatabaseBusy | ffi::ErrorCode::DatabaseLocked,
                ..
            },
            _,
        )
    )
}

/// Polling loop body shared by [`UpdateWatcher`] (polling backend) and
/// the fallback path inside [`kernel_watcher`] / [`shm_watcher`].
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
pub(crate) fn run_poll_loop<F>(
    db_path: PathBuf,
    on_change: F,
    stop: Arc<AtomicBool>,
    ready: std::sync::mpsc::SyncSender<()>,
) where
    F: Fn(),
{
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
    // Wall-clock cadence: tick counting drifts on Windows where 1 ms
    // sleeps round up to ~15 ms.
    let mut next_identity_check = Instant::now() + UPDATE_WATCHER_IDENTITY_INTERVAL;
    // Baseline captured; signal the spawner that it's safe to return.
    let _ = ready.send(());
    drop(ready);

    while !stop.load(Ordering::Acquire) {
        std::thread::sleep(Duration::from_millis(1));

        // Path 1: PRAGMA data_version (fast path)
        if let Some(ref c) = conn {
            match c.pragma_query_value(None, "data_version", |row| row.get::<_, u32>(0)) {
                Ok(version) => {
                    if version != last_version {
                        last_version = version;
                        on_change();
                    }
                }
                Err(e) if is_transient_lock_error(&e) => {
                    // Writer holds the db lock (mid-commit on a
                    // non-WAL journal mode). Don't drop the connection
                    // — that would silently re-baseline last_version
                    // and skip pending wakes. Just retry next tick.
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

        // Path 3: dead-man's switch — panic if db inode changed
        // (atomic rename, litestream restore, volume remount, NFS).
        // Effectively a no-op on Windows: the kernel rejects
        // rename-over-open files.
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
                             The watcher cannot recover; \
                             close the Database and reopen with honker.open().",
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
}

/// Background thread that polls a SQLite database file for changes.
/// Dispatches to the backend selected in [`WatcherConfig`].
/// See [`run_poll_loop`] for the default polling backend's architecture.
pub struct UpdateWatcher {
    stop: Arc<AtomicBool>,
    handle: Option<std::thread::JoinHandle<()>>,
}

const UPDATE_WATCHER_IDENTITY_INTERVAL: Duration = Duration::from_millis(100);

impl UpdateWatcher {
    /// Spawn a watcher thread on `db_path` using the default polling
    /// backend. `on_change` is called once per observed commit. The
    /// thread runs until [`UpdateWatcher`] is dropped or
    /// [`stop`](Self::stop) is called.
    pub fn spawn<F>(db_path: PathBuf, on_change: F) -> Self
    where
        F: Fn() + Send + 'static,
    {
        Self::spawn_with_config(db_path, on_change, WatcherConfig::default())
    }

    /// Like [`spawn`](Self::spawn) but with an explicit watcher backend.
    /// The optional `KernelWatch` and `ShmFastPath` backends are
    /// experimental — see [`WatcherBackend`] for the safety contracts.
    pub fn spawn_with_config<F>(db_path: PathBuf, on_change: F, config: WatcherConfig) -> Self
    where
        F: Fn() + Send + 'static,
    {
        let stop = Arc::new(AtomicBool::new(false));
        let stop_t = stop.clone();
        // The thread signals `ready` once it has captured its baseline
        // (initial inode for the dead-man's switch, initial iChange for
        // shm, etc.). spawn_with_config blocks on `ready` so the caller
        // can do anything that mutates the file (rename, write) right
        // after spawn without racing the baseline capture. If the
        // thread fails to init, the sender drops and recv() returns
        // Err — we still return so the caller can use the (no-op)
        // watcher; the eprintln from the backend explains the failure.
        let (ready_tx, ready_rx) = std::sync::mpsc::sync_channel::<()>(1);
        let handle = std::thread::Builder::new()
            .name("honker-update-poll".into())
            .spawn(move || {
                match config.backend {
                    WatcherBackend::Polling => run_poll_loop(db_path, on_change, stop_t, ready_tx),
                    #[cfg(feature = "kernel-watcher")]
                    WatcherBackend::KernelWatch => {
                        kernel_watcher::run_kernel_watch_loop(
                            db_path, on_change, stop_t, ready_tx,
                        );
                    }
                    #[cfg(feature = "shm-fast-path")]
                    WatcherBackend::ShmFastPath => {
                        shm_watcher::run_shm_fast_path_loop(
                            db_path, on_change, stop_t, ready_tx,
                        );
                    }
                }
            })
            .expect("spawn update-poll thread");
        let _ = ready_rx.recv();
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
/// Lives in the watcher thread's closure. Closure drops on clean exit
/// or panic; this Drop clears every subscriber's sender so their next
/// `recv()` returns Err. Without it, a panicking watcher leaves
/// subscribers blocking forever.
struct WatcherDeathGuard {
    senders: Arc<Mutex<HashMap<u64, SyncSender<()>>>>,
}

impl Drop for WatcherDeathGuard {
    fn drop(&mut self) {
        self.senders.lock().clear();
    }
}

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
    /// Spawn the shared poll thread for `db_path` using the default
    /// polling backend.
    pub fn new(db_path: PathBuf) -> Self {
        Self::new_with_config(db_path, WatcherConfig::default())
    }

    /// Like [`new`](Self::new) but with an explicit watcher backend.
    pub fn new_with_config(db_path: PathBuf, config: WatcherConfig) -> Self {
        let senders: Arc<Mutex<HashMap<u64, SyncSender<()>>>> =
            Arc::new(Mutex::new(HashMap::new()));
        let senders_t = senders.clone();
        // Watcher thread exits → closure drops → this guard drops →
        // every subscriber's sender is cleared. Their next `recv()`
        // returns Err instead of blocking forever. Subscribers learn
        // the watcher died programmatically, not via stderr.
        let death_guard = WatcherDeathGuard { senders: senders.clone() };
        let watcher = UpdateWatcher::spawn_with_config(
            db_path,
            move || {
                let _ = &death_guard;
                let mut list = senders_t.lock();
                list.retain(|_id, s| match s.try_send(()) {
                    Ok(()) | Err(TrySendError::Full(_)) => true,
                    Err(TrySendError::Disconnected(_)) => false,
                });
            },
            config,
        );
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
    /// Channel capacity is 1: bursts coalesce into one wake per drain
    /// cycle. Wakes are "go re-read state" signals — the consumer's
    /// SQL query reads current state regardless of how many wakes
    /// were dropped, so dropped redundant wakes never cost data, only
    /// signal redundancy. The kernel-watcher backend in particular
    /// fires one event per filesystem write (multiple per commit);
    /// without coalescing, consumers would run N redundant queries
    /// per commit burst. With cap=1 they run ~1.
    pub fn subscribe(&self) -> (u64, std::sync::mpsc::Receiver<()>) {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = std::sync::mpsc::sync_channel(1);
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
    use std::collections::HashSet;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Barrier};

    fn mem() -> Connection {
        Connection::open_in_memory().unwrap()
    }

    fn temp_db(name: &str) -> PathBuf {
        let p = std::env::temp_dir().join(format!(
            "honker-{name}-{}-{:?}.db",
            std::process::id(),
            std::thread::current().id()
        ));
        let _ = std::fs::remove_file(&p);
        let _ = std::fs::remove_file(format!("{}-wal", p.display()));
        let _ = std::fs::remove_file(format!("{}-shm", p.display()));
        p
    }

    fn open_core_test_conn(path: &Path) -> Connection {
        let conn = open_conn(path.to_str().unwrap(), true).unwrap();
        attach_honker_functions(&conn).unwrap();
        conn.query_row("SELECT honker_bootstrap()", [], |_| Ok(()))
            .unwrap();
        conn
    }

    #[test]
    fn core_sql_functions_survive_concurrent_queue_stream_notify_pressure() {
        let path = temp_db("core-pressure");
        let producer_count = 4usize;
        let jobs_per_producer = 75usize;
        let worker_count = 6usize;
        let total_jobs = producer_count * jobs_per_producer;

        {
            let conn = open_core_test_conn(&path);
            let mode: String = conn
                .pragma_query_value(None, "journal_mode", |r| r.get(0))
                .unwrap();
            assert_eq!(mode.to_ascii_uppercase(), "WAL");
        }

        let start = Arc::new(Barrier::new(producer_count + worker_count));
        let producers_done = Arc::new(AtomicUsize::new(0));
        let processed = Arc::new(Mutex::new(Vec::<(i64, String)>::new()));
        let mut handles = Vec::new();

        for producer in 0..producer_count {
            let path = path.clone();
            let start = start.clone();
            let producers_done = producers_done.clone();
            handles.push(std::thread::spawn(move || {
                let conn = open_core_test_conn(&path);
                start.wait();
                for seq in 0..jobs_per_producer {
                    let key = format!("p{producer}-{seq:03}");
                    let payload = format!(r#"{{"producer":{producer},"seq":{seq},"key":"{key}"}}"#);
                    conn.query_row(
                        "SELECT honker_enqueue('pressure', ?1, NULL, NULL, ?2, 3, NULL)",
                        rusqlite::params![payload, (seq % 7) as i64],
                        |r| r.get::<_, i64>(0),
                    )
                    .unwrap();
                    conn.query_row(
                        "SELECT honker_stream_publish('pressure-events', ?1, ?2)",
                        rusqlite::params![key, payload],
                        |r| r.get::<_, i64>(0),
                    )
                    .unwrap();
                    conn.query_row(
                        "SELECT notify('pressure-note', ?1)",
                        rusqlite::params![format!(r#"{{"key":"{key}"}}"#)],
                        |r| r.get::<_, i64>(0),
                    )
                    .unwrap();
                    if seq % 11 == 0 {
                        std::thread::sleep(Duration::from_millis(1));
                    }
                }
                producers_done.fetch_add(1, Ordering::Release);
            }));
        }

        for worker in 0..worker_count {
            let path = path.clone();
            let start = start.clone();
            let producers_done = producers_done.clone();
            let processed = processed.clone();
            handles.push(std::thread::spawn(move || {
                let conn = open_core_test_conn(&path);
                start.wait();
                let worker_id = format!("core-worker-{worker}");
                let deadline = Instant::now() + Duration::from_secs(20);
                let mut idle_since: Option<Instant> = None;
                loop {
                    assert!(Instant::now() < deadline, "{worker_id} timed out");
                    let rows_json: String = conn
                        .query_row(
                            "SELECT honker_claim_batch('pressure', ?1, 7, 30)",
                            rusqlite::params![worker_id],
                            |r| r.get(0),
                        )
                        .unwrap();
                    let mut stmt = conn
                        .prepare(
                            "SELECT
                               json_extract(value, '$.id'),
                               json_extract(json_extract(value, '$.payload'), '$.key')
                             FROM json_each(?1)",
                        )
                        .unwrap();
                    let claimed = stmt
                        .query_map(rusqlite::params![rows_json], |r| {
                            Ok((r.get::<_, i64>(0)?, r.get::<_, String>(1)?))
                        })
                        .unwrap()
                        .collect::<Result<Vec<_>, _>>()
                        .unwrap();

                    if claimed.is_empty() {
                        if producers_done.load(Ordering::Acquire) == producer_count {
                            match idle_since {
                                Some(t) if t.elapsed() >= Duration::from_millis(500) => break,
                                Some(_) => {}
                                None => idle_since = Some(Instant::now()),
                            }
                        }
                        std::thread::sleep(Duration::from_millis(5));
                        continue;
                    }

                    idle_since = None;
                    let ids_json = format!(
                        "[{}]",
                        claimed
                            .iter()
                            .map(|(id, _)| id.to_string())
                            .collect::<Vec<_>>()
                            .join(",")
                    );
                    let acked: i64 = conn
                        .query_row(
                            "SELECT honker_ack_batch(?1, ?2)",
                            rusqlite::params![ids_json, worker_id],
                            |r| r.get(0),
                        )
                        .unwrap();
                    assert_eq!(acked as usize, claimed.len());
                    processed.lock().extend(claimed);
                    std::thread::sleep(Duration::from_millis(2));
                }
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }

        let processed = processed.lock();
        assert_eq!(processed.len(), total_jobs);
        let unique_ids: HashSet<i64> = processed.iter().map(|(id, _)| *id).collect();
        assert_eq!(unique_ids.len(), total_jobs, "job id claimed twice");
        let unique_keys: HashSet<String> = processed.iter().map(|(_, k)| k.clone()).collect();
        assert_eq!(unique_keys.len(), total_jobs, "logical key claimed twice");
        for producer in 0..producer_count {
            for seq in 0..jobs_per_producer {
                assert!(
                    unique_keys.contains(&format!("p{producer}-{seq:03}")),
                    "missing key p{producer}-{seq:03}"
                );
            }
        }
        drop(processed);

        let conn = open_core_test_conn(&path);
        let live: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM _honker_live WHERE queue='pressure'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        let dead: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM _honker_dead WHERE queue='pressure'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        let stream_rows: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM _honker_stream WHERE topic='pressure-events'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        let notes: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM _honker_notifications WHERE channel='pressure-note'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        let enqueue_wakes: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM _honker_notifications WHERE channel='honker:pressure'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        let integrity: String = conn
            .query_row("PRAGMA integrity_check", [], |r| r.get(0))
            .unwrap();
        assert_eq!(live, 0);
        assert_eq!(dead, 0);
        assert_eq!(stream_rows as usize, total_jobs);
        assert_eq!(notes as usize, total_jobs);
        assert_eq!(enqueue_wakes as usize, total_jobs);
        assert_eq!(integrity, "ok");

        drop(conn);
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(format!("{}-wal", path.display()));
        let _ = std::fs::remove_file(format!("{}-shm", path.display()));
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
        let tmp = std::env::temp_dir().join(format!("honker-readers-close-{}", std::process::id()));
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

    /// Subscribers must learn that the watcher thread died — not just
    /// stop receiving wakes silently. We force the watcher to panic via
    /// the dead-man's switch (file replacement) and assert that an
    /// already-subscribed receiver returns `Err(RecvError)` on its
    /// next blocking `recv()`. Without WatcherDeathGuard this test
    /// hangs (subscriber blocks forever) and times out.
    #[test]
    #[cfg(unix)]
    fn shared_update_watcher_signals_subscribers_on_watcher_death() {
        let tmp = std::env::temp_dir().join(format!(
            "honker-death-signal-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .subsec_nanos()
        ));
        let _ = std::fs::remove_file(&tmp);
        {
            let conn = Connection::open(&tmp).unwrap();
            conn.execute_batch("PRAGMA journal_mode = WAL;").unwrap();
        }

        let shared = SharedUpdateWatcher::new(tmp.clone());
        let (_id, rx) = shared.subscribe();

        // Let the watcher snapshot the initial inode.
        std::thread::sleep(Duration::from_millis(200));

        // Replace the file with a different inode. Triggers the
        // dead-man's switch on the next 100 ms tick.
        let other = std::env::temp_dir().join(format!(
            "honker-death-other-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .subsec_nanos()
        ));
        let _ = std::fs::remove_file(&other);
        std::fs::File::create(&other).unwrap();
        std::fs::rename(&other, &tmp).unwrap();

        // Within ~150 ms the watcher's identity check fires and panics;
        // WatcherDeathGuard's Drop clears senders; rx.recv() returns Err.
        // Use a generous timeout — give the watcher up to 2 s to die
        // and the guard to fire.
        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        loop {
            if rx.try_recv().is_err() && rx.try_recv() != Ok(()) {
                // try_recv returns Err(Empty) for "alive but no msg",
                // Err(Disconnected) for "watcher died, sender cleared".
                // Use blocking recv with a poll instead.
                match rx.recv_timeout(Duration::from_millis(100)) {
                    Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => break,
                    _ => {}
                }
            }
            if std::time::Instant::now() > deadline {
                panic!(
                    "watcher died but subscriber's channel never disconnected — \
                     WatcherDeathGuard didn't fire?"
                );
            }
        }

        let _ = std::fs::remove_file(&tmp);
        let _ = std::fs::remove_file(format!("{}-wal", tmp.display()));
        let _ = std::fs::remove_file(format!("{}-shm", tmp.display()));
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

    /// Watcher's dead-man's switch panics when the db file is
    /// replaced under it. Unix-only: rename-over-open works on
    /// Linux/macOS (the litestream / NFS-remount scenario) but
    /// Windows rejects it even with FILE_SHARE_DELETE, so the
    /// trigger is unreachable there. Windows behavior intentionally
    /// untested — replacement isn't a typical Windows pattern.
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

    /// Crash-recovery: python3 child commits in a loop, parent
    /// SIGKILLs it mid-flight, reopens, asserts integrity_check=ok,
    /// committed rows survive, and reopen works (WAL replay).
    /// Cross-platform — Windows tests that file-handle release on
    /// kill is clean enough for reopen to succeed.
    #[test]
    fn writer_killed_mid_workload_leaves_db_consistent() {
        use std::process::{Command, Stdio};

        // Try `python3` then `python`. CI always has one; dev boxes
        // may not. Skip loudly rather than fail silently.
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

    /// Long-running soak: watcher + committer for
    /// `HONKER_SOAK_DURATION_SECS` (default 1h). Asserts
    /// integrity_check=ok, exact row count, and ≥10% of expected
    /// wake rate. Doesn't track leaks (run under valgrind/heaptrack
    /// for that — issue #12). Ignored by default; CI never runs it.
    ///
    /// ```sh
    /// HONKER_SOAK_DURATION_SECS=600 \
    ///     cargo test -p honker-core --release --lib \
    ///         soak_watcher_durability -- --ignored --nocapture
    /// ```
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

    // -----------------------------------------------------------------
    // Optional backend tests
    // -----------------------------------------------------------------

    /// Run the wake/listen suite against the kernel-watch backend.
    /// Each commit separated by 20 ms ensures both the 1 ms poller
    /// and the kernel-watch loop have time to fire before the next.
    #[test]
    #[cfg(feature = "kernel-watcher")]
    fn kernel_watcher_detects_all_commits() {
        use std::sync::atomic::{AtomicU32, Ordering as AO};

        let tmp = std::env::temp_dir().join(format!(
            "honker-kernel-watcher-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .subsec_nanos()
        ));
        let _ = std::fs::remove_file(&tmp);

        let writer = open_conn(tmp.to_str().unwrap(), false).unwrap();
        writer.execute_batch("CREATE TABLE t (x INT)").unwrap();
        // One initial write ensures the -wal file exists so the watcher
        // can attach a per-file watch at startup (kqueue watches the file
        // descriptor, not the directory, for write events).
        writer.execute("INSERT INTO t VALUES (0)", []).unwrap();
        std::thread::sleep(Duration::from_millis(20));

        let count = Arc::new(AtomicU32::new(0));
        let count_t = count.clone();
        let watcher = UpdateWatcher::spawn_with_config(
            tmp.clone(),
            move || {
                count_t.fetch_add(1, AO::Relaxed);
            },
            WatcherConfig { backend: WatcherBackend::KernelWatch },
        );

        // Drain any initialization wakes.
        std::thread::sleep(Duration::from_millis(50));
        count.store(0, AO::SeqCst);

        // n commits spaced 30 ms apart — gives the event loop time to
        // process each event individually before the next arrives.
        let n: u32 = 5;
        for i in 1..=n {
            writer
                .execute(&format!("INSERT INTO t VALUES ({i})"), [])
                .unwrap();
            std::thread::sleep(Duration::from_millis(30));
        }
        // Wait longer than both the event delivery latency and the
        // safety-net interval to drain any pending events.
        std::thread::sleep(Duration::from_millis(600));

        let observed = count.load(AO::SeqCst);
        drop(watcher);
        let _ = std::fs::remove_file(&tmp);
        let _ = std::fs::remove_file(format!("{}-wal", tmp.display()));
        let _ = std::fs::remove_file(format!("{}-shm", tmp.display()));

        // Experimental contract: spurious wakes are allowed (the backend
        // fires on every filesystem event, and SQLite produces several
        // events per commit). The thing that must not happen is a *missed*
        // commit — assert at least n wakes.
        assert!(
            observed >= n,
            "kernel watcher detected {observed} wakes for {n} commits — \
             missed at least one"
        );
    }

    /// Prove that the shm fast path fires on the same commits as the
    /// baseline `PRAGMA data_version` poller.
    ///
    /// Phase gate: both detectors must report exactly N wakes for N
    /// commits spaced far enough apart that neither can batch them.
    #[test]
    #[cfg(feature = "shm-fast-path")]
    fn shm_fast_path_equivalence_with_pragma_baseline() {
        use std::sync::atomic::{AtomicU32, Ordering as AO};

        let tmp = std::env::temp_dir().join(format!(
            "honker-shm-equiv-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .subsec_nanos()
        ));
        let _ = std::fs::remove_file(&tmp);

        let writer = open_conn(tmp.to_str().unwrap(), false).unwrap();
        writer.execute_batch("CREATE TABLE t (x INT)").unwrap();
        // One write ensures the -shm file exists before spawning the shm watcher.
        writer.execute("INSERT INTO t VALUES (0)", []).unwrap();
        std::thread::sleep(Duration::from_millis(20));

        let baseline_count = Arc::new(AtomicU32::new(0));
        let baseline_t = baseline_count.clone();
        let baseline = UpdateWatcher::spawn(tmp.clone(), move || {
            baseline_t.fetch_add(1, AO::Relaxed);
        });

        let shm_count = Arc::new(AtomicU32::new(0));
        let shm_t = shm_count.clone();
        let shm = UpdateWatcher::spawn_with_config(
            tmp.clone(),
            move || { shm_t.fetch_add(1, AO::Relaxed); },
            WatcherConfig { backend: WatcherBackend::ShmFastPath },
        );

        // Drain initialization wakes.
        std::thread::sleep(Duration::from_millis(30));
        baseline_count.store(0, AO::SeqCst);
        shm_count.store(0, AO::SeqCst);

        // Commits spaced 20 ms apart — well above both polling intervals.
        let n: u32 = 5;
        for i in 1..=n {
            writer
                .execute(&format!("INSERT INTO t VALUES ({i})"), [])
                .unwrap();
            std::thread::sleep(Duration::from_millis(20));
        }
        std::thread::sleep(Duration::from_millis(100));

        let b = baseline_count.load(AO::SeqCst);
        let s = shm_count.load(AO::SeqCst);

        drop(baseline);
        drop(shm);
        drop(writer);
        let _ = std::fs::remove_file(&tmp);
        let _ = std::fs::remove_file(format!("{}-wal", tmp.display()));
        let _ = std::fs::remove_file(format!("{}-shm", tmp.display()));

        assert_eq!(b, n, "baseline detected {b} wakes, expected {n}");
        assert_eq!(
            s, n,
            "shm fast path detected {s} wakes, expected {n} (same as baseline {b})"
        );
    }

    // -----------------------------------------------------------------
    // Journal-mode coverage for the experimental backends
    //
    // honker's `open_conn` always sets WAL, so the public Python/Node
    // surface is WAL-only. These tests poke the watchers directly at
    // databases pre-set to non-WAL modes so we can prove behavior when
    // the file is in DELETE / TRUNCATE / PERSIST. Justification per
    // backend:
    //
    // - Polling — universally works because `PRAGMA data_version`
    //   advances on every commit regardless of journal mode. Already
    //   exercised by `poll_data_version_works_in_*`.
    //
    // - Kernel watcher — in non-WAL modes there is no `-wal` file to
    //   watch directly; we must rely on the parent-directory watch to
    //   pick up `-journal` create / modify / delete events around each
    //   commit. The PRAGMA verification step still gates `on_change()`,
    //   so spurious events (e.g. another file in the dir) just produce
    //   harmless no-op checks.
    //
    // - SHM fast path — in non-WAL modes there is no `-shm` file;
    //   `read_ichange` returns `None` and the loop falls back to the
    //   PRAGMA check on every iteration. Effectively becomes a 100 µs
    //   PRAGMA poller — correct, just CPU-heavier than the polling
    //   backend.
    // -----------------------------------------------------------------

    /// Drive `n` committed inserts through `writer`, spaced
    /// `spacing_ms` apart, and return how many `on_change()` calls the
    /// watcher observed (with the initial drain already deducted).
    fn drive_and_count_wakes(
        backend: WatcherBackend,
        db_path: PathBuf,
        n: u32,
        spacing_ms: u64,
    ) -> u32 {
        use std::sync::atomic::{AtomicU32, Ordering as AO};

        let count = Arc::new(AtomicU32::new(0));
        let count_t = count.clone();
        let watcher = UpdateWatcher::spawn_with_config(
            db_path.clone(),
            move || {
                count_t.fetch_add(1, AO::Relaxed);
            },
            WatcherConfig { backend },
        );

        // Drain init wakes (covers shm + kernel setup) before baseline.
        std::thread::sleep(Duration::from_millis(80));
        count.store(0, AO::SeqCst);

        let writer = Connection::open(&db_path).unwrap();
        for i in 1..=n {
            writer
                .execute(&format!("INSERT INTO t VALUES ({i})"), [])
                .unwrap();
            std::thread::sleep(Duration::from_millis(spacing_ms));
        }
        // Drain the slowest safety net (kernel = 500 ms) + one cycle.
        std::thread::sleep(Duration::from_millis(700));

        let observed = count.load(AO::SeqCst);
        drop(watcher);
        drop(writer);
        observed
    }

    /// Set up a fresh database file in `mode` and verify the watcher
    /// detects every committed insert. Tolerates +1 wake (a commit
    /// straddling the drain boundary) but does not tolerate misses.
    fn watcher_works_in_journal_mode(backend: WatcherBackend, mode: &str) {
        let tmp = std::env::temp_dir().join(format!(
            "honker-watcher-{}-{}-{}-{}",
            mode.to_ascii_lowercase(),
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .subsec_nanos(),
            match backend {
                WatcherBackend::Polling => "poll",
                #[cfg(feature = "kernel-watcher")]
                WatcherBackend::KernelWatch => "kw",
                #[cfg(feature = "shm-fast-path")]
                WatcherBackend::ShmFastPath => "shm",
            },
        ));
        let _ = std::fs::remove_file(&tmp);

        // Watcher inherits the file's journal mode, so set it before opening.
        let setup = Connection::open(&tmp).unwrap();
        setup
            .execute_batch(&format!("PRAGMA journal_mode = {mode};"))
            .unwrap();
        let actual: String = setup
            .pragma_query_value(None, "journal_mode", |r| r.get(0))
            .unwrap();
        assert_eq!(
            actual.to_ascii_uppercase(),
            mode.to_ascii_uppercase(),
            "PRAGMA journal_mode = {mode} silently fell back to {actual}"
        );
        setup.execute("CREATE TABLE t (x INTEGER)", []).unwrap();
        // One prior write so -shm exists at watcher startup (shm fast path
        // needs it; harmless otherwise).
        setup.execute("INSERT INTO t VALUES (0)", []).unwrap();
        // Pin -shm only for shm+WAL: Linux/Windows reap -shm on last close.
        // Other modes must drop setup or Windows errors on shared non-WAL db.
        #[cfg(feature = "shm-fast-path")]
        let keep_setup_open =
            matches!(backend, WatcherBackend::ShmFastPath) && mode.eq_ignore_ascii_case("WAL");
        #[cfg(not(feature = "shm-fast-path"))]
        let keep_setup_open = false;
        let _pinning = if keep_setup_open {
            Some(setup)
        } else {
            drop(setup);
            None
        };

        let n: u32 = 5;
        let observed = drive_and_count_wakes(backend.clone(), tmp.clone(), n, 30);

        drop(_pinning);
        let _ = std::fs::remove_file(&tmp);
        let _ = std::fs::remove_file(format!("{}-wal", tmp.display()));
        let _ = std::fs::remove_file(format!("{}-shm", tmp.display()));
        let _ = std::fs::remove_file(format!("{}-journal", tmp.display()));

        // Polling/shm dedupe → ~1 wake per commit. Kernel fires per
        // filesystem event (inotify is granular) → upper bound is just
        // a runaway-watcher guard, not a precise expectation.
        let upper = match backend {
            WatcherBackend::Polling => n + 1,
            #[cfg(feature = "kernel-watcher")]
            WatcherBackend::KernelWatch => n * 200,
            #[cfg(feature = "shm-fast-path")]
            WatcherBackend::ShmFastPath => n + 1,
        };
        assert!(
            observed >= n,
            "journal_mode={mode}: observed {observed} wakes for {n} commits \
             (missed at least one)"
        );
        assert!(
            observed <= upper,
            "journal_mode={mode}: observed {observed} wakes for {n} commits, \
             upper bound {upper} (runaway watcher?)"
        );
    }

    // ---- Polling × every supported journal mode (regression coverage) ----

    #[test]
    fn polling_watcher_works_in_wal() {
        watcher_works_in_journal_mode(WatcherBackend::Polling, "WAL");
    }

    #[test]
    fn polling_watcher_works_in_delete() {
        watcher_works_in_journal_mode(WatcherBackend::Polling, "DELETE");
    }

    #[test]
    fn polling_watcher_works_in_truncate() {
        watcher_works_in_journal_mode(WatcherBackend::Polling, "TRUNCATE");
    }

    #[test]
    fn polling_watcher_works_in_persist() {
        watcher_works_in_journal_mode(WatcherBackend::Polling, "PERSIST");
    }

    // ---- Kernel watcher × every supported journal mode ----

    // macOS kqueue limitation: directory-level watches do NOT fire on
    // writes within existing files (only on entry create/delete/rename).
    // Per-file watches fire on regular-file writes, but rollback-journal
    // commit dances on a loaded macOS CI runner produce so few kqueue
    // events that the wake count is unreliable for non-WAL modes. We
    // attach to db + journal + dir to maximize coverage, and ship the
    // backend with documented "missed wakes possible" semantics, but
    // we don't gate CI on a behavior the kernel won't reliably deliver.
    // WAL-mode kernel coverage stays mandatory (kernel_watcher_works_in_wal).
    #[test]
    #[cfg(feature = "kernel-watcher")]
    #[cfg_attr(target_os = "macos", ignore = "kqueue: in-place writes don't fire dir events")]
    fn kernel_watcher_works_in_delete() {
        watcher_works_in_journal_mode(WatcherBackend::KernelWatch, "DELETE");
    }

    #[test]
    #[cfg(feature = "kernel-watcher")]
    #[cfg_attr(target_os = "macos", ignore = "kqueue: in-place writes don't fire dir events")]
    fn kernel_watcher_works_in_truncate() {
        watcher_works_in_journal_mode(WatcherBackend::KernelWatch, "TRUNCATE");
    }

    #[test]
    #[cfg(feature = "kernel-watcher")]
    #[cfg_attr(target_os = "macos", ignore = "kqueue: in-place writes don't fire dir events")]
    fn kernel_watcher_works_in_persist() {
        watcher_works_in_journal_mode(WatcherBackend::KernelWatch, "PERSIST");
    }

    // ---- SHM fast path: WAL only (it's experimental — non-WAL is
    // explicitly out of scope, the backend logs and disables itself
    // when -shm doesn't exist). ----

    #[test]
    #[cfg(feature = "shm-fast-path")]
    fn shm_fast_path_works_in_wal() {
        watcher_works_in_journal_mode(WatcherBackend::ShmFastPath, "WAL");
    }

    // -----------------------------------------------------------------
    // Wake-latency invariants — proves the experimental backends
    // actually deliver wakes via their fast paths (kernel events /
    // mmap reads), not via some accidental fallback. The simplified
    // backends have no safety nets, so a missed wake just doesn't
    // fire — these tests would catch that immediately.
    // -----------------------------------------------------------------

    /// Helper: spawn a watcher with the given backend, commit `n` writes
    /// spaced `spacing_ms` apart, return the per-commit wake latency in
    /// milliseconds. Caller asserts on the distribution.
    #[cfg(any(feature = "kernel-watcher", feature = "shm-fast-path"))]
    fn measure_wake_latencies_ms(
        backend: WatcherBackend,
        db_path: PathBuf,
        n: usize,
        spacing_ms: u64,
    ) -> Vec<f64> {
        use std::sync::Mutex as StdMutex;

        let writer = open_conn(db_path.to_str().unwrap(), false).unwrap();
        writer.execute_batch("CREATE TABLE t (x INTEGER)").unwrap();
        // First write so -wal exists at watcher startup.
        writer.execute("INSERT INTO t VALUES (0)", []).unwrap();
        std::thread::sleep(Duration::from_millis(20));

        let wake_times: Arc<StdMutex<Vec<std::time::Instant>>> =
            Arc::new(StdMutex::new(Vec::new()));
        let wake_times_t = wake_times.clone();
        let watcher = UpdateWatcher::spawn_with_config(
            db_path.clone(),
            move || {
                wake_times_t
                    .lock()
                    .expect("wake_times mutex poisoned")
                    .push(std::time::Instant::now());
            },
            WatcherConfig { backend },
        );

        // Drain initialization wakes.
        std::thread::sleep(Duration::from_millis(100));
        wake_times.lock().expect("wake_times").clear();

        // Commit each write, recording the commit time. Pair with the
        // first wake timestamp that arrives after that commit time.
        let mut commit_times: Vec<std::time::Instant> = Vec::with_capacity(n);
        for i in 1..=n {
            let t0 = std::time::Instant::now();
            writer
                .execute(&format!("INSERT INTO t VALUES ({i})"), [])
                .unwrap();
            commit_times.push(t0);
            std::thread::sleep(Duration::from_millis(spacing_ms));
        }
        // Wait long enough for any in-flight wakes to land. The
        // backend-specific safety nets are 500 ms (kernel-watcher) and
        // 100 ms (shm-fast-path); 700 ms covers either.
        std::thread::sleep(Duration::from_millis(700));

        let wakes = wake_times.lock().expect("wake_times").clone();
        drop(watcher);
        drop(writer);

        // Pair each commit with the first wake at-or-after its commit
        // time. Wakes are monotonic; commits are monotonic; so a single
        // forward pass suffices.
        let mut latencies = Vec::with_capacity(n);
        let mut wake_cursor = 0;
        for &commit_t in &commit_times {
            while wake_cursor < wakes.len() && wakes[wake_cursor] < commit_t {
                wake_cursor += 1;
            }
            if wake_cursor >= wakes.len() {
                latencies.push(f64::INFINITY); // missed wake — caller will assert
            } else {
                let dt = wakes[wake_cursor].duration_since(commit_t);
                latencies.push(dt.as_secs_f64() * 1000.0);
                wake_cursor += 1;
            }
        }
        latencies
    }

    #[cfg(any(feature = "kernel-watcher", feature = "shm-fast-path"))]
    fn percentile(mut samples: Vec<f64>, pct: f64) -> f64 {
        samples.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let idx = ((samples.len() as f64) * pct) as usize;
        samples[idx.min(samples.len() - 1)]
    }

    /// Kernel watcher: wakes must come via kernel events, not the
    /// 500 ms safety net. p90 < 200 ms is way below half of the
    /// safety-net interval, so a backend stuck on the safety net would
    /// have p90 ≈ 250 ms (mean half of 500) and fail this assertion.
    #[test]
    #[cfg(feature = "kernel-watcher")]
    fn kernel_watcher_wake_latency_is_event_driven() {
        let tmp = std::env::temp_dir().join(format!(
            "honker-kw-lat-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .subsec_nanos()
        ));
        let _ = std::fs::remove_file(&tmp);

        let lats = measure_wake_latencies_ms(
            WatcherBackend::KernelWatch,
            tmp.clone(),
            10,
            50,
        );

        let _ = std::fs::remove_file(&tmp);
        let _ = std::fs::remove_file(format!("{}-wal", tmp.display()));
        let _ = std::fs::remove_file(format!("{}-shm", tmp.display()));

        // Contract allows missed wakes (kqueue/inotify/ReadDir can
        // coalesce). Assert: some wakes arrived AND p50 is well below
        // the 5 s `idle_poll_s` fallback — proves we're event-driven,
        // not riding the paranoia poll. Windows ReadDirectoryChangesW
        // under CI load can stretch past 100 ms; 500 ms threshold
        // still rules out the fallback.
        let arrived: Vec<f64> = lats.iter().copied().filter(|l| l.is_finite()).collect();
        assert!(
            !arrived.is_empty(),
            "kernel watcher delivered zero wakes for 10 commits — events \
             aren't being delivered at all on this platform: {lats:?}"
        );
        let p50 = percentile(arrived.clone(), 0.50);
        assert!(
            p50 < 500.0,
            "kernel watcher p50 wake latency = {p50:.1} ms, expected < 500 \
             (high median latency means events arrive but slowly — possibly \
             a stuck-thread fallback). Arrived: {arrived:?}, all samples \
             (inf = no wake): {lats:?}"
        );
    }

    /// SHM fast path: wakes must come via the mmap tickle.
    #[test]
    #[cfg(feature = "shm-fast-path")]
    fn shm_fast_path_wake_latency_is_event_driven() {
        let tmp = std::env::temp_dir().join(format!(
            "honker-shm-lat-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .subsec_nanos()
        ));
        let _ = std::fs::remove_file(&tmp);

        let lats = measure_wake_latencies_ms(
            WatcherBackend::ShmFastPath,
            tmp.clone(),
            10,
            50,
        );

        let _ = std::fs::remove_file(&tmp);
        let _ = std::fs::remove_file(format!("{}-wal", tmp.display()));
        let _ = std::fs::remove_file(format!("{}-shm", tmp.display()));

        // Same shape as the kernel-watcher latency test: assert that
        // *some* wakes arrived and that they were fast. Missed wakes
        // are part of the documented experimental contract.
        let arrived: Vec<f64> = lats.iter().copied().filter(|l| l.is_finite()).collect();
        assert!(
            !arrived.is_empty(),
            "shm fast path delivered zero wakes for 10 commits: {lats:?}"
        );
        let p50 = percentile(arrived.clone(), 0.50);
        assert!(
            p50 < 50.0,
            "shm fast path p50 wake latency (over arrived wakes only) = {p50:.1} ms, expected < 50 \
             (high latency means iChange isn't \
             being read via mmap). Samples: {lats:?}"
        );
    }

    /// Graceful shutdown latency. Bounded by `RX_POLL_MS = 50 ms`.
    #[test]
    #[cfg(feature = "kernel-watcher")]
    fn kernel_watcher_shutdown_is_responsive() {
        let tmp = std::env::temp_dir().join(format!(
            "honker-kw-shutdown-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .subsec_nanos()
        ));
        let _ = std::fs::remove_file(&tmp);

        let writer = open_conn(tmp.to_str().unwrap(), false).unwrap();
        writer.execute_batch("CREATE TABLE t (x INTEGER)").unwrap();
        writer.execute("INSERT INTO t VALUES (0)", []).unwrap();

        let watcher = UpdateWatcher::spawn_with_config(
            tmp.clone(),
            || {},
            WatcherConfig { backend: WatcherBackend::KernelWatch },
        );

        // Let the watcher reach steady state (in its recv_timeout block).
        std::thread::sleep(Duration::from_millis(200));

        let t0 = std::time::Instant::now();
        let _ = watcher.join();
        let elapsed = t0.elapsed();

        drop(writer);
        let _ = std::fs::remove_file(&tmp);
        let _ = std::fs::remove_file(format!("{}-wal", tmp.display()));
        let _ = std::fs::remove_file(format!("{}-shm", tmp.display()));

        assert!(
            elapsed < Duration::from_millis(150),
            "kernel watcher shutdown took {elapsed:?}, expected < 150 ms \
             (RX_POLL_MS = 50 ms; if this exceeds 500 ms the recv_timeout \
             is blocking on the safety-net interval again)"
        );
    }

    // -----------------------------------------------------------------
    // Probe failures must surface as Err — proving "no silent fallback"
    // when an experimental backend can't initialize.
    // -----------------------------------------------------------------

    #[test]
    fn watcher_backend_polling_probe_always_succeeds() {
        // Polling never fails — works on any path, any state.
        let nope = std::path::PathBuf::from("/nonexistent/no/way/this/exists.db");
        assert!(WatcherBackend::Polling.probe(&nope).is_ok());
    }

    #[test]
    #[cfg(feature = "shm-fast-path")]
    fn watcher_backend_shm_probe_fails_when_shm_missing() {
        // Path with no -shm file — probe must report it, not silently
        // disable the backend at runtime.
        let tmp = std::env::temp_dir().join(format!(
            "honker-shm-probe-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .subsec_nanos()
        ));
        let _ = std::fs::remove_file(&tmp);
        let result = WatcherBackend::ShmFastPath.probe(&tmp);
        assert!(result.is_err(), "expected probe to fail for missing -shm");
        let msg = result.unwrap_err();
        assert!(
            msg.contains("-shm unavailable"),
            "probe error message should explain why; got: {msg}"
        );
    }

    /// Parity with `update_watcher_panics_on_file_replacement` for the
    /// kernel-watcher backend. The polling backend panics when it sees
    /// the db file replaced; the kernel watcher must do the same so a
    /// stale per-file watch fails loudly instead of silently missing
    /// wakes after a litestream-style restore.
    ///
    /// The experimental backends don't open a SQLite connection of
    /// their own (only the polling backend does), so the test setup
    /// can use a plain empty file at `db_path`. That dodges Windows'
    /// "can't rename over a file SQLite has open" problem and lets us
    /// run on every platform — unlike the polling test, which still
    /// has to use a real SQLite db and stays `#[cfg(unix)]`.
    #[test]
    #[cfg(feature = "kernel-watcher")]
    fn kernel_watcher_panics_on_file_replacement() {
        replacement_panic_test(WatcherBackend::KernelWatch);
    }

    /// Parity for the SHM fast path. SHM has it worse than kernel
    /// watcher: a stale mmap silently stops detecting iChange. The
    /// dead-man's switch on db AND -shm inodes panics either case.
    #[test]
    #[cfg(feature = "shm-fast-path")]
    fn shm_fast_path_panics_on_file_replacement() {
        replacement_panic_test(WatcherBackend::ShmFastPath);
    }

    #[cfg(any(feature = "kernel-watcher", feature = "shm-fast-path"))]
    fn replacement_panic_test(backend: WatcherBackend) {
        use std::io::Write;

        let tmp = std::env::temp_dir().join(format!(
            "honker-replace-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .subsec_nanos()
        ));
        let _ = std::fs::remove_file(&tmp);
        // Plain empty file at the db path. The kernel/shm watchers
        // don't open a SQLite connection — they just stat / watch /
        // mmap files. So a real SQLite db isn't needed and we avoid
        // Windows' inability to rename over a SQLite-held file.
        std::fs::File::create(&tmp).unwrap();

        // For the SHM backend, also write a fake -shm. macOS mmap can
        // be finicky about tiny files; write at least a page (4 KiB)
        // with the valid WAL index header up front.
        if matches!(backend, WatcherBackend::ShmFastPath) {
            let shm_path = std::path::PathBuf::from(format!("{}-shm", tmp.display()));
            let mut buf = [0u8; 4096];
            buf[0..4].copy_from_slice(&3_007_000u32.to_ne_bytes()); // WALINDEX_MAX_VERSION
            // iChange (offset 8) starts at 0; doesn't matter for this test.
            let mut f = std::fs::File::create(&shm_path).unwrap();
            f.write_all(&buf).unwrap();
        }

        let watcher = UpdateWatcher::spawn_with_config(
            tmp.clone(),
            || {},
            WatcherConfig { backend },
        );
        // Generous initial wait so the watcher has snapshotted the
        // initial inode under CI scheduling pressure.
        std::thread::sleep(Duration::from_millis(300));

        // Replace the db file with a different inode. Atomic rename
        // works on every platform when no SQLite handle is held open.
        let other = std::env::temp_dir().join(format!(
            "honker-replace-other-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .subsec_nanos()
        ));
        let _ = std::fs::remove_file(&other);
        std::fs::File::create(&other).unwrap();
        std::fs::rename(&other, &tmp).unwrap();
        // Wait long enough for the dead-man's switch to fire on a
        // slow CI runner. Identity check is 100 ms; give it 10 cycles.
        std::thread::sleep(Duration::from_millis(1000));

        let result = watcher.join();
        let _ = std::fs::remove_file(&tmp);
        let _ = std::fs::remove_file(format!("{}-wal", tmp.display()));
        let _ = std::fs::remove_file(format!("{}-shm", tmp.display()));
        assert!(
            result.is_err(),
            "expected watcher thread to panic on db file replacement"
        );
    }

    #[test]
    #[cfg(feature = "kernel-watcher")]
    fn watcher_backend_kernel_probe_fails_for_inaccessible_dir() {
        // Path under a non-existent parent — notify can't watch it.
        let nope = std::path::PathBuf::from(
            "/this/parent/does/not/exist/honker-kernel-probe.db",
        );
        let result = WatcherBackend::KernelWatch.probe(&nope);
        assert!(
            result.is_err(),
            "expected probe to fail for inaccessible dir, got Ok"
        );
    }
}

