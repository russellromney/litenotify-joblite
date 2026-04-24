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
//!   - [`WalWatcher`] — 1 ms PRAGMA-polling thread that fires a
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
use std::time::Duration;

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
/// the `.db-wal` file for change and SELECT new rows by channel.
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

    conn.create_scalar_function(
        "notify",
        2,
        FunctionFlags::SQLITE_UTF8,
        |ctx| {
            let channel: String = ctx.get(0)?;
            let payload: String = ctx.get(1)?;
            let db = unsafe { ctx.get_connection() }?;
            let mut ins = db.prepare_cached(
                "INSERT INTO _honker_notifications (channel, payload) VALUES (?1, ?2)",
            )?;
            let id = ins.insert(rusqlite::params![channel, payload])?;
            Ok(id)
        },
    )?;

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
pub struct Writer {
    slot: Mutex<Option<Connection>>,
    available: Condvar,
}

impl Writer {
    pub fn new(conn: Connection) -> Self {
        Self {
            slot: Mutex::new(Some(conn)),
            available: Condvar::new(),
        }
    }

    /// Blocking acquire. Waits on a condvar if the slot is held.
    pub fn acquire(&self) -> Connection {
        let mut guard = self.slot.lock();
        while guard.is_none() {
            self.available.wait(&mut guard);
        }
        guard.take().unwrap()
    }

    /// Non-blocking. Returns `Some(conn)` if the slot was immediately
    /// free, else `None`. Bindings use this for a fast path that
    /// avoids GIL release (Python) or async thread-hops (Node) when
    /// the slot is uncontended.
    pub fn try_acquire(&self) -> Option<Connection> {
        let mut guard = self.slot.lock();
        guard.take()
    }

    pub fn release(&self, conn: Connection) {
        let mut guard = self.slot.lock();
        *guard = Some(conn);
        self.available.notify_one();
    }
}

// ---------------------------------------------------------------------
// Reader pool
// ---------------------------------------------------------------------

/// Bounded pool of reader connections. Readers are cheap (one file
/// descriptor + a page cache) and WAL mode allows any number to run
/// concurrently with the writer.
pub struct Readers {
    pool: Mutex<Vec<Connection>>,
    outstanding: Mutex<usize>,
    available: Condvar,
    path: String,
    max: usize,
}

impl Readers {
    pub fn new(path: String, max: usize) -> Self {
        Self {
            pool: Mutex::new(Vec::new()),
            outstanding: Mutex::new(0),
            available: Condvar::new(),
            path,
            max: max.max(1),
        }
    }

    /// Acquire a reader. Pops a pooled one if available; otherwise
    /// opens a new connection up to `max`. Above `max`, waits on the
    /// condvar.
    pub fn acquire(&self) -> Result<Connection, Error> {
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
                return open_conn(&self.path, false);
            }
            drop(out);
            self.available.wait(&mut pool);
        }
    }

    pub fn release(&self, conn: Connection) {
        let mut pool = self.pool.lock();
        pool.push(conn);
        self.available.notify_one();
    }
}

// ---------------------------------------------------------------------
// Database file watcher
// ---------------------------------------------------------------------

/// Platform-specific file identity: `(dev, ino)` on Unix,
/// `(volume_serial, file_index)` on Windows. Used to detect when the
/// database file has been replaced underneath us (atomic rename,
/// litestream restore, volume remount).
#[cfg(unix)]
fn stat_identity(path: &Path) -> std::io::Result<(u64, u64)> {
    use std::os::unix::fs::MetadataExt;
    let m = std::fs::metadata(path)?;
    Ok((m.dev(), m.ino()))
}

#[cfg(windows)]
fn stat_identity(path: &Path) -> std::io::Result<(u64, u64)> {
    use std::os::windows::fs::MetadataExt;
    let m = std::fs::metadata(path)?;
    Ok((
        m.volume_serial_number().unwrap_or(0) as u64,
        m.file_index().unwrap_or(0),
    ))
}

#[cfg(not(any(unix, windows)))]
fn stat_identity(path: &Path) -> std::io::Result<(u64, u64)> {
    let _ = path;
    Ok((0, 0))
}

/// Read the pager's `data_version` counter via `PRAGMA data_version`.
/// Returns a monotonic u32 incremented on every commit by any
/// connection (and on checkpoint). Only `PRAGMA data_version` detects
/// cross-connection WAL commits; the file-control opcode with the same
/// name is per-pager and does not.
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
/// 3. **Identity check (every 100 ms):** `stat(db_path)` to compare
///    `(dev, ino)`. If the file was replaced, panic with a clear
///    message — continuing would silently watch stale data.
///
/// Why PRAGMA data_version instead of stat on the WAL? It is a
/// counter, not a timestamp — immune to clock skew, WAL truncation,
/// and exact-size collisions. It also ignores rolled-back transactions
/// that touch the WAL but never commit.
pub struct WalWatcher {
    stop: Arc<AtomicBool>,
}

impl WalWatcher {
    /// Spawn a watcher thread on `db_path`. `on_change` is called
    /// once per observed commit. The thread runs until [`WalWatcher`]
    /// is dropped or [`stop`](Self::stop) is called.
    pub fn spawn<F>(db_path: PathBuf, on_change: F) -> Self
    where
        F: Fn() + Send + 'static,
    {
        let stop = Arc::new(AtomicBool::new(false));
        let stop_t = stop.clone();
        std::thread::Builder::new()
            .name("honker-wal-poll".into())
            .spawn(move || {
                let mut conn = match Connection::open_with_flags(
                    &db_path,
                    OpenFlags::SQLITE_OPEN_READ_WRITE
                        | OpenFlags::SQLITE_OPEN_NO_MUTEX,
                ) {
                    Ok(c) => Some(c),
                    Err(e) => {
                        eprintln!(
                            "honker: failed to open watcher connection: {e}"
                        );
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
                        eprintln!(
                            "honker: failed to stat database for identity check: {e}"
                        );
                        (0, 0)
                    }
                };
                let mut tick: u64 = 0;

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
                                eprintln!(
                                    "honker: data_version poll failed: {e}"
                                );
                                conn = None;
                                on_change(); // conservative wake
                            }
                        }
                    } else {
                        // Path 2: reconnect after transient failure
                        match Connection::open_with_flags(
                            &db_path,
                            OpenFlags::SQLITE_OPEN_READ_WRITE
                                | OpenFlags::SQLITE_OPEN_NO_MUTEX,
                        ) {
                            Ok(c) => {
                                last_version =
                                    poll_data_version(&c).unwrap_or(0);
                                conn = Some(c);
                            }
                            Err(e) => {
                                eprintln!(
                                    "honker: reconnect failed: {e}"
                                );
                            }
                        }
                    }

                    // Path 3: stat identity check (dead-man's switch)
                    tick += 1;
                    if tick % 100 == 0 {
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
                                eprintln!(
                                    "honker: stat identity check failed: {e}"
                                );
                                conn = None;
                                on_change();
                            }
                        }
                    }
                }
            })
            .expect("spawn wal-poll thread");
        Self { stop }
    }

    /// Request the watcher thread to stop. Idempotent. Dropping the
    /// `WalWatcher` also stops the thread.
    pub fn stop(&self) {
        self.stop.store(true, Ordering::Release);
    }
}

impl Drop for WalWatcher {
    fn drop(&mut self) {
        self.stop();
    }
}

// ---------------------------------------------------------------------
// Shared WAL watcher (one thread per Database, N subscribers)
// ---------------------------------------------------------------------

/// Shared database-file watcher: one PRAGMA-poll thread per database
/// path, N subscribers. Each [`subscribe`](Self::subscribe) returns a
/// fresh `Receiver<()>` that sees a tick on every observed commit.
///
/// Previously every call to `db.wal_events()` spawned its own
/// stat-poll thread, so N listeners in one process meant N threads
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
pub struct SharedWalWatcher {
    /// Hold the underlying poll thread alive. Dropping this stops it.
    _watcher: WalWatcher,
    /// Shared with the watcher closure so it can fan out to every
    /// subscriber and prune disconnected ones opportunistically.
    senders: Arc<Mutex<HashMap<u64, SyncSender<()>>>>,
    next_id: AtomicU64,
}

impl SharedWalWatcher {
    /// Spawn the shared poll thread for `db_path`.
    pub fn new(db_path: PathBuf) -> Self {
        let senders: Arc<Mutex<HashMap<u64, SyncSender<()>>>> =
            Arc::new(Mutex::new(HashMap::new()));
        let senders_t = senders.clone();
        let watcher = WalWatcher::spawn(db_path, move || {
            let mut list = senders_t.lock();
            list.retain(|_id, s| match s.try_send(()) {
                Ok(()) | Err(TrySendError::Full(_)) => true,
                Err(TrySendError::Disconnected(_)) => false,
            });
        });
        Self {
            _watcher: watcher,
            senders,
            next_id: AtomicU64::new(0),
        }
    }

    /// Subscribe. Returns a subscriber id and a [`Receiver<()>`] that
    /// sees one tick per observed `.db-wal` change. Callers MUST
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
            .query_row(
                "SELECT COUNT(*) FROM _honker_notifications",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(n, 0);
    }

    #[test]
    fn writer_try_acquire_returns_none_when_held() {
        let w = Writer::new(Connection::open_in_memory().unwrap());
        let conn = w.acquire();
        assert!(w.try_acquire().is_none());
        w.release(conn);
        assert!(w.try_acquire().is_some());
    }

    #[test]
    fn shared_wal_watcher_fans_out_to_many_subscribers() {
        let tmp = std::env::temp_dir().join(format!(
            "honker-shared-test-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_file(&tmp);
        // Create a real SQLite database in WAL mode so the watcher can
        // open a read-only connection and poll data_version.
        {
            let conn = Connection::open(&tmp).unwrap();
            conn.execute_batch("PRAGMA journal_mode = WAL;").unwrap();
        }

        let shared = SharedWalWatcher::new(tmp.clone());
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
    fn shared_wal_watcher_explicit_unsubscribe_disconnects_receiver() {
        let tmp = std::env::temp_dir().join(format!(
            "honker-unsub-test-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_file(&tmp);
        {
            let conn = Connection::open(&tmp).unwrap();
            conn.execute_batch("PRAGMA journal_mode = WAL;").unwrap();
        }

        let shared = SharedWalWatcher::new(tmp.clone());
        let (id, rx) = shared.subscribe();
        assert_eq!(shared.subscriber_count(), 1);

        shared.unsubscribe(id);
        assert_eq!(shared.subscriber_count(), 0);

        // Receiver now sees Err on blocking recv — the contract that
        // lets a bridge thread exit cleanly when its WalEvents drops.
        assert!(rx.recv().is_err());

        let _ = std::fs::remove_file(&tmp);
    }

    #[test]
    fn shared_wal_watcher_prunes_subscribers_when_receiver_dropped() {
        let tmp = std::env::temp_dir().join(format!(
            "honker-prune-test-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_file(&tmp);
        {
            let conn = Connection::open(&tmp).unwrap();
            conn.execute_batch("PRAGMA journal_mode = WAL;").unwrap();
        }

        let shared = SharedWalWatcher::new(tmp.clone());
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
        while shared.subscriber_count() != 0
            && std::time::Instant::now() < deadline
        {
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
        let tmp = std::env::temp_dir().join(format!(
            "honker-dv-test-{}",
            std::process::id()
        ));
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
        let tmp = std::env::temp_dir().join(format!(
            "honker-dv-ckpt-test-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_file(&tmp);
        // Watcher connection — observe changes from the writer.
        let watcher = Connection::open(&tmp).unwrap();
        watcher.execute_batch("PRAGMA journal_mode = WAL;").unwrap();
        let w0 = poll_data_version(&watcher).unwrap();

        // Writer connection — make changes.
        let writer = Connection::open(&tmp).unwrap();
        writer.execute("CREATE TABLE t(x INTEGER)", []).unwrap();
        let w1 = poll_data_version(&watcher).unwrap();
        assert!(w1 > w0, "commit from other conn should increment data_version");

        // Checkpoint truncates WAL; watcher should still see the change.
        writer.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);").unwrap();
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

    #[test]
    fn stat_identity_detects_file_replacement() {
        let tmp = std::env::temp_dir().join(format!(
            "honker-id-test-{}",
            std::process::id()
        ));
        let tmp2 = std::env::temp_dir().join(format!(
            "honker-id-test2-{}",
            std::process::id()
        ));
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
        assert_eq!(id3, id2, "renamed file should carry the replacement's identity");

        let _ = std::fs::remove_file(&tmp);
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
