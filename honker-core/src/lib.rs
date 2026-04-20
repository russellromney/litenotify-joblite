//! Shared Rust core for the litenotify bindings.
//!
//! This crate is NOT intended for direct use. It's the plain-Rust
//! foundation that three binding crates depend on:
//!
//!   * `litenotify`           — PyO3 Python extension
//!   * `litenotify-extension` — SQLite loadable extension (cdylib)
//!   * `litenotify-node`      — napi-rs Node.js binding
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
//!   - [`attach_notify`] — create `_litenotify_notifications` and
//!     register the `notify(channel, payload)` SQL scalar function.
//!   - [`Writer`] — single-connection write slot with blocking
//!     acquire, non-blocking try_acquire, and release.
//!   - [`Readers`] — bounded pool of reader connections that open
//!     lazily up to a max.
//!   - [`stat_pair`] / [`WalWatcher`] — 1 ms stat-polling thread that
//!     fires a callback on every `.db-wal` change. Bindings wrap this
//!     to surface wake events to their language's async primitive.
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

/// Install the `_litenotify_notifications` table and the
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
        "CREATE TABLE IF NOT EXISTS _litenotify_notifications (
           id INTEGER PRIMARY KEY AUTOINCREMENT,
           channel TEXT NOT NULL,
           payload TEXT NOT NULL,
           created_at INTEGER NOT NULL DEFAULT (unixepoch())
         );
         CREATE INDEX IF NOT EXISTS _litenotify_notifications_recent
           ON _litenotify_notifications(channel, id);",
    )?;

    conn.create_scalar_function(
        "notify",
        2,
        FunctionFlags::SQLITE_UTF8,
        |ctx| {
            let channel: String = ctx.get(0)?;
            let payload: String = ctx.get(1)?;
            // Inside a user tx; INSERT inherits the tx. Rollback drops
            // the notification atomically with whatever else rolled back.
            let db = unsafe { ctx.get_connection() }?;
            let mut ins = db.prepare_cached(
                "INSERT INTO _litenotify_notifications (channel, payload) VALUES (?1, ?2)",
            )?;
            let id = ins.insert(rusqlite::params![channel, payload])?;
            Ok(id)
        },
    )?;

    Ok(())
}

// ---------------------------------------------------------------------
// joblite queue schema
// ---------------------------------------------------------------------

/// Canonical DDL for the joblite queue schema. Shared source of truth
/// so the Python binding's `Queue._init_schema`, the SQLite loadable
/// extension's `jl_bootstrap()`, and any future binding can't drift.
///
/// Schema:
///
///   * `_joblite_live`  — pending + processing jobs. Partial index
///     `_joblite_live_claim` restricts to those two states so dead-row
///     history never slows down the claim hot path.
///   * `_joblite_dead`  — terminal rows (retry-exhausted or explicitly
///     failed). Never scanned by the claim path; retention policy is
///     the user's problem.
///
/// Idempotent (`CREATE TABLE IF NOT EXISTS` / `CREATE INDEX IF NOT
/// EXISTS`). Views and schema-version cleanup live in the language
/// binding, not here — they're caller-specific.
pub const BOOTSTRAP_JOBLITE_SQL: &str = "
    CREATE TABLE IF NOT EXISTS _joblite_live (
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
    CREATE INDEX IF NOT EXISTS _joblite_live_claim
      ON _joblite_live(queue, priority DESC, run_at, id)
      WHERE state IN ('pending', 'processing');
    CREATE TABLE IF NOT EXISTS _joblite_dead (
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
    CREATE TABLE IF NOT EXISTS _joblite_locks (
      name TEXT PRIMARY KEY,
      owner TEXT NOT NULL,
      expires_at INTEGER NOT NULL
    );
    CREATE TABLE IF NOT EXISTS _joblite_rate_limits (
      name TEXT NOT NULL,
      window_start INTEGER NOT NULL,
      count INTEGER NOT NULL DEFAULT 0,
      PRIMARY KEY (name, window_start)
    );
    CREATE TABLE IF NOT EXISTS _joblite_scheduler_tasks (
      name TEXT PRIMARY KEY,
      queue TEXT NOT NULL,
      cron_expr TEXT NOT NULL,
      payload TEXT NOT NULL,
      priority INTEGER NOT NULL DEFAULT 0,
      expires_s INTEGER,
      next_fire_at INTEGER NOT NULL
    );
    CREATE TABLE IF NOT EXISTS _joblite_results (
      job_id INTEGER PRIMARY KEY,
      value TEXT,
      created_at INTEGER NOT NULL DEFAULT (unixepoch()),
      expires_at INTEGER
    );
    CREATE TABLE IF NOT EXISTS _joblite_stream (
      offset INTEGER PRIMARY KEY AUTOINCREMENT,
      topic TEXT NOT NULL,
      key TEXT,
      payload TEXT NOT NULL,
      created_at INTEGER NOT NULL DEFAULT (unixepoch())
    );
    CREATE INDEX IF NOT EXISTS _joblite_stream_topic
      ON _joblite_stream(topic, offset);
    CREATE TABLE IF NOT EXISTS _joblite_stream_consumers (
      name TEXT NOT NULL,
      topic TEXT NOT NULL,
      offset INTEGER NOT NULL DEFAULT 0,
      PRIMARY KEY (name, topic)
    );
";

/// Install the joblite queue schema on `conn`. Idempotent. See
/// [`BOOTSTRAP_JOBLITE_SQL`] for the DDL and rationale.
pub fn bootstrap_joblite_schema(conn: &Connection) -> Result<(), Error> {
    conn.execute_batch(BOOTSTRAP_JOBLITE_SQL)?;
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
// WAL file watcher
// ---------------------------------------------------------------------

/// Snapshot of the WAL file's `(size, mtime_ns)`. Both 0 if the file
/// does not exist. Compared as a tuple across polls to detect change.
/// The WAL grows on every commit in WAL mode and is truncated on
/// checkpoint; either direction produces a visible delta.
pub fn stat_pair(path: &Path) -> (u64, i128) {
    match std::fs::metadata(path) {
        Ok(m) => {
            let len = m.len();
            let mt = m
                .modified()
                .ok()
                .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                .map(|d| d.as_nanos() as i128)
                .unwrap_or(0);
            (len, mt)
        }
        Err(_) => (0, 0),
    }
}

/// Background thread that stat()s a `.db-wal` file every 1 ms and
/// fires a callback on every observed change.
///
/// Bindings construct this and wire the callback to whatever async
/// primitive their language uses (Python `asyncio.Queue`, Node
/// mpsc channel drained into a Promise, etc). The watcher thread
/// itself is language-agnostic.
///
/// Why stat-polling over `inotify` / `kqueue` / `FSEvents`? macOS
/// FSEvents (the default on darwin via the `notify` crate) silently
/// drops same-process writes, which means a listener and an enqueuer
/// in the same Python process would never see each other. A dedicated
/// 1 ms stat loop works identically on every platform at ~0 CPU cost.
pub struct WalWatcher {
    stop: Arc<AtomicBool>,
}

impl WalWatcher {
    /// Spawn a watcher thread on `wal_path`. `on_change` is called
    /// once per observed change. The thread runs until [`WalWatcher`]
    /// is dropped or [`stop`](Self::stop) is called.
    pub fn spawn<F>(wal_path: PathBuf, on_change: F) -> Self
    where
        F: Fn() + Send + 'static,
    {
        let stop = Arc::new(AtomicBool::new(false));
        let stop_t = stop.clone();
        std::thread::Builder::new()
            .name("litenotify-wal-poll".into())
            .spawn(move || {
                let mut last = stat_pair(&wal_path);
                while !stop_t.load(Ordering::Acquire) {
                    std::thread::sleep(Duration::from_millis(1));
                    let cur = stat_pair(&wal_path);
                    if cur != last {
                        last = cur;
                        on_change();
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

/// Shared WAL-file watcher: one stat-poll thread per `.db-wal` path,
/// N subscribers. Each [`subscribe`](Self::subscribe) returns a fresh
/// `Receiver<()>` that sees a tick on every observed change.
///
/// Previously every call to `db.wal_events()` spawned its own
/// stat-poll thread, so N listeners in one process meant N threads
/// hammering `stat(2)` on the same file at 1 ms cadence. A web process
/// with 100 active SSE subscribers was doing ~200k stat syscalls/sec
/// against one file. Now a single shared thread services all
/// subscribers — 1 ms cadence, same kernel cost regardless of N.
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
    /// Spawn the shared poll thread for `wal_path`.
    pub fn new(wal_path: PathBuf) -> Self {
        let senders: Arc<Mutex<HashMap<u64, SyncSender<()>>>> =
            Arc::new(Mutex::new(HashMap::new()));
        let senders_t = senders.clone();
        let watcher = WalWatcher::spawn(wal_path, move || {
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
                "SELECT COUNT(*) FROM _litenotify_notifications WHERE channel='orders'",
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
                "SELECT COUNT(*) FROM _litenotify_notifications",
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
    fn stat_pair_handles_missing_file() {
        let (size, mt) = stat_pair(std::path::Path::new("/nonexistent/path/nope"));
        assert_eq!(size, 0);
        assert_eq!(mt, 0);
    }

    #[test]
    fn shared_wal_watcher_fans_out_to_many_subscribers() {
        let tmp = std::env::temp_dir().join(format!(
            "litenotify-shared-test-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_file(&tmp);
        std::fs::write(&tmp, b"").unwrap();

        let shared = SharedWalWatcher::new(tmp.clone());
        let subs: Vec<(u64, std::sync::mpsc::Receiver<()>)> =
            (0..50).map(|_| shared.subscribe()).collect();

        for i in 0..5 {
            std::thread::sleep(Duration::from_millis(5));
            std::fs::write(&tmp, format!("{}", i).as_bytes()).unwrap();
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
            "litenotify-unsub-test-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_file(&tmp);
        std::fs::write(&tmp, b"").unwrap();

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
            "litenotify-prune-test-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_file(&tmp);
        std::fs::write(&tmp, b"").unwrap();

        let shared = SharedWalWatcher::new(tmp.clone());
        {
            let _subs: Vec<_> = (0..10).map(|_| shared.subscribe()).collect();
            assert_eq!(shared.subscriber_count(), 10);
        }
        std::fs::write(&tmp, b"wake").unwrap();
        // Poll for pruning instead of sleeping a fixed duration —
        // the 1 ms poll thread needs to notice the file change AND
        // attempt to send on each dropped receiver AND prune. Under
        // parallel test load (`cargo test` runs threads in parallel),
        // 30 ms is not enough; previously this flaked ~5% of runs.
        // 2 s gives plenty of headroom without slowing the test.
        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        while shared.subscriber_count() != 0
            && std::time::Instant::now() < deadline
        {
            std::thread::sleep(Duration::from_millis(5));
            // Keep poking the file so the stat-poll thread has a
            // change to react to. Without this, a missed wake on
            // the first poke means we wait the full 2s.
            std::fs::write(&tmp, b"wake").unwrap();
        }
        assert_eq!(shared.subscriber_count(), 0);

        let _ = std::fs::remove_file(&tmp);
    }

    #[test]
    fn bootstrap_joblite_schema_creates_tables_and_index() {
        let conn = mem();
        bootstrap_joblite_schema(&conn).unwrap();

        // Idempotent.
        bootstrap_joblite_schema(&conn).unwrap();

        // _joblite_live has the 12 columns we expect (Python binding
        // and the extension have historically disagreed on _joblite_dead
        // column count; this pins both).
        let live_cols: Vec<String> = conn
            .prepare("SELECT name FROM pragma_table_info('_joblite_live')")
            .unwrap()
            .query_map([], |r| r.get::<_, String>(0))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(live_cols.len(), 12);
        assert!(live_cols.contains(&"expires_at".to_string()));

        let dead_cols: Vec<String> = conn
            .prepare("SELECT name FROM pragma_table_info('_joblite_dead')")
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
                 WHERE type='index' AND name='_joblite_live_claim'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(idx, 1);

        // _joblite_locks table present for db.lock() support.
        let locks_cols: Vec<String> = conn
            .prepare("SELECT name FROM pragma_table_info('_joblite_locks')")
            .unwrap()
            .query_map([], |r| r.get::<_, String>(0))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(locks_cols, vec!["name", "owner", "expires_at"]);

        // _joblite_rate_limits table present for db.try_rate_limit().
        let rl_cols: Vec<String> = conn
            .prepare("SELECT name FROM pragma_table_info('_joblite_rate_limits')")
            .unwrap()
            .query_map([], |r| r.get::<_, String>(0))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(rl_cols, vec!["name", "window_start", "count"]);

        // _joblite_scheduler_tasks table present for Scheduler's
        // per-task registration + next_fire_at persistence.
        let sched_cols: Vec<String> = conn
            .prepare("SELECT name FROM pragma_table_info('_joblite_scheduler_tasks')")
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

        // _joblite_results table for task result storage.
        let res_cols: Vec<String> = conn
            .prepare("SELECT name FROM pragma_table_info('_joblite_results')")
            .unwrap()
            .query_map([], |r| r.get::<_, String>(0))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(
            res_cols,
            vec!["job_id", "value", "created_at", "expires_at"]
        );

        // _joblite_stream + _joblite_stream_consumers tables for
        // durable pub/sub streams.
        let stream_cols: Vec<String> = conn
            .prepare("SELECT name FROM pragma_table_info('_joblite_stream')")
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
            .prepare("SELECT name FROM pragma_table_info('_joblite_stream_consumers')")
            .unwrap()
            .query_map([], |r| r.get::<_, String>(0))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(sc_cols, vec!["name", "topic", "offset"]);
    }
}
