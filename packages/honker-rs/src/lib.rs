//! Ergonomic Rust wrapper over `honker-core`. Durable queues,
//! streams, pub/sub, and scheduler on SQLite.
//!
//! ```no_run
//! use honker::{Database, EnqueueOpts, QueueOpts};
//! use serde_json::json;
//!
//! let db = Database::open("app.db").unwrap();
//! let q = db.queue("emails", QueueOpts::default());
//!
//! q.enqueue(&json!({"to": "alice@example.com"}), EnqueueOpts::default()).unwrap();
//!
//! if let Some(job) = q.claim_one("worker-1").unwrap() {
//!     job.ack().unwrap();
//! }
//! ```
//!
//! This crate opens its own connection, registers every `honker_*`
//! SQL function via `honker_core::attach_honker_functions`, and
//! bootstraps the schema. No `.dylib` load needed at runtime.
//! Use the loadable extension (`honker-extension`) only when mixing
//! with other SQLite clients on the same file.

use std::path::Path;
use std::sync::Arc;
use std::sync::mpsc::{Receiver, RecvTimeoutError};
use std::time::Duration;

use parking_lot::{Mutex, MutexGuard};
use rusqlite::{Connection, params};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use thiserror::Error;

use honker_core::SharedUpdateWatcher;

#[derive(Debug, Error)]
pub enum Error {
    #[error("sqlite error: {0}")]
    Sqlite(#[from] rusqlite::Error),
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("core error: {0}")]
    Core(String),
    #[error("update channel closed")]
    UpdateClosed,
}

pub type Result<T> = std::result::Result<T, Error>;

// ---------------------------------------------------------------------
// Shared connection handle
// ---------------------------------------------------------------------

struct Inner {
    conn: Mutex<Connection>,
    updates: SharedUpdateWatcher,
}

impl Inner {
    fn with_conn<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&Connection) -> R,
    {
        let guard = self.conn.lock();
        f(&guard)
    }
}

/// A Honker database handle. Cheap to clone — internally an `Arc`.
///
/// Concurrency: every operation serializes through a single
/// `Connection` behind a mutex. SQLite in WAL mode allows many
/// concurrent reader *processes*, but in-process this wrapper is
/// single-threaded. Open additional `Database` instances for
/// parallel readers inside one process.
#[derive(Clone)]
pub struct Database {
    inner: Arc<Inner>,
}

impl Database {
    /// Open (or create) a SQLite database at `path`. Applies default
    /// PRAGMAs, registers the honker SQL functions, and bootstraps
    /// the schema.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path_ref = path.as_ref();
        let path_str = path_ref.to_string_lossy().into_owned();

        let conn =
            honker_core::open_conn(&path_str, true).map_err(|e| Error::Core(e.to_string()))?;
        honker_core::attach_honker_functions(&conn)?;
        honker_core::bootstrap_honker_schema(&conn).map_err(|e| Error::Core(e.to_string()))?;

        let updates = SharedUpdateWatcher::new(path_ref.to_path_buf());

        Ok(Self {
            inner: Arc::new(Inner {
                conn: Mutex::new(conn),
                updates,
            }),
        })
    }

    /// Get a named queue handle. Defaults: 300s visibility timeout,
    /// 3 max attempts.
    pub fn queue(&self, name: &str, opts: QueueOpts) -> Queue {
        Queue {
            inner: self.inner.clone(),
            name: name.to_string(),
            opts,
        }
    }

    /// Get a named stream handle.
    pub fn stream(&self, name: &str) -> Stream {
        Stream {
            inner: self.inner.clone(),
            name: name.to_string(),
        }
    }

    /// Scheduler facade. Cheap; does not allocate persistent state.
    pub fn scheduler(&self) -> Scheduler {
        Scheduler {
            inner: self.inner.clone(),
        }
    }

    /// Fire a `pg_notify`-style pub/sub signal. Payload is serialized
    /// via `serde_json`. Returns the notification row id.
    pub fn notify<P: Serialize>(&self, channel: &str, payload: &P) -> Result<i64> {
        let json = serde_json::to_string(payload)?;
        Ok(self.inner.with_conn(|c| {
            c.query_row("SELECT notify(?1, ?2)", params![channel, json], |r| {
                r.get::<_, i64>(0)
            })
        })?)
    }

    /// Fire a notification inside an open transaction. The signal is
    /// visible to listeners only after the transaction commits.
    pub fn notify_tx<P: Serialize>(
        &self,
        tx: &Transaction<'_>,
        channel: &str,
        payload: &P,
    ) -> Result<i64> {
        let json = serde_json::to_string(payload)?;
        tx.query_row("SELECT notify(?1, ?2)", params![channel, json], |r| {
            r.get::<_, i64>(0)
        })
    }

    /// Subscribe to a channel. Returns a blocking iterator that wakes
    /// on database updates and yields rows where `channel = ?` and
    /// `id > MAX(id)` at attach time. Historical notifications are
    /// not replayed.
    pub fn listen(&self, channel: &str) -> Result<Subscription> {
        let last_id: i64 = self.inner.with_conn(|c| {
            c.query_row(
                "SELECT COALESCE(MAX(id), 0) FROM _honker_notifications",
                [],
                |r| r.get(0),
            )
        })?;
        let (sub_id, rx) = self.inner.updates.subscribe();
        Ok(Subscription {
            inner: self.inner.clone(),
            sub_id,
            rx,
            channel: channel.to_string(),
            last_id,
            pending: std::collections::VecDeque::new(),
            closed: false,
        })
    }

    /// Raw update waker. `UpdateEvents::recv` blocks until the next
    /// database update; `try_recv` is non-blocking. Useful when you
    /// want to drive a custom poll loop (batch claim, custom stream
    /// read pattern) without polling SQLite on a timer.
    pub fn update_events(&self) -> UpdateEvents {
        let (sub_id, rx) = self.inner.updates.subscribe();
        UpdateEvents {
            inner: self.inner.clone(),
            sub_id,
            rx,
        }
    }

    /// Begin a transaction. Commit or rollback explicitly; on drop,
    /// an uncommitted transaction is rolled back.
    ///
    /// The returned `Transaction` pins the connection mutex for its
    /// lifetime. Other `Database` calls from **another thread** block
    /// until you commit or drop; calls on the **same thread** that
    /// holds the transaction deadlock (the mutex is not reentrant).
    /// Always route same-thread operations through `*_tx` methods:
    /// `queue.enqueue_tx(&tx, ...)`, `stream.publish_tx(&tx, ...)`,
    /// `stream.save_offset_tx(&tx, ...)`.
    pub fn transaction(&self) -> Result<Transaction<'_>> {
        let guard = self.inner.conn.lock();
        guard.execute("BEGIN IMMEDIATE", [])?;
        Ok(Transaction { guard, done: false })
    }

    /// Try to acquire an advisory lock. Returns `Some(Lock)` if you
    /// won, `None` if someone else holds it. The returned `Lock`
    /// releases on drop or via `Lock::release`.
    pub fn try_lock(&self, name: &str, owner: &str, ttl_s: i64) -> Result<Option<Lock>> {
        let acquired: i64 = self.inner.with_conn(|c| {
            c.query_row(
                "SELECT honker_lock_acquire(?1, ?2, ?3)",
                params![name, owner, ttl_s],
                |r| r.get(0),
            )
        })?;
        if acquired == 1 {
            Ok(Some(Lock {
                inner: self.inner.clone(),
                name: name.to_string(),
                owner: owner.to_string(),
                released: false,
            }))
        } else {
            Ok(None)
        }
    }

    /// Fixed-window rate limit. Returns true if the request fits
    /// within `limit` per `per` seconds.
    pub fn try_rate_limit(&self, name: &str, limit: i64, per: i64) -> Result<bool> {
        let ok: i64 = self.inner.with_conn(|c| {
            c.query_row(
                "SELECT honker_rate_limit_try(?1, ?2, ?3)",
                params![name, limit, per],
                |r| r.get(0),
            )
        })?;
        Ok(ok == 1)
    }

    /// Persist a job result for later retrieval via `get_result`.
    pub fn save_result(&self, job_id: i64, value: &str, ttl_s: i64) -> Result<()> {
        self.inner.with_conn(|c| {
            c.query_row(
                "SELECT honker_result_save(?1, ?2, ?3)",
                params![job_id, value, ttl_s],
                |_| Ok(()),
            )
        })?;
        Ok(())
    }

    /// Fetch a stored result. Returns `None` if absent or expired.
    pub fn get_result(&self, job_id: i64) -> Result<Option<String>> {
        Ok(self.inner.with_conn(|c| {
            c.query_row("SELECT honker_result_get(?1)", params![job_id], |r| {
                r.get::<_, Option<String>>(0)
            })
        })?)
    }

    /// Drop expired results. Returns rows deleted.
    pub fn sweep_results(&self) -> Result<i64> {
        Ok(self.inner.with_conn(|c| {
            c.query_row("SELECT honker_result_sweep()", [], |r| r.get::<_, i64>(0))
        })?)
    }

    /// Delete notifications older than `older_than_s` seconds. Returns
    /// the number of rows deleted.
    pub fn prune_notifications(&self, older_than_s: i64) -> Result<i64> {
        let n = self.inner.with_conn(|c| {
            c.execute(
                "DELETE FROM _honker_notifications
                 WHERE created_at < unixepoch() - ?1",
                params![older_than_s],
            )
        })?;
        Ok(n as i64)
    }

    /// Keep only the most recent `max_keep` notifications. Returns
    /// the number of rows deleted.
    pub fn prune_notifications_keep_latest(&self, max_keep: i64) -> Result<i64> {
        let n = self.inner.with_conn(|c| {
            c.execute(
                "DELETE FROM _honker_notifications
                 WHERE id < (
                   SELECT COALESCE(MIN(id), 0) FROM (
                     SELECT id FROM _honker_notifications
                     ORDER BY id DESC LIMIT ?1
                   )
                 )",
                params![max_keep],
            )
        })?;
        Ok(n as i64)
    }

    /// Escape hatch: run a closure with a borrowed `&Connection`. The
    /// connection mutex is held for the closure's lifetime.
    pub fn with_conn<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&Connection) -> R,
    {
        self.inner.with_conn(f)
    }
}

// ---------------------------------------------------------------------
// Transactions
// ---------------------------------------------------------------------

/// An open transaction. Pins the database's connection mutex for its
/// lifetime. Call [`commit`](Self::commit) or [`rollback`](Self::rollback)
/// to release; on drop without either, the transaction rolls back.
pub struct Transaction<'db> {
    guard: MutexGuard<'db, Connection>,
    done: bool,
}

impl<'db> Transaction<'db> {
    /// Run an `execute` on this transaction's connection.
    pub fn execute<P: rusqlite::Params>(&self, sql: &str, params: P) -> Result<usize> {
        Ok(self.guard.execute(sql, params)?)
    }

    /// Run a `query_row` on this transaction's connection.
    pub fn query_row<T, P, F>(&self, sql: &str, params: P, f: F) -> Result<T>
    where
        P: rusqlite::Params,
        F: FnOnce(&rusqlite::Row<'_>) -> rusqlite::Result<T>,
    {
        Ok(self.guard.query_row(sql, params, f)?)
    }

    /// The underlying connection. Use for `prepare` and custom queries.
    pub fn conn(&self) -> &Connection {
        &self.guard
    }

    pub fn commit(mut self) -> Result<()> {
        self.guard.execute("COMMIT", [])?;
        self.done = true;
        Ok(())
    }

    pub fn rollback(mut self) -> Result<()> {
        self.guard.execute("ROLLBACK", [])?;
        self.done = true;
        Ok(())
    }
}

impl<'db> Drop for Transaction<'db> {
    fn drop(&mut self) {
        if !self.done {
            let _ = self.guard.execute("ROLLBACK", []);
        }
    }
}

// ---------------------------------------------------------------------
// Queues
// ---------------------------------------------------------------------

/// Per-queue configuration.
#[derive(Debug, Clone)]
pub struct QueueOpts {
    pub visibility_timeout_s: i64,
    pub max_attempts: i64,
}

impl Default for QueueOpts {
    fn default() -> Self {
        Self {
            visibility_timeout_s: 300,
            max_attempts: 3,
        }
    }
}

/// Per-enqueue options. Use `Option<i64>` so callers distinguish
/// unset from zero.
#[derive(Debug, Clone, Default)]
pub struct EnqueueOpts {
    pub delay: Option<i64>,
    pub run_at: Option<i64>,
    pub priority: i64,
    pub expires: Option<i64>,
}

pub struct Queue {
    inner: Arc<Inner>,
    name: String,
    opts: QueueOpts,
}

impl Queue {
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Enqueue on a freshly acquired connection.
    pub fn enqueue<P: Serialize>(&self, payload: &P, opts: EnqueueOpts) -> Result<i64> {
        let json = serde_json::to_string(payload)?;
        Ok(self
            .inner
            .with_conn(|c| enqueue_on(c, &self.name, &json, self.opts.max_attempts, &opts))?)
    }

    /// Enqueue inside an open transaction. Atomic with whatever else
    /// ran on the same `tx`.
    pub fn enqueue_tx<P: Serialize>(
        &self,
        tx: &Transaction<'_>,
        payload: &P,
        opts: EnqueueOpts,
    ) -> Result<i64> {
        let json = serde_json::to_string(payload)?;
        Ok(enqueue_on(
            tx.conn(),
            &self.name,
            &json,
            self.opts.max_attempts,
            &opts,
        )?)
    }

    /// Atomically claim up to `n` jobs.
    pub fn claim_batch(&self, worker_id: &str, n: i64) -> Result<Vec<Job>> {
        let rows_json: String = self.inner.with_conn(|c| {
            c.query_row(
                "SELECT honker_claim_batch(?1, ?2, ?3, ?4)",
                params![self.name, worker_id, n, self.opts.visibility_timeout_s],
                |r| r.get(0),
            )
        })?;
        let raw: Vec<RawJob> = serde_json::from_str(&rows_json)?;
        Ok(raw
            .into_iter()
            .map(|r| Job {
                inner: self.inner.clone(),
                id: r.id,
                queue: r.queue,
                payload: r.payload.into_bytes(),
                worker_id: r.worker_id,
                attempts: r.attempts,
            })
            .collect())
    }

    /// Claim a single job, or `None` if the queue is empty.
    pub fn claim_one(&self, worker_id: &str) -> Result<Option<Job>> {
        Ok(self.claim_batch(worker_id, 1)?.into_iter().next())
    }

    /// Ack multiple jobs in one transaction. Returns count acked.
    pub fn ack_batch(&self, ids: &[i64], worker_id: &str) -> Result<i64> {
        let ids_json = serde_json::to_string(ids)?;
        Ok(self.inner.with_conn(|c| {
            c.query_row(
                "SELECT honker_ack_batch(?1, ?2)",
                params![ids_json, worker_id],
                |r| r.get::<_, i64>(0),
            )
        })?)
    }

    /// Sweep expired processing rows back to pending. Returns rows touched.
    pub fn sweep_expired(&self) -> Result<i64> {
        Ok(self.inner.with_conn(|c| {
            c.query_row("SELECT honker_sweep_expired(?1)", params![self.name], |r| {
                r.get::<_, i64>(0)
            })
        })?)
    }

    /// Wait on database updates and yield jobs as they land. Blocks. Cancel
    /// by dropping the `Database` or calling `stop()` on a shared
    /// `Arc<AtomicBool>` you thread in yourself — this helper is the
    /// minimum that actually replaces a polling loop.
    pub fn claim_waker(&self) -> ClaimWaker {
        let (sub_id, rx) = self.inner.updates.subscribe();
        ClaimWaker {
            inner: self.inner.clone(),
            name: self.name.clone(),
            visibility_timeout_s: self.opts.visibility_timeout_s,
            sub_id,
            rx,
        }
    }

}

fn enqueue_on(
    conn: &Connection,
    queue: &str,
    payload_json: &str,
    max_attempts: i64,
    opts: &EnqueueOpts,
) -> rusqlite::Result<i64> {
    conn.query_row(
        "SELECT honker_enqueue(?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        params![
            queue,
            payload_json,
            opts.run_at,
            opts.delay,
            opts.priority,
            max_attempts,
            opts.expires
        ],
        |r| r.get::<_, i64>(0),
    )
}

#[derive(Deserialize)]
struct RawJob {
    id: i64,
    queue: String,
    payload: String,
    worker_id: String,
    attempts: i64,
    #[serde(rename = "claim_expires_at")]
    #[allow(dead_code)]
    claim_expires_at: i64,
}

/// A claimed unit of work. `payload` is raw JSON bytes.
pub struct Job {
    inner: Arc<Inner>,
    pub id: i64,
    pub queue: String,
    pub payload: Vec<u8>,
    pub worker_id: String,
    pub attempts: i64,
}

impl Job {
    pub fn payload_as<T: DeserializeOwned>(&self) -> Result<T> {
        Ok(serde_json::from_slice(&self.payload)?)
    }

    /// DELETE the row if the claim is still valid.
    pub fn ack(&self) -> Result<bool> {
        let n: i64 = self.inner.with_conn(|c| {
            c.query_row(
                "SELECT honker_ack(?1, ?2)",
                params![self.id, self.worker_id],
                |r| r.get(0),
            )
        })?;
        Ok(n > 0)
    }

    /// Put the job back with a delay, or move to `_honker_dead` after
    /// `max_attempts` retries.
    pub fn retry(&self, delay_s: i64, error: &str) -> Result<bool> {
        let n: i64 = self.inner.with_conn(|c| {
            c.query_row(
                "SELECT honker_retry(?1, ?2, ?3, ?4)",
                params![self.id, self.worker_id, delay_s, error],
                |r| r.get(0),
            )
        })?;
        Ok(n > 0)
    }

    /// Unconditionally move the claim to `_honker_dead`.
    pub fn fail(&self, error: &str) -> Result<bool> {
        let n: i64 = self.inner.with_conn(|c| {
            c.query_row(
                "SELECT honker_fail(?1, ?2, ?3)",
                params![self.id, self.worker_id, error],
                |r| r.get(0),
            )
        })?;
        Ok(n > 0)
    }

    /// Extend the visibility timeout. Returns false if the claim has
    /// already expired or been taken by someone else.
    pub fn heartbeat(&self, extend_s: i64) -> Result<bool> {
        let n: i64 = self.inner.with_conn(|c| {
            c.query_row(
                "SELECT honker_heartbeat(?1, ?2, ?3)",
                params![self.id, self.worker_id, extend_s],
                |r| r.get(0),
            )
        })?;
        Ok(n > 0)
    }
}

/// WAL-wake claim helper. Call `next(worker_id)` to claim the next
/// available job; blocks until one arrives.
pub struct ClaimWaker {
    inner: Arc<Inner>,
    name: String,
    visibility_timeout_s: i64,
    sub_id: u64,
    rx: Receiver<()>,
}

impl ClaimWaker {
    /// Block until a job is claimable, then claim and return it.
    /// Returns `None` only if the shared update watcher is dropped.
    pub fn next(&self, worker_id: &str) -> Result<Option<Job>> {
        loop {
            let rows_json: String = self.inner.with_conn(|c| {
                c.query_row(
                    "SELECT honker_claim_batch(?1, ?2, ?3, ?4)",
                    params![self.name, worker_id, 1, self.visibility_timeout_s],
                    |r| r.get(0),
                )
            })?;
            let raw: Vec<RawJob> = serde_json::from_str(&rows_json)?;
            if let Some(r) = raw.into_iter().next() {
                return Ok(Some(Job {
                    inner: self.inner.clone(),
                    id: r.id,
                    queue: r.queue,
                    payload: r.payload.into_bytes(),
                    worker_id: r.worker_id,
                    attempts: r.attempts,
                }));
            }
            let next_at: i64 = self.inner.with_conn(|c| {
                c.query_row(
                    "SELECT honker_queue_next_claim_at(?1)",
                    params![self.name],
                    |r| r.get::<_, i64>(0),
                )
            })?;
            if next_at > 0 && next_at <= chrono_like_now() {
                continue;
            }
            match recv_until(&self.rx, next_at) {
                Ok(true) => continue,
                Ok(false) => continue,
                Err(RecvTimeoutError::Disconnected) => return Ok(None),
                Err(RecvTimeoutError::Timeout) => continue,
            }
        }
    }

    /// Try to claim without blocking. Returns `None` if the queue is
    /// empty right now.
    pub fn try_next(&self, worker_id: &str) -> Result<Option<Job>> {
        let rows_json: String = self.inner.with_conn(|c| {
            c.query_row(
                "SELECT honker_claim_batch(?1, ?2, ?3, ?4)",
                params![self.name, worker_id, 1, self.visibility_timeout_s],
                |r| r.get(0),
            )
        })?;
        let raw: Vec<RawJob> = serde_json::from_str(&rows_json)?;
        Ok(raw.into_iter().next().map(|r| Job {
            inner: self.inner.clone(),
            id: r.id,
            queue: r.queue,
            payload: r.payload.into_bytes(),
            worker_id: r.worker_id,
            attempts: r.attempts,
        }))
    }
}

impl Drop for ClaimWaker {
    fn drop(&mut self) {
        self.inner.updates.unsubscribe(self.sub_id);
    }
}

// ---------------------------------------------------------------------
// Streams
// ---------------------------------------------------------------------

pub struct Stream {
    inner: Arc<Inner>,
    name: String,
}

impl Stream {
    pub fn topic(&self) -> &str {
        &self.name
    }

    /// Publish an event. Returns the assigned offset.
    pub fn publish<P: Serialize>(&self, payload: &P) -> Result<i64> {
        self.publish_with_key_opt(None, payload)
    }

    /// Publish with a partition key (used for per-key ordering downstream).
    pub fn publish_with_key<P: Serialize>(&self, key: &str, payload: &P) -> Result<i64> {
        self.publish_with_key_opt(Some(key), payload)
    }

    /// Publish inside an open transaction.
    pub fn publish_tx<P: Serialize>(&self, tx: &Transaction<'_>, payload: &P) -> Result<i64> {
        let json = serde_json::to_string(payload)?;
        tx.query_row(
            "SELECT honker_stream_publish(?1, NULL, ?2)",
            params![self.name, json],
            |r| r.get::<_, i64>(0),
        )
    }

    fn publish_with_key_opt<P: Serialize>(&self, key: Option<&str>, payload: &P) -> Result<i64> {
        let json = serde_json::to_string(payload)?;
        Ok(self.inner.with_conn(|c| {
            c.query_row(
                "SELECT honker_stream_publish(?1, ?2, ?3)",
                params![self.name, key, json],
                |r| r.get::<_, i64>(0),
            )
        })?)
    }

    /// Read events after `offset`, up to `limit`.
    pub fn read_since(&self, offset: i64, limit: i64) -> Result<Vec<StreamEvent>> {
        let rows_json: String = self.inner.with_conn(|c| {
            c.query_row(
                "SELECT honker_stream_read_since(?1, ?2, ?3)",
                params![self.name, offset, limit],
                |r| r.get(0),
            )
        })?;
        let raw: Vec<RawStreamEvent> = serde_json::from_str(&rows_json)?;
        Ok(raw.into_iter().map(StreamEvent::from).collect())
    }

    /// Read events after this consumer's saved offset. Does NOT advance
    /// the offset — call `save_offset` after processing.
    pub fn read_from_consumer(&self, consumer: &str, limit: i64) -> Result<Vec<StreamEvent>> {
        let offset = self.get_offset(consumer)?;
        self.read_since(offset, limit)
    }

    /// Save a consumer's offset. Monotonic: saving a lower offset is ignored.
    pub fn save_offset(&self, consumer: &str, offset: i64) -> Result<bool> {
        let n: i64 = self.inner.with_conn(|c| {
            c.query_row(
                "SELECT honker_stream_save_offset(?1, ?2, ?3)",
                params![consumer, self.name, offset],
                |r| r.get(0),
            )
        })?;
        Ok(n > 0)
    }

    /// Save offset inside an open transaction. Use this when you want
    /// exactly-once-within-a-business-transaction semantics.
    pub fn save_offset_tx(
        &self,
        tx: &Transaction<'_>,
        consumer: &str,
        offset: i64,
    ) -> Result<bool> {
        let n: i64 = tx.query_row(
            "SELECT honker_stream_save_offset(?1, ?2, ?3)",
            params![consumer, self.name, offset],
            |r| r.get(0),
        )?;
        Ok(n > 0)
    }

    /// Current saved offset for `consumer`, or 0 if never saved.
    pub fn get_offset(&self, consumer: &str) -> Result<i64> {
        Ok(self.inner.with_conn(|c| {
            c.query_row(
                "SELECT honker_stream_get_offset(?1, ?2)",
                params![consumer, self.name],
                |r| r.get::<_, i64>(0),
            )
        })?)
    }

    /// Subscribe as a named consumer. Resumes from saved offset, wakes
    /// on database updates, auto-saves after `save_every_n` events (or on
    /// drop). Blocking iterator.
    pub fn subscribe(&self, consumer: &str) -> Result<StreamSubscription> {
        let offset = self.get_offset(consumer)?;
        let (sub_id, rx) = self.inner.updates.subscribe();
        Ok(StreamSubscription {
            inner: self.inner.clone(),
            topic: self.name.clone(),
            consumer: consumer.to_string(),
            sub_id,
            rx,
            last_offset: offset,
            last_saved: offset,
            save_every_n: 1000,
            pending: std::collections::VecDeque::new(),
        })
    }
}

#[derive(Deserialize)]
struct RawStreamEvent {
    offset: i64,
    topic: String,
    key: Option<String>,
    payload: String,
    created_at: i64,
}

pub struct StreamEvent {
    pub offset: i64,
    pub topic: String,
    pub key: Option<String>,
    pub payload: Vec<u8>,
    pub created_at: i64,
}

impl StreamEvent {
    pub fn payload_as<T: DeserializeOwned>(&self) -> Result<T> {
        Ok(serde_json::from_slice(&self.payload)?)
    }
}

impl From<RawStreamEvent> for StreamEvent {
    fn from(r: RawStreamEvent) -> Self {
        Self {
            offset: r.offset,
            topic: r.topic,
            key: r.key,
            payload: r.payload.into_bytes(),
            created_at: r.created_at,
        }
    }
}

/// Iterator over stream events for a named consumer. Saves offset
/// every `save_every_n` events and again on drop.
pub struct StreamSubscription {
    inner: Arc<Inner>,
    topic: String,
    consumer: String,
    sub_id: u64,
    rx: Receiver<()>,
    last_offset: i64,
    last_saved: i64,
    save_every_n: i64,
    pending: std::collections::VecDeque<StreamEvent>,
}

impl StreamSubscription {
    /// Override the auto-save batch size. `0` disables auto-save —
    /// call `save_offset` manually (often inside a business tx).
    pub fn save_every(mut self, n: i64) -> Self {
        self.save_every_n = n;
        self
    }

    /// Current offset of the last-yielded event.
    pub fn offset(&self) -> i64 {
        self.last_offset
    }

    /// Explicitly persist the current offset.
    pub fn save_offset(&mut self) -> Result<()> {
        if self.last_offset > self.last_saved {
            let n: i64 = self.inner.with_conn(|c| {
                c.query_row(
                    "SELECT honker_stream_save_offset(?1, ?2, ?3)",
                    params![self.consumer, self.topic, self.last_offset],
                    |r| r.get(0),
                )
            })?;
            if n > 0 {
                self.last_saved = self.last_offset;
            }
        }
        Ok(())
    }

    fn refill(&mut self) -> Result<()> {
        let rows_json: String = self.inner.with_conn(|c| {
            c.query_row(
                "SELECT honker_stream_read_since(?1, ?2, ?3)",
                params![self.topic, self.last_offset, 100],
                |r| r.get(0),
            )
        })?;
        let raw: Vec<RawStreamEvent> = serde_json::from_str(&rows_json)?;
        for r in raw {
            self.pending.push_back(StreamEvent::from(r));
        }
        Ok(())
    }
}

impl Iterator for StreamSubscription {
    type Item = Result<StreamEvent>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(ev) = self.pending.pop_front() {
                self.last_offset = ev.offset;
                if self.save_every_n > 0
                    && self.last_offset - self.last_saved >= self.save_every_n
                    && let Err(e) = self.save_offset()
                {
                    return Some(Err(e));
                }
                return Some(Ok(ev));
            }
            if let Err(e) = self.refill() {
                return Some(Err(e));
            }
            if !self.pending.is_empty() {
                continue;
            }
            // Recv first, then drain — the opposite order would lose
            // a wakeup when a publish lands between refill() and drain.
            match self.rx.recv() {
                Ok(()) => {
                    while self.rx.try_recv().is_ok() {}
                    continue;
                }
                Err(_) => return None,
            }
        }
    }
}

impl Drop for StreamSubscription {
    fn drop(&mut self) {
        let _ = self.save_offset();
        self.inner.updates.unsubscribe(self.sub_id);
    }
}

// ---------------------------------------------------------------------
// Pub/sub listen
// ---------------------------------------------------------------------

pub struct Notification {
    pub id: i64,
    pub channel: String,
    pub payload: String,
}

impl Notification {
    pub fn payload_as<T: DeserializeOwned>(&self) -> Result<T> {
        Ok(serde_json::from_str(&self.payload)?)
    }
}

pub struct Subscription {
    inner: Arc<Inner>,
    sub_id: u64,
    rx: Receiver<()>,
    channel: String,
    last_id: i64,
    pending: std::collections::VecDeque<Notification>,
    closed: bool,
}

impl Subscription {
    /// Block until the next notification arrives. Returns `None` only
    /// when the update watcher is dropped. Also exposed via
    /// `impl Iterator` so you can write `for n in sub { ... }`.
    pub fn recv(&mut self) -> Option<Result<Notification>> {
        if self.closed {
            return None;
        }
        loop {
            if let Some(n) = self.pending.pop_front() {
                self.last_id = n.id;
                return Some(Ok(n));
            }
            if let Err(e) = self.refill() {
                return Some(Err(e));
            }
            if !self.pending.is_empty() {
                continue;
            }
            // Recv first, then drain — the opposite order would lose
            // a wakeup when a notification lands between refill() and
            // drain.
            match self.rx.recv() {
                Ok(()) => {
                    while self.rx.try_recv().is_ok() {}
                    continue;
                }
                Err(_) => {
                    self.closed = true;
                    return None;
                }
            }
        }
    }

    /// Non-blocking next. Returns `Ok(None)` if nothing is pending
    /// right now.
    pub fn try_next(&mut self) -> Result<Option<Notification>> {
        if let Some(n) = self.pending.pop_front() {
            self.last_id = n.id;
            return Ok(Some(n));
        }
        self.refill()?;
        if let Some(n) = self.pending.pop_front() {
            self.last_id = n.id;
            return Ok(Some(n));
        }
        Ok(None)
    }

    /// Recv with timeout. Returns `Ok(None)` on timeout.
    pub fn recv_timeout(&mut self, timeout: Duration) -> Result<Option<Notification>> {
        if let Some(n) = self.pending.pop_front() {
            self.last_id = n.id;
            return Ok(Some(n));
        }
        let deadline = std::time::Instant::now() + timeout;
        loop {
            self.refill()?;
            if let Some(n) = self.pending.pop_front() {
                self.last_id = n.id;
                return Ok(Some(n));
            }
            let now = std::time::Instant::now();
            if now >= deadline {
                return Ok(None);
            }
            match self.rx.recv_timeout(deadline - now) {
                Ok(()) => {
                    while self.rx.try_recv().is_ok() {}
                    continue;
                }
                Err(RecvTimeoutError::Timeout) => return Ok(None),
                Err(RecvTimeoutError::Disconnected) => {
                    self.closed = true;
                    return Ok(None);
                }
            }
        }
    }

    fn refill(&mut self) -> Result<()> {
        let rows: Vec<(i64, String, String)> = self.inner.with_conn(|c| {
            let mut stmt = c.prepare_cached(
                "SELECT id, channel, payload
                 FROM _honker_notifications
                 WHERE id > ?1 AND channel = ?2
                 ORDER BY id ASC
                 LIMIT 1000",
            )?;
            let iter = stmt.query_map(params![self.last_id, self.channel], |r| {
                Ok((
                    r.get::<_, i64>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, String>(2)?,
                ))
            })?;
            iter.collect::<rusqlite::Result<Vec<_>>>()
        })?;
        for (id, channel, payload) in rows {
            self.pending.push_back(Notification {
                id,
                channel,
                payload,
            });
        }
        Ok(())
    }
}

impl Iterator for Subscription {
    type Item = Result<Notification>;
    fn next(&mut self) -> Option<Self::Item> {
        Subscription::recv(self)
    }
}

impl Drop for Subscription {
    fn drop(&mut self) {
        self.inner.updates.unsubscribe(self.sub_id);
    }
}

/// Raw update waker. Useful for building your own poll loops.
pub struct UpdateEvents {
    inner: Arc<Inner>,
    sub_id: u64,
    rx: Receiver<()>,
}

impl UpdateEvents {
    pub fn recv(&self) -> Result<()> {
        self.rx.recv().map_err(|_| Error::UpdateClosed)
    }

    pub fn try_recv(&self) -> Option<()> {
        self.rx.try_recv().ok()
    }

    pub fn recv_timeout(&self, timeout: Duration) -> Result<Option<()>> {
        match self.rx.recv_timeout(timeout) {
            Ok(()) => Ok(Some(())),
            Err(RecvTimeoutError::Timeout) => Ok(None),
            Err(RecvTimeoutError::Disconnected) => Err(Error::UpdateClosed),
        }
    }
}

impl Drop for UpdateEvents {
    fn drop(&mut self) {
        self.inner.updates.unsubscribe(self.sub_id);
    }
}

// ---------------------------------------------------------------------
// Scheduler
// ---------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct ScheduledTask {
    pub name: String,
    pub queue: String,
    pub schedule: String,
    pub payload: serde_json::Value,
    pub priority: i64,
    pub expires_s: Option<i64>,
}

#[derive(Debug, Deserialize)]
pub struct ScheduledFire {
    pub name: String,
    pub queue: String,
    pub fire_at: i64,
    pub job_id: i64,
}

pub struct Scheduler {
    inner: Arc<Inner>,
}

impl Scheduler {
    /// Register a recurring task. Idempotent by `name`.
    pub fn add(&self, task: ScheduledTask) -> Result<()> {
        let payload_json = serde_json::to_string(&task.payload)?;
        self.inner.with_conn(|c| {
            c.query_row(
                "SELECT honker_scheduler_register(?1, ?2, ?3, ?4, ?5, ?6)",
                params![
                    task.name,
                    task.queue,
                    task.schedule,
                    payload_json,
                    task.priority,
                    task.expires_s,
                ],
                |_| Ok(()),
            )
        })?;
        Ok(())
    }

    pub fn remove(&self, name: &str) -> Result<i64> {
        Ok(self.inner.with_conn(|c| {
            c.query_row(
                "SELECT honker_scheduler_unregister(?1)",
                params![name],
                |r| r.get::<_, i64>(0),
            )
        })?)
    }

    /// Fire due boundaries and return what was enqueued.
    pub fn tick(&self) -> Result<Vec<ScheduledFire>> {
        let now = chrono_like_now();
        let rows_json: String = self.inner.with_conn(|c| {
            c.query_row("SELECT honker_scheduler_tick(?1)", params![now], |r| {
                r.get(0)
            })
        })?;
        Ok(serde_json::from_str(&rows_json)?)
    }

    /// Soonest `next_fire_at` across all tasks, or 0 if no tasks.
    pub fn soonest(&self) -> Result<i64> {
        Ok(self.inner.with_conn(|c| {
            c.query_row("SELECT honker_scheduler_soonest()", [], |r| {
                r.get::<_, i64>(0)
            })
        })?)
    }

    /// Run the scheduler loop with leader election. Blocks until
    /// `stop` is set. Only the process holding the `honker-scheduler`
    /// lock fires; standbys wait for the lock to expire.
    ///
    /// Errors from individual ticks release the lock before returning,
    /// so a standby can pick up immediately without waiting for the
    /// TTL to elapse.
    pub fn run(&self, stop: Arc<std::sync::atomic::AtomicBool>, owner: &str) -> Result<()> {
        const LOCK_TTL: i64 = 60;
        const HEARTBEAT: Duration = Duration::from_secs(20);
        let (sub_id, rx) = self.inner.updates.subscribe();

        let result = (|| -> Result<()> {
            while !stop.load(std::sync::atomic::Ordering::Acquire) {
                let acquired =
                    lock_try_acquire(&self.inner, "honker-scheduler", owner, LOCK_TTL)?;
                if !acquired {
                    match rx.recv_timeout(Duration::from_secs(5)) {
                        Ok(()) => {
                            while rx.try_recv().is_ok() {}
                            continue;
                        }
                        Err(RecvTimeoutError::Timeout) => continue,
                        Err(RecvTimeoutError::Disconnected) => return Ok(()),
                    }
                }

                let leader_result = self.leader_loop(&stop, owner, LOCK_TTL, HEARTBEAT, &rx);
                let _ = lock_release(&self.inner, "honker-scheduler", owner);
                leader_result?;
            }
            Ok(())
        })();

        self.inner.updates.unsubscribe(sub_id);
        result
    }

    fn leader_loop(
        &self,
        stop: &Arc<std::sync::atomic::AtomicBool>,
        owner: &str,
        lock_ttl: i64,
        heartbeat: Duration,
        rx: &Receiver<()>,
    ) -> Result<()> {
        let mut last_heartbeat = std::time::Instant::now();
        while !stop.load(std::sync::atomic::Ordering::Acquire) {
            self.tick()?;
            if last_heartbeat.elapsed() >= heartbeat {
                let still_ours =
                    lock_try_acquire(&self.inner, "honker-scheduler", owner, lock_ttl)?;
                if !still_ours {
                    // Lost the lock (TTL expired, new leader). Drop
                    // out of the leader loop so we don't double-fire
                    // alongside whoever has it now.
                    return Ok(());
                }
                last_heartbeat = std::time::Instant::now();
            }
            let mut wait_for = heartbeat.saturating_sub(last_heartbeat.elapsed());
            let soonest = self.soonest()?;
            if soonest > 0 {
                let now = chrono_like_now();
                let until_soonest = if soonest <= now {
                    Duration::ZERO
                } else {
                    Duration::from_secs((soonest - now) as u64)
                };
                if until_soonest < wait_for {
                    wait_for = until_soonest;
                }
            }
            match rx.recv_timeout(wait_for) {
                Ok(()) => {
                    while rx.try_recv().is_ok() {}
                }
                Err(RecvTimeoutError::Timeout) => {}
                Err(RecvTimeoutError::Disconnected) => return Ok(()),
            }
        }
        Ok(())
    }
}

fn recv_until(rx: &Receiver<()>, unix_sec: i64) -> std::result::Result<bool, RecvTimeoutError> {
    if unix_sec <= 0 {
        match rx.recv() {
            Ok(()) => {
                while rx.try_recv().is_ok() {}
                Ok(true)
            }
            Err(_) => Err(RecvTimeoutError::Disconnected),
        }
    } else {
        let now = chrono_like_now();
        let timeout = if unix_sec <= now {
            Duration::ZERO
        } else {
            Duration::from_secs((unix_sec - now) as u64)
        };
        match rx.recv_timeout(timeout) {
            Ok(()) => {
                while rx.try_recv().is_ok() {}
                Ok(true)
            }
            Err(e) => Err(e),
        }
    }
}

fn lock_try_acquire(inner: &Inner, name: &str, owner: &str, ttl_s: i64) -> Result<bool> {
    let n: i64 = inner.with_conn(|c| {
        c.query_row(
            "SELECT honker_lock_acquire(?1, ?2, ?3)",
            params![name, owner, ttl_s],
            |r| r.get(0),
        )
    })?;
    Ok(n == 1)
}

fn lock_release(inner: &Inner, name: &str, owner: &str) -> Result<bool> {
    let n: i64 = inner.with_conn(|c| {
        c.query_row(
            "SELECT honker_lock_release(?1, ?2)",
            params![name, owner],
            |r| r.get(0),
        )
    })?;
    Ok(n > 0)
}

fn chrono_like_now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

// ---------------------------------------------------------------------
// Advisory locks (RAII)
// ---------------------------------------------------------------------

pub struct Lock {
    inner: Arc<Inner>,
    name: String,
    owner: String,
    released: bool,
}

impl Lock {
    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn owner(&self) -> &str {
        &self.owner
    }

    /// Explicit release. Returns true if we still held it.
    pub fn release(mut self) -> Result<bool> {
        let n: i64 = self.inner.with_conn(|c| {
            c.query_row(
                "SELECT honker_lock_release(?1, ?2)",
                params![self.name, self.owner],
                |r| r.get(0),
            )
        })?;
        self.released = true;
        Ok(n > 0)
    }

    /// Extend the TTL. Returns `Ok(true)` if we still hold the lock,
    /// `Ok(false)` if the lock was stolen (TTL expired and someone
    /// else acquired it). Check the return value — holding the `Lock`
    /// value alone does not guarantee you still own the lock.
    pub fn heartbeat(&self, ttl_s: i64) -> Result<bool> {
        lock_try_acquire(&self.inner, &self.name, &self.owner, ttl_s)
    }
}

impl Drop for Lock {
    fn drop(&mut self) {
        if !self.released {
            let _ = self.inner.with_conn(|c| {
                c.query_row(
                    "SELECT honker_lock_release(?1, ?2)",
                    params![self.name, self.owner],
                    |_| Ok(()),
                )
            });
        }
    }
}
