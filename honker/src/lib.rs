//! Commit-hook driven NOTIFY/LISTEN for SQLite.
//!
//! `Notifier` maintains a per-channel subscriber registry. Each `subscribe`
//! hands back a `Subscription` carrying a unique id and a tokio broadcast
//! `Receiver`. When a connection calls the registered `honk()` scalar
//! function inside a transaction, the notification is buffered until
//! commit; on commit the hook groups pending notifications by channel and
//! fans out to exactly the subscribers of that channel. On rollback the
//! buffer is dropped.
//!
//! The `unsubscribe` method removes a subscriber by id; this is what lets
//! the language bindings implement proper Drop-based cleanup so bridge
//! threads waiting on a receiver can exit when the subscription goes away.
//!
//! IMPORTANT: SQLite's `commit_hook` does NOT fire for `BEGIN DEFERRED`
//! transactions with no writes. Callers must use `BEGIN IMMEDIATE` (or
//! perform at least one write) for `honk()` notifications to be emitted.

use parking_lot::Mutex;
use rusqlite::Connection;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::broadcast;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Database error: {0}")]
    Sqlite(#[from] rusqlite::Error),
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq)]
pub struct Notification {
    pub channel: String,
    pub payload: String,
}

/// Per-subscriber handle; drop this (via `Notifier::unsubscribe`) to release
/// the registry slot.
pub struct Subscription {
    pub id: u64,
    pub channel: String,
    pub rx: broadcast::Receiver<Notification>,
}

struct SubscriberEntry {
    id: u64,
    tx: broadcast::Sender<Notification>,
}

#[derive(Default)]
struct NotifierInner {
    by_channel: HashMap<String, Vec<SubscriberEntry>>,
}

pub struct Notifier {
    inner: Arc<Mutex<NotifierInner>>,
    next_id: AtomicU64,
    buffer_size: usize,
}

impl Default for Notifier {
    fn default() -> Self {
        Self::new()
    }
}

impl Notifier {
    pub fn new() -> Self {
        Self::with_buffer(1024)
    }

    /// `buffer_size` is the per-subscriber broadcast ring size. Slow
    /// subscribers drop oldest messages when full (same semantics as
    /// `pg_notify` from the receiver's point of view).
    pub fn with_buffer(buffer_size: usize) -> Self {
        Self {
            inner: Arc::new(Mutex::new(NotifierInner::default())),
            next_id: AtomicU64::new(1),
            buffer_size: buffer_size.max(1),
        }
    }

    /// Subscribe to a single channel. Returns a unique `id` and a
    /// `Receiver`. Call `unsubscribe(id)` when done.
    pub fn subscribe(&self, channel: impl Into<String>) -> Subscription {
        let channel = channel.into();
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = broadcast::channel(self.buffer_size);
        let mut inner = self.inner.lock();
        inner
            .by_channel
            .entry(channel.clone())
            .or_default()
            .push(SubscriberEntry { id, tx });
        Subscription { id, channel, rx }
    }

    /// Remove a subscriber by id. Idempotent; a no-op if the id is unknown.
    /// Dropping the returned `Subscription` from `subscribe()` does NOT
    /// automatically unsubscribe — call this from your binding's Drop impl.
    pub fn unsubscribe(&self, id: u64) {
        let mut inner = self.inner.lock();
        let mut empty_channels: Vec<String> = Vec::new();
        for (ch, subs) in inner.by_channel.iter_mut() {
            let before = subs.len();
            subs.retain(|s| s.id != id);
            if subs.len() != before && subs.is_empty() {
                empty_channels.push(ch.clone());
            }
        }
        for ch in empty_channels {
            inner.by_channel.remove(&ch);
        }
    }

    /// Attach the `honk(channel, payload)` scalar function plus commit and
    /// rollback hooks to `conn`.
    pub fn attach(&self, conn: &Connection) -> Result<(), Error> {
        let pending: Arc<Mutex<Vec<Notification>>> =
            Arc::new(Mutex::new(Vec::new()));

        let pending_honk = Arc::clone(&pending);
        conn.create_scalar_function(
            "honk",
            2,
            rusqlite::functions::FunctionFlags::SQLITE_UTF8,
            move |ctx| {
                let channel: String = ctx.get(0)?;
                let payload: String = ctx.get(1)?;
                pending_honk.lock().push(Notification { channel, payload });
                Ok(true)
            },
        )?;

        let pending_commit = Arc::clone(&pending);
        let inner_commit = Arc::clone(&self.inner);
        let _ = conn.commit_hook(Some(move || {
            // Drain the pending buffer first so we hold its lock for the
            // minimum time.
            let drained: Vec<Notification> =
                std::mem::take(&mut *pending_commit.lock());
            if drained.is_empty() {
                return false;
            }
            let registry = inner_commit.lock();
            for notif in drained {
                if let Some(subs) = registry.by_channel.get(&notif.channel) {
                    for sub in subs.iter() {
                        let _ = sub.tx.send(notif.clone());
                    }
                }
            }
            false
        }));

        let pending_rollback = Arc::clone(&pending);
        let _ = conn.rollback_hook(Some(move || {
            pending_rollback.lock().clear();
        }));

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn setup() -> (Connection, Notifier) {
        let conn = Connection::open_in_memory().unwrap();
        let notifier = Notifier::new();
        notifier.attach(&conn).unwrap();
        (conn, notifier)
    }

    fn begin(conn: &Connection) {
        conn.execute_batch("BEGIN IMMEDIATE;").unwrap();
    }
    fn commit(conn: &Connection) {
        conn.execute_batch("COMMIT;").unwrap();
    }
    fn rollback(conn: &Connection) {
        conn.execute_batch("ROLLBACK;").unwrap();
    }

    #[test]
    fn test_honk_rollback_produces_no_notification() {
        let (conn, notifier) = setup();
        let mut sub = notifier.subscribe("test");

        begin(&conn);
        conn.query_row("SELECT honk('test', 'data')", [], |_| Ok(()))
            .unwrap();
        rollback(&conn);

        assert!(sub.rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn test_honk_commit_produces_exactly_one_notification_per_subscriber() {
        let (conn, notifier) = setup();
        let mut sub1 = notifier.subscribe("test");
        let mut sub2 = notifier.subscribe("test");

        begin(&conn);
        conn.query_row("SELECT honk('test', 'data')", [], |_| Ok(()))
            .unwrap();
        commit(&conn);

        let msg1 = sub1.rx.recv().await.unwrap();
        assert_eq!(msg1.channel, "test");
        assert_eq!(msg1.payload, "data");
        assert!(sub1.rx.try_recv().is_err());

        let msg2 = sub2.rx.recv().await.unwrap();
        assert_eq!(msg2, msg1);
        assert!(sub2.rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn test_cross_channel_isolation() {
        // Listener on "cold" must not see messages on "hot", and must not
        // get Lagged because "hot" spammed — they have independent ring
        // buffers.
        let (conn, notifier) = setup();
        let mut cold = notifier.subscribe("cold");

        begin(&conn);
        for i in 0..5000 {
            conn.query_row(
                "SELECT honk('hot', ?1)",
                [format!("p{}", i)],
                |_| Ok(()),
            )
            .unwrap();
        }
        conn.query_row("SELECT honk('cold', 'only')", [], |_| Ok(()))
            .unwrap();
        commit(&conn);

        let n = cold.rx.recv().await.unwrap();
        assert_eq!(n.channel, "cold");
        assert_eq!(n.payload, "only");
        assert!(cold.rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn test_channel_routing_does_not_cross() {
        let (conn, notifier) = setup();
        let mut a = notifier.subscribe("a");
        let mut b = notifier.subscribe("b");

        begin(&conn);
        conn.query_row("SELECT honk('a', '1')", [], |_| Ok(())).unwrap();
        conn.query_row("SELECT honk('b', '2')", [], |_| Ok(())).unwrap();
        conn.query_row("SELECT honk('a', '3')", [], |_| Ok(())).unwrap();
        commit(&conn);

        let n1 = a.rx.recv().await.unwrap();
        assert_eq!(n1.payload, "1");
        let n2 = a.rx.recv().await.unwrap();
        assert_eq!(n2.payload, "3");
        assert!(a.rx.try_recv().is_err());

        let m1 = b.rx.recv().await.unwrap();
        assert_eq!(m1.payload, "2");
        assert!(b.rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn test_unsubscribe_frees_the_slot_and_closes_receiver() {
        let (conn, notifier) = setup();
        let sub = notifier.subscribe("x");
        let id = sub.id;
        let mut rx = sub.rx;
        // Unsubscribe. The broadcast::Sender in the registry is dropped,
        // which means any subsequent recv on this receiver returns Closed
        // once the channel has no senders left.
        notifier.unsubscribe(id);

        // With sender dropped, recv() should resolve to Err(Closed).
        let err = rx.recv().await.unwrap_err();
        matches!(err, tokio::sync::broadcast::error::RecvError::Closed);

        // Further honks on this channel no longer route to anyone.
        begin(&conn);
        conn.query_row("SELECT honk('x', 'nope')", [], |_| Ok(()))
            .unwrap();
        commit(&conn);
        // Channel entry removed; no subscribers; nothing to assert beyond
        // "didn't panic".
    }

    #[test]
    fn test_unsubscribe_unknown_id_is_noop() {
        let notifier = Notifier::new();
        notifier.unsubscribe(999);
    }

    #[tokio::test]
    async fn test_multiple_honks_in_one_tx_are_delivered_in_order() {
        let (conn, notifier) = setup();
        let mut sub = notifier.subscribe("ch");

        begin(&conn);
        for i in 0..5 {
            conn.query_row("SELECT honk('ch', ?1)", [format!("p{}", i)], |_| Ok(()))
                .unwrap();
        }
        commit(&conn);

        for i in 0..5 {
            let n = sub.rx.recv().await.unwrap();
            assert_eq!(n.payload, format!("p{}", i));
        }
        assert!(sub.rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn test_full_rollback_drops_all_pending() {
        let (conn, notifier) = setup();
        let mut sub = notifier.subscribe("a");
        let mut sub_b = notifier.subscribe("b");

        begin(&conn);
        conn.query_row("SELECT honk('a', '1')", [], |_| Ok(())).unwrap();
        conn.execute_batch("SAVEPOINT sp1;").unwrap();
        conn.query_row("SELECT honk('b', '2')", [], |_| Ok(())).unwrap();
        conn.execute_batch("RELEASE sp1;").unwrap();
        rollback(&conn);

        assert!(sub.rx.try_recv().is_err());
        assert!(sub_b.rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn test_unicode_and_large_payloads_roundtrip() {
        let (conn, notifier) = setup();
        let mut sub = notifier.subscribe("ch");

        let big = "a".repeat(1_000_000);
        let unicode = "héllo 🦆 мир";
        begin(&conn);
        conn.query_row("SELECT honk('ch', ?1)", [unicode], |_| Ok(()))
            .unwrap();
        conn.query_row("SELECT honk('ch', ?1)", [big.clone()], |_| Ok(()))
            .unwrap();
        commit(&conn);

        let a = sub.rx.recv().await.unwrap();
        assert_eq!(a.payload, unicode);
        let b = sub.rx.recv().await.unwrap();
        assert_eq!(b.payload.len(), 1_000_000);
        assert_eq!(b.payload, big);
    }

    #[tokio::test]
    async fn test_subscribe_before_attach_is_safe() {
        let notifier = Notifier::new();
        let mut sub = notifier.subscribe("x");
        let conn = Connection::open_in_memory().unwrap();
        notifier.attach(&conn).unwrap();

        begin(&conn);
        conn.query_row("SELECT honk('x', 'y')", [], |_| Ok(()))
            .unwrap();
        commit(&conn);

        let n = sub.rx.recv().await.unwrap();
        assert_eq!(n.channel, "x");
        assert_eq!(n.payload, "y");
    }

    #[test]
    fn test_deferred_readonly_transaction_does_not_deliver_honk() {
        // Documented contract: SQLite skips commit_hook for read-only
        // DEFERRED transactions. Python/TS bindings always use
        // BEGIN IMMEDIATE, which is why this works in practice there.
        let (conn, notifier) = setup();
        let mut sub = notifier.subscribe("ch");

        conn.execute_batch("BEGIN DEFERRED;").unwrap();
        conn.query_row("SELECT honk('ch', 'lost')", [], |_| Ok(()))
            .unwrap();
        conn.execute_batch("COMMIT;").unwrap();

        assert!(sub.rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn test_no_subscribers_honk_is_dropped_silently() {
        let (conn, _notifier) = setup();
        begin(&conn);
        conn.query_row("SELECT honk('orphan', 'lost')", [], |_| Ok(()))
            .unwrap();
        commit(&conn); // no panic; no receiver registered; hook just no-ops
    }
}
