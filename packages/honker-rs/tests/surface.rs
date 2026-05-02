//! Feature-parity surface tests: transactions, streams, listen,
//! scheduler, locks, rate limits, results.

use honker::{Database, EnqueueOpts, QueueOpts, ScheduledFire, ScheduledTask};
use serde_json::json;
use std::sync::{Arc, atomic::AtomicBool};
use std::time::Duration;

#[test]
fn enqueue_tx_atomic_with_business_write() {
    let tmp = tempfile::tempdir().unwrap();
    let db = Database::open(tmp.path().join("t.db")).unwrap();
    let q = db.queue("emails", QueueOpts::default());

    db.with_conn(|c| {
        c.execute(
            "CREATE TABLE orders (id INTEGER PRIMARY KEY, total INTEGER)",
            [],
        )
        .unwrap();
    });

    // Happy path: commit persists both rows.
    {
        let tx = db.transaction().unwrap();
        tx.execute("INSERT INTO orders (id, total) VALUES (?1, ?2)", [1, 100])
            .unwrap();
        q.enqueue_tx(&tx, &json!({"order_id": 1}), EnqueueOpts::default())
            .unwrap();
        tx.commit().unwrap();
    }
    let orders: i64 = db.with_conn(|c| {
        c.query_row("SELECT COUNT(*) FROM orders", [], |r| r.get(0))
            .unwrap()
    });
    let live: i64 = db.with_conn(|c| {
        c.query_row("SELECT COUNT(*) FROM _honker_live", [], |r| r.get(0))
            .unwrap()
    });
    assert_eq!(orders, 1);
    assert_eq!(live, 1);

    // Rollback drops both.
    {
        let tx = db.transaction().unwrap();
        tx.execute("INSERT INTO orders (id, total) VALUES (?1, ?2)", [2, 200])
            .unwrap();
        q.enqueue_tx(&tx, &json!({"order_id": 2}), EnqueueOpts::default())
            .unwrap();
        tx.rollback().unwrap();
    }
    let orders_after: i64 = db.with_conn(|c| {
        c.query_row("SELECT COUNT(*) FROM orders", [], |r| r.get(0))
            .unwrap()
    });
    let live_after: i64 = db.with_conn(|c| {
        c.query_row("SELECT COUNT(*) FROM _honker_live", [], |r| r.get(0))
            .unwrap()
    });
    assert_eq!(orders_after, 1, "rollback must not persist orders");
    assert_eq!(live_after, 1, "rollback must not persist queue row");
}

#[test]
fn transaction_drop_rolls_back() {
    let tmp = tempfile::tempdir().unwrap();
    let db = Database::open(tmp.path().join("t.db")).unwrap();
    let q = db.queue("emails", QueueOpts::default());

    {
        let tx = db.transaction().unwrap();
        q.enqueue_tx(&tx, &json!({"x": 1}), EnqueueOpts::default())
            .unwrap();
        // Drop without commit.
    }

    let n: i64 = db.with_conn(|c| {
        c.query_row("SELECT COUNT(*) FROM _honker_live", [], |r| r.get(0))
            .unwrap()
    });
    assert_eq!(n, 0, "dropped tx must roll back");
}

#[test]
fn batch_claim_and_ack() {
    let tmp = tempfile::tempdir().unwrap();
    let db = Database::open(tmp.path().join("t.db")).unwrap();
    let q = db.queue("bulk", QueueOpts::default());

    for i in 0..5 {
        q.enqueue(&json!({"i": i}), EnqueueOpts::default()).unwrap();
    }

    let jobs = q.claim_batch("w", 10).unwrap();
    assert_eq!(jobs.len(), 5);

    let ids: Vec<i64> = jobs.iter().map(|j| j.id).collect();
    let acked = q.ack_batch(&ids, "w").unwrap();
    assert_eq!(acked, 5);

    assert!(q.claim_one("w").unwrap().is_none());
}

#[test]
fn heartbeat_extends_claim() {
    let tmp = tempfile::tempdir().unwrap();
    let db = Database::open(tmp.path().join("t.db")).unwrap();
    let q = db.queue(
        "hb",
        QueueOpts {
            visibility_timeout_s: 1,
            ..Default::default()
        },
    );

    q.enqueue(&json!({}), EnqueueOpts::default()).unwrap();
    let job = q.claim_one("w").unwrap().unwrap();
    assert!(job.heartbeat(60).unwrap(), "fresh claim should heartbeat");
    assert!(job.ack().unwrap());
}

#[test]
fn stream_publish_and_read() {
    let tmp = tempfile::tempdir().unwrap();
    let db = Database::open(tmp.path().join("t.db")).unwrap();
    let s = db.stream("orders");

    let off1 = s.publish(&json!({"id": 1})).unwrap();
    let off2 = s.publish(&json!({"id": 2})).unwrap();
    assert!(off2 > off1);

    let events = s.read_since(0, 10).unwrap();
    assert_eq!(events.len(), 2);
    let first: serde_json::Value = events[0].payload_as().unwrap();
    assert_eq!(first["id"], 1);
    assert_eq!(events[0].topic, "orders");
}

#[test]
fn stream_consumer_offsets() {
    let tmp = tempfile::tempdir().unwrap();
    let db = Database::open(tmp.path().join("t.db")).unwrap();
    let s = db.stream("events");

    s.publish(&json!({"n": 1})).unwrap();
    s.publish(&json!({"n": 2})).unwrap();
    s.publish(&json!({"n": 3})).unwrap();

    // Consumer hasn't saved anything yet -> reads from 0.
    let batch = s.read_from_consumer("email-worker", 100).unwrap();
    assert_eq!(batch.len(), 3);

    // Advance and save.
    let last = batch.last().unwrap().offset;
    assert!(s.save_offset("email-worker", last).unwrap());

    // Next read is empty until new publishes.
    assert!(
        s.read_from_consumer("email-worker", 100)
            .unwrap()
            .is_empty()
    );

    s.publish(&json!({"n": 4})).unwrap();
    let next = s.read_from_consumer("email-worker", 100).unwrap();
    assert_eq!(next.len(), 1);
    let p: serde_json::Value = next[0].payload_as().unwrap();
    assert_eq!(p["n"], 4);
}

#[test]
fn stream_save_offset_tx() {
    let tmp = tempfile::tempdir().unwrap();
    let db = Database::open(tmp.path().join("t.db")).unwrap();
    let s = db.stream("t");

    s.publish(&json!({"n": 1})).unwrap();

    // Save offset in a rolled-back tx — offset must NOT advance.
    {
        let tx = db.transaction().unwrap();
        s.save_offset_tx(&tx, "c", 1).unwrap();
        tx.rollback().unwrap();
    }
    assert_eq!(s.get_offset("c").unwrap(), 0);

    // Save offset in a committed tx — offset DOES advance.
    {
        let tx = db.transaction().unwrap();
        s.save_offset_tx(&tx, "c", 1).unwrap();
        tx.commit().unwrap();
    }
    assert_eq!(s.get_offset("c").unwrap(), 1);
}

#[test]
fn listen_delivers_notifications() {
    let tmp = tempfile::tempdir().unwrap();
    let db = Database::open(tmp.path().join("t.db")).unwrap();

    let mut sub = db.listen("orders").unwrap();

    // Publish from a background thread so the main thread can block on recv.
    let db2 = db.clone();
    let t = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(20));
        db2.notify("orders", &json!({"id": 99})).unwrap();
    });

    let notif = sub
        .recv_timeout(Duration::from_secs(2))
        .unwrap()
        .expect("should receive within 2s");
    assert_eq!(notif.channel, "orders");
    let p: serde_json::Value = notif.payload_as().unwrap();
    assert_eq!(p["id"], 99);

    t.join().unwrap();
}

#[test]
fn listen_filters_by_channel() {
    let tmp = tempfile::tempdir().unwrap();
    let db = Database::open(tmp.path().join("t.db")).unwrap();
    let mut sub = db.listen("orders").unwrap();

    db.notify("shipments", &json!({"id": 1})).unwrap();
    db.notify("orders", &json!({"id": 2})).unwrap();
    db.notify("shipments", &json!({"id": 3})).unwrap();

    let n = sub
        .recv_timeout(Duration::from_secs(2))
        .unwrap()
        .expect("should get the orders notification");
    let p: serde_json::Value = n.payload_as().unwrap();
    assert_eq!(p["id"], 2);

    // No more orders pending.
    let next = sub.recv_timeout(Duration::from_millis(100)).unwrap();
    assert!(
        next.is_none(),
        "shipments notifs must not leak into orders sub"
    );
}

#[test]
fn scheduler_register_and_tick() {
    let tmp = tempfile::tempdir().unwrap();
    let db = Database::open(tmp.path().join("t.db")).unwrap();
    let sched = db.scheduler();

    sched
        .add(ScheduledTask {
            name: "every-minute".into(),
            queue: "health".into(),
            schedule: "* * * * *".into(),
            payload: json!({"k": "v"}),
            priority: 0,
            expires_s: None,
        })
        .unwrap();

    let soonest = sched.soonest().unwrap();
    assert!(soonest > 0);

    // Unregister.
    let removed = sched.remove("every-minute").unwrap();
    assert_eq!(removed, 1);
    assert_eq!(sched.soonest().unwrap(), 0);
}

#[test]
fn advisory_lock_mutual_exclusion() {
    let tmp = tempfile::tempdir().unwrap();
    let db = Database::open(tmp.path().join("t.db")).unwrap();

    let lock = db.try_lock("critical", "owner-a", 60).unwrap();
    assert!(lock.is_some());

    assert!(
        db.try_lock("critical", "owner-b", 60).unwrap().is_none(),
        "second acquire must fail while a owns the lock"
    );

    drop(lock);

    let lock_b = db.try_lock("critical", "owner-b", 60).unwrap();
    assert!(lock_b.is_some(), "lock must release on drop");
}

#[test]
fn lock_explicit_release() {
    let tmp = tempfile::tempdir().unwrap();
    let db = Database::open(tmp.path().join("t.db")).unwrap();

    let l = db.try_lock("r", "owner", 60).unwrap().unwrap();
    let released = l.release().unwrap();
    assert!(released);

    assert!(
        db.try_lock("r", "other", 60).unwrap().is_some(),
        "released lock is immediately re-acquirable"
    );
}

#[test]
fn rate_limit_allows_up_to_limit() {
    let tmp = tempfile::tempdir().unwrap();
    let db = Database::open(tmp.path().join("t.db")).unwrap();

    for _ in 0..3 {
        assert!(db.try_rate_limit("api", 3, 60).unwrap());
    }
    assert!(!db.try_rate_limit("api", 3, 60).unwrap(), "4th must fail");
}

#[test]
fn results_save_and_get() {
    let tmp = tempfile::tempdir().unwrap();
    let db = Database::open(tmp.path().join("t.db")).unwrap();

    db.save_result(42, r#"{"ok":true}"#, 3600).unwrap();
    let v = db.get_result(42).unwrap();
    assert_eq!(v.as_deref(), Some(r#"{"ok":true}"#));

    assert_eq!(
        db.get_result(999).unwrap(),
        None,
        "missing key returns None"
    );
}

#[test]
fn scheduler_run_stops_on_signal() {
    let tmp = tempfile::tempdir().unwrap();
    let db = Database::open(tmp.path().join("t.db")).unwrap();
    let sched = db.scheduler();

    let stop = Arc::new(AtomicBool::new(false));
    let stop_t = stop.clone();
    let handle = std::thread::spawn(move || {
        sched.run(stop_t, "test-owner").unwrap();
    });

    // Let it acquire the lock and tick once.
    std::thread::sleep(Duration::from_millis(200));
    stop.store(true, std::sync::atomic::Ordering::Release);

    // Join within a reasonable window — the run loop sleeps 1s between
    // ticks when it holds the lock, so up to ~1.1s to exit cleanly.
    let joined = handle.join();
    assert!(joined.is_ok());
}

#[test]
fn lock_heartbeat_extends_ttl() {
    let tmp = tempfile::tempdir().unwrap();
    let db = Database::open(tmp.path().join("t.db")).unwrap();

    let lock = db.try_lock("key", "me", 1).unwrap().unwrap();
    // Original TTL would expire in 1s; heartbeat extends to 60s.
    assert!(
        lock.heartbeat(60).unwrap(),
        "still ours right after acquire"
    );

    // A different owner trying to acquire should fail.
    assert!(
        db.try_lock("key", "other", 60).unwrap().is_none(),
        "heartbeat keeps us the owner"
    );
}

#[test]
fn notify_tx_rolls_back_with_tx() {
    let tmp = tempfile::tempdir().unwrap();
    let db = Database::open(tmp.path().join("t.db")).unwrap();

    let before: i64 = db.with_conn(|c| {
        c.query_row("SELECT COUNT(*) FROM _honker_notifications", [], |r| {
            r.get(0)
        })
        .unwrap()
    });

    {
        let tx = db.transaction().unwrap();
        db.notify_tx(&tx, "orders", &json!({"id": 1})).unwrap();
        tx.rollback().unwrap();
    }

    let after: i64 = db.with_conn(|c| {
        c.query_row("SELECT COUNT(*) FROM _honker_notifications", [], |r| {
            r.get(0)
        })
        .unwrap()
    });
    assert_eq!(before, after, "rolled-back notify must not persist");
}

#[test]
fn prune_notifications_keep_latest() {
    let tmp = tempfile::tempdir().unwrap();
    let db = Database::open(tmp.path().join("t.db")).unwrap();

    for i in 0..10 {
        db.notify("c", &json!({"i": i})).unwrap();
    }

    let deleted = db.prune_notifications_keep_latest(3).unwrap();
    assert_eq!(deleted, 7);

    let count: i64 = db.with_conn(|c| {
        c.query_row("SELECT COUNT(*) FROM _honker_notifications", [], |r| {
            r.get(0)
        })
        .unwrap()
    });
    assert_eq!(count, 3);
}

#[test]
fn claim_waker_wakes_on_enqueue() {
    let tmp = tempfile::tempdir().unwrap();
    let db = Database::open(tmp.path().join("t.db")).unwrap();
    let q = db.queue("live", QueueOpts::default());
    let waker = q.claim_waker();

    // Empty queue — waker.try_next returns None.
    assert!(waker.try_next("w").unwrap().is_none());

    // Enqueue from another thread; the main thread blocks on waker.next.
    let db2 = db.clone();
    std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(30));
        let q2 = db2.queue("live", QueueOpts::default());
        q2.enqueue(&json!({"k": "v"}), EnqueueOpts::default())
            .unwrap();
    });

    // Use a timer-bounded assertion: the waker should return within 1s.
    let (tx, rx) = std::sync::mpsc::channel();
    let worker_waker = q.claim_waker();
    std::thread::spawn(move || {
        let job = worker_waker.next("w").unwrap();
        tx.send(job).unwrap();
    });
    // Close the first, unused waker so it unsubscribes.
    drop(waker);

    let got = rx
        .recv_timeout(Duration::from_secs(2))
        .expect("waker should wake within 2s");
    let job = got.expect("should have a job");
    let p: serde_json::Value = job.payload_as().unwrap();
    assert_eq!(p["k"], "v");
    job.ack().unwrap();
}

#[test]
fn claim_waker_wakes_on_run_at_deadline() {
    let tmp = tempfile::tempdir().unwrap();
    let db = Database::open(tmp.path().join("t.db")).unwrap();
    let q = db.queue("runat", QueueOpts::default());

    q.enqueue(
        &json!({"x": 1}),
        EnqueueOpts {
            run_at: Some(
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_secs() as i64
                    + 1,
            ),
            ..EnqueueOpts::default()
        },
    )
    .unwrap();

    let waker = q.claim_waker();
    let start = std::time::Instant::now();
    let job = waker.next("w").unwrap().expect("should have a job");
    let elapsed = start.elapsed();
    assert!(
        elapsed >= Duration::from_millis(700),
        "run_at wake came too early: {:?}",
        elapsed
    );
    assert!(
        elapsed <= Duration::from_millis(2500),
        "run_at wake came too late: {:?}",
        elapsed
    );
    job.ack().unwrap();
}

#[test]
fn scheduler_accepts_every_second_expression() {
    let tmp = tempfile::tempdir().unwrap();
    let db = Database::open(tmp.path().join("t.db")).unwrap();
    let sched = db.scheduler();

    sched
        .add(ScheduledTask {
            name: "fast".into(),
            queue: "beats".into(),
            schedule: "@every 1s".into(),
            payload: json!({"ok": true}),
            priority: 0,
            expires_s: None,
        })
        .unwrap();

    let soonest = sched.soonest().unwrap();
    assert!(soonest > 0);

    let rows_json: String = db
        .with_conn(|c| {
            c.query_row("SELECT honker_scheduler_tick(?1)", [soonest], |r| r.get(0))
                .unwrap()
        });
    let fires: Vec<ScheduledFire> = serde_json::from_str(&rows_json).unwrap();
    assert_eq!(fires.len(), 1);
}

#[test]
fn update_events_wake_on_commit() {
    let tmp = tempfile::tempdir().unwrap();
    let db = Database::open(tmp.path().join("t.db")).unwrap();

    let events = db.update_events();

    let db2 = db.clone();
    std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(30));
        db2.notify("anything", &json!({})).unwrap();
    });

    let got = events.recv_timeout(Duration::from_secs(2)).unwrap();
    assert_eq!(got, Some(()), "should wake within 2s of the commit");
}
