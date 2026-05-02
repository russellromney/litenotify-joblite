use honker::{Database, EnqueueOpts, QueueOpts};
use serde_json::json;

#[test]
fn enqueue_claim_ack() {
    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("t.db");
    let db = Database::open(&db_path).unwrap();

    let q = db.queue("emails", QueueOpts::default());

    let id = q
        .enqueue(&json!({"to": "alice@example.com"}), EnqueueOpts::default())
        .unwrap();
    assert!(id > 0);

    let job = q.claim_one("worker-1").unwrap().expect("should claim");
    assert_eq!(job.id, id);
    assert_eq!(job.attempts, 1);

    let payload: serde_json::Value = job.payload_as().unwrap();
    assert_eq!(payload["to"], "alice@example.com");

    assert!(job.ack().unwrap(), "fresh claim should ack successfully");
    assert!(
        q.claim_one("worker-1").unwrap().is_none(),
        "queue empty after ack"
    );
}

#[test]
fn retry_to_dead() {
    let tmp = tempfile::tempdir().unwrap();
    let db = Database::open(tmp.path().join("t.db")).unwrap();
    let q = db.queue(
        "retries",
        QueueOpts {
            max_attempts: 2,
            ..Default::default()
        },
    );

    q.enqueue(&json!({"i": 1}), EnqueueOpts::default()).unwrap();

    let job = q.claim_one("w").unwrap().unwrap();
    assert!(job.retry(0, "first").unwrap());

    let job2 = q.claim_one("w").unwrap().unwrap();
    assert_eq!(job2.attempts, 2);
    assert!(job2.retry(0, "second").unwrap());
    assert!(q.claim_one("w").unwrap().is_none(), "should be in dead now");

    let dead_count: i64 = db.with_conn(|c| {
        c.query_row(
            "SELECT COUNT(*) FROM _honker_dead WHERE queue='retries'",
            [],
            |r| r.get(0),
        )
        .unwrap()
    });
    assert_eq!(dead_count, 1);
}

#[test]
fn notify_inserts_row() {
    let tmp = tempfile::tempdir().unwrap();
    let db = Database::open(tmp.path().join("t.db")).unwrap();
    let id = db.notify("orders", &json!({"id": 42})).unwrap();
    assert!(id > 0);

    let (channel, payload): (String, String) = db.with_conn(|c| {
        c.query_row(
            "SELECT channel, payload FROM _honker_notifications WHERE id = ?1",
            [id],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap()
    });
    assert_eq!(channel, "orders");
    assert_eq!(payload, r#"{"id":42}"#);
}
