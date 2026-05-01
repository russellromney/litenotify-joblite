//! Basic example: enqueue a few jobs, claim them, ack each.
//!
//! cargo run --example basic

use honker::{Database, EnqueueOpts, QueueOpts};
use serde_json::json;

fn main() -> honker::Result<()> {
    let db = Database::open("demo.db")?;
    let q = db.queue("emails", QueueOpts::default());

    for i in 0..3 {
        q.enqueue(
            &json!({"to": format!("user-{}@example.com", i)}),
            EnqueueOpts::default(),
        )?;
    }

    while let Some(job) = q.claim_one("worker-1")? {
        let body: serde_json::Value = job.payload_as()?;
        println!("processing job {}: to={}", job.id, body["to"]);
        job.ack()?;
    }

    Ok(())
}
