use honker::{Database, EnqueueOpts, QueueOpts};
use serde_json::json;
use std::path::{Path, PathBuf};
use std::process::Command;

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("..")
        .canonicalize()
        .unwrap()
}

fn interop_python() -> Option<PathBuf> {
    if let Ok(path) = std::env::var("HONKER_INTEROP_PYTHON") {
        let status = Command::new(&path)
            .arg("-c")
            .arg(python_probe())
            .env("PYTHONPATH", python_path())
            .status()
            .unwrap_or_else(|e| panic!("run HONKER_INTEROP_PYTHON={path}: {e}"));
        assert!(
            status.success(),
            "HONKER_INTEROP_PYTHON cannot import honker"
        );
        return Some(path.into());
    }

    let root = repo_root();
    for candidate in [
        root.join(".venv").join("bin").join("python"),
        root.join(".venv").join("Scripts").join("python.exe"),
        PathBuf::from("python3"),
        PathBuf::from("python"),
    ] {
        if Command::new(&candidate)
            .arg("-c")
            .arg(python_probe())
            .env("PYTHONPATH", python_path())
            .status()
            .map(|s| s.success())
            .unwrap_or(false)
        {
            return Some(candidate);
        }
    }
    None
}

fn python_probe() -> &'static str {
    r#"import os, tempfile, honker
p = tempfile.mktemp(prefix="honker-probe-", suffix=".db")
db = honker.open(p)
db.query("SELECT 1")
db = None
try:
    os.remove(p)
except OSError:
    pass
"#
}

fn python_path() -> String {
    let root = repo_root();
    let sep = if cfg!(windows) { ";" } else { ":" };
    [
        root.join("packages").join("honker").join("python"),
        root.join("packages"),
    ]
    .iter()
    .map(|p| p.to_string_lossy().into_owned())
    .collect::<Vec<_>>()
    .join(sep)
}

fn run_python(python: &Path, db_path: &Path, script: &str) -> String {
    let out = Command::new(python)
        .arg("-c")
        .arg(script)
        .env("PYTHONPATH", python_path())
        .env("DB_PATH", db_path)
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "python interop failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8(out.stdout).unwrap()
}

#[test]
fn rust_wrapper_and_python_share_queue_stream_and_notify() {
    let Some(python) = interop_python() else {
        eprintln!("python honker binding unavailable; skipping rust/python interop");
        return;
    };

    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("rust-python.db");
    let db = Database::open(&db_path).unwrap();

    let rust_q = db.queue("rust-to-python", QueueOpts::default());
    for i in 0..25 {
        rust_q
            .enqueue(
                &json!({"source": "rust", "seq": i, "key": format!("rust-{i:02}")}),
                EnqueueOpts::default(),
            )
            .unwrap();
    }
    db.notify("from-rust", &json!({"source": "rust", "count": 25}))
        .unwrap();
    db.stream("interop")
        .publish(&json!({"source": "rust", "kind": "stream"}))
        .unwrap();

    let out = run_python(
        &python,
        &db_path,
        r#"
import json
import os
import honker

db = honker.open(os.environ["DB_PATH"])

rust_jobs = db.queue("rust-to-python").claim_batch("python-worker", 50)
rust_payloads = [job.payload for job in rust_jobs]
acked = db.queue("rust-to-python").ack_batch(
    [job.id for job in rust_jobs],
    "python-worker",
)

rust_note = db.query(
    "SELECT payload FROM _honker_notifications "
    "WHERE channel='from-rust' ORDER BY id DESC LIMIT 1"
)
rust_events = db.stream("interop")._read_since(0, 10)

py_q = db.queue("python-to-rust")
for i in range(25):
    py_q.enqueue({"source": "python", "seq": i, "key": f"py-{i:02d}"})
with db.transaction() as tx:
    tx.notify("from-python", {"source": "python", "count": len(rust_jobs)})
db.stream("interop").publish({"source": "python", "kind": "stream"})

print(json.dumps({
    "acked": acked,
    "rust_payloads": rust_payloads,
    "rust_note": json.loads(rust_note[0]["payload"]),
    "rust_event_count": len(rust_events),
}))
"#,
    );

    let observed: serde_json::Value = serde_json::from_str(&out).unwrap();
    assert_eq!(observed["acked"], 25);
    assert_eq!(observed["rust_payloads"].as_array().unwrap().len(), 25);
    assert_eq!(observed["rust_note"]["source"], "rust");
    assert_eq!(observed["rust_note"]["count"], 25);
    assert_eq!(observed["rust_event_count"], 1);

    let py_q = db.queue("python-to-rust", QueueOpts::default());
    let py_jobs = py_q.claim_batch("rust-worker", 50).unwrap();
    assert_eq!(py_jobs.len(), 25);
    let mut seqs = py_jobs
        .iter()
        .map(|job| {
            let payload: serde_json::Value = job.payload_as().unwrap();
            assert_eq!(payload["source"], "python");
            payload["seq"].as_i64().unwrap()
        })
        .collect::<Vec<_>>();
    seqs.sort_unstable();
    assert_eq!(seqs, (0..25).collect::<Vec<_>>());
    let ids = py_jobs.iter().map(|job| job.id).collect::<Vec<_>>();
    assert_eq!(py_q.ack_batch(&ids, "rust-worker").unwrap(), 25);

    let py_note: String = db.with_conn(|c| {
        c.query_row(
            "SELECT payload FROM _honker_notifications WHERE channel='from-python' ORDER BY id DESC LIMIT 1",
            [],
            |r| r.get(0),
        )
        .unwrap()
    });
    let py_note: serde_json::Value = serde_json::from_str(&py_note).unwrap();
    assert_eq!(py_note["source"], "python");
    assert_eq!(py_note["count"], 25);

    let events = db.stream("interop").read_since(0, 10).unwrap();
    assert_eq!(events.len(), 2);
}
