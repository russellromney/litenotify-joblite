//! Optional kernel-watch backend (feature = `kernel-watcher`).
//!
//! **Experimental.** Weaker correctness contract than the polling
//! backend, in exchange for lower idle CPU and lower wake latency.
//!
//! # Contract
//!
//! `on_change()` fires on every non-`Access` filesystem event in the
//! database's parent directory. **There is no `PRAGMA data_version`
//! verification, no safety-net poll, and no per-file watch.** This
//! means:
//!
//! - **Spurious wakes are possible.** Any file change in the directory
//!   (other apps writing nearby files, the OS touching metadata, etc.)
//!   produces a wake. Consumers re-read state on every wake anyway, so
//!   this is wasted work, not incorrect.
//!
//! - **Missed wakes are possible.** If the OS drops or coalesces
//!   notifications, or fails to deliver an event for a SQLite commit,
//!   `on_change()` will not fire for that commit. The consumer's
//!   `idle_poll_s` (default 5 s) is the only backstop.
//!
//! - **Setup failures raise at `open()`.** [`probe`] runs at
//!   `honker.open()` time and surfaces any init failure as an error
//!   so the user knows immediately. No silent backend disable.
//!
//! Tests assert that wakes do fire, with bounded latency, on the
//! platforms we support. If a test fails, the backend is broken on
//! that platform — not "fall back to polling and pretend it worked".

use crate::stat_identity;
use notify::{EventKind, RecursiveMode, Watcher};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::time::{Duration, Instant};

/// How long `recv_timeout` blocks before sampling the stop flag.
/// Bounds graceful shutdown latency at this value.
const RX_POLL_MS: u64 = 50;
/// Cadence for the dead-man's switch (db-file replacement detection).
/// Same as the polling backend so file-replacement detection latency
/// doesn't depend on which backend the user picked.
const IDENTITY_CHECK_MS: u64 = 100;

pub(crate) fn run_kernel_watch_loop<F>(
    db_path: PathBuf,
    on_change: F,
    stop: Arc<AtomicBool>,
    ready: std::sync::mpsc::SyncSender<()>,
) where
    F: Fn() + Send + 'static,
{
    let (tx, rx) = mpsc::channel::<notify::Result<notify::Event>>();
    let mut watcher = match notify::recommended_watcher(tx) {
        Ok(w) => w,
        Err(e) => {
            eprintln!("honker: kernel-watcher init failed: {e}. Backend disabled.");
            return;
        }
    };

    // Attach watches: db file (catches in-place writes, the only signal
    // for non-WAL on macOS kqueue), parent dir (catches journal/wal/shm
    // create+delete in DELETE mode), and -wal/-shm/-journal directly
    // when present. No re-attach if files churn mid-flight — the
    // per-file watch goes stale and the consumer's `idle_poll_s`
    // backstop covers it. Experimental: restart to recover.
    let watch_dir = db_path
        .parent()
        .unwrap_or(std::path::Path::new("."))
        .to_path_buf();
    let wal = PathBuf::from(format!("{}-wal", db_path.display()));
    let shm = PathBuf::from(format!("{}-shm", db_path.display()));
    let journal = PathBuf::from(format!("{}-journal", db_path.display()));

    // Try each path; missing files / inaccessible dirs error here and
    // we just skip them. As long as at least one watch attached, we go.
    let attached = [&watch_dir, &db_path, &wal, &shm, &journal]
        .into_iter()
        .filter(|p| watcher.watch(p, RecursiveMode::NonRecursive).is_ok())
        .count();
    if attached == 0 {
        eprintln!("honker: kernel-watcher couldn't attach to db dir or -wal/-shm. Backend disabled.");
        return;
    }

    // Dead-man's switch: snapshot db inode; panic if it changes
    // (atomic rename, litestream restore, NFS remount). Per-file
    // watches would silently sit on the dead inode otherwise.
    let initial_id = match stat_identity(&db_path) {
        Ok(id) => id,
        Err(e) => {
            eprintln!("honker: failed to stat database for identity check: {e}");
            (0, 0)
        }
    };
    let mut last_id_check = Instant::now();
    // Baseline captured; signal the spawner that it's safe to return.
    let _ = ready.send(());
    drop(ready);

    while !stop.load(Ordering::Acquire) {
        match rx.recv_timeout(Duration::from_millis(RX_POLL_MS)) {
            Ok(Ok(event)) if !matches!(event.kind, EventKind::Access(_)) => on_change(),
            Ok(Err(e)) => {
                // Notify error — fire conservatively so the consumer
                // doesn't sit idle on a transient backend hiccup.
                eprintln!("honker: kernel-watcher event error: {e}");
                on_change();
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => break,
            _ => {} // timeout, or Access event — ignore
        }
        if last_id_check.elapsed() >= Duration::from_millis(IDENTITY_CHECK_MS) {
            if check_db_identity(&db_path, initial_id) {
                on_change();
            }
            last_id_check = Instant::now();
        }
    }
}

/// Panics if the db file at `db_path` has been replaced since startup.
/// Returns `true` on stat error so caller can fire a conservative wake.
fn check_db_identity(db_path: &std::path::Path, initial: (u64, u64)) -> bool {
    match stat_identity(db_path) {
        Ok(current) => {
            if current != initial {
                panic!(
                    "honker: database file replaced: \
                     expected (dev={}, ino={}), found (dev={}, ino={}) at {:?}. \
                     The watcher cannot recover; \
                     close the Database and reopen with honker.open().",
                    initial.0, initial.1, current.0, current.1, db_path
                );
            }
            false
        }
        Err(e) => {
            eprintln!("honker: stat identity check failed: {e}");
            true
        }
    }
}

/// Probe at `honker.open()` so a misconfigured backend errors
/// immediately instead of silently producing no wakes.
pub(crate) fn probe(db_path: &std::path::Path) -> Result<(), String> {
    let (tx, _rx) = mpsc::channel::<notify::Result<notify::Event>>();
    let mut w = notify::recommended_watcher(tx)
        .map_err(|e| format!("notify init failed: {e}"))?;
    let dir = db_path
        .parent()
        .unwrap_or(std::path::Path::new("."));
    w.watch(dir, RecursiveMode::NonRecursive)
        .map_err(|e| format!("can't watch {dir:?}: {e}"))?;
    // Drop the watcher; it's recreated when actually needed.
    Ok(())
}
