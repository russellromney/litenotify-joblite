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
//! - **Setup failures are silent and final.** If `notify` can't
//!   initialize a watcher or attach to the directory, this thread
//!   logs to stderr and exits. No wakes will fire from this backend
//!   for the rest of the process lifetime.
//!
//! Tests assert that wakes do fire, with bounded latency, on the
//! platforms we support. If a test fails, the backend is broken on
//! that platform — not "fall back to polling and pretend it worked".

use notify::{EventKind, RecursiveMode, Watcher};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::time::Duration;

/// How long `recv_timeout` blocks before sampling the stop flag.
/// Bounds graceful shutdown latency at this value.
const RX_POLL_MS: u64 = 50;

pub(crate) fn run_kernel_watch_loop<F>(db_path: PathBuf, on_change: F, stop: Arc<AtomicBool>)
where
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

    // Attach watches at startup. We watch the parent directory (so
    // file create/delete around commits in DELETE/TRUNCATE/PERSIST
    // modes fire) AND -wal/-shm directly if they exist (because
    // directory-level watches on macOS kqueue don't fire on writes
    // *within* existing files — only on directory entry changes).
    //
    // No re-attach logic if files come and go mid-flight. That's the
    // experimental tradeoff — if the WAL is unlinked and recreated,
    // the per-file watch goes stale and the consumer's `idle_poll_s`
    // backstop catches up. Restart the process to recover the fast path.
    let watch_dir = db_path
        .parent()
        .unwrap_or(std::path::Path::new("."))
        .to_path_buf();
    let wal = PathBuf::from(format!("{}-wal", db_path.display()));
    let shm = PathBuf::from(format!("{}-shm", db_path.display()));

    let mut attached_any = false;
    for path in [&watch_dir, &wal, &shm] {
        if !path.exists() {
            continue;
        }
        if watcher.watch(path, RecursiveMode::NonRecursive).is_ok() {
            attached_any = true;
        }
    }
    if !attached_any {
        eprintln!(
            "honker: kernel-watcher couldn't attach to db dir or -wal/-shm. \
             Backend disabled."
        );
        return;
    }

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
    }
}
