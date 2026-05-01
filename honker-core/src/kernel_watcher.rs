//! Optional kernel-watch backend (feature = `kernel-watcher`).
//!
//! Replaces the fixed 1 ms sleep in [`crate::run_poll_loop`] with OS
//! filesystem notifications via `notify-rs`. Notifications are **wake
//! hints only** — `PRAGMA data_version` is still called on every wake to
//! confirm a real commit; SQLite remains the source of truth.
//!
//! # Platform notes
//!
//! - **Linux** — uses `inotify`. Watches the `-wal` file directly for
//!   `IN_MODIFY` events. Reliable, sub-millisecond wake latency.
//! - **macOS (kqueue, via `macos_kqueue` feature)** — watches the
//!   `-wal` file directly. kqueue `NOTE_WRITE` fires on every `pwrite(2)`
//!   SQLite issues to the WAL. Low latency when the WAL file exists.
//!   While the WAL is absent (between checkpoint and first post-checkpoint
//!   write), the 500 ms safety net covers detection.
//! - **Windows** — uses `ReadDirectoryChangesW`. Directory-level change
//!   notifications include file modification events.
//! - **Fallback** — if watcher setup fails for any reason, the loop
//!   transparently falls back to 1 ms PRAGMA polling.
//!
//! # Safety contract
//!
//! - Kernel events can be dropped, coalesced, or arrive out of order.
//!   A 500 ms safety-net poll fires even without an event so missed
//!   notifications don't cause permanent wake starvation.
//! - If the notify watcher fails to initialize or watch the target,
//!   the loop falls back to 1 ms polling automatically.
//! - File-replacement detection (dead-man's switch) runs every ~30 s
//!   regardless of backend.

use crate::{poll_data_version, run_poll_loop, stat_identity};
use notify::{EventKind, RecursiveMode, Watcher};
use rusqlite::{Connection, OpenFlags};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::time::Duration;

/// How long to block waiting for a kernel event before running the
/// safety-net data_version check. Large enough to slash idle CPU versus
/// 1 ms polling, small enough to not stall detection on missed events.
const SAFETY_NET_MS: u64 = 500;

/// Run the kernel-watch loop on the calling thread.
///
/// Called from the `honker-update-poll` thread spawned by
/// [`UpdateWatcher::spawn_with_config`](crate::UpdateWatcher::spawn_with_config).
pub(crate) fn run_kernel_watch_loop<F>(db_path: PathBuf, on_change: F, stop: Arc<AtomicBool>)
where
    F: Fn() + Send + 'static,
{
    let wal_path = PathBuf::from(format!("{}-wal", db_path.display()));
    let watch_dir = db_path.parent().unwrap_or(Path::new(".")).to_path_buf();

    let (tx, rx) = mpsc::channel::<notify::Result<notify::Event>>();
    let mut watcher = match notify::recommended_watcher(tx) {
        Ok(w) => w,
        Err(e) => {
            eprintln!("honker: kernel watcher unavailable ({e}), using 1ms polling");
            run_poll_loop(db_path, on_change, stop);
            return;
        }
    };

    // Primary: watch the -wal file directly for write events (kqueue NOTE_WRITE,
    // inotify IN_MODIFY, etc.). This is the hot path — every SQLite commit in WAL
    // mode appends a frame to the WAL file via pwrite(2).
    //
    // Secondary: also watch the parent directory so we catch -wal file creation
    // when the database transitions from "freshly checkpointed, WAL deleted" back
    // to "WAL active". Without this, writes after a full checkpoint would be
    // invisible until the safety-net fires.
    let mut watching_any = false;
    if wal_path.exists() {
        if watcher.watch(&wal_path, RecursiveMode::NonRecursive).is_ok() {
            watching_any = true;
        }
    }
    match watcher.watch(&watch_dir, RecursiveMode::NonRecursive) {
        Ok(()) => watching_any = true,
        Err(e) => {
            if !watching_any {
                eprintln!("honker: kernel watcher setup failed ({e}), using 1ms polling");
                run_poll_loop(db_path, on_change, stop);
                return;
            }
        }
    }

    // Open a reader connection for data_version verification.
    let mut conn = open_reader(&db_path);
    let mut last_version = conn.as_ref().and_then(|c| poll_data_version(c).ok()).unwrap_or(0);
    let initial_identity = stat_identity(&db_path).unwrap_or((0, 0));
    // tick counts event-loop iterations (each up to SAFETY_NET_MS long).
    // Identity check runs roughly every 30 s (60 × 500 ms).
    let mut tick: u64 = 0;
    // Track whether we are watching the WAL file directly; re-add the watch
    // when the WAL is recreated after a checkpoint.
    let mut watching_wal = watching_any && wal_path.exists();

    while !stop.load(Ordering::Acquire) {
        let should_check = match rx.recv_timeout(Duration::from_millis(SAFETY_NET_MS)) {
            Ok(Ok(event)) => {
                // On directory event, check if the -wal file was created and
                // add a per-file watch if we don't have one yet.
                if !watching_wal && wal_path.exists() {
                    if watcher.watch(&wal_path, RecursiveMode::NonRecursive).is_ok() {
                        watching_wal = true;
                    }
                }
                is_relevant(&event)
            }
            Ok(Err(e)) => {
                eprintln!("honker: kernel watcher error: {e}");
                true // conservative: check on any watcher error
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {
                // Safety-net: also attempt to (re-)attach WAL watch if missing.
                if !watching_wal && wal_path.exists() {
                    if watcher.watch(&wal_path, RecursiveMode::NonRecursive).is_ok() {
                        watching_wal = true;
                    }
                }
                true
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => break,
        };

        if should_check {
            check_data_version(&db_path, &mut conn, &mut last_version, &on_change);
        }

        tick += 1;
        if tick % 60 == 0 {
            identity_check(&db_path, initial_identity, &mut conn, &on_change);
        }
    }
}

/// Re-run the `PRAGMA data_version` check and call `on_change` if it
/// advanced. Reconnects if the connection was previously lost.
fn check_data_version<F>(
    db_path: &Path,
    conn: &mut Option<Connection>,
    last_version: &mut u32,
    on_change: &F,
) where
    F: Fn(),
{
    if let Some(ref c) = *conn {
        match poll_data_version(c) {
            Ok(v) => {
                if v != *last_version {
                    *last_version = v;
                    on_change();
                }
            }
            Err(e) => {
                eprintln!("honker: data_version poll failed: {e}");
                *conn = None;
                on_change(); // conservative wake
            }
        }
    } else {
        // Try to reconnect after a previous failure.
        *conn = open_reader(db_path);
        if let Some(ref c) = *conn {
            *last_version = poll_data_version(c).unwrap_or(0);
        }
    }
}

/// Check file identity, panic on replacement, reset conn on stat error.
fn identity_check<F>(
    db_path: &Path,
    initial_identity: (u64, u64),
    conn: &mut Option<Connection>,
    on_change: &F,
) where
    F: Fn(),
{
    match stat_identity(db_path) {
        Ok(current) if current != initial_identity => panic!(
            "honker: database file replaced: expected (dev={}, ino={}), \
             found (dev={}, ino={}) at {:?}. Restart required.",
            initial_identity.0, initial_identity.1, current.0, current.1, db_path
        ),
        Err(e) => {
            eprintln!("honker: stat identity check failed: {e}");
            *conn = None;
            on_change();
        }
        _ => {}
    }
}

fn is_relevant(event: &notify::Event) -> bool {
    // Read-only access events don't signal a write — skip them.
    !matches!(event.kind, EventKind::Access(_))
}

fn open_reader(path: &Path) -> Option<Connection> {
    Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .ok()
}
