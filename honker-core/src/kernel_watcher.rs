//! Optional kernel-watch backend (feature = `kernel-watcher`).
//!
//! Replaces the fixed 1 ms sleep in [`crate::run_poll_loop`] with OS
//! filesystem notifications via `notify-rs`. Notifications are **wake
//! hints only** — `PRAGMA data_version` is still called on every wake to
//! confirm a real commit; SQLite remains the source of truth.
//!
//! # Watched targets
//!
//! Two layers of watch, both journal-mode-agnostic:
//!
//! 1. **Parent directory** (always). Catches `-wal`/`-journal` file
//!    create/delete around each commit — the universal per-commit
//!    signal in every supported journal mode (DELETE creates+deletes
//!    `-journal` per txn; TRUNCATE/PERSIST modify it per txn; WAL
//!    appends to `-wal` and the directory still sees `-shm` updates).
//!
//! 2. **`-wal` file directly** (WAL mode only, opportunistic). When
//!    the database is in WAL mode, the `-wal` file persists across
//!    commits, so a per-file watch picks up `pwrite(2)` events
//!    without going through the directory. Re-attached when the
//!    `-wal` inode changes (e.g. after `wal_checkpoint(TRUNCATE)`
//!    deletes and recreates it).
//!
//! In non-WAL modes (DELETE/TRUNCATE/PERSIST) there is no persistent
//! file to per-file-watch — the rollback journal is created at BEGIN
//! and deleted/truncated at COMMIT — so the directory watch carries
//! the load. Empirically verified by `kernel_watcher_works_in_*` tests.
//!
//! # Platform notes
//!
//! - **Linux** uses `inotify` (via `notify` 6's recommended watcher).
//! - **macOS** uses `kqueue` (`notify` 6's `macos_kqueue` feature).
//!   Default FSEvents has a 1 s batching latency, unusable for our wake
//!   semantics; kqueue `NOTE_WRITE` fires on every `pwrite(2)`.
//! - **Windows** uses `ReadDirectoryChangesW`.
//! - **Fallback** — if watcher setup fails for any reason, the loop
//!   transparently falls back to 1 ms PRAGMA polling.
//!
//! # Safety contract
//!
//! - Kernel events can be dropped, coalesced, or arrive out of order.
//!   A 500 ms safety-net `PRAGMA data_version` check runs on its own
//!   clock, so missed notifications stall detection by at most that.
//! - The recv loop's blocking timeout (`RX_POLL_MS`) is independent
//!   of the safety-net interval — it bounds graceful-shutdown latency
//!   without changing how often the safety net actually fires.
//! - File-replacement detection (dead-man's switch) runs every 100 ms,
//!   matching the polling backend.

use crate::{poll_data_version, run_poll_loop, stat_identity};
use notify::{EventKind, RecursiveMode, Watcher};
use rusqlite::{Connection, OpenFlags};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::time::Duration;

/// How often to fire the safety-net `PRAGMA data_version` check when
/// no kernel event has arrived. Large enough to slash idle CPU vs
/// 1 ms polling, small enough that missed/coalesced notifications can
/// only stall detection by this interval.
const SAFETY_NET_MS: u64 = 500;
/// Maximum time we block on the event channel before sampling the stop
/// flag. Keeps graceful-shutdown latency bounded at ~RX_POLL_MS instead
/// of [`SAFETY_NET_MS`]. Independent of safety-net firing — that runs
/// on its own clock.
const RX_POLL_MS: u64 = 50;

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
    // Mode-agnostic primary signal: the parent directory. In every
    // supported journal mode something gets created or deleted in the
    // directory around each commit (DELETE creates+removes -journal,
    // WAL creates -wal/-shm on first write, etc.).
    let dir_watch_ok = watcher
        .watch(&watch_dir, RecursiveMode::NonRecursive)
        .is_ok();

    // WAL-mode-only optimization: the -wal file persists across commits,
    // so a per-file watch picks up writes without depending on directory
    // events. Track the file's identity so we can re-attach when it is
    // deleted and recreated (e.g. after `wal_checkpoint(TRUNCATE)`).
    // In non-WAL modes -wal doesn't exist; the per-file watch stays
    // unattached and the directory watch carries the load.
    let mut watched_wal_id: Option<(u64, u64)> = None;
    if wal_path.exists() {
        if watcher.watch(&wal_path, RecursiveMode::NonRecursive).is_ok() {
            watched_wal_id = stat_identity(&wal_path).ok();
        }
    }

    if !dir_watch_ok && watched_wal_id.is_none() {
        eprintln!("honker: kernel watcher setup failed, using 1ms polling");
        run_poll_loop(db_path, on_change, stop);
        return;
    }

    // Open a reader connection for data_version verification.
    let mut conn = open_reader(&db_path);
    let mut last_version = conn
        .as_ref()
        .and_then(|c| poll_data_version(c).ok())
        .unwrap_or(0);
    let initial_identity = stat_identity(&db_path).unwrap_or((0, 0));
    // tick counts event-loop iterations (each up to SAFETY_NET_MS long).
    // Identity check runs every ~3 s (6 × 500 ms) — slower than polling's
    // 100 ms but the dead-man's switch only fires on rare events
    // (atomic rename, NFS remount, etc.).
    // Time-tracked safety-net + identity-check schedules. Decoupling
    // them from `recv_timeout` lets us keep the recv timeout short
    // (snappy shutdown) without changing how often the safety net or
    // identity check actually fire.
    let started = std::time::Instant::now();
    let mut last_safety_net = started;
    let mut last_identity_check = started;

    while !stop.load(Ordering::Acquire) {
        let should_check = match rx.recv_timeout(Duration::from_millis(RX_POLL_MS)) {
            Ok(Ok(event)) => {
                // Any directory-scope event is a potential WAL identity
                // change — re-check on every event so post-checkpoint
                // writes wake on real events, not on the safety net.
                reattach_wal_watch_if_needed(&mut watcher, &wal_path, &mut watched_wal_id);
                is_relevant(&event)
            }
            Ok(Err(e)) => {
                eprintln!("honker: kernel watcher error: {e}");
                true // conservative: check on any watcher error
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {
                let now = std::time::Instant::now();
                if now.duration_since(last_safety_net)
                    >= Duration::from_millis(SAFETY_NET_MS)
                {
                    // Safety-net: also re-check WAL identity in case we
                    // missed the directory event for the recreation.
                    reattach_wal_watch_if_needed(
                        &mut watcher, &wal_path, &mut watched_wal_id,
                    );
                    last_safety_net = now;
                    true
                } else {
                    false
                }
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => break,
        };

        if should_check {
            check_data_version(&db_path, &mut conn, &mut last_version, &on_change);
        }

        // Run two 100 ms-cadence checks together:
        //   * `identity_check` on the db file — dead-man's switch
        //     (matches polling backend's cadence so file-replacement
        //     detection latency doesn't depend on which backend is in
        //     use).
        //   * `reattach_wal_watch_if_needed` on the -wal file — bounds
        //     worst-case post-WAL-recreation latency at 100 ms even if
        //     the directory event for the recreation was missed (e.g.
        //     dropped due to backend overflow). Without this it would
        //     fall back to the 500 ms safety-net cycle.
        let now = std::time::Instant::now();
        if now.duration_since(last_identity_check) >= Duration::from_millis(100) {
            identity_check(&db_path, initial_identity, &mut conn, &on_change);
            reattach_wal_watch_if_needed(&mut watcher, &wal_path, &mut watched_wal_id);
            last_identity_check = now;
        }
    }
}

/// (Re-)attach the `-wal` per-file watch when its identity changes
/// (created, deleted+recreated by checkpoint, etc.). Idempotent: a no-op
/// when the watched WAL still has the same inode.
fn reattach_wal_watch_if_needed(
    watcher: &mut notify::RecommendedWatcher,
    wal_path: &Path,
    watched_wal_id: &mut Option<(u64, u64)>,
) {
    let current = stat_identity(wal_path).ok();
    if current == *watched_wal_id {
        return;
    }
    // Identity changed (None → Some, Some → None, or new inode).
    // Drop the old watch first; ignore errors (the watched path may
    // already be gone, which `unwatch` reports as an error).
    if watched_wal_id.is_some() {
        let _ = watcher.unwatch(wal_path);
    }
    if current.is_some() {
        if watcher.watch(wal_path, RecursiveMode::NonRecursive).is_ok() {
            *watched_wal_id = current;
        } else {
            // Couldn't attach to the new -wal; safety net will cover us.
            *watched_wal_id = None;
        }
    } else {
        *watched_wal_id = None;
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
