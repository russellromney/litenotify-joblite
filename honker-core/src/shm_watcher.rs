//! Optional mmap `-shm` fast path (feature = `shm-fast-path`).
//!
//! **Experimental.** Weaker correctness contract than the polling
//! backend, in exchange for sub-millisecond wake latency.
//!
//! # Contract
//!
//! `on_change()` fires when the `iChange` counter at byte offset 8 of
//! the WAL index header (`-shm` file) advances. **There is no `PRAGMA
//! data_version` verification, no safety-net poll, no inode re-mmap.**
//! This means:
//!
//! - **WAL mode required.** No `-shm` exists in DELETE/TRUNCATE/
//!   PERSIST modes. If the file isn't present at startup the backend
//!   logs to stderr and exits — no wakes ever fire.
//!
//! - **Trusts the on-disk shm layout.** Reads `iChange` at a fixed
//!   offset and assumes it tracks `PRAGMA data_version`. Verified by
//!   the equivalence test (`shm_fast_path_equivalence_with_pragma_baseline`)
//!   on every supported SQLite version. If a future SQLite version
//!   changes the layout, this breaks silently.
//!
//! - **WAL reset / db replacement: panic.** If `-shm` or the db file
//!   is deleted and recreated mid-flight (cross-process close+reopen,
//!   atomic rename, litestream restore), the watcher panics with a
//!   "Restart required" message. Same dead-man's-switch shape as the
//!   polling backend — louder failure than silent missed wakes.
//!
//! Tests assert that wakes fire with sub-millisecond latency in WAL
//! mode. If a test fails, the backend is broken — not "fall back to
//! polling and pretend it worked".

use crate::stat_identity;
use memmap2::Mmap;
use std::fs::File;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

const WALINDEX_MAX_VERSION: u32 = 3_007_000;
const ICHANGE_OFFSET: usize = 8;
/// Same cadence as the polling backend. Shm reads are nearly free; the
/// win over polling is "PRAGMA → memory load" (~3.5 µs → ns), not
/// "1 ms → 100 µs". Going faster would just burn extra sleep syscalls
/// for latency nobody can perceive.
const POLL_INTERVAL_MS: u64 = 1;
/// Cadence for the dead-man's switch (db / -shm replacement detection).
/// Same as the polling backend.
const IDENTITY_CHECK_TICKS: u64 = 100;

pub(crate) fn run_shm_fast_path_loop<F>(db_path: PathBuf, on_change: F, stop: Arc<AtomicBool>)
where
    F: Fn() + Send + 'static,
{
    if cfg!(target_endian = "big") {
        eprintln!("honker: shm-fast-path requires little-endian platform. Backend disabled.");
        return;
    }
    let shm_path = PathBuf::from(format!("{}-shm", db_path.display()));
    // Open + mmap. SAFETY: read-only; SQLite owns the write lock.
    let f = match File::open(&shm_path) {
        Ok(f) => f,
        Err(e) => {
            eprintln!("honker: shm-fast-path disabled: -shm unavailable ({e}). Needs WAL + open conn.");
            return;
        }
    };
    let mmap = match unsafe { Mmap::map(&f) } {
        Ok(m) if m.len() >= 12 => m,
        _ => {
            eprintln!("honker: shm-fast-path disabled: mmap failed or -shm too small.");
            return;
        }
    };
    // Sanity: WAL index version we know how to read. A future SQLite
    // that bumps this fails the check instead of reading garbage.
    let iversion = u32::from_ne_bytes(mmap[0..4].try_into().unwrap());
    if iversion != WALINDEX_MAX_VERSION {
        eprintln!(
            "honker: shm-fast-path disabled: WAL index version {iversion} != {WALINDEX_MAX_VERSION}."
        );
        return;
    }

    let read_ichange = || u32::from_ne_bytes(mmap[ICHANGE_OFFSET..ICHANGE_OFFSET + 4].try_into().unwrap());
    let mut last = read_ichange();

    // Dead-man's switch: snapshot db and -shm inodes at startup; if
    // either changes mid-flight, panic. Both go stale silently
    // otherwise — the mmap stays on the old -shm inode and iChange
    // never advances again. Same cadence as the polling backend so
    // file-replacement detection latency doesn't depend on backend.
    let initial_db_id = match stat_identity(&db_path) {
        Ok(id) => id,
        Err(e) => {
            eprintln!("honker: failed to stat database for identity check: {e}");
            (0, 0)
        }
    };
    let initial_shm_id = match stat_identity(&shm_path) {
        Ok(id) => id,
        Err(e) => {
            eprintln!("honker: failed to stat -shm for identity check: {e}");
            (0, 0)
        }
    };
    let mut tick: u64 = 0;

    while !stop.load(Ordering::Acquire) {
        std::thread::sleep(Duration::from_millis(POLL_INTERVAL_MS));
        let current = read_ichange();
        if current != last {
            last = current;
            on_change();
        }
        tick += 1;
        if tick % IDENTITY_CHECK_TICKS == 0 {
            // Either stat error or successful inode-unchanged result
            // returns false; only an actual replacement panics. If
            // either stat errored, fire a conservative on_change so
            // the consumer re-checks state — matches polling.
            let any_err = check_identity(&db_path, initial_db_id, "database file")
                | check_identity(&shm_path, initial_shm_id, "-shm file");
            if any_err {
                on_change();
            }
        }
    }
}

/// Panics if the file at `path` has been replaced since startup. Used
/// for both the db file (parity with polling backend's dead-man's
/// switch) and the -shm file (specific failure mode for this backend
/// — a recreated -shm leaves our mmap on a deleted inode).
///
/// Returns `true` on stat error so the caller can fire a conservative
/// `on_change` (matches polling's "stat error → wake to re-check" path).
fn check_identity(path: &std::path::Path, initial: (u64, u64), label: &str) -> bool {
    match stat_identity(path) {
        Ok(current) => {
            if current != initial {
                panic!(
                    "honker: {label} replaced: \
                     expected (dev={}, ino={}), found (dev={}, ino={}) at {:?}. \
                     Restart required.",
                    initial.0, initial.1, current.0, current.1, path
                );
            }
            false
        }
        Err(e) => {
            eprintln!("honker: stat identity check failed for {label}: {e}");
            true
        }
    }
}

/// Verify the shm fast path can run for this `db_path`. Called from
/// `WatcherBackend::probe` at `honker.open()` time so a misconfigured
/// backend errors immediately rather than silently producing no wakes.
pub(crate) fn probe(db_path: &std::path::Path) -> Result<(), String> {
    if cfg!(target_endian = "big") {
        return Err("shm-fast-path requires little-endian platform".into());
    }
    let shm = format!("{}-shm", db_path.display());
    let f = File::open(&shm).map_err(|e| {
        format!("-shm unavailable ({e}). WAL mode + open connection required.")
    })?;
    let m = unsafe { Mmap::map(&f) }.map_err(|e| format!("mmap failed: {e}"))?;
    if m.len() < 12 {
        return Err("-shm too small".into());
    }
    let iv = u32::from_ne_bytes(m[0..4].try_into().unwrap());
    if iv != WALINDEX_MAX_VERSION {
        return Err(format!(
            "WAL index version {iv} != {WALINDEX_MAX_VERSION} (unsupported SQLite layout)"
        ));
    }
    Ok(())
}
