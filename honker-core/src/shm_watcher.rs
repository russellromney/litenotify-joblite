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
//! - **Stale mmap on WAL reset.** If the `-shm` file is deleted and
//!   recreated mid-flight (e.g. all connections close, then reopen),
//!   the mmap is on the deleted inode. `iChange` reads will be stale
//!   and wakes will not fire. Consumer's `idle_poll_s` is the
//!   backstop.
//!
//! Tests assert that wakes fire with sub-millisecond latency in WAL
//! mode. If a test fails, the backend is broken — not "fall back to
//! polling and pretend it worked".

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
    while !stop.load(Ordering::Acquire) {
        std::thread::sleep(Duration::from_millis(POLL_INTERVAL_MS));
        let current = read_ichange();
        if current != last {
            last = current;
            on_change();
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
