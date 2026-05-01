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
const HDR_READ_LEN: usize = 12;
const POLL_INTERVAL_US: u64 = 100;

pub(crate) fn run_shm_fast_path_loop<F>(db_path: PathBuf, on_change: F, stop: Arc<AtomicBool>)
where
    F: Fn() + Send + 'static,
{
    if cfg!(target_endian = "big") {
        eprintln!("honker: shm-fast-path requires little-endian platform. Backend disabled.");
        return;
    }
    let shm_path = PathBuf::from(format!("{}-shm", db_path.display()));
    let f = match File::open(&shm_path) {
        Ok(f) => f,
        Err(e) => {
            eprintln!(
                "honker: shm-fast-path: -shm not available ({e}). \
                 Backend disabled (requires WAL mode + at least one open connection)."
            );
            return;
        }
    };
    // SAFETY: mmap is read-only. SQLite owns the write lock; we observe.
    let mmap = match unsafe { Mmap::map(&f) } {
        Ok(m) if m.len() >= HDR_READ_LEN => m,
        Ok(_) => {
            eprintln!("honker: shm-fast-path: -shm too small. Backend disabled.");
            return;
        }
        Err(e) => {
            eprintln!("honker: shm-fast-path: mmap failed ({e}). Backend disabled.");
            return;
        }
    };
    // Guard: known WAL index version. Catches a future SQLite that
    // changes the layout — fail loudly instead of returning garbage.
    let iversion = u32::from_ne_bytes(mmap[0..4].try_into().expect("4 bytes"));
    if iversion != WALINDEX_MAX_VERSION {
        eprintln!(
            "honker: shm-fast-path: unexpected WAL index version {iversion} \
             (expected {WALINDEX_MAX_VERSION}). Backend disabled."
        );
        return;
    }

    let read_ichange = || {
        u32::from_ne_bytes(mmap[ICHANGE_OFFSET..ICHANGE_OFFSET + 4].try_into().expect("4 bytes"))
    };
    let mut last = read_ichange();

    while !stop.load(Ordering::Acquire) {
        std::thread::sleep(Duration::from_micros(POLL_INTERVAL_US));
        let current = read_ichange();
        if current != last {
            last = current;
            on_change();
        }
    }
}
