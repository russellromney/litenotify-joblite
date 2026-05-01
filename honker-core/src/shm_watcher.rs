//! Optional mmap `-shm` fast path (feature = `shm-fast-path`).
//!
//! Reads `iChange` from the SQLite WAL index shared-memory file at
//! 100 µs cadence instead of running `PRAGMA data_version`. Each
//! committed write increments `iChange` in the WAL index header. A
//! persistent `Mmap` is opened once and re-opened when the file's
//! identity changes (WAL reset after checkpoint).
//!
//! # Safety boundary
//!
//! The following guards must all pass before the fast path activates:
//!
//! 1. **Platform is little-endian.** The WAL index header fields are
//!    written in native byte order by the process that initializes the
//!    WAL. All current honker targets (Linux/macOS/Windows on
//!    ARM64/x86-64) are little-endian. Big-endian platforms fall back
//!    to PRAGMA polling.
//!
//! 2. **`iVersion` at offset 0 == `WALINDEX_MAX_VERSION` (3 007 000).**
//!    This is the WAL index format version baked in since SQLite 3.7.0
//!    (2010). If a future SQLite changes the on-disk format it will
//!    bump this value; the guard disables the fast path and falls back.
//!
//! 3. **The `-shm` file exists.** It is created lazily on the first
//!    write. Before the first write (or after a full checkpoint that
//!    removes it), the fast path is inactive and PRAGMA polling covers
//!    detection.
//!
//! This fast path is an **accelerator**, not a new correctness
//! dependency. The `on_change` callback is called when `iChange`
//! advances — the same signal as `PRAGMA data_version`. The
//! equivalence harness in the test suite proves they fire on the same
//! set of commits given sufficient inter-commit spacing.

use crate::{poll_data_version, stat_identity};
use memmap2::Mmap;
use rusqlite::{Connection, OpenFlags};
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

/// WAL index format version from SQLite 3.7.0 — unchanged since 2010.
const WALINDEX_MAX_VERSION: u32 = 3_007_000;
/// Byte offset of `iVersion` in `WalIndexHdr`.
const IVERSION_OFFSET: usize = 0;
/// Byte offset of `iChange` in `WalIndexHdr` — the per-commit counter.
const ICHANGE_OFFSET: usize = 8;
/// Minimum WAL index header size we need to read.
const HDR_READ_LEN: usize = 12;
/// Poll interval for the shm fast path.
const SHM_POLL_INTERVAL_US: u64 = 100;
/// Identity re-check cadence: every N iterations × SHM_POLL_INTERVAL_US.
/// 1 000 × 100 µs = 100 ms, consistent with the polling backend.
const IDENTITY_CHECK_CADENCE: u64 = 1_000;

/// Run the shm fast-path loop on the calling thread.
pub(crate) fn run_shm_fast_path_loop<F>(db_path: PathBuf, on_change: F, stop: Arc<AtomicBool>)
where
    F: Fn() + Send + 'static,
{
    // Guard 1: only little-endian platforms; see module doc.
    if cfg!(target_endian = "big") {
        eprintln!("honker: shm fast path disabled on big-endian platform, using PRAGMA polling");
        run_pragma_fallback(db_path, on_change, stop);
        return;
    }

    let shm_path = PathBuf::from(format!("{}-shm", db_path.display()));

    let mut reader = ShmReader::new(shm_path.clone());
    // Fallback connection for when the shm file is absent or fails guard.
    let mut fallback_conn = open_reader(&db_path);
    let mut fallback_last_ver = fallback_conn
        .as_ref()
        .and_then(|c| poll_data_version(c).ok())
        .unwrap_or(0);

    let initial_identity = stat_identity(&db_path).unwrap_or((0, 0));
    let mut tick: u64 = 0;

    while !stop.load(Ordering::Acquire) {
        std::thread::sleep(Duration::from_micros(SHM_POLL_INTERVAL_US));

        match reader.read_ichange() {
            Some(ichange) => {
                // Fast path active. `iChange` advanced → write happened.
                if reader.last_ichange != ichange {
                    reader.last_ichange = ichange;
                    on_change();
                }
            }
            None => {
                // shm unavailable (file missing, guard failed, read error).
                // Fall through to PRAGMA check so no commit goes undetected.
                check_pragma(&db_path, &mut fallback_conn, &mut fallback_last_ver, &on_change);
            }
        }

        tick += 1;
        if tick % IDENTITY_CHECK_CADENCE == 0 {
            identity_check(&db_path, initial_identity, &mut fallback_conn, &on_change);
        }
    }
}

// ---------- ShmReader ------------------------------------------------

struct ShmReader {
    shm_path: PathBuf,
    mmap: Option<Mmap>,
    /// inode/device identity of the currently mapped file so we detect WAL resets.
    file_id: (u64, u64),
    /// Last `iChange` value observed.
    pub last_ichange: u32,
}

impl ShmReader {
    fn new(shm_path: PathBuf) -> Self {
        let mut s = Self {
            shm_path,
            mmap: None,
            file_id: (0, 0),
            last_ichange: 0,
        };
        s.try_open();
        s
    }

    /// (Re-)open the shm file and map it. Called on startup and when the
    /// file identity changes (WAL reset after checkpoint).
    fn try_open(&mut self) {
        let f = match File::open(&self.shm_path) {
            Ok(f) => f,
            Err(_) => {
                self.mmap = None;
                self.file_id = (0, 0);
                return;
            }
        };
        let id = stat_identity(&self.shm_path).unwrap_or((0, 0));
        // SAFETY: We treat all mmap reads as hints. If the OS returns
        // stale data the worst outcome is an extra `on_change()` call
        // (conservative). The SQLite process that wrote the file owns
        // the exclusive write lock; we're a read-only observer.
        self.mmap = unsafe { Mmap::map(&f).ok() };
        self.file_id = id;
    }

    /// Read `iChange` from the mapped header. Returns `None` when the
    /// shm file is absent, too small, or fails the `iVersion` guard.
    /// Transparently re-opens the map when the shm file is recreated.
    fn read_ichange(&mut self) -> Option<u32> {
        // Check if the shm file was recreated (WAL reset).
        if let Ok(current_id) = stat_identity(&self.shm_path) {
            if current_id != self.file_id {
                self.try_open();
            }
        } else if self.mmap.is_some() {
            // File disappeared — drop the stale map.
            self.mmap = None;
            self.file_id = (0, 0);
        }

        let mmap = self.mmap.as_ref()?;
        if mmap.len() < HDR_READ_LEN {
            return None;
        }

        // Guard 2: validate WAL index format version.
        let iversion =
            u32::from_ne_bytes(mmap[IVERSION_OFFSET..IVERSION_OFFSET + 4].try_into().ok()?);
        if iversion != WALINDEX_MAX_VERSION {
            // Unknown WAL index layout — don't read iChange.
            return None;
        }

        let ichange =
            u32::from_ne_bytes(mmap[ICHANGE_OFFSET..ICHANGE_OFFSET + 4].try_into().ok()?);
        Some(ichange)
    }
}

// ---------- helpers --------------------------------------------------

fn check_pragma<F>(
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
            Err(_) => {
                *conn = None;
                on_change(); // conservative
            }
        }
    } else {
        *conn = open_reader(db_path);
        if let Some(ref c) = *conn {
            *last_version = poll_data_version(c).unwrap_or(0);
        }
    }
}

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

fn run_pragma_fallback<F>(db_path: PathBuf, on_change: F, stop: Arc<AtomicBool>)
where
    F: Fn(),
{
    crate::run_poll_loop(db_path, on_change, stop);
}

fn open_reader(path: &Path) -> Option<Connection> {
    Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .ok()
}
