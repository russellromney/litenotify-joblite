//! Optional mmap `-shm` fast path (feature = `shm-fast-path`).
//!
//! Reads `iChange` from the SQLite WAL index shared-memory file as a
//! fast tickle for `PRAGMA data_version` checks. Each committed write
//! increments `iChange` in the WAL index header; an mmap read picks
//! that up at memory-load latency. A persistent `Mmap` is opened once
//! and re-opened when the file's identity changes (WAL reset after
//! checkpoint).
//!
//! # Architecture
//!
//! Three independent layers, none of which can cause a missed wake:
//!
//! 1. **Fast path** (every 100 µs) — read `iChange` from the mmap. If
//!    it advanced, run a `PRAGMA data_version` check.
//! 2. **Safety net** (every 100 ms) — run a `PRAGMA data_version`
//!    check unconditionally. Fires on its own clock, independent of
//!    what the shm read returned.
//! 3. **Identity check** (every 100 ms) — `stat(db_path)` for the
//!    dead-man's switch (file replacement detection).
//!
//! `on_change()` is gated on `PRAGMA data_version` advancing, which
//! is the source of truth in every supported journal mode. The shm
//! read is **only** a wake hint: a stale mmap (still pointing at a
//! deleted inode after WAL reset) cannot cause a missed wake because
//! the 100 ms safety net fires regardless.
//!
//! # Safety boundary
//!
//! The shm-tickle path activates only when these guards pass; otherwise
//! detection rides entirely on the safety-net PRAGMA layer (correct,
//! just slower, equivalent to ~100 ms polling):
//!
//! 1. **Platform is little-endian.** WAL index header fields are in
//!    native byte order. All current honker targets (Linux/macOS/
//!    Windows on ARM64/x86-64) are little-endian.
//! 2. **`iVersion` at offset 0 == `WALINDEX_MAX_VERSION` (3 007 000).**
//!    The WAL index format version, unchanged since SQLite 3.7.0
//!    (2010). A future SQLite that bumps this will trip the guard
//!    and disable the fast path.
//! 3. **The `-shm` file exists.** Created lazily on the first WAL
//!    write. Absent in non-WAL journal modes (DELETE/TRUNCATE/PERSIST).

use crate::{poll_data_version, run_poll_loop, stat_identity};
use memmap2::Mmap;
use rusqlite::{Connection, OpenFlags};
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

/// WAL index format version from SQLite 3.7.0 — unchanged since 2010.
/// See SQLite source `src/wal.c` for `WALINDEX_MAX_VERSION`.
const WALINDEX_MAX_VERSION: u32 = 3_007_000;
/// Byte offset of `iVersion` in `WalIndexHdr`.
const IVERSION_OFFSET: usize = 0;
/// Byte offset of `iChange` in `WalIndexHdr` — the per-commit counter
/// that backs `PRAGMA data_version` in WAL mode.
const ICHANGE_OFFSET: usize = 8;
/// Minimum WAL index header size we need to read.
const HDR_READ_LEN: usize = 12;
/// Fast-path mmap-read interval. ~10 k reads/sec is essentially free
/// (one memory load + one bounds check per iteration); the win over
/// the polling backend's 1 ms cadence is 10× lower wake latency on
/// the happy path.
const SHM_POLL_INTERVAL_US: u64 = 100;
/// Safety-net PRAGMA cadence. Independent of the shm tickle so a
/// stale mmap or a guard failure can never cause a missed wake.
const SAFETY_NET_MS: u64 = 100;
/// Identity check (dead-man's switch) cadence — matches the polling
/// backend so file-replacement detection latency doesn't depend on
/// which backend the user picked.
const IDENTITY_CHECK_MS: u64 = 100;
/// How often to re-stat the `-shm` file to detect WAL reset (delete
/// + recreate by checkpoint). Cheaper than per-iteration stat without
/// sacrificing correctness, because the safety-net PRAGMA above will
/// still fire on real commits regardless of mmap freshness.
const SHM_REOPEN_CHECK_MS: u64 = 100;

/// Run the shm fast-path loop on the calling thread.
pub(crate) fn run_shm_fast_path_loop<F>(db_path: PathBuf, on_change: F, stop: Arc<AtomicBool>)
where
    F: Fn() + Send + 'static,
{
    // Guard 1: only little-endian platforms; see module doc.
    if cfg!(target_endian = "big") {
        eprintln!("honker: shm fast path disabled on big-endian platform, using PRAGMA polling");
        run_poll_loop(db_path, on_change, stop);
        return;
    }

    let shm_path = PathBuf::from(format!("{}-shm", db_path.display()));

    let mut reader = ShmReader::new(shm_path.clone());

    // If `-shm` isn't usable at startup (file absent in non-WAL modes,
    // version guard fails), fall back to the proven 1 ms poll loop.
    // The shm tickle adds no value without an `-shm` to mmap, and the
    // 100 ms safety-net interval would batch commits that arrive faster
    // than that — slower than the polling backend, defeating the point.
    //
    // Tradeoff: if the database later transitions to WAL mode (e.g. an
    // external process changes `journal_mode`), this watcher won't
    // upgrade to the fast path. That's acceptable — honker's
    // `open_conn` always sets WAL, so the realistic startup state is
    // either "WAL active, -shm exists" (fast path) or "the user is
    // using non-WAL deliberately" (poll loop is correct).
    if reader.read_ichange().is_none() {
        run_poll_loop(db_path, on_change, stop);
        return;
    }

    // Single PRAGMA-truth connection used by both the shm tickle and
    // the safety-net path. Same `last_version` cursor so duplicate
    // wakes are naturally deduped.
    let mut conn = open_reader(&db_path);
    let mut last_version = conn
        .as_ref()
        .and_then(|c| poll_data_version(c).ok())
        .unwrap_or(0);
    let initial_identity = stat_identity(&db_path).unwrap_or((0, 0));

    let started = Instant::now();
    let mut last_safety_net = started;
    let mut last_identity_check = started;
    let mut last_shm_reopen_check = started;

    while !stop.load(Ordering::Acquire) {
        std::thread::sleep(Duration::from_micros(SHM_POLL_INTERVAL_US));

        let now = Instant::now();

        // Periodically re-check shm identity for WAL reset detection.
        // Doing this on its own clock instead of every iteration cuts
        // ~10 k stat syscalls/sec down to ~10/sec — and the safety net
        // below catches anything we'd otherwise miss in between.
        if now.duration_since(last_shm_reopen_check) >= Duration::from_millis(SHM_REOPEN_CHECK_MS) {
            reader.refresh_identity();
            last_shm_reopen_check = now;
        }

        // Fast path: did iChange advance? If so, verify with PRAGMA
        // and (if confirmed) wake immediately.
        if let Some(ichange) = reader.read_ichange() {
            if reader.last_ichange != ichange {
                reader.last_ichange = ichange;
                check_pragma(&db_path, &mut conn, &mut last_version, &on_change);
                continue;
            }
        }

        // Safety net: even if shm was silent (or stale), check PRAGMA
        // periodically. This is the single load-bearing correctness
        // guarantee — everything above is optimization.
        if now.duration_since(last_safety_net) >= Duration::from_millis(SAFETY_NET_MS) {
            check_pragma(&db_path, &mut conn, &mut last_version, &on_change);
            last_safety_net = now;
        }

        if now.duration_since(last_identity_check) >= Duration::from_millis(IDENTITY_CHECK_MS) {
            identity_check(&db_path, initial_identity, &mut conn, &on_change);
            last_identity_check = now;
        }
    }
}

// ---------- ShmReader ------------------------------------------------

pub(crate) struct ShmReader {
    shm_path: PathBuf,
    mmap: Option<Mmap>,
    /// inode/device identity of the currently mapped file so we detect
    /// WAL resets that delete and recreate the `-shm`.
    file_id: (u64, u64),
    /// Last `iChange` value observed.
    pub last_ichange: u32,
}

impl ShmReader {
    pub(crate) fn new(shm_path: PathBuf) -> Self {
        let mut s = Self {
            shm_path,
            mmap: None,
            file_id: (0, 0),
            last_ichange: 0,
        };
        s.try_open();
        s
    }

    /// (Re-)open the shm file and map it. Called on startup and when
    /// `refresh_identity` detects an inode change.
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
        // SAFETY: We treat mmap reads as wake hints only. Stale data
        // is at worst a missed tickle — the safety-net PRAGMA in the
        // outer loop fires on its own clock and remains the source of
        // truth. The SQLite process that wrote the file owns the
        // exclusive write lock; we're a read-only observer.
        self.mmap = unsafe { Mmap::map(&f).ok() };
        self.file_id = id;
    }

    /// Re-stat `-shm`; if the inode changed (WAL reset), drop the
    /// stale map and try to re-attach. Cheap (one syscall) and called
    /// on its own throttled clock by the outer loop.
    pub(crate) fn refresh_identity(&mut self) {
        match stat_identity(&self.shm_path) {
            Ok(current) if current != self.file_id => self.try_open(),
            Err(_) if self.mmap.is_some() => {
                self.mmap = None;
                self.file_id = (0, 0);
            }
            _ => {}
        }
    }

    /// Read `iChange` from the mapped header, or `None` when the shm
    /// file is absent / too small / fails the `iVersion` guard.
    pub(crate) fn read_ichange(&self) -> Option<u32> {
        let mmap = self.mmap.as_ref()?;
        if mmap.len() < HDR_READ_LEN {
            return None;
        }
        let iversion =
            u32::from_ne_bytes(mmap[IVERSION_OFFSET..IVERSION_OFFSET + 4].try_into().ok()?);
        if iversion != WALINDEX_MAX_VERSION {
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
            Err(e) => {
                eprintln!("honker: data_version poll failed: {e}");
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

fn open_reader(path: &Path) -> Option<Connection> {
    Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .ok()
}
