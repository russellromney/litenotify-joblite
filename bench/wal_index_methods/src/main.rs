//! wal_index_methods — comparing four ways to detect cross-connection
//! WAL commits from a watcher that does no other SQL.
//!
//! Background: honker's `WalWatcher` polls a SQLite database every
//! 1 ms to wake subscribers when a commit lands. The thread holds a
//! connection but issues no other SQL — only the change-detection
//! call. This bench measures the four candidate primitives:
//!
//!   1. `SQLITE_FCNTL_DATA_VERSION` — file-control opcode.
//!   2. `PRAGMA data_version`       — re-prepared each call.
//!   3. `PRAGMA data_version`       — cached prepared statement.
//!   4. mmap of `<db>-shm`          — read the wal-index header
//!                                    `iChange` / `mxFrame` fields
//!                                    directly from shared memory.
//!   5. `stat(2)` of `<db>-wal`     — the original honker mechanism,
//!                                    polling WAL file size + mtime.
//!
//! Run from this directory with `cargo run --release`.
//!
//! See `bench/wal_index_methods/README.md` for the headline numbers
//! and the "why" behind each method.

use memmap2::Mmap;
use rusqlite::{Connection, ffi};
use std::fs::File;
use std::os::raw::c_void;
use std::path::{Path, PathBuf};
use std::sync::atomic::{Ordering, fence};
use std::time::Instant;

// ---------------------------------------------------------------------
// Method 1 + 2 + 3: SQLite C API (FCNTL, PRAGMA)
// ---------------------------------------------------------------------

fn fcntl_dv(conn: &Connection) -> u32 {
    let mut v: u32 = 0;
    let main_db = b"main\0";
    // SAFETY: conn.handle() is a valid sqlite3*; main_db is a NUL-
    // terminated static; &mut v outlives the call.
    let rc = unsafe {
        ffi::sqlite3_file_control(
            conn.handle(),
            main_db.as_ptr() as *const _,
            ffi::SQLITE_FCNTL_DATA_VERSION,
            &mut v as *mut u32 as *mut c_void,
        )
    };
    assert_eq!(rc, ffi::SQLITE_OK);
    v
}

fn pragma_query_dv(conn: &Connection) -> u32 {
    conn.pragma_query_value(None, "data_version", |r| r.get(0))
        .unwrap()
}

// ---------------------------------------------------------------------
// Method 4: wal-index (-shm) header reader.
//
// Layout per https://www.sqlite.org/walformat.html — two copies of a
// 48-byte WalIndexHdr at offsets 0 and 48:
//
//   off  size  field
//     0   u32  iVersion         (currently 3007000)
//     4   u32  (unused/padding)
//     8   u32  iChange          ← bumped on every commit
//    12   u8   isInit
//    13   u8   bigEndCksum
//    14   u16  szPage
//    16   u32  mxFrame          ← last valid frame in the WAL
//    20   u32  nPage
//    24   u32  aFrameCksum[2]
//    32   u32  aSalt[2]
//    40   u32  aCksum[2]
//
// Atomicity: a writer may be updating one copy while we read. Read
// h1, memory barrier, read h2; if they match we have a clean read.
// This is the same protocol SQLite uses internally in walIndexTryHdr
// (sqlite3.c). Without the dual read, you can race a writer commit
// and see a torn value — fine if the caller tolerates spurious wakes.
// ---------------------------------------------------------------------

const HDR_SIZE: usize = 48;
const OFF_ICHANGE: usize = 8;
const OFF_MXFRAME: usize = 16;

fn shm_path(db_path: &Path) -> PathBuf {
    let mut p = db_path.as_os_str().to_owned();
    p.push("-shm");
    PathBuf::from(p)
}

struct ShmReader {
    _file: File,
    mmap: Mmap,
}

impl ShmReader {
    fn open(db_path: &Path) -> Self {
        let p = shm_path(db_path);
        let file = File::open(&p).expect("open -shm");
        // SAFETY: -shm is owned by SQLite and updated in place. We
        // only read; the dual-copy header protocol handles races.
        let mmap = unsafe { Mmap::map(&file) }.expect("mmap -shm");
        assert!(mmap.len() >= 2 * HDR_SIZE, "-shm too small");
        Self { _file: file, mmap }
    }

    /// Dual-copy read of `iChange` with retry on torn read.
    #[inline]
    fn ichange_safe(&self) -> u32 {
        let bytes = self.mmap.as_ref();
        loop {
            let h1 = u32::from_le_bytes(
                bytes[OFF_ICHANGE..OFF_ICHANGE + 4].try_into().unwrap(),
            );
            fence(Ordering::Acquire);
            let h2 = u32::from_le_bytes(
                bytes[HDR_SIZE + OFF_ICHANGE..HDR_SIZE + OFF_ICHANGE + 4]
                    .try_into()
                    .unwrap(),
            );
            if h1 == h2 {
                return h1;
            }
            std::hint::spin_loop();
        }
    }

    /// Dual-copy read of `mxFrame` with retry on torn read.
    #[inline]
    fn mxframe_safe(&self) -> u32 {
        let bytes = self.mmap.as_ref();
        loop {
            let h1 = u32::from_le_bytes(
                bytes[OFF_MXFRAME..OFF_MXFRAME + 4].try_into().unwrap(),
            );
            fence(Ordering::Acquire);
            let h2 = u32::from_le_bytes(
                bytes[HDR_SIZE + OFF_MXFRAME..HDR_SIZE + OFF_MXFRAME + 4]
                    .try_into()
                    .unwrap(),
            );
            if h1 == h2 {
                return h1;
            }
            std::hint::spin_loop();
        }
    }

    /// Single-copy read — no retry, no barrier. Marginally faster but
    /// can return torn values during a writer commit (caller must
    /// tolerate spurious wakes).
    #[inline]
    fn ichange_dirty(&self) -> u32 {
        let bytes = self.mmap.as_ref();
        u32::from_le_bytes(bytes[OFF_ICHANGE..OFF_ICHANGE + 4].try_into().unwrap())
    }
}

// ---------------------------------------------------------------------
// Method 5: stat(2) on the -wal file.
//
// The original honker mechanism — and what infogulch asked about in
// issue #5. Each commit appends frames to the WAL, so file size and
// mtime both change. Returns a (size, mtime_ns) tuple; any change in
// either signals a commit. Does not detect commits absorbed into the
// db file by a checkpoint between polls (size goes back to 0 after
// TRUNCATE), and is bounded by mtime resolution on platforms that
// don't expose nanosecond precision.
// ---------------------------------------------------------------------

fn wal_path(db_path: &Path) -> PathBuf {
    let mut p = db_path.as_os_str().to_owned();
    p.push("-wal");
    PathBuf::from(p)
}

fn stat_wal(db_path: &Path) -> (u64, i128) {
    use std::os::unix::fs::MetadataExt;
    let m = std::fs::metadata(wal_path(db_path)).expect("stat -wal");
    let mtime_ns = m.mtime() as i128 * 1_000_000_000 + m.mtime_nsec() as i128;
    (m.size(), mtime_ns)
}

// ---------------------------------------------------------------------
// bench harness
// ---------------------------------------------------------------------

fn time_it<F: FnMut()>(label: &str, n: usize, mut f: F) {
    for _ in 0..1000 {
        f();
    }
    let start = Instant::now();
    for _ in 0..n {
        f();
    }
    let elapsed = start.elapsed();
    let per_ns = elapsed.as_nanos() as f64 / n as f64;
    println!("  {:<60} {:>9.1} ns/poll", label, per_ns);
}

fn main() {
    let tmp = std::env::temp_dir().join(format!(
        "honker-wal-index-bench-{}",
        std::process::id()
    ));
    let _ = std::fs::remove_file(&tmp);
    let _ = std::fs::remove_file(shm_path(&tmp));
    let _ = std::fs::remove_file(format!("{}-wal", tmp.display()));

    // Set up: WAL mode, table created, watcher primed with one real read
    // (so its pager has done a pagerBeginReadTransaction at least once).
    let watcher = Connection::open(&tmp).unwrap();
    watcher
        .execute_batch("PRAGMA journal_mode = WAL;")
        .unwrap();
    let writer = Connection::open(&tmp).unwrap();
    writer.execute("CREATE TABLE t(x INTEGER)", []).unwrap();
    let _: i64 = watcher
        .query_row("SELECT count(*) FROM t", [], |r| r.get(0))
        .unwrap();

    // -shm exists now — open the mmap reader.
    let shm = ShmReader::open(&tmp);

    // Sanity-check the wal-index version field. If SQLite ever changes
    // the wal format, the iVersion at offset 0 changes too — that's our
    // canary to fall back to PRAGMA polling.
    let iversion = u32::from_le_bytes(shm.mmap[0..4].try_into().unwrap());
    println!("wal-index iVersion = {} (expect 3007000)", iversion);
    println!();

    // ------------------------------------------------------------
    // Part 1: correctness — does each method see cross-conn writes
    //         on a watcher that does NO sql at all?
    // ------------------------------------------------------------
    println!("=== correctness on idle watcher (no SQL between polls) ===");

    let f0 = fcntl_dv(&watcher);
    let i0 = shm.ichange_safe();
    let m0 = shm.mxframe_safe();
    let s0 = stat_wal(&tmp);
    writer.execute("INSERT INTO t VALUES (1)", []).unwrap();
    let f1 = fcntl_dv(&watcher);
    let i1 = shm.ichange_safe();
    let m1 = shm.mxframe_safe();
    let s1 = stat_wal(&tmp);
    println!(
        "  FCNTL alone:                  {} -> {}  detect={}",
        f0,
        f1,
        f0 != f1
    );
    println!(
        "  shm iChange  (safe dual):     {} -> {}  detect={}",
        i0,
        i1,
        i0 != i1
    );
    println!(
        "  shm mxFrame  (safe dual):     {} -> {}  detect={}",
        m0,
        m1,
        m0 != m1
    );
    println!(
        "  stat(2) on -wal (size,mtime): {:?} -> {:?}  detect={}",
        s0,
        s1,
        s0 != s1
    );

    // A few more commits, see counter behavior.
    let i_a = shm.ichange_safe();
    let m_a = shm.mxframe_safe();
    for _ in 0..3 {
        writer.execute("INSERT INTO t VALUES (99)", []).unwrap();
    }
    let i_b = shm.ichange_safe();
    let m_b = shm.mxframe_safe();
    println!("  after 3 more commits:");
    println!(
        "    iChange  {} -> {}  delta={}",
        i_a,
        i_b,
        i_b.wrapping_sub(i_a)
    );
    println!(
        "    mxFrame  {} -> {}  delta={}",
        m_a,
        m_b,
        m_b.wrapping_sub(m_a)
    );
    println!();

    // ------------------------------------------------------------
    // Checkpoint behavior — does iChange / mxFrame survive a TRUNCATE?
    // ------------------------------------------------------------
    println!("=== checkpoint survival ===");
    let i_pre = shm.ichange_safe();
    let m_pre = shm.mxframe_safe();
    writer
        .execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
        .unwrap();
    let i_post = shm.ichange_safe();
    let m_post = shm.mxframe_safe();
    println!("  before checkpoint: iChange={} mxFrame={}", i_pre, m_pre);
    println!("  after  checkpoint: iChange={} mxFrame={}", i_post, m_post);
    writer.execute("INSERT INTO t VALUES (777)", []).unwrap();
    let i_after_write = shm.ichange_safe();
    let m_after_write = shm.mxframe_safe();
    println!("  after one more commit:");
    println!(
        "    iChange  {} -> {} (still progresses: {})",
        i_post,
        i_after_write,
        i_after_write != i_post
    );
    println!(
        "    mxFrame  {} -> {} (still progresses: {})",
        m_post,
        m_after_write,
        m_after_write != m_post
    );
    println!();

    // ------------------------------------------------------------
    // Part 2: latency
    // ------------------------------------------------------------
    println!("=== latency: ns per poll, no concurrent writes ===");

    let n: usize = 1_000_000;

    time_it(
        "(baseline) FCNTL alone — incorrect for our use",
        n,
        || {
            std::hint::black_box(fcntl_dv(&watcher));
        },
    );

    time_it(
        "PRAGMA data_version via pragma_query_value (current honker)",
        200_000,
        || {
            std::hint::black_box(pragma_query_dv(&watcher));
        },
    );

    {
        let mut stmt = watcher
            .prepare_cached("PRAGMA data_version")
            .unwrap();
        time_it(
            "PRAGMA data_version via cached prepared stmt",
            200_000,
            || {
                let v: u32 = stmt.query_row([], |r| r.get(0)).unwrap();
                std::hint::black_box(v);
            },
        );
    }

    time_it("shm iChange (safe dual-read + barrier)", n, || {
        std::hint::black_box(shm.ichange_safe());
    });

    time_it("shm mxFrame (safe dual-read + barrier)", n, || {
        std::hint::black_box(shm.mxframe_safe());
    });

    time_it("shm iChange (single read, no consistency)", n, || {
        std::hint::black_box(shm.ichange_dirty());
    });

    time_it(
        "stat(2) on -wal file (size + mtime) — original honker",
        200_000,
        || {
            std::hint::black_box(stat_wal(&tmp));
        },
    );

    let _ = std::fs::remove_file(&tmp);
    let _ = std::fs::remove_file(shm_path(&tmp));
    let _ = std::fs::remove_file(wal_path(&tmp));
}
