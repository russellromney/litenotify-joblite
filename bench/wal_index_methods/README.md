# wal_index_methods — comparing five ways to detect cross-connection WAL commits

Honker's `WalWatcher` polls a SQLite database every 1 ms to wake
subscribers when a commit lands. The thread holds a connection but
issues no other SQL — only the change-detection call. This bench
compares the candidate primitives.

```bash
cd bench/wal_index_methods
cargo run --release
```

## Headline numbers

Apple Silicon M-series, release build, rusqlite 0.39 + bundled SQLite 3.51.3.

| method | ns/poll | detects cross-conn? | notes |
|---|---:|---|---|
| `SQLITE_FCNTL_DATA_VERSION` alone | 19 | ❌ | reads cached `pPager->iDataVersion` — never refreshes for an idle watcher |
| `PRAGMA data_version` (re-prepared, current honker) | ~2050 | ✅ | re-parses each call |
| `PRAGMA data_version` (cached prepared stmt) | ~1535 | ✅ | one-time parse, ~25% win |
| `stat(2)` on `<db>-wal` (size + mtime) | ~1610 | ✅ | original honker mechanism, before PRAGMA |
| **mmap `<db>-shm` `iChange` (dual-read + barrier)** | **0.6** | **✅** | **monotonic, survives `wal_checkpoint(TRUNCATE)`** |
| mmap `<db>-shm` `mxFrame` (dual-read + barrier) | 0.6 | ✅ | resets to 0 on TRUNCATE — still triggers a wake but isn't monotonic |
| mmap `<db>-shm` `iChange` (single read, no barrier) | 0.3 | ✅ | torn-read possible during writer commit |

The mmap path is **~3,400× faster** than today's PRAGMA call, **~2,500× faster** than the cached-prepared-stmt option, and **~2,700× faster** than the stat(2) approach. And it's the only sub-µs option that correctly detects cross-connection commits with no read transaction.

## Why FCNTL alone fails

`SQLITE_FCNTL_DATA_VERSION` reads `pPager->iDataVersion`, a cached
field on the connection's pager. Two places in `sqlite3.c` ever
increment it:

- `sqlite3PagerCommitPhaseTwo` — this connection's own commit path.
  (`sqlite3.c:65628`)
- `pager_reset` — called from `pagerBeginReadTransaction` after
  `walTryBeginRead` notices the wal-index header has moved.
  (`sqlite3.c:60694`)

`PRAGMA data_version` works because `sqlite3_step()` on the PRAGMA
implicitly opens a read transaction → `pagerBeginReadTransaction` →
`walTryBeginRead` → `pager_reset` → `iDataVersion++`. The FCNTL
opcode skips that whole path and just reads the cached value.

For a writer, or any connection that runs other queries between polls,
FCNTL works fine — there's always been a read transaction in between.
For an idle watcher that only polls FCNTL, the value never refreshes.

The misleading earlier conclusion in `scripts/proof_fcntl_vs_pragma.py`
came from interleaving PRAGMA calls with FCNTL calls in the same
script — the PRAGMAs were silently doing the wal-index check that
made FCNTL appear to work. See that script's header comment.

## Why mmap of `-shm` works (and is so fast)

The `-shm` file is SQLite's wal-index — a shared-memory region all
connections to the same database map and update in lockstep. Its
header layout is documented at https://www.sqlite.org/walformat.html:

```
offset  size  field
     0   u32  iVersion        (currently 3007000 — version guard)
     4   u32  (unused)
     8   u32  iChange         ← bumped on every commit
    12   u8   isInit
    13   u8   bigEndCksum
    14   u16  szPage
    16   u32  mxFrame         ← last valid frame in the WAL
   ... etc
```

Two copies live at offsets 0 and 48; a writer updates one before the
other, so a reader does **dual-read with a memory barrier between**
and retries on mismatch. This is the same `walIndexTryHdr` protocol
SQLite uses internally.

When we mmap `-shm` and read `iChange`, we are reading the same
shared-memory location SQLite reads inside `walTryBeginRead` — we
just skip the rest of `pagerBeginReadTransaction` (cache invalidation,
snapshot consistency, lock acquisition) because we don't need any of
it. We only need the counter.

`iChange` is monotonic across `wal_checkpoint(TRUNCATE)`. `mxFrame`
resets to 0 after TRUNCATE, so it would still trigger a wake on the
next commit but it isn't a clean monotonic counter — `iChange` is the
right field to watch.

## What this means for honker

CPU savings are mostly cosmetic — 1 ms polling at 2 µs/poll is
already 0.2% per database. The real wins are architectural:

1. **The watcher doesn't need a SQLite connection at all.** Today
   honker holds an entire `Connection` per watched DB just to call
   PRAGMA. With mmap, the watcher needs a `File` handle and a
   96-byte view. No SQLite mutex, no pager state, no PRAGMA on open.
2. **Bindings get simpler.** Each language binding currently goes
   through its language's SQLite binding to call PRAGMA. They could
   each implement the wal-index reader natively in ~50 lines — no
   FFI, no shared connection state, the shared core gets *smaller*.
3. **Polling can go faster cheaply** if we ever care.

Costs:

- Reads SQLite's documented-but-private wal-index layout. Check the
  `iVersion` field on open and fall back to PRAGMA polling if it's
  anything other than 3007000.
- `-shm` only exists in WAL mode. Guard with `PRAGMA journal_mode`
  check.
- `-shm` is removed when the *last* connection to the DB closes.
  Hold our own `File` handle for the lifetime of the watcher.

## See also

- Issue #5: https://github.com/russellromney/honker/issues/5
- WAL format: https://www.sqlite.org/walformat.html
- Pager source: `sqlite3.c:60694`, `:65628`, `:189063`
- Earlier (misleading) proof: `scripts/proof_fcntl_vs_pragma.py`
