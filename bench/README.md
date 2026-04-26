# Benchmarks

```bash
uv run python bench/honker_bench.py --n 5000
uv run python bench/stream_bench.py --n 5000
```

## `wal_index_methods/` — five ways to detect cross-connection commits

Standalone Rust crate. Compares `SQLITE_FCNTL_DATA_VERSION`,
`PRAGMA data_version` (re-prepared and cached), `stat(2)` on `-wal`,
and direct `mmap` of `-shm` for honker's `WalWatcher` polling.
Headline: mmap of `-shm` is ~3,000× faster than the current PRAGMA
path (0.6 ns/poll vs ~2050 ns), and FCNTL alone *does not detect*
cross-connection commits despite what the docs imply (it returns a
cached pager-local value). See `bench/wal_index_methods/README.md`
for full numbers and the source-code reasoning.

```bash
cd bench/wal_index_methods && cargo run --release
```

Why honker still uses PRAGMA: CPU savings are 0.2% per DB → ~0% per DB,
which doesn't justify the architectural complexity at current scale.
The bench is kept as a proof of what's possible if scale ever changes.
Context: https://github.com/russellromney/honker/issues/5

## Baseline — Apple Silicon M-series, release build, 2026-04

Median of 3 runs. WAL + `synchronous=NORMAL`, default `busy_timeout=5000`.

### `honker.queue`

```
enqueue (1 job / tx):                   6,000 ops/s   (~170us/job)
enqueue (100 jobs / tx):              110,000 ops/s   (~9us/job)
claim + ack (1 job / 2 tx):             3,700 ops/s   (~270us/job)
claim_batch + ack_batch (batch=8):     23,500 ops/s
claim_batch + ack_batch (batch=32):    60,000 ops/s
claim_batch + ack_batch (batch=128):   80,000 ops/s
end-to-end (async iter, batch=32):      3,500 ops/s   p50 ~720ms (drain pattern)
```

The async iterator (`async for job in queue.claim(worker_id)`) uses
`claim_batch(batch_size=32)` internally; application code gets the
60k number unchanged.

### `honker.stream`

```
publish (1 / tx):                       5,800 events/s
replay (reader pool):               1,000,000 events/s
live e2e:                              p50 = 0.23ms   p99 = 7ms   (1000 events)
```

Replay became a million-per-second after swapping `list.pop(0)` for
`collections.deque.popleft()` on the 1000-row refresh buffer — the
O(n) shift-everything was the bottleneck, not the SQL.

## Platform ceiling

Raw Python `sqlite3` single-tx on the same WAL+`synchronous=NORMAL`
file measures ~47k ops/s on this machine. That's the floor. Our
single-tx enqueue at 6k/s is ~8x below that, which is per-tx fixed
cost (writer mutex acquire, GIL detach/reacquire, three PyO3 boundary
crossings). Batched enqueue at 110k/s is **faster than raw Python
`sqlite3`** single-tx because SQLite writes fewer WAL pages when
inserts amortize into one transaction.

## Perf history

| Pass | Change | Impact |
|------|--------|--------|
| 1 | `prepare_cached` for every SQL path including BEGIN/COMMIT/ROLLBACK | +73% batched enqueue |
| 2 | Partial index `WHERE state IN (...)`, drop `state` from key, add explicit `IN` to query so planner matches | claim+ack 1k → 3.1k (3x); end-to-end 820 → 3k (3.7x) |
| 3 | Writer mutex `try_acquire` fast-path skips `py.detach` on uncontended tx | enqueue +15%, batch=128 +10% |
| 4 | `collections.deque` for iterator buffers (`list.pop(0)` was O(n)) | replay 400k → 1M (2.5x); batch=128 +30% |
| 5 | Lazy `json.loads` on `Job.payload` / `Event.payload` | small win, scales with miss rate |
| 6 | `Arc<Notification>` in notifier broadcast so fan-out is ref-count bumps, not `String` clones | measurable at multi-subscriber |

## Comparing to Redis

`redis-cli LPUSH` + `BRPOP` on localhost clears ~100k ops/s in a
separate process, without atomic coupling to your application writes.
`claim_batch + ack_batch` at 80k/s is in the same order of magnitude
and keeps the tx coupling. For bulk loaders or workers that naturally
batch, use the batch API. For per-request single enqueues, 6k/s is
well above what most apps actually produce.
