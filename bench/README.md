# Benchmarks

```bash
uv run python bench/joblite_bench.py --n 5000
uv run python bench/stream_bench.py --n 5000
```

## Baseline — Apple Silicon M-series, release build, 2026-04

Median of 3 runs. WAL + `synchronous=NORMAL`, default `busy_timeout=5000`.

### `joblite.queue`

```
enqueue (1 job / tx):                 5,000 ops/s   (~200us/job)
enqueue (100 jobs / tx):             94,000 ops/s   (~11us/job)
claim + ack (1 job / 2 tx):           3,100 ops/s   (~320us/job)
claim_batch + ack_batch (batch=8):   18,500 ops/s
claim_batch + ack_batch (batch=32):  48,500 ops/s
claim_batch + ack_batch (batch=128): 61,000 ops/s
end-to-end (async iter, batch=32):    3,050 ops/s   p50~850ms in drain pattern
```

The async iterator (`async for job in queue.claim(worker_id)`) uses
`claim_batch(batch_size=32)` internally; you get the 48k number
without touching application code.

### `joblite.stream`

```
publish (1 / tx):                     5,700 events/s
replay (reader pool):               400,000 events/s
live e2e:                            p50 = 0.24ms   p99 = 8ms   (1000 events)
```

Live e2e is commit-hook → tokio broadcast → `call_soon_threadsafe` →
consumer. Sub-millisecond typical.

## Ceilings and gaps

Raw Python `sqlite3` single-tx on the same WAL+`synchronous=NORMAL` file
measures ~47k ops/s. That's the platform ceiling. The gap to our 5k/s
enqueue is per-tx fixed cost:

- Writer-mutex acquire + release
- `py.detach` (GIL release + reacquire)
- Three PyO3 boundary crossings per tx (enter, execute, exit)
- rusqlite `prepare_cached` lookup × 3 (BEGIN, body, COMMIT)

Batch into one `COMMIT` and these amortize away. 94k/s for batched
enqueue is **faster than** raw Python `sqlite3` single-tx because SQLite
fsyncs less often when a tx covers more rows (WAL frame gets fewer
checkpoints per row written).

## The partial-index fix

Before 2026-04-18 the claim index was keyed on
`(queue, state, priority DESC, run_at, id)`, meaning every state
transition (pending → processing → done) reshuffled the row in the
B-tree. Combined with an `OR`-based WHERE that the planner couldn't
match against the index, the claim query was doing a full table scan.
Measured cost: ~1k/s claim+ack even on a 5k-row queue.

Fix: partial index on `(queue, priority DESC, run_at, id)
WHERE state IN ('pending', 'processing')`, plus an explicit
`state IN ('pending', 'processing')` in the claim SELECT so the planner
matches it. Result:

```
claim + ack:  ~1k/s  -> ~3.1k/s    (3x, single tx per op)
end-to-end:   ~820/s -> ~3.05k/s   (3.7x, async iter)
```

Check the plan yourself with `EXPLAIN QUERY PLAN`:

```
SEARCH _joblite_jobs USING INDEX _joblite_jobs_claim_v2 (queue=?)
```

## Comparing to Redis

`redis-cli LPUSH` + `BRPOP` on localhost clears ~100k ops/s in a
separate process, without atomic coupling to your application writes.
`claim_batch + ack_batch` at 61k/s is in the same order of magnitude
and keeps the tx coupling. For bulk loaders or workers that naturally
batch, use the batch API.
