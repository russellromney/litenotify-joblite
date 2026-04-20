# litenotify/joblite

`litenotify` is a SQLite extension that adds Postgres-style `NOTIFY`/`LISTEN` semantics to SQLite with built-in durable pub/sub, task queue, and event streams without a daemon/broker or client polling.

`joblite` adds language bindings and framework integrations that let listeners/workers consume and act on messages from `litenotify`.

`litenotify` works by adding messages/tasks to tables in SQLite, and takes advantage of event notifications on SQLite's WAL file to replace a polling interval with push semantics achieving cross-process notifications with single-digit millisecond latency.

> Experimental. API may change.

## At a glance

```python
import joblite

db = joblite.open("app.db")
emails = db.queue("emails")

# Enqueue
emails.enqueue({"to": "alice@example.com"})

# Consume (worker process)
async for job in emails.claim("worker-1"):
    send(job.payload)
    job.ack()
```

Any enqueue can be atomic with a business write. Rollback drops both.

```python
with db.transaction() as tx:
    tx.execute("INSERT INTO orders (user_id) VALUES (?)", [42])
    emails.enqueue({"to": "alice@example.com"}, tx=tx)
```

## Features

Today:

- Notify/listen across processes on one `.db` file
- Durable streams with per-consumer offsets
- Work queues with retries, priority, delayed jobs, and a dead-letter table
- Any send can be atomic with your business write (commit together or roll back together)
- Single-digit millisecond cross-process reaction time, no polling
- Handler timeouts, declarative retries with exponential backoff
- Delayed jobs, task expiration, named locks, rate-limiting
- Crontab-style periodic tasks with a leader-elected scheduler
- Opt-in task result storage (`enqueue` returns an id, worker persists the
  return value, caller awaits `queue.wait_result(id)`)
- Durable streams with per-consumer offsets and configurable flush interval
- SQLite loadable extension so any SQLite client can read the same tables
- Python and Node.js bindings

Planned:

- Go and Ruby bindings
- `joblite-node` TypeScript port of the higher-level API
- Framework plugins (FastAPI, Django, Flask, Express, Rails) — cut
  for now; the core API is small enough that wiring joblite into
  your web framework is ~20 lines. See `examples/` once it lands.

Deliberately out of scope: task pipelines/chains/groups/chords, multi-writer replication, workflow orchestration with DAGs.

## Quick start

```bash
pip install litenotify-joblite
```

### Python — queue (durable at-least-once work)

```python
import joblite
db = joblite.open("app.db")
emails = db.queue("emails")

with db.transaction() as tx:
    tx.execute("INSERT INTO orders (user_id) VALUES (?)", [42])
    emails.enqueue({"to": "alice@example.com"}, tx=tx)   # atomic with order

# Then in a worker, do: 
async for job in emails.claim("worker-1"):               # wakes on any WAL commit
    try:
        send(job.payload); job.ack()
    except Exception as e:
        job.retry(delay_s=60, error=str(e))
```

`claim()` is an async iterator. Each iteration is one `claim_batch(worker_id, 1)` — one row, one write transaction. Wakes on any WAL commit, falls back to a 5 s paranoia poll only if the WAL watcher can't fire. For batched work, call `claim_batch(worker_id, n)` explicitly and ack with `queue.ack_batch(ids, worker_id)`. Defaults: visibility 300 s.

### Python — stream (durable pub/sub)

```python
stream = db.stream("user-events")

with db.transaction() as tx:
    tx.execute("UPDATE users SET name=? WHERE id=?", [name, uid])
    stream.publish({"user_id": uid, "change": "name"}, tx=tx)

async for event in stream.subscribe(consumer="dashboard"):
    await push_to_browser(event)
```

Each named consumer tracks its own offset in the `_joblite_stream_consumers` table. `subscribe` replays rows past the saved offset, then transitions to live delivery on WAL wake. The iterator auto-saves offset at most every 1000 events or every 1 second (whichever first) — one UPSERT per window, not per event — so a high-throughput stream doesn't hammer the single-writer slot. Override with `save_every_n=` / `save_every_s=`, or set both to 0 to disable auto-save and call `stream.save_offset(consumer, offset, tx=tx)` yourself (atomic with whatever you just did in that tx). At-least-once: a crash re-delivers in-flight events up to the last flushed offset.

### Python — notify (ephemeral pub/sub)

```python
async for n in db.listen("orders"):
    print(n.channel, n.payload)

with db.transaction() as tx:
    tx.execute("INSERT INTO orders (id, total) VALUES (?, ?)", [42, 99.99])
    tx.notify("orders", {"id": 42})
```

Listeners attach at current `MAX(id)`; history is not replayed. Use `db.stream()` if you need durable replay. The notifications table is not auto-pruned - call `db.prune_notifications(older_than_s=…, max_keep=…)` from a scheduled task. Task payloads have to be valid JSON so a Python writer and Node reader can share a channel.

### Node.js

```js
const lit = require('@litenotify/node');
const db = lit.open('app.db');
const tx = db.transaction();
tx.execute('INSERT INTO orders (id) VALUES (?)', [42]);
tx.notify('orders', { id: 42 });
tx.commit();

const ev = db.walEvents();
let last = 0;
while (running) {
  await ev.next();
  const rows = db.query(
    'SELECT id, payload FROM _litenotify_notifications WHERE id > ? ORDER BY id', [last]);
  for (const r of rows) { handle(JSON.parse(r.payload)); last = r.id; }
}
```

### SQLite extension (any SQLite 3.9+ client)

```sql
.load ./liblitenotify_ext
SELECT jl_bootstrap();
INSERT INTO _joblite_live (queue, payload) VALUES ('emails', '{"to":"alice"}');
SELECT jl_claim_batch('emails', 'worker-1', 32, 300);    -- JSON array
SELECT jl_ack_batch('[1,2,3]', 'worker-1');              -- DELETEs; returns count
SELECT jl_sweep_expired('emails');                       -- count moved to dead
SELECT jl_lock_acquire('backup', 'me', 60);              -- 1 = got it, 0 = held
SELECT jl_lock_release('backup', 'me');                  -- 1 = released
SELECT jl_rate_limit_try('api', 10, 60);                 -- 1 = under, 0 = at limit
SELECT jl_rate_limit_sweep(3600);                        -- drop windows >1h old
SELECT jl_cron_next_after('0 3 * * *', unixepoch());     -- unix ts of next fire
SELECT jl_scheduler_register('nightly', 'backups',
  '0 3 * * *', '"go"', 0, NULL);                         -- register periodic task
SELECT jl_scheduler_tick(unixepoch());                   -- JSON: fires due
SELECT jl_scheduler_soonest();                           -- min next_fire_at
SELECT jl_scheduler_unregister('nightly');               -- 1 = deleted
SELECT jl_stream_publish('orders', 'k', '{"id":42}');    -- returns offset
SELECT jl_stream_read_since('orders', 0, 1000);          -- JSON array
SELECT jl_stream_save_offset('worker', 'orders', 42);    -- monotonic upsert
SELECT jl_stream_get_offset('worker', 'orders');         -- offset or 0
SELECT jl_result_save(42, '{"ok":true}', 3600);          -- save w/ 1h TTL
SELECT jl_result_get(42);                                -- value or NULL
SELECT jl_result_sweep();                                -- prune expired
SELECT notify('orders', '{"id":42}');
```

The extension shares `_joblite_live`, `_joblite_dead`, and `_litenotify_notifications` with the Python binding, so a Python worker can claim jobs any other language pushed via the extension. Schema compatibility is pinned by `tests/test_extension_interop.py`.

## Design

This repo includes the `litenotify` SQLite loadable extension and `joblite` language bindings for Python and Node today, with Go/Rust/Ruby planned. Framework-integration plugins (FastAPI/Django/Flask) were cut for now — the core API is small enough that wiring joblite into your web framework is ~20 lines per framework, and we prefer to keep that surface as cookbook examples rather than maintained packages until a real user needs something different.

For most applications, [SQLite alone is sufficient](https://www.epicweb.dev/why-you-should-probably-be-using-sqlite). There are already great libraries that leverage SQLite for durable messaging. [Huey](https://github.com/coleifer/huey) is one; [`diskcache`](https://github.com/grantjenks/python-diskcache) is another. This project is inspired by them and seeks to do something similar across languages and frameworks by moving package logic into a SQLite extension.

For Postgres-backed apps, [`pg_notify`](https://www.postgresql.org/docs/current/sql-notify.html) + [pg-boss](https://github.com/timgit/pg-boss) or [Oban](https://hexdocs.pm/oban/) is the equivalent. This library is for apps where SQLite is the primary datastore.

The extension has three primitives that tie it together: ephemeral pub/sub (`notify()`), durable pub/sub with per-consumer offsets (`stream()`), at-least-once work queue (`queue()`). All three are INSERTs inside your transaction, which lets a task "send" be atomic with your business write, and rollback drops everything.

The explicit goal is to do `NOTIFY`/`LISTEN` semantics without constant polling, to achieve single-digit ms reaction time. If you use your app's existing SQLite file containing business logic, it will notify workers on every WAL commit. This means that most triggers will not result in anything happening - workers just read the message/queue with no result. This "overtriggering" is on purpose and is the tradeoff for push semantics and fast reaction time.

The library/extension is a small coordination layer built on the properties of SQLite and single-server architecture.

- **One `.db` + one `.db-wal`** is the entire system, with all the benefits of SQLite (embedded, local, durable, snapshot-able, etc.) that your app already usees
- **WAL mode: one writer, concurrent readers.** Claim = `UPDATE … RETURNING` via a partial index. Ack = `DELETE`.
- **WAL file grows on every commit.** `(size, mtime)` is the cross-process commit signal.
- **SQLite has no wire protocol.** Consumers must initiate reads; server-push is impossible. Wake signal = file change → `SELECT`.
- **Transactions are cheap.** Jobs, events, and notifications are rows in the caller's open `with db.transaction()` block in an "outbox"-type pattern.
- **Cross-platform via `stat(2)`, not `FSEvents`/`inotify`/`kqueue`.** FSEvents drops same-process writes on macOS — a listener and enqueuer in the same Python process would never see each other. `stat(2)` works identically on Linux/macOS/Windows at ~1 ms granularity for negligible CPU. Cost: ~0.5 ms of latency vs kernel notifications.
- **Single machine, single writer.** SQLite's locking is designed for a single host. Two servers writing one `.db` over NFS will corrupt it. Shard by file, or switch to Postgres.

## Architecture

### Wake path

- One `stat(2)` thread per `Database`, polls `.db-wal` every 1 ms
- `(size, mtime)` change → fan out a tick to each subscriber's bounded channel
- Each subscriber runs `SELECT … WHERE id > last_seen` against a partial index, yields rows, returns to wait
- 100 subscribers = 1 stat thread (not 100)
- Idle listeners run zero SQL queries

Idle cost is a single `stat(2)` per millisecond per database — no SQL, no page-cache pressure, no writer-lock contention. Listener count scales for free because the wake signal is a file stat, not a query.

`SharedWalWatcher` (in `litenotify-core`) owns the poll thread and fans out to N subscribers via bounded `SyncSender<()>` channels keyed by subscriber id. Each `db.wal_events()` call registers a subscriber and returns a handle whose `Drop` auto-unsubscribes, so a dropped listener causes the bridge thread's `rx.recv() -> Err` and exits cleanly.

### Queue schema

- `_joblite_live`: pending + processing rows
- Partial index: `(queue, priority DESC, run_at, id) WHERE state IN ('pending','processing')`
- Claim = one `UPDATE … RETURNING` via that index
- Ack = one `DELETE`
- Retry-exhausted → `_joblite_dead` (never scanned by claim path)

Partial-index on state means the claim hot path is bounded by the *working-set* size, not the *history* size. A queue with 100k dead rows claims as fast as a queue with zero.

### Claim iterator

- `async for job in q.claim(id)` yields one job at a time via `claim_batch(id, 1)` — one write transaction per job.
- `Job.ack()` is one `DELETE` in its own transaction. Return is an honest bool: `True` iff the claim was still valid, `False` if the visibility window elapsed and another worker reclaimed.
- Wakes on WAL commit from any process; a 5 s paranoia poll is the only fallback.

For batched work, call `claim_batch(worker_id, n)` directly and ack with `queue.ack_batch(ids, worker_id)`. The library doesn't hide batching behind the iterator — the per-tx cost and the at-most-once visibility semantics are easier to reason about when the API doesn't try to be clever.

### Transactional coupling

- `notify()` is a SQL scalar function registered on the writer connection
- INSERTs into `_litenotify_notifications` under the caller's open tx
- `queue.enqueue(…, tx=tx)` and `stream.publish(…, tx=tx)` do the same
- Rollback drops the job/event/notification with the rest of the tx

This is the transactional outbox pattern, by default, without a library to install. Business write and side-effect enqueue commit or roll back together. There is no separate dispatch table and no separate dispatcher process — the side-effect row *is* the committed row, and any process watching the WAL picks it up within ~1 ms.

### Over-trigger, don't under-trigger

- A WAL change wakes *every* subscriber on that `Database`, not just the ones whose channel committed
- Each wasted wake = one indexed SELECT (microseconds)
- A missed wake = a silent correctness bug

The library prefers waking ten listeners that don't care over missing one that does. Channel filtering happens in the SELECT, not in the wake path.

### Retention

- Queue jobs persist until ack; retry-exhausted rows move to `_joblite_dead`
- Stream events persist; each named consumer tracks its own offset
- Notify is fire-and-forget and not auto-pruned

The caller chooses retention per primitive. `db.prune_notifications(older_than_s=…, max_keep=…)` is a tool you invoke, not a background timer inside the library. This keeps retention policy visible in the caller's code instead of inherited from a library default.

## Crash recovery

- **Rollback** drops jobs/events/notifications with your business write (SQLite ACID).
- **SIGKILL mid-tx**: safe. WAL rollback on next open leaves no stale state. Verified in `tests/test_crash_recovery.py` (subprocess killed pre-COMMIT, `PRAGMA integrity_check == 'ok'`, fresh notifies still flow).
- **Worker crash mid-job**: the claim expires after `visibility_timeout_s` (default 300 s). Another worker reclaims; `attempts` increments. After `max_attempts` (default 3), the row moves to `_joblite_dead`.
- **Listener offline during prune**: pruned events are lost. For durable replay, use `db.stream()`, which tracks per-consumer offsets.

## Wiring into your web framework

No framework plugins today. The core API is small enough that a
minimal FastAPI / Django / Flask integration is ~20 lines:

```python
# FastAPI: enqueue in a request, run workers via lifespan.
@app.on_event("startup")
async def _start_workers():
    async def worker_loop():
        async for job in db.queue("emails").claim("worker"):
            await joblite._worker.run_task(
                job, send_email, timeout=30, retries=3, backoff=2.0
            )
    app.state._worker = asyncio.create_task(worker_loop())

@app.post("/orders")
async def create_order(order: dict):
    with db.transaction() as tx:
        tx.execute("INSERT INTO orders (user_id) VALUES (?)", [order["user_id"]])
        db.queue("emails").enqueue({"to": order["email"]}, tx=tx)
    return {"ok": True}
```

SSE endpoints are ~30 lines of `async def stream(...): yield f"data: ...\n\n"` over `db.listen(channel)` or `db.stream(name).subscribe(...)`. Django/Flask workers live in a dedicated CLI process (same pattern as Celery/RQ) because those frameworks fork per-request and you don't want a worker pool in each fork.

If demand for a packaged version grows we'll bring plugins back as their own repos. Until then: copy the 20 lines into your app.

## Performance

Handles thousands of messages per second on a modern laptop, with cross-process wake latency bounded by the 1 ms stat-poll cadence (~1–2 ms median on M-series). Run `bench/wake_latency_bench.py` and `bench/real_bench.py` to measure on your hardware.

## Development

Layout:

```
litenotify-core/              # Rust rlib shared across all bindings
litenotify-extension/         # SQLite loadable extension (cdylib)
packages/
  litenotify/                 # PyO3 Python binding
  litenotify-node/            # napi-rs Node.js binding
  joblite/                    # Python higher-level Queue/Stream/Outbox
tests/                        # integration tests (cross-package)
bench/                        # benches
```

Each `packages/*` directory is self-contained (own `Cargo.toml` / `pyproject.toml` / `package.json`) and is intended to become its own repository published as a git submodule into this directory. Today it lives inline for fast iteration while the APIs settle.

```bash
make test                   # default: rust + python + node (fast, ~10s)
make test-python-slow       # soak + real-time cron tests (~2 min)
make test-all               # everything including slow marks

make build                  # PyO3 maturin develop + loadable extension

python bench/wake_latency_bench.py --samples 500
python bench/real_bench.py --workers 4 --enqueuers 2 --seconds 15
python bench/ext_bench.py
```

### Coverage

One-time: `make install-coverage-deps` (installs `coverage.py` + `cargo-llvm-cov`).

```bash
make coverage               # both HTML reports into coverage/
make coverage-python        # joblite + litenotify python paths
make coverage-rust          # litenotify-core Rust unit tests
```

Python coverage reflects the full joblite test suite (~92% of `packages/joblite/`). Rust coverage reflects only `cargo test` — many `joblite_ops.rs` paths (`jl_enqueue`, `jl_claim_batch`, etc.) are only exercised via the Python test suite and won't show up in the Rust report. Combined cross-language coverage is non-trivial (LLVM profile-data merging across PyO3 boundaries) and is deferred.

## License

Apache 2.0. See [LICENSE](LICENSE).
