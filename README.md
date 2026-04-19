# litenotify/joblite

litenotify/joblite gives SQLite apps Postgres-style `NOTIFY`/`LISTEN` and durable background work on a single `.db` file. Cross-process wake latency measured at **0.5 ms p50** with no daemon, no broker, no shared service — the `.db-wal` file itself is the coordination primitive.

Three primitives in the box: ephemeral cross-process pub/sub (`tx.notify`), durable pub/sub with per-consumer offsets (`db.stream`), and an at-least-once work queue (`db.queue`). Jobs, events, and notifications commit atomically with your business writes because they *are* writes to the same file.

> **Experimental.** Test suite is thorough (12 Rust + 112 Python, real subprocesses and real uvicorn, no mocks), but the library is young, unpublished, and no wheels are on PyPI yet. The API may shift before 1.0.

SQLite has had the primitives for this: `BEGIN IMMEDIATE` for atomic claims, WAL mode for concurrent readers, and a write-ahead log that grows on every commit. What was missing was the tiny layer that turns those into push-based pub/sub across processes. This library is that layer.

Scope is **one machine, one `.db` file.** Cross-machine delivery is an application-level concern — wire it through your framework's normal transport (HTTP fan-out, Kafka, whatever fits). The framework plugins (`joblite_fastapi`, `joblite_django`) are where that glue goes.

If you run Postgres, keep Postgres — `pg_notify`, `pg-boss`, and Oban are excellent. This project explores what's possible on one SQLite file when you don't want Redis, RabbitMQ, or a second service.

Ships as a Rust crate (`litenotify`, with PyO3 bindings), a Python package (`joblite`), a SQLite loadable extension (`liblitenotify_ext.dylib`/`.so`), and two framework plugins (`joblite_fastapi`, `joblite_django`). Node, Go, and Ruby bindings are on the [ROADMAP](ROADMAP.md).

## Goals

1. **SQLite-native.** All state — jobs, notifications, stream events, consumer offsets — lives in the same `.db` file as your application data. `cp` the file and you've copied the system.
2. **Transactional coupling.** `queue.enqueue(..., tx=tx)`, `stream.publish(..., tx=tx)`, `tx.notify(...)` are INSERTs inside the caller's transaction. Commit makes them visible to every process. Rollback makes them never have happened.
3. **No polling interval to tune.** No `idle_poll_ms` knob. Idle listeners park on a filesystem watch and do zero database work until something commits.
4. **No database load from idle listeners.** The wake signal is `stat(2)` on the `.db-wal` file — a filesystem syscall, not a SQL query. Idle listeners don't touch SQLite, don't compete for the write lock, don't pollute the page cache.
5. **Sub-millisecond cross-process delivery.** Every commit grows the WAL file. A 1 ms `stat` loop per listener catches the change and wakes it. Measured: 0.5 ms p50 under realistic multi-process load.
6. **Over-triggering is cheaper than under-triggering.** Any commit wakes every listener, regardless of channel. Each wasted wake costs one indexed SELECT (microseconds). We prefer waking ten listeners who don't care to missing one who does.
7. **Durable where it matters, ephemeral where it doesn't.** Queue jobs persist until ack. Stream events persist with per-consumer offsets. `notify()` is a short-term buffer you prune yourself (`db.prune_notifications`).

## Performance

Apple Silicon M-series, release build, WAL + `synchronous=NORMAL`, April 2026. Numbers from `bench/real_bench.py` — multiple worker subprocesses and enqueuer subprocesses running concurrent load, not a single-threaded microbench.

| Scenario | Throughput | p50 latency |
|----------|-----------|-------------|
| 4 workers + 2 enqueuers, 2k eps each | 3,900 jobs/s | 0.5 ms |
| Same, with 100,000 dead rows in history | 3,800 jobs/s | 0.5 ms |
| Batched claim+ack (`batch=128`) | 100,000 jobs/s | — |
| Stream replay (reader-pool SELECT) | 1,000,000 events/s | — |
| Enqueue (1 job / tx) | ~6–8k /s | — |
| Enqueue (100 jobs / 1 tx) | ~110k /s | — |

Claim throughput stays flat as dead-row history accumulates because the partial index on pending+processing never touches dead rows. Raw Python `sqlite3` single-tx on the same file tops out around 47k/s (the WAL ceiling on this machine); our single-tx is ~4× slower due to PyO3 boundary crossings and the writer-mutex acquire — batching closes the gap.

Redis `LPUSH`/`BRPOP` clears ~100k/s on localhost but lives in a separate process, doesn't commit atomically with your business writes, and doesn't survive a Redis restart. `pg_notify` is fast but requires Postgres. This project trades peak single-tx throughput for transactional coupling, one-file deployment, and cross-process push without a daemon.

## Quick start

```bash
git clone https://github.com/russellromney/litenotify-joblite && cd litenotify-joblite
uv venv && source .venv/bin/activate
cd litenotify && maturin develop --uv && cd ..
uv pip install fastapi uvicorn django pytest pytest-asyncio pytest-xdist pytest-django
```

### Queue: durable at-least-once work

```python
import joblite

db = joblite.open("app.db")
emails = db.queue("emails")

# Enqueue atomically with your business write. Rollback drops the job too.
with db.transaction() as tx:
    tx.execute("INSERT INTO orders (user_id) VALUES (?)", [42])
    emails.enqueue({"to": "alice@example.com"}, tx=tx)

# In a worker process:
async for job in emails.claim("worker-1"):
    try:
        send(job.payload)
        job.ack()
    except Exception as e:
        job.retry(delay_s=60, error=str(e))
```

`claim()` is an async iterator that pipelines ack-of-previous with claim-of-next into a single transaction, wakes on commit to any process, and falls back to a 5 s paranoia poll only if the filesystem watch can't be established.

### Stream: durable pub/sub with consumer offsets

```python
stream = db.stream("user-events")

# Publish transactionally.
with db.transaction() as tx:
    tx.execute("UPDATE users SET name=? WHERE id=?", [name, uid])
    stream.publish({"user_id": uid, "change": "name"}, tx=tx)

# Late-joining consumer — replay from their last seen offset, then live.
async for event in stream.subscribe(consumer="dashboard"):
    await push_to_browser(event)
```

Each named consumer tracks its own offset. The subscribe iterator replays rows with `offset > saved_offset`, then transitions to live delivery on WAL wake.

### Notify: ephemeral cross-process pub/sub

```python
# Subscribe (starts at current MAX(id) — no historical replay)
async for n in db.listen("orders"):
    print(n.channel, n.payload)

# Publish from anywhere that opens the file (same process or a different one)
with db.transaction() as tx:
    tx.execute("INSERT INTO orders (id, total) VALUES (?, ?)", [42, 99.99])
    tx.notify("orders", {"id": 42})
```

Notifications are rows in an internal `_litenotify_notifications` table. They're ordered by a monotonic id (dedup is intrinsic) and delivered in commit order.

> The notifications table is not auto-pruned. `notify()` is for fire-and-forget signaling; if you need longer replay, use `stream` instead. Call `db.prune_notifications(older_than_s=10)` on a cadence that fits your app — startup, scheduled task, framework housekeeping hook. We don't hide a magic timer in the SQL path.

## Design

| SQLite property | What we do with it |
|------------------|--------------------|
| Single file; state moves with the file | All coordination lives in `app.db` + `app.db-wal` |
| WAL mode: one writer, concurrent readers | One writer connection per process; atomic claim is `UPDATE ... RETURNING` via a partial index |
| WAL file grows on every commit | Its mtime is the cross-process commit signal |
| SQLite has no wire protocol | No server push; listeners initiate reads, driven by the WAL signal |
| Commit hooks run only in the writing process | Dropped them — everything goes through the WAL-file signal instead |
| Transactions are cheap | Jobs, notifications, and stream events are just rows in your tx |

### How listeners wake

Every process that opens the database spawns a background thread that `stat(2)`s the `.db-wal` file every 1 ms. When the file's `(size, mtime)` pair changes, the thread pushes a tick onto an asyncio Queue and goes back to waiting. Each listener awaits its own queue, wakes on the tick, and runs a narrow `SELECT ... WHERE id > last_seen` against the table it cares about. That SELECT is an indexed range scan, 1–20 μs. If it finds rows, they're yielded to the user. If not, the listener goes back to waiting.

This is the "no polling interval" story: idle listeners make one `stat` syscall per millisecond (~1 μs of kernel work) and zero database queries. Active listeners make one indexed SELECT per wake. Nothing ever polls the database on a timer.

> We considered `inotify` / `kqueue` / `FSEvents` via the `notify` crate. FSEvents on macOS silently drops events for same-process writes, which means a listener and an enqueuer in the same Python process would never see each other. A stat-polling thread works identically on every platform at ~1 ms granularity, with negligible CPU cost. We traded ~0.5 ms of latency for portability and correctness.

### Transactional coupling

Every primitive (`queue.enqueue`, `stream.publish`, `tx.notify`) accepts a `tx=tx` argument. When passed, the INSERT joins the caller's transaction. Commit makes the job/event/notification visible to every reader in every process. Rollback makes it never have happened. The transactional-outbox pattern is the default, not an advanced trick.

### Queue schema and claim

`_joblite_live` holds pending and processing jobs. A partial index on `(queue, priority DESC, run_at, id) WHERE state IN ('pending','processing')` keeps the claim hot path indexed. Claim is one `UPDATE ... RETURNING` via that index — state transitions `pending → processing` stay inside the partial predicate, so the B-tree doesn't reshuffle. Ack is one `DELETE` (no `'done'` state remains anywhere). Retry-exhausted and fail rows move to a separate `_joblite_dead` table that's never scanned by the claim path — so dead-row history never slows claims down.

### Async iterator pipelining

`async for job in queue.claim("w"):` yields jobs one at a time but runs ack-of-previous + claim-of-next in a single transaction per batch (default `batch_size=32`). `Job.ack()` on iterator-owned jobs defers into a pending list that's flushed on the next claim. Worker loops get the batched speed without changing application code.

## Where it's a fit

- Single-box web apps that want durable background jobs without adding Redis.
- SSE or WebSocket servers that need to push DB-driven events to connected clients, with `Last-Event-ID` reconnect.
- CLIs and desktop apps that want transactional side effects (webhooks, emails, third-party API calls) that survive a crash.
- Multi-process workers on the same machine (web + `joblite_worker` processes sharing one `.db` file).

## Where it isn't

- **Not multi-machine.** Two servers writing to the same `.db` file over NFS will corrupt it — SQLite's locking is designed for a single host. If you need to scale past one host, shard by file (queue-per-tenant, queue-per-database) and route at the application layer, or switch to Postgres.
- **Not a distributed pub/sub.** Cross-machine delivery is the application's job: your web handler writes to SQLite, then posts to whatever mechanism (HTTP, Kafka, NATS) your other machines subscribe to. Framework plugins are where that glue belongs, and future work will add built-in adapters.
- **Not a workflow orchestrator.** No DAGs, no compensation, no human-in-the-loop. For those, use Temporal, Hatchet, or Inngest.

## Framework plugins

`joblite_fastapi.JobliteApp(app, db)` registers SSE endpoints, a worker pool, and an `authorize(user, target)` hook.
`joblite_django.task("emails")` + `python manage.py joblite_worker` gives Django workers on the same shape. Async views expose `/joblite/subscribe/{channel}` and `/joblite/stream/{name}`.

Both plugins are thin. See `joblite_fastapi/joblite_fastapi.py` and `joblite_django/views.py` for the ~400-line total wiring.

## Tests and bench

```bash
cargo test -p litenotify                  # 12 Rust tests (commit-hook semantics, rollback, etc.)
pytest tests/                             # 112 Python tests, ~7–13s parallel
python bench/real_bench.py --workers 4 --enqueuers 2 --seconds 15
python bench/ext_bench.py                 # raw-SQL ceiling via the loadable extension
```

## License

Apache 2.0. See [LICENSE](LICENSE).
