# litenotify/joblite

litenotify/joblite gives SQLite apps Postgres-style `NOTIFY`/`LISTEN` and durable background work on a single `.db` file. Cross-process wake latency is bounded by a 1 ms stat-poll cadence — p50 ~1-2 ms on M-series — with no daemon, no broker, no shared service. The `.db-wal` file itself is the coordination primitive.

Three primitives in the box: ephemeral cross-process pub/sub (`tx.notify`), durable pub/sub with per-consumer offsets (`db.stream`), and an at-least-once work queue (`db.queue`). Jobs, events, and notifications commit atomically with your business writes because they *are* writes to the same file.

> **Experimental.** Test suite is thorough (126 Python + 8 Rust + 8 Node, real subprocesses and real uvicorn, no mocks), but the library is young, unpublished, and no wheels are on PyPI yet. The API may shift before 1.0.

SQLite has had the primitives for this: `BEGIN IMMEDIATE` for atomic claims, WAL mode for concurrent readers, and a write-ahead log that grows on every commit. What was missing was the tiny layer that turns those into push-based pub/sub across processes. This library is that layer.

Scope is **one machine, one `.db` file.** Cross-machine delivery is an application-level concern — wire it through your framework's normal transport (HTTP fan-out, Kafka, whatever fits). The framework plugins (`joblite_fastapi`, `joblite_django`) are where that glue goes.

If you run Postgres, keep Postgres — `pg_notify`, `pg-boss`, and Oban are excellent. This project explores what's possible on one SQLite file when you don't want Redis, RabbitMQ, or a second service.

Ships as a Rust workspace with a PyO3 Python binding (`litenotify`), a Node.js binding via napi-rs (`litenotify-node`), a Python package for the higher-level queue / stream / outbox primitives (`joblite`), a SQLite loadable extension (`liblitenotify_ext.dylib`/`.so`) that any SQLite client can load, and three framework plugins (`joblite_fastapi`, `joblite_django`, `joblite_flask`). Cross-language interop is end-to-end tested: `litenotify-node/test/cross_lang.js` watches a `.db-wal` from Node while a Python subprocess fires notifications against the same file. Go and Ruby bindings are on the [ROADMAP](ROADMAP.md).

## Goals

1. **SQLite-native.** All state — jobs, notifications, stream events, consumer offsets — lives in the same `.db` file as your application data. `cp` the file and you've copied the system.
2. **Transactional coupling.** `queue.enqueue(..., tx=tx)`, `stream.publish(..., tx=tx)`, `tx.notify(...)` are INSERTs inside the caller's transaction. Commit makes them visible to every process. Rollback makes them never have happened.
3. **No polling interval to tune.** No `idle_poll_ms` knob. Idle listeners park on a filesystem watch and do zero database work until something commits.
4. **No database load from idle listeners.** The wake signal is `stat(2)` on the `.db-wal` file — a filesystem syscall, not a SQL query. Idle listeners don't touch SQLite, don't compete for the write lock, don't pollute the page cache.
5. **Low-single-digit-ms cross-process wake.** Every commit grows the WAL file. A single 1 ms `stat(2)` poll thread per `Database` fans out to every listener subscribed on this DB, so wake latency is bounded by the poll cadence, not by listener count. Measured ~1.2 ms p50 / 2.4 ms p90 on M-series (`bench/wake_latency_bench.py`).
6. **Over-triggering is cheaper than under-triggering.** Any commit wakes every listener, regardless of channel. Each wasted wake costs one indexed SELECT (microseconds). We prefer waking ten listeners who don't care to missing one who does.
7. **Durable where it matters, ephemeral where it doesn't.** Queue jobs persist until ack. Stream events persist with per-consumer offsets. `notify()` is a short-term buffer you prune yourself (`db.prune_notifications`).

## Performance

Apple Silicon M-series, release build, WAL + `synchronous=NORMAL`, April 2026. Numbers from `bench/real_bench.py` (multi-process saturation) and `bench/wake_latency_bench.py` (idle cross-process wake) — no single-threaded microbench.

| Scenario | Throughput | p50 e2e |
|----------|-----------|---------|
| 4 workers + 2 enqueuers, 2k eps each | 3,900 jobs/s | 0.5 ms |
| Same, with 100,000 dead rows in history | 3,800 jobs/s | 0.5 ms |
| Batched claim+ack (`batch=128`) | 100,000 jobs/s | — |
| Stream replay (reader-pool SELECT) | 1,000,000 events/s | — |
| Enqueue (1 job / tx) | ~6–8k /s | — |
| Enqueue (100 jobs / 1 tx) | ~110k /s | — |
| **Cross-process idle-listener wake** | — | **1.2 ms p50 / 2.4 ms p90** |

The 0.5 ms "e2e" number is enqueue-to-ack under saturation — Little's Law on a ~3,900 j/s pipeline with queue depth ~2. The wake-latency row is what you actually care about for cross-process push: one commit in process A, one idle listener in process B, measure the delta. Bounded by the 1 ms stat-poll cadence.

Claim throughput stays flat as dead-row history accumulates because the partial index on pending+processing never touches dead rows. Raw Python `sqlite3` single-tx on the same file tops out around 47k/s (the WAL ceiling on this machine); our single-tx is ~3× slower due to PyO3 boundary crossings and the writer-mutex acquire — batching closes the gap.

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

`payload` can be any JSON-serializable value (dict, list, str, int, float, bool, None). It's `json.dumps`'d on the way in and `json.loads`'d on the way out — same wire format as `queue.enqueue` and `stream.publish`, and identical across all three language bindings. Notifications are rows in an internal `_litenotify_notifications` table, ordered by a monotonic id (dedup is intrinsic), delivered in commit order.

> The notifications table is NOT auto-pruned. `notify()` is fire-and-forget signaling. If you need longer replay, use `stream` instead. Call `db.prune_notifications(older_than_s=10)` or `max_keep=10000` on whatever cadence fits your app — startup, a scheduled task, a framework housekeeping hook. No magic background timer. When both arguments are passed, rows matching EITHER condition are removed (OR semantics) — typical usage is one argument at a time.

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

Every `Database` lazily spawns a single background thread that `stat(2)`s the `.db-wal` file every 1 ms. When the file's `(size, mtime)` pair changes, the thread fans out a tick to every subscribed listener's per-listener bounded channel. Each listener awaits its own queue, wakes on the tick, and runs a narrow `SELECT ... WHERE id > last_seen` against the table it cares about. That SELECT is an indexed range scan on the partial index. If it finds rows, they're yielded to the user. If not, the listener goes back to waiting.

This is the "no polling interval" story: idle listeners make zero database queries and share one `stat` syscall per millisecond (~1 μs of kernel work) across the whole `Database`, no matter how many listeners are attached. Active listeners make one indexed SELECT per wake. Nothing ever polls the database on a timer.

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

All three plugins share the same API shape: SSE endpoints, an `authorize(user, target)` hook (sync or async), a task decorator. They intentionally differ on worker lifecycle and user resolution, each matching its framework's conventions:

| Plugin | Worker lifecycle | User resolution |
|---|---|---|
| `joblite_fastapi.JobliteApp` | In-process via FastAPI `startup`/`shutdown` hooks | `user_dependency=Depends(...)` |
| `joblite_django` | CLI only (`python manage.py joblite_worker`) | `request.user` by default, or `set_user_factory(fn)` to override |
| `joblite_flask.JobliteFlask` | CLI only (`flask joblite_worker`) | `user_factory=lambda req: ...` |

**Why the divergence.** FastAPI has lifespan hooks and a single-process async runtime; putting workers in the web process is idiomatic there. Django and Flask are WSGI-first: Gunicorn/uwsgi fork the app across many worker processes, and you don't want a joblite worker pool in *each* fork. A dedicated worker process (CLI) is the right shape for those — same pattern as Celery/RQ workers.

If `authorize` raises, all three return HTTP 500 and never open the SSE stream. Plugins are thin (~200-400 lines each): `joblite_fastapi/`, `joblite_django/`, `joblite_flask/`.

## Tests and bench

```bash
# Rust (shared core: writer/readers pool, shared WAL watcher,
# notify attach, bootstrap schema, stat poll)
cargo test -p litenotify-core

# Python (queue, stream, outbox, listener, fastapi, django, flask,
# extension interop, cross-process wake latency)
pytest tests/                            # 126 tests

# Node (basic ops, payload round-trip, cross-language Python→Node
# wake, thread-leak regression)
cd litenotify-node && npm test           # 8 tests

# Cross-process wake-latency microbench (idle listener in a
# subprocess, p50/p90/p99 from N samples)
python bench/wake_latency_bench.py --samples 500

# Realistic cross-process concurrent throughput bench
python bench/real_bench.py --workers 4 --enqueuers 2 --seconds 15

# Raw-SQL engine ceiling via the loadable extension
python bench/ext_bench.py
```

## License

Apache 2.0. See [LICENSE](LICENSE).
