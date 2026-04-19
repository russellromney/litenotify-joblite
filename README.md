# litenotify/joblite

litenotify is a SQLite library that delivers Postgres-style `NOTIFY`/`LISTEN` across processes on a single `.db` file, with cross-process wake latency of ~1.2ms p50 / 2.4ms p90 on M-series. The `.db-wal` file is the coordination primitive; a 1ms `stat(2)` poll thread per process catches commits and wakes subscribed listeners. No daemon, no broker, no service.

joblite builds three primitives on litenotify: ephemeral cross-process pub/sub (`tx.notify`), durable pub/sub with per-consumer offsets (`db.stream`), and an at-least-once work queue (`db.queue`). Each is an INSERT in the caller's transaction, so commits are atomic with business writes and rollbacks drop jobs/events/notifications automatically.

> litenotify/joblite is **experimental**. It is new and may contain bugs. API may shift before 1.0.

SQLite in WAL mode appends to the `.db-wal` file on every commit. turbopuffer and [Oban](https://hexdocs.pm/oban/) taught us that you can architect around a single constraint if you understand it well. This library does that for SQLite's WAL file: anything that commits bumps the file's `(size, mtime)`, so a `stat(2)` loop is enough to push notifications cross-process at single-digit-ms latency. The mechanism is identical on Linux, macOS, and Windows; we use stat-polling instead of `inotify`/`kqueue`/`FSEvents` because macOS FSEvents silently drops same-process writes.

litenotify/joblite ships as a Rust workspace with:
- A Python binding via [PyO3](https://pyo3.rs)
- A Node.js binding via [napi-rs](https://napi.rs)
- A [SQLite loadable extension](#sqlite-loadable-extension) (`.so`/`.dylib`) that any SQLite 3.9+ client can load
- Framework plugins: `joblite_fastapi`, `joblite_django`, `joblite_flask`

Cross-language interop is tested: a Python process writes notifications, a Node process reads them via `walEvents` + `SELECT` against the same file.

If you run Postgres, use Postgres. [`pg_notify`](https://www.postgresql.org/docs/current/sql-notify.html), [pg-boss](https://github.com/timgit/pg-boss), and [Oban](https://hexdocs.pm/oban/) are mature and excellent. litenotify/joblite is for SQLite-native apps that want the same shape without adding a service.

If you want to contribute or find a bug, open an issue or PR.

## Performance

Apple Silicon M-series, release build, WAL + `synchronous=NORMAL`. Bench: `bench/real_bench.py` (multi-process saturation) and `bench/wake_latency_bench.py` (idle cross-process wake).

| Scenario | Throughput | p50 e2e |
|----------|-----------|---------|
| 4 workers + 2 enqueuers @ 2k eps each | 3,900 jobs/s | 0.5 ms |
| Same, with 100,000 dead rows in history | 3,800 jobs/s | 0.5 ms |
| Batched claim+ack (batch=128) | 100,000 jobs/s | — |
| Stream replay (reader-pool SELECT) | 1,000,000 events/s | — |
| Enqueue (1 job / tx) | 6–8k /s | — |
| Enqueue (100 jobs / 1 tx) | 110k /s | — |
| Cross-process idle-listener wake | — | **1.2 ms p50 / 2.4 ms p90** |

The "e2e" column is enqueue-to-ack under saturation, which is dominated by pipeline depth (Little's Law on a 3,900 j/s pipeline with queue depth ~2). The cross-process wake row is the meaningful latency number for pub/sub: one commit in process A, one idle listener in process B, measure the delta. Bounded by the 1ms stat-poll cadence.

Claim throughput stays flat as the dead-row table grows because the partial claim index never touches dead rows. Raw Python `sqlite3` single-tx tops out ~47k/s on this machine (the WAL ceiling); litenotify runs ~14k/s through the PyO3 boundary for single-write transactions — 3× slower — and batched inserts close the gap.

### Wake-latency vs alternatives

| System | Wake latency | Deploy overhead | Atomic with app writes |
|--------|--------------|-----------------|------------------------|
| litenotify (this) | 1.2 ms p50 cross-proc | zero services | yes |
| [Redis pub/sub](https://redis.io/docs/latest/develop/pubsub/) | sub-ms cross-proc | Redis service | no |
| [pg_notify](https://www.postgresql.org/docs/current/sql-notify.html) | 5–20 ms cross-proc | Postgres service | yes (inside PG tx) |
| Kafka | single-digit ms | cluster + KRaft/ZK + schema registry | no |
| Polling (1s) | up to 1s | none | via query at poll time |

Redis is faster but non-durable and separate from your app's commits. pg_notify is transactional but requires Postgres. Kafka is overkill for one-machine apps. Polling adds per-poll read load and cold latency. litenotify is the only entry in this matrix with zero extra services AND transactional coupling.

## Quick start

### Python

```bash
git clone https://github.com/russellromney/litenotify-joblite && cd litenotify-joblite
uv venv && source .venv/bin/activate
cd litenotify && maturin develop --uv && cd ..
uv pip install fastapi uvicorn django pytest pytest-asyncio pytest-xdist pytest-django
```

```python
import joblite

db = joblite.open("app.db")
emails = db.queue("emails")

# Enqueue atomically with your business write. Rollback drops the job.
with db.transaction() as tx:
    tx.execute("INSERT INTO orders (user_id) VALUES (?)", [42])
    emails.enqueue({"to": "alice@example.com"}, tx=tx)

# Worker process:
async for job in emails.claim("worker-1"):
    try:
        send(job.payload)
        job.ack()
    except Exception as e:
        job.retry(delay_s=60, error=str(e))
```

`claim()` is an async iterator that pipelines ack-of-previous with claim-of-next in a single transaction, wakes on WAL commit from any process, and falls back to a 5s poll only if the WAL watcher somehow fails. Default batch size 32, default visibility timeout 300s.

#### Stream (durable pub/sub)

```python
stream = db.stream("user-events")

with db.transaction() as tx:
    tx.execute("UPDATE users SET name=? WHERE id=?", [name, uid])
    stream.publish({"user_id": uid, "change": "name"}, tx=tx)

async for event in stream.subscribe(consumer="dashboard"):
    await push_to_browser(event)
```

Each named consumer tracks its own offset in the same database. `subscribe` replays rows past the saved offset, then transitions to live delivery on WAL wake.

#### Notify (ephemeral cross-process pub/sub)

```python
async for n in db.listen("orders"):
    print(n.channel, n.payload)

with db.transaction() as tx:
    tx.execute("INSERT INTO orders (id, total) VALUES (?, ?)", [42, 99.99])
    tx.notify("orders", {"id": 42})
```

Listeners start at `MAX(id)` on the channel at attach time, so reconnecting listeners don't replay history. Use `db.stream()` if you need durable replay.

Payloads are any JSON-serializable value. The wire format is `json.dumps(payload)` on write and `json.loads(raw)` on read, identical across Python and Node, so a Python writer and Node reader can share a channel. The notifications table is not auto-pruned; call `db.prune_notifications(older_than_s=10)` or `max_keep=10000` from a scheduled task or startup hook.

### Node.js

```bash
cd litenotify-node && npm install && npm run build
```

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
    'SELECT id, payload FROM _litenotify_notifications WHERE id > ? ORDER BY id',
    [last]
  );
  for (const r of rows) { handle(JSON.parse(r.payload)); last = r.id; }
}
```

A `joblite-node` TypeScript port that wraps `@litenotify/node` with Queue/Stream/Outbox is on the [ROADMAP](ROADMAP.md).

### SQLite loadable extension

Any SQLite 3.9+ client can load the extension and get raw SQL access:

```sql
.load ./liblitenotify_ext
SELECT jl_bootstrap();                                  -- idempotent schema
INSERT INTO _joblite_live (queue, payload) VALUES ('emails', '{"to":"alice"}');

SELECT jl_claim_batch('emails', 'worker-1', 32, 300);   -- JSON of claimed rows
SELECT jl_ack_batch('[1,2,3]', 'worker-1');             -- DELETEs, returns count

SELECT notify('orders', '{"id":42}');                   -- also installs notify() +
                                                        -- _litenotify_notifications
```

The extension shares `_joblite_live`, `_joblite_dead`, and `_litenotify_notifications` with the Python binding. A Python worker can claim jobs any other language (Go, Rust, shell) pushed via the extension. Schema compatibility is enforced by `tests/test_extension_interop.py`.

## Design goals

**SQLite-native.** Jobs, notifications, stream events, and consumer offsets all live in the same `.db` file as your application data. `cp` copies the system. No separate broker, no separate message store, no separate durable log.

**Transactional coupling by default.** `queue.enqueue(..., tx=tx)`, `stream.publish(..., tx=tx)`, and `tx.notify(...)` are INSERTs inside the caller's transaction. Commit makes them visible to every reader in every process atomically with the business write. Rollback makes them never have happened. The transactional-outbox pattern is not a library feature to opt into, it's the shape of the API.

**No polling interval to tune.** There is no `idle_poll_ms` knob. Idle listeners park on a stat-polled WAL file watch and do zero database queries. Wake fires when SQLite commits, not on a timer. The only "poll" knob in the library is a 5-second paranoia fallback that fires if the stat watcher somehow can't be established (sandboxed filesystems, odd container mounts).

**No database load from idle listeners.** The wake signal is `stat(2)` on the `.db-wal` file, not a SQL query. Idle listeners don't touch SQLite, don't compete for the write lock, don't warm the page cache. A hundred idle listeners cost the same kernel work as one.

**Over-triggering is cheaper than under-triggering.** A WAL change wakes every subscriber on that `Database` regardless of which channel committed. Each wasted wake is one indexed SELECT against the partial index (microseconds). We prefer waking ten listeners that don't care to missing one that does.

**Durable where it matters, ephemeral where it doesn't.** Queue jobs persist until ack, with a separate `_joblite_dead` table for retry-exhausted rows. Stream events persist and each named consumer tracks its own offset. `notify()` is fire-and-forget and not auto-pruned; call `db.prune_notifications()` from a scheduled task or startup hook when you want to trim. Users get to decide the retention policy per primitive instead of inheriting a library default.

**Cross-platform via `stat`, not `inotify`/`kqueue`/`FSEvents`.** macOS FSEvents silently drops events for same-process writes, which means an enqueuer and a listener in the same Python process would never see each other. Stat-polling is portable, works identically at ~1ms granularity everywhere, and costs a single syscall per millisecond. We traded ~0.5ms of latency against better kernel notifications for portability and correctness.

## Design

litenotify adds a tiny coordination layer on top of SQLite's write-ahead log. Every decision flows from SQLite's model:

| SQLite property | Implication |
|-----------------|-------------|
| **One file, state moves with the file** | All coordination lives in `app.db` + `app.db-wal`; `cp` the file and you've copied the system. |
| **WAL mode: one writer, concurrent readers** | Single writer connection per process; claim is `UPDATE ... RETURNING` via a partial index. |
| **WAL file grows on every commit** | `(size, mtime)` is the cross-process commit signal. |
| **SQLite has no wire protocol** | Listeners initiate reads; server-push is impossible, so the wake signal triggers a narrow SELECT. |
| **Transactions are cheap** | Jobs, events, and notifications are just rows inside your own `with db.transaction()` block. |

### Wake path

Every `Database` lazily spawns a single background thread that `stat`s the `.db-wal` file every millisecond. On any `(size, mtime)` change, the thread fans out a tick to every subscribed listener's bounded channel. Each listener's `__anext__` wakes, runs `SELECT ... WHERE id > last_seen` against a partial index, yields rows, and returns to waiting.

A process with 100 subscribers uses one stat thread, not 100. Idle listeners run zero database queries.

`inotify`/`kqueue`/`FSEvents` via the [`notify` crate](https://crates.io/crates/notify) was evaluated and rejected. FSEvents on macOS silently drops events for same-process writes, so a listener and an enqueuer in the same Python process would never see each other. Stat-polling works identically on every platform at ~1ms granularity for negligible CPU cost. We traded ~0.5 ms of latency for portability and correctness.

### Queue schema

`_joblite_live` holds pending and processing jobs. A partial index on `(queue, priority DESC, run_at, id) WHERE state IN ('pending','processing')` keeps the claim hot path small regardless of history size. Claim is one `UPDATE ... RETURNING` via that index. Ack is one `DELETE`. Retry-exhausted rows move to `_joblite_dead`, which the claim path never scans.

`async for job in queue.claim("w")` yields jobs one at a time but runs ack-of-previous + claim-of-next in a single transaction per batch (default 32). `Job.ack()` on iterator-owned jobs defers into a pending list that flushes on the next claim. Worker loops get batched speed without changing application code.

### Transactional coupling

Every primitive takes a `tx=tx` argument. `queue.enqueue(..., tx=tx)`, `stream.publish(..., tx=tx)`, and `tx.notify(...)` are INSERTs that join the caller's transaction. Commit makes them visible to every reader in every process. Rollback makes them never have happened. The transactional-outbox pattern is the default shape of the API.

### Shared WAL watcher

`litenotify_core::SharedWalWatcher` owns one stat-poll thread per `.db-wal` path and fans out to N subscribers via bounded `SyncSender<()>` channels keyed by subscriber id. Each `db.wal_events()` call registers a subscriber and returns a handle whose `Drop` auto-unsubscribes (and disconnects the channel, so the bridge thread exits cleanly on `rx.recv() -> Err`). 100 listeners in one process = 1 stat thread + 100 lightweight receivers.

## Current limitations

- **Single machine, single writer.** Two servers writing to the same `.db` file over NFS will corrupt it. SQLite's locking is designed for a single host. Shard by file (queue-per-tenant, queue-per-database) or switch to Postgres if you need cross-host writers.
- **Cross-machine delivery is the application's job.** The web handler writes to SQLite atomically, then fans out to HTTP / Kafka / NATS / whatever transport the other machines subscribe to. Framework plugins are where that glue goes today; built-in fan-out adapters are on the [ROADMAP](ROADMAP.md).
- **Not a workflow orchestrator.** No DAGs, compensation, or human-in-the-loop. Use [Temporal](https://temporal.io), [Hatchet](https://hatchet.run), or [Inngest](https://inngest.com) for those.
- **No wheels on PyPI, no CI yet.** Build from source (see [Quick start](#quick-start)). GitHub Actions wheels for PyPI + npm prebuilds for the Node binding are on the [ROADMAP](ROADMAP.md).
- **Windows is untested.** The stat-polling mechanism should work (Windows `stat` returns mtime the same way), but we don't run CI there yet. Linux and macOS are exercised by the tests.

## Crash recovery

- **Rollback drops everything.** `raise` inside a `with db.transaction() as tx` block rolls the whole transaction back. No half-delivered notification, no orphaned job — SQLite's ACID guarantees handle it.
- **SIGKILL mid-transaction is safe.** WAL rollback on next open leaves no stale state. `tests/test_crash_recovery.py` spawns a subprocess, kills it before COMMIT, verifies `PRAGMA integrity_check == 'ok'`, confirms no ghost rows, and checks fresh notifies still flow.
- **Visibility timeouts reclaim dropped work.** A worker crashed mid-handler leaves its claim on the row. After `visibility_timeout_s` (default 300s) another worker reclaims and `attempts` increments. After `max_attempts` (default 3), the row moves to `_joblite_dead`.
- **Listeners miss events pruned while offline.** Listeners attach at `MAX(id)` and skip pruned events. For durable replay, use `db.stream()`, which tracks consumer offsets.

## Framework plugins

Three plugins, same API shape: SSE endpoints for subscribing to channels/streams, an `authorize(user, target)` hook (sync or async), a task decorator. They diverge on worker lifecycle and user resolution because the frameworks do.

| Plugin | Worker lifecycle | User resolution |
|---|---|---|
| `joblite_fastapi.JobliteApp` | In-process via FastAPI `startup`/`shutdown` hooks | `user_dependency=Depends(...)` |
| `joblite_django` | CLI only (`python manage.py joblite_worker`) | `request.user` by default, or `set_user_factory(fn)` |
| `joblite_flask.JobliteFlask` | CLI only (`flask joblite_worker`) | `user_factory=lambda req: ...` |

FastAPI has lifespan hooks and a single-process async runtime, so workers run in the same process. Django and Flask are WSGI-first: Gunicorn forks the app across N workers, and you don't want a joblite worker pool in each fork. A dedicated worker process is the right shape, same as [Celery](https://docs.celeryq.dev/) and [RQ](https://python-rq.org/).

### FastAPI

```python
from fastapi import FastAPI
import joblite
from joblite_fastapi import JobliteApp

app = FastAPI()
db = joblite.open("app.db")
jl = JobliteApp(app, db, authorize=lambda user, target: True)

@jl.task("emails", concurrency=4)
async def send_email(payload):
    await mailer.send(payload["to"])

@app.post("/orders")
async def create_order(order: dict):
    with db.transaction() as tx:
        tx.execute("INSERT INTO orders (user_id) VALUES (?)", [order["user_id"]])
        db.queue("emails").enqueue({"to": order["email"]}, tx=tx)
    return {"ok": True}

# GET /joblite/subscribe/<channel>  SSE stream of notifications
# GET /joblite/stream/<name>        SSE stream of durable events
```

### Django

```python
# settings.py
INSTALLED_APPS = [..., "joblite_django"]
JOBLITE_DB_PATH = BASE_DIR / "app.db"

# tasks.py
import joblite_django

@joblite_django.task("emails", concurrency=4)
async def send_email(payload):
    await mailer.send(payload["to"])

# urls.py
from joblite_django.views import stream_sse, subscribe_sse
urlpatterns = [
    path("joblite/subscribe/<str:channel>", subscribe_sse),
    path("joblite/stream/<str:name>", stream_sse),
]

# workers:
#   python manage.py joblite_worker
```

### Flask

```python
from flask import Flask
import joblite
from joblite_flask import JobliteFlask

app = Flask(__name__)
db = joblite.open("app.db")
jl = JobliteFlask(app, db, user_factory=lambda req: req.headers.get("X-User"))

@jl.task("emails", concurrency=4)
async def send_email(payload):
    await mailer.send(payload["to"])

# workers:
#   flask --app app joblite_worker
```

If `authorize` raises, all three return HTTP 500 without opening the SSE stream. Plugins are 200–400 lines each.

## Related projects

### Queue systems on Postgres

- [**pg-boss**](https://github.com/timgit/pg-boss) (Node): durable jobs on Postgres with cron, rate limiting, and dead-letter. Same role as joblite for Postgres-backed apps.
- [**Oban**](https://hexdocs.pm/oban/) (Elixir): the gold standard for Postgres job queues. Workflows, rate limiting, metrics, web UI.
- [**graphile-worker**](https://github.com/graphile/worker) (Node): high-performance Postgres queue using `LISTEN`/`NOTIFY` for wake. Deletes on ack, same as joblite.

### Queue systems on Redis

- [**Sidekiq**](https://sidekiq.org/) (Ruby), [**RQ**](https://python-rq.org/) (Python), [**Dramatiq**](https://dramatiq.io/) (Python): mature, Redis-backed, not transactional with app writes.
- [**Celery**](https://docs.celeryq.dev/): the reference point. Broker-agnostic, heavy, task-oriented.

### SQLite-based queues

- [**litequeue**](https://github.com/litements/litequeue): SQLite queue using a `state='DONE'` column, which accumulates rows in the live table forever (we explicitly fixed this in our extension). No pub/sub or cross-process wake primitive.
- [**huey**](https://huey.readthedocs.io) with SQLite backend: simple task queue; the SQLite backend polls.
- [**persist-queue**](https://github.com/peter-wangxu/persist-queue): thread-safe local queue, single-process.

### Pub/sub

- [**pg_notify**](https://www.postgresql.org/docs/current/sql-notify.html): the direct inspiration. Cross-process atomic-with-transaction pub/sub, but requires Postgres.
- [**NATS**](https://nats.io), [**Redis pub/sub**](https://redis.io/docs/latest/develop/pubsub/): separate services; great fan-out, no transactional coupling with app writes.

### SQLite replication

- [**Litestream**](https://github.com/benbjohnson/litestream): continuous WAL shipping to S3. Complementary to litenotify; operates at a different layer.
- [**LiteFS**](https://github.com/superfly/litefs): FUSE-based primary/replica. Different problem (availability).

### Workflow orchestrators

- [**Temporal**](https://temporal.io/), [**Hatchet**](https://hatchet.run/), [**Inngest**](https://inngest.com/): DAGs, compensation, human-in-the-loop. Different problem (durable workflows, not pub/sub primitives).

### Where litenotify differs

| | litenotify | pg-boss / Oban | Sidekiq / Celery | litequeue | NATS / Redis pub/sub |
|---|---|---|---|---|---|
| Transactional with app writes | yes (same SQLite tx) | yes (inside PG tx) | no | no | no |
| External service required | none | Postgres | Redis | none | NATS / Redis |
| Cross-process wake mechanism | WAL `stat(2)` poll | PG `LISTEN`/`NOTIFY` | Redis BRPOP / pub/sub | polling | native pub/sub |
| Cross-process wake latency | ~1 ms p50 | 5–20 ms | sub-ms | poll-interval bound | sub-ms |
| Durable pub/sub with offsets | yes (`db.stream`) | yes | no (separate streams) | no | no (Redis streams has it) |
| Framework plugins | FastAPI / Django / Flask | various | various | none | none |

## Tests

```bash
# Rust: shared core (writer/readers pool, shared WAL watcher,
# notify attach, bootstrap schema, stat poll)
cargo test -p litenotify-core

# Python: queue, stream, outbox, listener, fastapi, django, flask,
# extension interop, cross-process wake latency
pytest tests/                            # 126 tests

# Node: basic ops, payload round-trip, cross-language Python->Node wake,
# thread-leak regression
cd litenotify-node && npm test           # 8 tests
```

## Bench

```bash
# Cross-process wake-latency microbench: idle listener in a subprocess,
# p50/p90/p99 from N samples
python bench/wake_latency_bench.py --samples 500

# Realistic cross-process concurrent throughput bench
python bench/real_bench.py --workers 4 --enqueuers 2 --seconds 15

# Raw-SQL engine ceiling via the loadable extension
python bench/ext_bench.py
```

## License

Apache 2.0. See [LICENSE](LICENSE).
