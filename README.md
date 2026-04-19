# litenotify/joblite

You've got a SQLite-backed app. You need background jobs, or pub/sub to connected clients, or a way for another process to find out when something just committed. The usual next step is to add [Redis](https://redis.io/docs/latest/develop/pubsub/) or [Postgres](https://www.postgresql.org/docs/current/sql-notify.html), and now you own a second service to run, monitor, and back up. But your entire application state is already sitting in one `.db` file. What if it did the pub/sub and the queue too?

It mostly can, and this library is the thin layer that gets you there.

SQLite in WAL mode appends to the `.db-wal` file every time you commit. That file is sitting right there on the filesystem, visible to every process that opened the database. If you `stat(2)` the WAL and the `(size, mtime)` pair changed since the last check, something committed. One syscall, a microsecond of kernel work, zero database queries. Every `Database` spawns a single background thread that does this once a millisecond, fans out to every subscribed listener in the process, and goes back to waiting. The WAL file works like a shared bulletin board: anyone in any process can write on it by committing, and every listener watching the file sees the change within about a millisecond.

Given that wake signal, a single table pattern gives you three useful shapes. The smallest is `notify(channel, payload)`: INSERT a row in a notifications table inside your transaction, and listeners on that channel wake and SELECT what's new. Keep the rows and let each consumer track its own offset, and it's a durable stream. Add a partial claim index and a visibility timeout, and it's an at-least-once work queue with retries and a dead-letter table. Each one is an INSERT in your own transaction, so the transactional-outbox pattern comes for free: if the business write rolls back, the notification and the job roll back with it.

On an M-series laptop, the cross-process wake from one commit to an idle listener in a different process runs about 1.2 ms at the median, bounded by the 1 ms stat-poll cadence rather than by listener count. A process with a hundred active subscribers uses one stat thread. If you already run Postgres, keep Postgres. [`pg_notify`](https://www.postgresql.org/docs/current/sql-notify.html), [pg-boss](https://github.com/timgit/pg-boss), and [Oban](https://hexdocs.pm/oban/) are excellent, and "let's add Postgres" is a small step for an app that already runs it. This library is for SQLite-native apps that want the same shape without a second service.

The library ships as a Rust workspace. There's a Python binding via [PyO3](https://pyo3.rs), a Node.js binding via [napi-rs](https://napi.rs), a SQLite loadable extension any SQLite 3.9+ client can load, and framework plugins for FastAPI, Django, and Flask. Tests run against real subprocesses, a real uvicorn server, and a real Node process reading notifications written by Python against the same file: 126 Python + 8 Rust + 8 Node, no mocks.

It's early. The library is young and unpublished, no wheels on PyPI yet, and the API may still shift before 1.0.

## Quick start

```bash
git clone https://github.com/russellromney/litenotify-joblite && cd litenotify-joblite
uv venv && source .venv/bin/activate
cd litenotify && maturin develop --uv && cd ..
uv pip install fastapi uvicorn django pytest pytest-asyncio pytest-xdist pytest-django
```

For the Node binding:

```bash
cd litenotify-node && npm install && npm run build
```

### Queue

Your web handler takes an order and wants to send an email. You want the email send to happen atomically with the order write, so that if the order fails for some reason, you don't also send the email. Enqueue the job inside the same transaction:

```python
import joblite

db = joblite.open("app.db")
emails = db.queue("emails")

with db.transaction() as tx:
    tx.execute("INSERT INTO orders (user_id) VALUES (?)", [42])
    emails.enqueue({"to": "alice@example.com"}, tx=tx)
```

Somewhere else, a worker drains the queue:

```python
async for job in emails.claim("worker-1"):
    try:
        send(job.payload)
        job.ack()
    except Exception as e:
        job.retry(delay_s=60, error=str(e))
```

`claim()` is an async iterator. It pipelines the ack for the previous job with the claim for the next one in a single transaction, wakes on any commit from any process, and falls back to a 5-second poll only if the filesystem watch somehow can't be established. Default batch size is 32, default visibility timeout is 300 seconds.

### Stream

A user renames themselves. Your dashboard, running in a different process, wants to push that change to the browser over SSE without polling:

```python
stream = db.stream("user-events")

with db.transaction() as tx:
    tx.execute("UPDATE users SET name=? WHERE id=?", [name, uid])
    stream.publish({"user_id": uid, "change": "name"}, tx=tx)
```

In the dashboard process:

```python
async for event in stream.subscribe(consumer="dashboard"):
    await push_to_browser(event)
```

Each named consumer tracks its own offset in the same database. `subscribe` replays everything past that offset, then transitions to live delivery on WAL wake. A consumer that reconnects picks up where it left off without losing events.

### Notify

If you don't need durability, notify is smaller and cheaper. Listeners start at the current `MAX(id)` on the channel and only see events published after they attached, like [`pg_notify`](https://www.postgresql.org/docs/current/sql-notify.html).

```python
async for n in db.listen("orders"):
    print(n.channel, n.payload)
```

```python
with db.transaction() as tx:
    tx.execute("INSERT INTO orders (id, total) VALUES (?, ?)", [42, 99.99])
    tx.notify("orders", {"id": 42})
```

Payloads are JSON-encoded on the way in and decoded on the way out. The wire format matches queue and stream, and it's identical across Python and Node, so a Python writer and a Node reader can use the same channel.

The notifications table doesn't auto-prune. Call `db.prune_notifications(older_than_s=10)` or `max_keep=10000` from a scheduled task or startup hook. Both arguments together mean OR, so rows matching either condition go. Typical usage is one argument at a time.

### Node.js

Same three primitives. Types are idiomatic JS; payloads round-trip with Python.

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
  for (const r of rows) {
    handle(JSON.parse(r.payload));
    last = r.id;
  }
}
```

A TypeScript `joblite-node` port that wraps `@litenotify/node` with the higher-level Queue/Stream/Outbox is on the [ROADMAP](ROADMAP.md).

### SQLite loadable extension

Any SQLite 3.9+ client can load the extension and get raw SQL access to the queue primitives:

```sql
.load ./liblitenotify_ext
SELECT jl_bootstrap();                                  -- idempotent schema
INSERT INTO _joblite_live (queue, payload)
VALUES ('emails', '{"to":"alice"}');

SELECT jl_claim_batch('emails', 'worker-1', 32, 300);   -- JSON of claimed rows
SELECT jl_ack_batch('[1,2,3]', 'worker-1');             -- DELETEs, returns count

SELECT notify('orders', '{"id":42}');                   -- installs notify() +
                                                        -- _litenotify_notifications
```

The extension shares `_joblite_live`, `_joblite_dead`, and `_litenotify_notifications` tables with the Python binding, so a Python worker can claim jobs a Go or Rust or shell-scripted enqueuer pushed, and vice versa. An interop test in `tests/test_extension_interop.py` enforces it.

## Integrating with your web framework

Three plugins, same API shape. They give you SSE endpoints for subscribing to channels and streams, an `authorize(user, target)` hook (sync or async), and a task decorator. They diverge on worker lifecycle and user resolution because the frameworks do.

FastAPI has lifespan hooks and a single-process async runtime, so workers run in the same process as the web server. Django and Flask are WSGI-first. Gunicorn forks the app across N workers, and you don't want a joblite worker pool inside every fork, so workers run as a dedicated CLI process. Same pattern as [Celery](https://docs.celeryq.dev/) and [RQ](https://python-rq.org/).

User resolution follows the same logic. FastAPI has `Depends(...)` and everyone uses it, so the plugin takes `user_dependency=...`. Django's convention is `request.user` set by auth middleware, so the plugin reads that by default and offers `set_user_factory(fn)` when middleware isn't installed. Flask has no strong opinion, so the plugin takes a callable directly.

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

# tasks.py (imported at app startup)
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

# run workers:
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

# run workers:
#   flask --app app joblite_worker
```

If `authorize` raises, all three return HTTP 500 without opening the SSE stream. The plugins are around 200 to 400 lines each. If something's missing, read one and copy it.

## Compared to

For a SQLite-native single-box app, the realistic alternatives are adding Redis or Postgres, or polling. Here's what each costs you.

[Redis pub/sub](https://redis.io/docs/latest/develop/pubsub/) wakes in sub-millisecond time, which is faster than what we do. It runs in a separate process, doesn't commit atomically with your application writes, and loses everything on restart. That's great for real-time fan-out of ephemeral signals, and a poor fit for "I just updated this order, run this job once."

[`pg_notify`](https://www.postgresql.org/docs/current/sql-notify.html) wakes in 5 to 20 ms cross-process, commits atomically inside your Postgres transaction, and is the right answer if you already run Postgres. But "let's just add Postgres" is not a small step for a SQLite-backed app.

[Kafka](https://kafka.apache.org/) gives you durable pub/sub with single-digit-ms wake times. It wants a cluster, KRaft or ZooKeeper, and a schema registry. Overkill for a web app on one machine.

Polling every second works and is the simplest possible thing. Wake latency is bounded by the poll interval, so you trade fresh data for CPU and lock contention. Every poll is a SELECT. A hundred processes polling once a second adds a hundred background reads per second, for the privilege of noticing changes up to a second late.

What this library does is trade peak single-transaction throughput for three things: one-process deployment, transactional coupling with application writes, and cross-process wake around a millisecond. Raw Python `sqlite3` clears about 47k tx/s on this machine; we run about 14k tx/s through the PyO3 boundary for the single-write case, and batching closes the gap (110k tx/s with 100 inserts per transaction). The operational surface is your application process and a `.db` file.

## How it works

Every `Database` lazily spawns a single background thread that `stat`s the `.db-wal` file every millisecond. On any change, the thread fans out a tick to every subscribed listener's bounded channel. Each subscriber's `__anext__` wakes, runs `SELECT ... WHERE id > last_seen` against a partial index, yields any rows, and goes back to waiting.

A process with a hundred subscribers uses one stat thread, idle listeners run zero database queries, and active listeners run one indexed SELECT per wake.

We considered `inotify`, `kqueue`, and `FSEvents` via the [`notify` crate](https://crates.io/crates/notify). FSEvents on macOS silently drops events for same-process writes, which means a listener and an enqueuer in the same Python process would never see each other. Stat-polling works identically on every platform at ~1 ms granularity for negligible CPU cost. We traded about 0.5 ms of latency for portability and the ability to hear ourselves write.

`_joblite_live` holds pending and processing jobs. A partial index on `(queue, priority DESC, run_at, id) WHERE state IN ('pending','processing')` keeps the claim hot path small regardless of how much dead-row history accumulates. Claim is one `UPDATE ... RETURNING` via that index. Ack is one `DELETE`. Retry-exhausted rows move to a separate `_joblite_dead` table the claim path never touches, so dead-row history doesn't slow claims down.

`async for job in queue.claim("w")` yields jobs one at a time but runs ack-of-previous and claim-of-next in a single transaction per batch (default 32). `Job.ack()` on iterator-owned jobs defers into a pending list that flushes on the next claim. A worker loop gets batched speed without changing application code.

Every primitive takes a `tx=tx` argument. `queue.enqueue(..., tx=tx)`, `stream.publish(..., tx=tx)`, and `tx.notify(...)` are INSERTs that join the caller's transaction. Commit makes them visible to every reader in every process. Rollback makes them never have happened. That's the transactional-outbox pattern by default.

## Crash recovery

A `raise` inside a `with db.transaction() as tx` block rolls the whole transaction back. No half-delivered notification, no orphaned job. SQLite handles this.

SIGKILL mid-transaction is safe. WAL rollback on next open leaves no stale state. The uncommitted INSERT is simply gone. `tests/test_crash_recovery.py` spawns a real subprocess, kills it before COMMIT, verifies `PRAGMA integrity_check == 'ok'`, checks the ghost rows don't exist, and confirms fresh notifies still flow.

A worker that crashes mid-handler leaves its claim on the row. After `visibility_timeout_s` (default 300s) another worker's claim reclaims it and `attempts` increments. After `max_attempts` (default 3), the row moves to `_joblite_dead` and waits there until you decide what to do with it.

Listeners start at `MAX(id)` on the channel at attach time, so a reconnecting listener doesn't replay history from the notifications table. If you need replay past the tail, use `db.stream(...)`. Streams track each consumer's last committed offset and replay everything past it on subscribe.

## Performance

Numbers below are on an Apple Silicon M-series laptop, release build, WAL + `synchronous=NORMAL`, from `bench/real_bench.py` (multi-process saturation) and `bench/wake_latency_bench.py` (idle cross-process wake).

Four worker subprocesses plus two enqueuer subprocesses at 2k events per second each push 3,900 jobs/s through the queue with an end-to-end latency of 0.5 ms at the median. With 100,000 dead rows in the history table, throughput barely moves: 3,800 jobs/s at the same 0.5 ms. Batched claim+ack at batch size 128 clears 100,000 jobs/s. Stream replay through the reader pool hits a million events per second.

The 0.5 ms end-to-end is Little's Law on a 3,900 j/s pipeline with queue depth around 2. What you actually care about for cross-process push is the wake latency: one commit in process A, one idle listener in process B, measure the delta. That's 1.2 ms at the median and 2.4 ms at the 90th percentile, bounded by the 1 ms stat-poll cadence.

Enqueue throughput is around 6-8k jobs per second for single-job transactions and 110k jobs per second when batched 100 per transaction. Raw Python `sqlite3` single-tx tops out around 47k/s on the same file, which is the WAL ceiling on this machine. Our 3x gap is PyO3 boundary crossings and the writer-mutex acquire, which batched workloads close.

## Where it's a fit

Single-box web apps that want durable background jobs without adding Redis. SSE and WebSocket servers pushing database-driven events to browsers, with `Last-Event-ID` reconnect. CLIs and desktop apps that need transactional side effects that survive a crash. Multi-process workers on the same machine sharing one `.db` file.

## Where it isn't

SQLite's locking is designed for a single host. Two servers writing to the same file over NFS will corrupt it. If you need to scale past one host, shard by file (queue-per-tenant, queue-per-database) or switch to Postgres.

Cross-machine delivery is your application's job. Your web handler writes to SQLite, then posts to whatever transport your other machines subscribe to: HTTP, Kafka, NATS, whatever fits. Framework plugins are where that glue goes, and future work will add built-in adapters.

Workflow orchestration with DAGs, compensation, and human-in-the-loop isn't here. For those, look at [Temporal](https://temporal.io/), [Hatchet](https://hatchet.run/), or [Inngest](https://inngest.com/).

## Tests and bench

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

# Cross-process wake-latency microbench: idle listener in a subprocess,
# p50/p90/p99 from N samples
python bench/wake_latency_bench.py --samples 500

# Realistic cross-process concurrent throughput bench
python bench/real_bench.py --workers 4 --enqueuers 2 --seconds 15

# Raw-SQL engine ceiling via the loadable extension
python bench/ext_bench.py
```

See [ROADMAP.md](ROADMAP.md) for what's coming and [CHANGELOG.md](CHANGELOG.md) for what's shipped.

## License

Apache 2.0. See [LICENSE](LICENSE).
