# litenotify/joblite

litenotify brings Postgres-style `NOTIFY`/`LISTEN` to SQLite. joblite builds on top: durable queues, streams, and an outbox, all inside a single `.db` file. FastAPI and Django plugins ship SSE endpoints and worker runners in a few lines.

> **This is experimental.** The test suite is comprehensive (12 Rust + 109 Python, real subprocesses and real uvicorn, no mocks), but the library is young, unpublished, and no wheels are on PyPI yet. The API may shift before 1.0. Use it, file bugs, but don't ship it to prod without reading the code.

SQLite has always had the primitives. A commit hook fires after every transaction. WAL mode lets readers run concurrent with a writer. `BEGIN IMMEDIATE` gives you a serialized write lock. What it didn't have was the small layer on top that turns those primitives into a real pub/sub, plus a jobs table you can enqueue into atomically with your business writes. That's this project.

If you already run Postgres, keep Postgres; `pg_notify` and `SELECT ... FOR UPDATE SKIP LOCKED` are excellent. litenotify explores what you can do when your whole stack is one SQLite file and you don't want Redis, RabbitMQ, or a second daemon to run background work.

litenotify ships as:

- `honker` — Rust crate. SQLite commit-hook NOTIFY/LISTEN, per-channel subscriber registry, tokio broadcast fan-out.
- `litenotify` — Rust + PyO3. Python binding: `Database` / `Transaction` / `Listener` / `Notification`, one dedicated writer, bounded reader pool under WAL.
- `joblite` — pure Python. `Queue` (at-least-once claim with visibility timeout), `Stream` (durable pub/sub with Last-Event-ID), `Outbox` (transactional side effects), `Retryable` signal.
- `joblite_fastapi` — `JobliteApp(app, db)`, worker pool managed by FastAPI lifespan events, SSE endpoints with Last-Event-ID replay, `authorize` hook (sync or async).
- `joblite_django` — `@joblite_django.task(...)`, async SSE views, `python manage.py joblite_worker`.

Node and Express bindings are on the [ROADMAP](ROADMAP.md).

If you want to contribute, open an issue or a pull request. The code is small and the test suite is fast (~8–13s parallel).

## Performance

Apple Silicon M-series, single writer, default WAL settings, April 2026.

### `joblite.queue`

| Operation | Throughput | Notes |
|-----------|-----------|-------|
| `enqueue()` | 10,208 /s | One `BEGIN IMMEDIATE` + INSERT + COMMIT per call. Batch into one tx for 100k+/s. |
| `claim + ack` | 823 /s | Two write transactions per job (claim and ack). A batched claim would push this well into the thousands. |
| end-to-end | 763 /s | Enqueue-all-then-drain pattern. Realistic overlapped workloads track closer to `1 / throughput` s per job. |

### `joblite.stream`

| Operation | Throughput / Latency | Notes |
|-----------|----------------------|-------|
| `publish()` | 9,454 events/s | Same single-writer ceiling as enqueue. |
| replay | 319,417 events/s | Reader-pool `SELECT` of pre-seeded rows. Dominated by JSON decode, no write lock. |
| live e2e | p50 = 52ms, p99 = 108ms | commit-hook → tokio broadcast → `call_soon_threadsafe` → `asyncio.Queue` → consumer. |

### How it compares

Redis `LPUSH`/`BRPOP` on localhost clears ~100k ops/s but lives in a separate process and doesn't commit atomically with your application writes. Postgres `pg_notify` is fast but requires Postgres. joblite trades peak throughput for transactional coupling and one-file deployment. For most applications the ceiling here is well above the job volume they produce.

Run your own: `python bench/joblite_bench.py --n 5000` and `python bench/stream_bench.py --n 5000`.

## Quick start

```bash
git clone https://github.com/russellromney/litenotify-joblite
cd litenotify-joblite
uv venv && source .venv/bin/activate
cd litenotify && maturin develop --uv && cd ..
uv pip install fastapi uvicorn httpx pytest pytest-asyncio pytest-xdist pytest-django django
```

### Enqueue and process a job

```python
import asyncio
import joblite

db = joblite.open("app.db")
emails = db.queue("emails")

# In a web request:
with db.transaction() as tx:
    tx.execute("INSERT INTO orders (user_id) VALUES (?)", [42])
    emails.enqueue({"to": "alice@example.com", "order_id": 42}, tx=tx)
# The job is only durable if the business write also committed.

# In a worker process:
async def run():
    async for job in emails.claim(worker_id="w1"):
        try:
            send(job.payload)
            job.ack()
        except Exception as e:
            job.retry(delay_s=60, error=str(e))

asyncio.run(run())
```

### Listen for notifications

```python
import asyncio
import joblite

db = joblite.open("app.db")

async def main():
    listener = db.listen("orders")
    async for notif in listener:
        print(notif.channel, notif.payload)

asyncio.run(main())

# Somewhere else, in any process that writes to the same `.db`:
with db.transaction() as tx:
    tx.execute("INSERT INTO orders (user_id) VALUES (?)", [99])
    tx.honk("orders", {"order_id": 99, "user_id": 99})
```

`honk()` buffers inside the transaction; nothing is delivered until `COMMIT`. If the transaction rolls back, listeners see nothing.

### FastAPI plugin

```python
from fastapi import FastAPI
import joblite
from joblite_fastapi import JobliteApp

app = FastAPI()
db = joblite.open("app.db")
jl = JobliteApp(app, db)

@jl.task("emails")
async def send_email(payload):
    await mailer.send(payload["to"], payload["subject"])

@app.post("/orders")
async def create_order(body: dict):
    with db.transaction() as tx:
        tx.execute("INSERT INTO orders (user_id) VALUES (?)", [body["user_id"]])
        db.queue("emails").enqueue(body["email_payload"], tx=tx)
    return {"ok": True}
```

`GET /joblite/subscribe/{channel}` streams raw NOTIFY events as SSE. `GET /joblite/stream/{name}` streams a durable stream with `Last-Event-ID` replay. Both accept an `authorize(user, target)` callable that may be sync or async.

### Django plugin

```python
# settings.py
INSTALLED_APPS = [..., "joblite_django"]
JOBLITE_DB_PATH = BASE_DIR / "app.db"

# tasks.py
import joblite_django

@joblite_django.task("emails")
async def send_email(payload):
    ...

# urls.py
from joblite_django.views import stream_sse, subscribe_sse
urlpatterns = [
    path("joblite/stream/<str:name>", stream_sse),
    path("joblite/subscribe/<str:channel>", subscribe_sse),
]

# in one terminal
python manage.py joblite_worker
```

Two or more `joblite_worker` processes against the same `.db` split work with zero overlap; the test suite proves this against a real subprocess pair.

## Design

litenotify is designed for SQLite's constraints. Every decision flows from this model:

| SQLite constraint | Implication |
|-------------------|-------------|
| One writer at a time (WAL mode) | Single dedicated writer connection; readers pool separately. No fight for the write lock. |
| `BEGIN IMMEDIATE` is the only atomic-claim primitive | Every job claim is one `UPDATE ... RETURNING` inside a `BEGIN IMMEDIATE`. |
| `commit_hook` fires only after `COMMIT` | `honk()` buffers inside the transaction; rollback drops the buffer; commit fans out per channel. This is how you get transactional pub/sub. |
| Commit hooks run in the writing process | The notifier registry is per-process. Workers in different processes wake via `db.listen()` in their own process plus an `idle_poll_s` fallback poll. By design. |

### honker: per-channel subscriber registry

Early versions shared one `tokio::broadcast::channel(1024)` across all subscribers. A flood on channel `"hot"` would push messages out of the ring and force a listener on channel `"cold"` to see `Lagged(_)`. Cross-channel starvation is a real bug on a shared ring.

Now `Notifier` keeps a `HashMap<channel, Vec<Subscriber>>` with a per-subscriber ring. `subscribe(channel)` hands back a `{id, channel, rx}`. The commit hook routes each pending honk only to subscribers of its channel. `unsubscribe(id)` drops the sender, closes the receiver, and the bridge thread exits; a Python `Listener` has a `Drop` impl that calls this, so SSE-heavy services don't leak threads.

### litenotify: one writer, bounded reader pool

Under WAL mode, readers are concurrent with a writer but writers are serialized. litenotify maintains:

- One `Writer` slot, acquired and released around every `db.transaction()`. `BEGIN IMMEDIATE` on acquire, `COMMIT`/`ROLLBACK` on release, always returned to the pool even if the body raised or the commit failed.
- A `Readers` pool (bounded, default 8) for `db.query()`. Reader connections are opened lazily up to `max_readers`.

The listener bridge runs on a plain `std::thread` that does `broadcast::blocking_recv` and then `loop.call_soon_threadsafe(queue.put_nowait, notif)` at delivery time. The loop is captured at `__aiter__` time, which means listeners work on any asyncio loop (Starlette TestClient portals, anyio portals, Jupyter kernels, whatever).

### joblite: the small set of patterns on top

- **`Queue`.** One row per job in `_joblite_jobs`, claim via atomic `UPDATE ... RETURNING`, at-least-once delivery with a visibility timeout. If a worker crashes mid-job, another worker reclaims after `visibility_timeout_s`. The stuck worker's eventual `ack()` returns `False`, which is the at-least-once contract made visible to the caller.
- **`Stream`.** Append-only `_joblite_stream` with monotonic offsets. `publish(payload, tx=?)` inside a transaction, `subscribe(from_offset=?, consumer=?)` yields events and transitions from replay to live NOTIFY delivery. Named consumers save offsets with monotonic semantics (lower values ignored).
- **`Outbox`.** Transactional side-effect delivery built on `Queue`. `enqueue(payload, tx=tx)` couples the side effect to your business write. A background worker drives delivery with exponential backoff up to `max_attempts`, then the job lands in `dead`.
- **`Retryable`.** Exception type that asks for a scheduled retry with a specific delay. Other exceptions retry with a generic backoff and capture the traceback in `last_error`.

### What the plugins actually add

Plugins are small wrappers, not frameworks. The FastAPI plugin registers SSE routes, starts/stops a worker pool on app lifespan, and wraps an `authorize(user, target)` callable. The Django plugin is a management command plus a pair of async views and a global task registry. Both plugins accept sync or async `authorize`. If the callable raises, the framework returns HTTP 500 and the SSE stream is never opened. No hang, no half-response.

## Strengths and limitations

### Where litenotify/joblite fits

- **Single-box deploys.** Web + worker + DB on one machine. One `.db` file.
- **Transactional coupling.** The jobs and the business write land in the same `COMMIT`, or neither does. This is the whole pitch over Redis.
- **Durable pub/sub.** `Stream` survives restarts and reconnects. SSE `Last-Event-ID` replay is wired through.
- **Crash safety.** SIGKILL during a write transaction leaves the DB intact, no stale locks, no phantom notifications. Proven by `test_crash_recovery.py`.

### What doesn't work

- **Multi-machine.** SQLite isn't a distributed DB. Two servers writing to the same `.db` corrupts the file. A single NFS/EFS mount doesn't fix this.
- **Cross-process `NOTIFY`.** The notifier registry is per-process. A worker in one process does not wake a listener in a different process; workers fall back to `idle_poll_s` polling (default 5s). This is by design; see ROADMAP for the fcntl-based wakeup idea if it matters to you.
- **Peak throughput.** A single-writer DB maxes at ~10k enqueue/s with default settings. Batched-claim (one tx, many jobs) is on the [ROADMAP](ROADMAP.md).

### What's stable

- `honker` and `litenotify` public surfaces are settled. Changes would be new features, not breaking renames.
- `joblite.Queue` / `Stream` / `Outbox` shapes are settled.
- Plugins are settled for the features listed above.

### What might change

- Node bindings (`honker-node`, `litenotify-node`, `joblite-node`, `joblite-express`) are on the ROADMAP as their own multi-day chunk.
- `Queue.claim_batch(n)` for worker throughput.
- Flask and Rails plugins.

## Running tests and benchmarks

```bash
# Rust
cargo test -p honker

# Python (runs under -n auto, ~8 to 13 seconds)
pytest tests/

# Benchmarks
python bench/joblite_bench.py --n 5000
python bench/stream_bench.py --n 5000
```

## Directory layout

```
honker/              Rust crate: commit-hook NOTIFY/LISTEN, subscriber registry
litenotify/          Rust + PyO3: Database, Transaction, Listener
joblite/             Python: Queue, Stream, Outbox, Retryable, Event, Job
joblite_fastapi/     FastAPI plugin: JobliteApp, SSE, authorize
joblite_django/      Django plugin: views, management command, @task registry
tests/               Python test suite (also exercises honker through PyO3)
bench/               Throughput and latency harness
demo.py              FastAPI end-to-end demo
ROADMAP.md           What's next
CHANGELOG.md         What shipped
```

## License

MIT. See [LICENSE](LICENSE).
