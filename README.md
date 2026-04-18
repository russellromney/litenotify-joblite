# litenotify/joblite

Postgres-style `NOTIFY`/`LISTEN` and durable background work in a single SQLite file. No Redis, no RabbitMQ, no second daemon. Queues, streams, and an outbox, all coupled to your business writes through one `COMMIT`.

> **Experimental.** Test suite is thorough (12 Rust + 109 Python, real subprocesses, real uvicorn, no mocks) but the API may shift before 1.0 and no wheels are on PyPI yet.

If you run Postgres, keep Postgres. This explores what one SQLite file can do when you don't want to run anything else.

## What's in the box

| Package | What it is |
|---------|-----------|
| `honker` | Rust crate. SQLite commit-hook NOTIFY/LISTEN with per-channel subscriber registry. |
| `litenotify` | PyO3 binding. `Database` / `Transaction` / `Listener`. One writer, bounded reader pool. |
| `joblite` | Python. `Queue` (at-least-once, visibility timeout), `Stream` (durable pub/sub, Last-Event-ID), `Outbox` (transactional side effects). |
| `joblite_fastapi` | `JobliteApp(app, db)`: worker pool + SSE endpoints + `authorize` hook. |
| `joblite_django` | `@task`, async SSE views, `python manage.py joblite_worker`. |

Node bindings and Express/Flask/Rails plugins on [ROADMAP](ROADMAP.md).

## Performance

Apple Silicon M-series, default WAL settings, April 2026. Numbers are honest but not yet tuned; see [Known perf work](#known-perf-work).

| Operation | Number | Notes |
|-----------|--------|-------|
| `enqueue` | 10,208 /s | One `BEGIN IMMEDIATE` + INSERT + COMMIT per call. |
| `claim + ack` | 823 /s | Two write transactions per job. `claim_batch(n)` is on ROADMAP and should 10x this. |
| `publish` | 9,454 /s | Same single-writer ceiling as enqueue. |
| replay | 319,417 /s | Reader-pool `SELECT`, no write lock. |
| live e2e | sub-ms typical | Commit hook to consumer; p50 in `bench/` is a harness artifact (non-yielding publisher). |

Redis `LPUSH`/`BRPOP` clears ~100k/s but lives in a separate process and doesn't atomically commit with your writes. `pg_notify` is fast but requires Postgres. joblite trades peak throughput for transactional coupling and one-file deployment.

## Quick start

```bash
git clone https://github.com/russellromney/litenotify-joblite && cd litenotify-joblite
uv venv && source .venv/bin/activate
cd litenotify && maturin develop --uv && cd ..
uv pip install fastapi uvicorn django pytest pytest-asyncio pytest-xdist pytest-django
```

### Enqueue and process

```python
import joblite

db = joblite.open("app.db")
emails = db.queue("emails")

with db.transaction() as tx:
    tx.execute("INSERT INTO orders (user_id) VALUES (?)", [42])
    emails.enqueue({"to": "alice@example.com"}, tx=tx)
# Job is only durable if the business write commits.

async for job in emails.claim("w1"):
    try:
        send(job.payload); job.ack()
    except Exception as e:
        job.retry(delay_s=60, error=str(e))
```

### Listen

```python
async for notif in db.listen("orders"):
    print(notif.channel, notif.payload)

# elsewhere:
with db.transaction() as tx:
    tx.execute("INSERT INTO orders ...")
    tx.honk("orders", {"order_id": 99})
```

`honk()` buffers inside the transaction. Rollback drops it.

### FastAPI

```python
from fastapi import FastAPI
import joblite
from joblite_fastapi import JobliteApp

app = FastAPI()
db = joblite.open("app.db")
jl = JobliteApp(app, db)

@jl.task("emails")
async def send_email(payload):
    await mailer.send(payload["to"])
```

`GET /joblite/subscribe/{channel}` and `GET /joblite/stream/{name}` are SSE endpoints. `authorize(user, target)` may be sync or async; if it raises, FastAPI returns 500.

### Django

```python
# settings.py:   JOBLITE_DB_PATH = BASE_DIR / "app.db"
# tasks.py
import joblite_django
@joblite_django.task("emails")
async def send_email(payload): ...

# urls.py
from joblite_django.views import stream_sse, subscribe_sse
urlpatterns = [
    path("joblite/stream/<str:name>", stream_sse),
    path("joblite/subscribe/<str:channel>", subscribe_sse),
]
```

```bash
python manage.py joblite_worker
```

Two or more worker processes split work with zero overlap. Proven by `test_joblite_django.py::test_management_command_two_workers_split_work_exclusively` against real subprocesses.

## Design

| Constraint | Implication |
|------------|-------------|
| WAL: one writer at a time | Single dedicated writer connection; readers pool separately. |
| `BEGIN IMMEDIATE` is the claim primitive | Every job claim is one atomic `UPDATE ... RETURNING` inside a `BEGIN IMMEDIATE`. |
| `commit_hook` fires after `COMMIT` | `honk()` buffers inside the tx; rollback drops the buffer; commit fans out per channel. Transactional pub/sub. |
| Commit hooks run in the writing process | The notifier is per-process. Cross-process workers fall back to polling (`idle_poll_s`, default 5s). |

### honker

Each channel has its own `Vec<Subscriber>` with a per-subscriber `tokio::broadcast` ring. A flood on `"hot"` can't starve a listener on `"cold"`. `unsubscribe(id)` drops the sender and closes the receiver; Python `Listener.__del__` calls it, so SSE-heavy services don't leak threads.

### litenotify

One `Writer` slot (mutex + condvar), always released around `db.transaction()` even if the body raised or commit failed. Bounded reader pool (default 8) for `db.query()`. The listener bridge is a `std::thread` doing `blocking_recv` then `loop.call_soon_threadsafe`, which means listeners work on any asyncio loop (Starlette portals, anyio, Jupyter).

### joblite

- `Queue`: one row per job, claim via atomic `UPDATE ... RETURNING`, visibility-timeout reclaim. A stuck worker's `ack()` returns `False`; that's the at-least-once contract made visible.
- `Stream`: append-only `_joblite_stream` with monotonic offsets. `subscribe(from_offset=?)` replays then transitions to live delivery.
- `Outbox`: side-effect delivery on `Queue` with exponential backoff.
- `Retryable`: exception that asks for a specific retry delay.

## What works, what doesn't

**Works:** single-box deploys, transactional coupling (jobs + business writes in one `COMMIT`), SSE reconnect with real `Last-Event-ID`, SIGKILL during a write tx leaves the DB clean (locked in by `test_crash_recovery.py`).

**Doesn't:** multi-machine — SQLite is not distributed, two servers writing to the same `.db` corrupts it. Cross-process `NOTIFY` — notifier is per-process by design; workers poll.

### Known perf work

- `Queue.claim_batch(n)`: one tx, many jobs. Should push claim+ack into the tens of thousands.
- `bench/stream_bench.py` e2e: the publisher doesn't yield, so p50 measures publish-loop duration, not library latency. Fix the harness and rerun.
- Profile `enqueue` to see how much of the 100μs/call is PyO3 vs rusqlite vs SQLite.

## Tests and bench

```bash
cargo test -p honker                        # 12 tests
pytest tests/                               # 109 tests, ~8 to 13s parallel
python bench/joblite_bench.py --n 5000
python bench/stream_bench.py --n 5000
```

## License

MIT. See [LICENSE](LICENSE).
