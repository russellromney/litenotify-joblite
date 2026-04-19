# litenotify/joblite

`litenotify` is a SQLite extension that adds Postgres-style `NOTIFY`/`LISTEN` semantics to SQLite with built-in durable pub/sub, task queue, and event streams without a daemon/broker or client polling.

`joblite` adds language bindings and framework integrations that let listeners/workers consume and act on messages from `litenotify`.

For most applications, [SQLite alone is sufficient](https://www.epicweb.dev/why-you-should-probably-be-using-sqlite). This project's goal is to make that easier. 

`litenotify` works by adding messages/tasks to tables in SQLite, similar to other task libraries that use SQLite as a durable backend. Unlike other task libraries, it takes advantage of kernel-level event notifications on SQLite's WAL file to replace a polling interval with push semantics. A 1 ms `stat(2)` thread on the WAL file turns `(size, mtime)` changes into cross-process notifications with single-digit millisecond latency.

Three primitives on top: ephemeral pub/sub (`notify()`), durable pub/sub with per-consumer offsets (`stream()`), at-least-once work queue (`queue()`). All three are INSERTs inside your transaction, which lets a task "send" be atomic with your business write, and rollback drops everything.

The explicit goal is to do `NOTIFY`/`LISTEN` semantics without constant polling, to achieve single-digit ms reaction time. If you use your app's existing SQLite file containing business logic, it will notify workers on every WAL commit. This means that most triggers will not result in anything happening - workers just read the message/queue with no result. This "overtriggering" is on purpose and is the tradeoff for push semantics and fast reaction time.

This repo includes the `litenotify` SQLite loadable extension, `joblite` language bindings for Python, Node, Go, Rust (library), and Ruby, and `joblite` framework plugins for FastAPI, Django, Flask, Rails, Express, and ... .

For Postgres-backed apps, [`pg_notify`](https://www.postgresql.org/docs/current/sql-notify.html) + [pg-boss](https://github.com/timgit/pg-boss) or [Oban](https://hexdocs.pm/oban/) is the equivalent. This library is for apps where SQLite is the primary datastore.

> Experimental. API may shift before 1.0.

## Design

The library/extension is a small coordination layer built on the properties of SQLite and single-server architecture.

- **One `.db` + one `.db-wal`** is the entire system, with all the benefits of SQLite (embedded, local, durable, snapshot-able, etc.) that your app already usees
- **WAL mode: one writer, concurrent readers.** Claim = `UPDATE … RETURNING` via a partial index. Ack = `DELETE`.
- **WAL file grows on every commit.** `(size, mtime)` is the cross-process commit signal.
- **SQLite has no wire protocol.** Consumers must initiate reads; server-push is impossible. Wake signal = file change → `SELECT`.
- **Transactions are cheap.** Jobs, events, and notifications are rows in the caller's open `with db.transaction()` block in an "outbox"-type pattern.
- **Cross-platform via `stat(2)`, not `FSEvents`/`inotify`/`kqueue`.** FSEvents drops same-process writes on macOS — a listener and enqueuer in the same Python process would never see each other. `stat(2)` works identically on Linux/macOS/Windows at ~1 ms granularity for negligible CPU. Cost: ~0.5 ms of latency vs kernel notifications.
- **Single machine, single writer.** SQLite's locking is designed for a single host. Two servers writing one `.db` over NFS will corrupt it. Shard by file, or switch to Postgres.

# Quick start


## Python - queue-style workflow
```bash
pip install litenotify-joblite
```

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
SELECT notify('orders', '{"id":42}');
```

The extension shares `_joblite_live`, `_joblite_dead`, and `_litenotify_notifications` with the Python binding, so a Python worker can claim jobs any other language pushed via the extension. Schema compatibility is pinned by `tests/test_extension_interop.py`.

## Performance

Handles thousands of messages per second on a modern laptop, with cross-process wake latency bounded by the 1 ms stat-poll cadence (~1–2 ms median on M-series). Run `bench/wake_latency_bench.py` and `bench/real_bench.py` to measure on your hardware.

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

## Framework plugins

| | Workers | User |
|---|---|---|
| `joblite_fastapi.JobliteApp` | in-process (FastAPI lifespan) | `user_dependency=Depends(…)` |
| `joblite_django` | CLI: `manage.py joblite_worker` | `request.user` / `set_user_factory(fn)` |
| `joblite_flask.JobliteFlask` | CLI: `flask joblite_worker` | `user_factory=fn(req)` |

Each exposes `GET /joblite/subscribe/<channel>` + `GET /joblite/stream/<name>` (SSE), a `@task(...)` decorator, and an `authorize(user, target)` hook (sync or async). If `authorize` raises, the handler returns HTTP 500 without opening the SSE stream.

FastAPI runs workers in-process because it has a single-process async runtime and lifespan hooks. Django and Flask are WSGI-first — Gunicorn forks the app across N workers, so a worker pool in each fork would over-subscribe the database. Workers run out-of-process via the CLI instead.

Usage: each plugin's directory.

## Development

```bash
cargo test -p litenotify-core                # Rust core: writer/readers pool, watcher, schema
pytest tests/                                # Python: 126 tests incl. cross-lang + extension interop
cd litenotify-node && npm test               # Node: 8 tests incl. Python→Node wake

python bench/wake_latency_bench.py --samples 500
python bench/real_bench.py --workers 4 --enqueuers 2 --seconds 15
python bench/ext_bench.py
```

## License

Apache 2.0. See [LICENSE](LICENSE).
