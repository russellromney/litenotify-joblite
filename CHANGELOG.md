# CHANGELOG

## Unreleased — perf pass 4: pipeline ack+claim, narrow RETURNING, PRAGMA tuning

Three targeted changes. Biggest win by far is the ack+claim pipeline.

### Changes

- **Pipelined ack-of-previous with claim-of-next.** The async iterator
  `queue.claim(...)` now runs both operations in one transaction via
  a new `Queue.ack_and_claim_batch(ack_ids, worker, n)` method.
  `Job.ack()` on iterator-owned jobs appends to a pending-ack list;
  the next batch's claim flushes them in the same tx. Halves the
  write-tx count for the common `async for job: handle; job.ack()`
  pattern. Jobs from direct `claim_one()` / `claim_batch()` still go
  through the old per-tx ack for accurate bool return.
- **Narrow RETURNING on claim.** `claim_batch` now returns
  `id, queue, payload, worker_id, attempts, claim_expires_at` instead
  of `*`. `Job` tolerates a missing row field by defaulting sensibly
  (state='processing', priority=0, etc). Saves ~20% per claim on
  the 11-col RETURNING.
- **PRAGMA tuning.** Added `cache_size=-32000` (32MB page cache, up
  from 2MB default), `temp_store=MEMORY` (temp B-trees for ORDER BY /
  DISTINCT stay in RAM), `wal_autocheckpoint=10000` (fsync every
  10k WAL pages rather than 1k). Cheaper, larger, less frequent
  disk flush.

### Numbers (median of 3, M-series, release)

| Operation | Before | After |
|-----------|--------|-------|
| enqueue (1/tx) | ~6,000 /s | ~8,000 /s |
| claim + ack (direct) | ~3,700 /s | ~4,500 /s |
| **async iter end-to-end** | **~3,500 /s** | **~6,500 /s** |
| claim_batch+ack_batch (32) | ~60k/s | ~75k/s |
| claim_batch+ack_batch (128) | ~80k/s | ~110k/s |
| async iter p50 latency | ~720ms | ~370ms |

Test suite unchanged: 12 Rust + 109 Python all pass. `Job.ack()` on
iterator-owned jobs returns `True` optimistically (safe within the
millisecond pipeline window); direct callers still get the accurate
`False` return on claim-expired races.

## Unreleased — rename: fold `honker` into `litenotify`, `honk()` -> `notify()`

Architectural cleanup. No functional changes; all 12 Rust + 109 Python
tests still pass with the new names.

- Deleted the `honker/` crate. Its contents now live in
  `litenotify/src/notifier.rs` as a private module inside the
  `litenotify` crate. One crate, one cdylib.
- SQL scalar function `honk(channel, payload)` -> `notify(channel, payload)`,
  parallel to `pg_notify`.
- Rust `Transaction::honk()` and Python `tx.honk()` -> `notify()`.
- All `tx.honk(...)` call sites in `joblite`, tests, bench, and docs
  updated.
- Workspace `Cargo.toml` now has a single member (`litenotify`).

Breaking change for any external caller of `tx.honk(...)`. There aren't
any.

## Unreleased — perf pass 3: try_acquire + deque + lazy JSON + Arc<Notification>

Four targeted changes, each validated by bench. Ordered by impact.

### Changes

- **`collections.deque` for iterator buffers.** `_StreamIter._buffer` and
  `_WorkerQueueIter._buffer` were `list` and yielded via `list.pop(0)`
  which is O(n). Swapped for `deque` + `popleft()` (O(1)). `_StreamIter`
  refreshes in batches of 1000 rows; the old code was spending
  quadratic time shifting each popped row's successors leftward.
- **Writer-mutex `try_acquire` fast-path** on `litenotify.Transaction.
  __enter__`. If the slot is free at `parking_lot::Mutex::lock()` time,
  take it without `py.detach` (saves ~5us/tx of GIL release+reacquire).
  Slow path still drops GIL before blocking on the condvar.
- **Lazy `json.loads` on `Job.payload` / `Event.payload`.** Previously
  `__init__` unconditionally decoded the payload column. Now a
  `@property` decodes on first access via a sentinel-guarded cache.
  Handlers that only read `job.id` / `job.worker_id` skip N JSON parses
  per batch.
- **`Arc<Notification>` in notifier's broadcast channel.** The commit hook's
  fan-out loop cloned a `Notification { channel: String, payload: String }`
  per subscriber. Swapped to `broadcast::Sender<Arc<Notification>>` so
  subscriber sends are ref-count bumps, not `String` reallocations. No
  user-visible change (Python side still copies strings into a
  `NotificationResult` once per delivery, same as before).

### Numbers (median of 3, release build, M-series)

| Operation | Before | After this pass | Pass-1 baseline |
|-----------|--------|-----------------|-----------------|
| enqueue (1/tx) | ~5,000 /s | ~6,000 /s | same |
| enqueue (100/tx) | ~94,000 /s | ~110,000 /s | ~45,000 |
| claim + ack | ~3,100 /s | ~3,700 /s | ~1,000 |
| claim_batch+ack_batch (32) | ~48,500 /s | ~60,000 /s | n/a |
| claim_batch+ack_batch (128) | ~61,000 /s | ~80,000 /s | n/a |
| end-to-end (async iter) | ~3,050 /s | ~3,500 /s | ~820 |
| **stream replay** | ~400,000 /s | **~1,000,000 /s** | ~319,000 |
| stream live e2e p50 | 0.24ms | 0.23ms | ~52ms (harness bug) |

All 12 Rust + 109 Python tests still pass.

## Unreleased — perf pass 2: the real claim bottleneck

Previous "perf pass 1" claim was `synchronous=NORMAL` puts us on a fsync
ceiling. Verified with a proper probe (`OFF` vs `NORMAL` vs `FULL`
comparison on the same path): `NORMAL` in WAL does **not** fsync per
commit. The actual claim+ack bottleneck was a query-plan / index-schema
issue hiding behind the misread.

### Root causes

1. **The claim index had `state` as a key column.** Every transition
   `pending -> processing -> done` forced the row to reshuffle in the
   B-tree. Pure CPU cost, nothing to do with disk.
2. **The OR-based WHERE clause couldn't match the partial predicate.**
   Even after rewriting the index as `WHERE state IN ('pending',
   'processing')`, the planner couldn't prove that `(state='pending'
   OR state='processing')` implies the partial index's WHERE, so it
   fell back to a **full table scan** on every claim. `EXPLAIN QUERY
   PLAN` showed `SCAN _joblite_jobs` + `USE TEMP B-TREE FOR ORDER BY`.

### Fix

- Drop `state` from the index key; keep only `(queue, priority DESC,
  run_at, id)`.
- Make the index **partial**: `WHERE state IN ('pending', 'processing')`.
  Rows drop out of the index entirely on `ack()` / `fail()`, which is a
  cheap single-key delete rather than a B-tree rewrite.
- Add an explicit `state IN ('pending', 'processing')` to the claim
  inner SELECT so the planner matches the partial index. Logically a
  no-op given the OR clause; necessary for planner inference. Plan now
  reads `SEARCH _joblite_jobs USING INDEX _joblite_jobs_claim_v2 (queue=?)`.
- Index renamed `_joblite_jobs_claim_v2`; old `_joblite_jobs_claim`
  dropped idempotently on `Queue._init_schema` for existing DBs.

### New batch APIs

- `Queue.claim_batch(worker_id, n) -> list[Job]`: atomic UPDATE of N
  rows in one tx, one RETURNING *.
- `Queue.ack_batch(ids, worker_id) -> int`: one UPDATE via `json_each`
  over a JSON array of ids, so SQL text is constant across batch sizes
  (prepare_cached hit).
- `Queue.claim(worker_id, batch_size=32)` async iterator now claims in
  batches internally and yields one job at a time. Existing call sites
  (`async for job in q.claim("w1")`) get the batched speed for free.

### Numbers, median of 3, release build, Apple Silicon M-series

| Operation | Before | After |
|-----------|--------|-------|
| claim + ack (1 job) | ~1,000 /s | ~3,100 /s |
| claim_batch + ack_batch (32) | n/a | ~48,500 /s |
| claim_batch + ack_batch (128) | n/a | ~61,000 /s |
| end-to-end (async iter) | ~820 /s | ~3,050 /s |

All 12 Rust + 109 Python tests still pass with the new schema.

## Unreleased — perf pass 1

Commit-hook path is healthy. Per-tx Python path has a 4x gap to raw
Python `sqlite3` on the same file. Started closing it.

### What changed
- **Prepared-statement cache on all SQL paths.** `run_execute` and
  `run_query` now use `Connection::prepare_cached` instead of
  re-preparing on every call. `BEGIN IMMEDIATE` / `COMMIT` /
  `ROLLBACK` go through a new `run_cached_noparams` helper that also
  hits the cache. Biggest win for batched inserts — 45k/s -> 94k/s
  (100 jobs / tx).
- **Bench harness fix.** `bench/stream_bench.py` now yields between
  publishes (`await asyncio.sleep(0)`), so live e2e p50 reflects the
  library instead of the publish-loop duration. p50 went from ~50ms
  (harness artifact) to 0.24ms (real).

### Honest numbers (median of 3, M-series, release)
- `enqueue` single-tx: ~4.5k/s
- `enqueue` batched (100/tx): ~94k/s
- `claim + ack`: ~1k/s
- `publish` single-tx: ~5.7k/s
- replay: ~400k/s
- live stream e2e: p50 = 0.24ms, p99 = 8ms

### Known remaining gap
Raw Python `sqlite3` is ~47k/s single-tx on the same file; we're at
12k/s for plain `litenotify.tx + execute`. The 4x gap is the PyO3
boundary, the writer-mutex acquire+release, and GIL detach/reacquire.
Uncontended fast path planned — `try_acquire` on the writer mutex so
we skip `py.detach` when the slot is immediately free. Documented in
ROADMAP.

## Unreleased — hardening pass 2

Closes the four test gaps from the previous hardening pass. Test suite:
12 Rust + 109 Python (~12 s parallel).

### What got proven

- **SIGKILL mid-transaction crash recovery.** A subprocess opens the DB,
  starts a `BEGIN IMMEDIATE` transaction (enqueue OR notify inside), is
  `os.kill(pid, SIGKILL)`-ed before COMMIT. Afterwards the file passes
  `PRAGMA integrity_check == 'ok'`, the in-flight write did not land
  (zero rows), a fresh writer can acquire the write lock immediately
  (no stale reserved lock from WAL recovery), and a full enqueue +
  claim + ack round-trip still works. For the notify-bearing case, a
  pre-attached listener sees zero leaked notifications from the killed
  tx, and a subsequent committed notify flows normally.
- **Django management-command concurrency.** Two `python manage.py
  joblite_worker` subprocesses against the same `.db` split 200 jobs
  with zero overlap and both workers participate — proving the
  command's signal handler + task registry + `asyncio.run()` wrapper
  doesn't break the `BEGIN IMMEDIATE` claim exclusivity that
  `test_multiprocess.py` already proved for bare joblite. Synchronized
  with a `READY` handshake so the test isn't flaky on the worker-boot
  race.
- **Django request-level end-to-end.** New tests using
  `django.test.AsyncClient` drive the full request cycle through a
  `FakeUserMiddleware` that sets `request.user`. The authorize callable
  receives the real `request.user` instance the middleware installed,
  not `None` (which the existing RequestFactory-based tests would not
  catch). Last-Event-ID replay works end-to-end through the real
  request pipeline, and the deny path still 403s.
- **Authorize callable policy.** Both `JobliteApp(authorize=fn)` and
  `joblite_django.set_authorize(fn)` now accept sync OR async
  callables. Async is detected by looking at the return value (coroutine
  → `await`), so callables with an async `__call__` also work. If
  authorize raises (sync or async), the exception propagates unchanged
  and the framework returns HTTP 500; the SSE stream is never opened,
  and there is no ambiguous half-open state.

### Policy decisions (user-facing)

- **async authorize:** supported. Upgrade is transparent for existing
  sync callables.
- **authorize raises:** propagates as HTTP 500 in both plugins. Users
  wanting a custom error page should install their framework's normal
  exception handler.

### Tests (23 new)

- `test_crash_recovery.py` (4): SIGKILL-mid-enqueue leaves DB clean,
  SIGKILL doesn't leave a stale write lock, SIGKILL-mid-notify produces
  no phantom notification (fresh listener), SIGKILL-mid-notify produces
  no leak into a pre-attached listener.
- `test_joblite_django.py` (+11):
  - Two-workers management-command concurrency (200 jobs, zero overlap).
  - 3× AsyncClient e2e (stream Last-Event-ID replay through middleware,
    subscribe forwards `request.user` to authorize, deny path still 403).
  - 6× authorize policy (helper unit test for sync/async/raise, 2×
    async deny, 2× raise-returns-500, 1× stream raise-returns-500).
  - Changes `settings.configure(MIDDLEWARE=[...])` to install the
    `FakeUserMiddleware` + adds `urlpatterns` that mount
    `subscribe_sse` and `stream_sse`.
- `test_joblite_fastapi.py` (+8): 4× `_run_authorize` unit tests
  (sync/async truthy/falsy, raise propagates, None passes), 4× HTTP
  integration (async deny-subscribe, async deny-stream, sync/async
  raise-returns-500, stream raise-returns-500).

### Code changes (non-test)

- `joblite_fastapi.joblite_fastapi._run_authorize` (new): evaluates
  authorize; awaits if the return is a coroutine; propagates raises.
  Both SSE paths go through it.
- `joblite_django.views._run_authorize` (new): same contract.
- Docstrings on `JobliteApp` and `joblite_django.set_authorize` document
  the new policy.

## Unreleased — hardening pass

Proves the loudest production claims with tests that would have caught real
bugs. Test suite: 12 Rust + 86 Python (~7 s parallel).

### What got proven
- **Multi-process claim exclusivity.** Two worker subprocesses on the same
  `.db` split 200 jobs with zero overlap; a third process enqueues live
  while two workers drain — every job processed exactly once. This is the
  disk-level BEGIN IMMEDIATE story, previously only proven in-process.
- **Stuck-handler reclaim.** A handler hanging past `visibility_timeout_s`
  releases its claim; another worker reclaims via the atomic claim query;
  the stuck worker's eventual `ack()` returns `False` (at-least-once
  contract visible to the caller). Flip side: a long-running handler that
  heartbeats never gets its claim stolen.
- **Resource bounds under churn.** 300 listener create/consume/drop cycles
  leave thread count within `baseline + 20`; 100 simultaneous listeners
  reap back to baseline after drop; 1000 sustained notifications grow RSS
  by < 50 MB.
- **Real SSE reconnect.** Client reads 4 events, disconnects, reconnects
  with the actual `Last-Event-ID` the server sent, receives the remaining
  replay + new events published during the gap. No duplicates, no gaps,
  ids strictly increasing across the seam.
- **Stream failure modes.** `publish(datetime)`, `Decimal`, `set`, or a
  custom class all raise `TypeError` at publish time — no silent swallow,
  no stale notify left in the transaction buffer. A failed publish followed
  by a valid one still works.

### Tests (12 new)
- `test_multiprocess.py` (3): two-process exclusivity, seeder+worker split
  across processes, live-enqueuer while two workers drain.
- `test_outbox.py` (2): stuck-handler reclaim, heartbeat prevents reclaim.
- `test_resource_bounds.py` (3): listener churn, bounded concurrent
  listeners, sustained notify RSS bound.
- `test_joblite_fastapi.py` (2): real mid-stream reconnect with actual
  Last-Event-ID, reconnect without header replays from start.
- `test_stream.py` (2): non-JSON payload raises, failed publish doesn't
  poison subsequent valid publishes.

## Unreleased — notifier per-channel registry refactor

Fixes two real production issues (thread leak, cross-channel starvation)
with a minimal architecture change. Test suite: 12 Rust + 74 Python
(~7 s parallel).

### What changed
- **notifier::Notifier** now keeps a `HashMap<channel, Vec<Subscriber>>` with
  its own per-subscriber `broadcast::channel(1024)` instead of a single
  shared global channel. `subscribe(channel)` returns a `Subscription`
  with `{id, channel, rx}`; the commit hook fans out only to subscribers
  of the channel the message was notifyed on.
- **New `Notifier::unsubscribe(id)`** removes a subscriber by id and drops
  its broadcast::Sender, which causes any `blocking_recv()` waiting on
  that receiver to return `Closed`. Idempotent.
- **litenotify::Listener** gets a `Drop` impl that calls
  `notifier.unsubscribe(self.subscription_id)` — the bridge thread's
  `blocking_recv` unblocks and the thread exits cleanly.
- **Listener channel filter removed** — the per-channel registry means we
  only receive our own channel's messages, so the old
  `if n.channel == self.channel { continue }` loop is dead code.

### Bugs fixed
1. **Thread leak per SSE connection.** Previously, every `db.listen(...)`
   spawned a bridge thread that lived until the whole `Database` was
   dropped, because all subscribers shared one `broadcast::Sender`.
   An SSE-heavy service would accrue threads with no natural bound.
   Now: Python drops the `Listener`, Drop deregisters the subscriber,
   the Sender for that subscriber drops, blocking_recv returns Closed,
   thread exits.
2. **Cross-channel starvation.** Previously, a single 1024-slot ring was
   shared by all subscribers. A flood on channel `"hot"` would push
   messages out of the ring and force a listener on channel `"cold"` to
   drop its single message (visible as `Lagged(_)`). Now: each subscriber
   has its own ring, and the commit hook only routes to channels that
   have live subscribers.

### Tests (5 new)
- Rust: channel routing does not cross between subscribers; cross-channel
  isolation under 5k "hot" msgs + 1 "cold" msg; unsubscribe frees the slot
  and closes the receiver; unsubscribe of unknown id is a no-op; notify with
  no subscribers is dropped silently.
- Python: cross-channel starvation immunity (3000 "hot" + 1 "cold" still
  delivers); churning 50 short-lived listeners doesn't wedge the notifier.

## Unreleased — Day 3

FastAPI SSE Last-Event-ID replay, joblite-django plugin, benchmark harness.
Test suite: 9 Rust + 72 Python (~8 s parallel).

### Additions
- **joblite-fastapi**: new `GET /joblite/stream/{name}` endpoint that uses
  `db.stream(name).subscribe(from_offset=...)` and parses the SSE
  `Last-Event-ID` header for resume. Each yielded event carries
  `id: {event.offset}` so browsers echo it back on reconnect.
  `_parse_last_event_id` tolerates missing, empty, and malformed headers
  (falls back to `from_offset=0`).
- **joblite-django**: new package. `joblite_django.db()` (lazy), `@task(...)`
  registry, async `stream_sse` and `subscribe_sse` views with
  Last-Event-ID + authorize hook, and `python manage.py joblite_worker`
  management command.
- **joblite core**: `Retryable` moved here so Django and FastAPI plugins can
  share the signal without depending on each other. `joblite_fastapi` still
  re-exports it for import-path compatibility.
- **Benchmark harness** (`bench/`): `joblite_bench.py` (enqueue/claim+ack/
  e2e throughput + latency), `stream_bench.py` (publish/replay/live e2e),
  and a `bench/README.md` with baseline numbers. Scripts are standalone
  (no `PYTHONPATH` needed).

### Fixes
- Bench scripts now insert the repo root into `sys.path` themselves.
- FastAPI e2e SSE test refactored to a shared session-scoped uvicorn server,
  cutting startup cost from ~3× (per-test) to ~1× (per-xdist-worker).

### Scoped out
- **Node bindings + joblite-express**: multi-day chunk (napi-rs + Node-side
  joblite port + express plugin). Deferred to its own branch rather than
  shipping a skeleton — see ROADMAP.

### Tests (18 new)
- joblite-fastapi: stream endpoint replay from Last-Event-ID (real HTTP),
  authorize on stream endpoint, malformed Last-Event-ID → 0, out-of-range
  Last-Event-ID stays 200.
- joblite-django: `db()` lazy-opens and memoizes, raises when
  `JOBLITE_DB_PATH` unset, `@task` registers, `set/get_authorize`, stream
  view returns `StreamingHttpResponse`, authorize blocks both views,
  management command consumes one job then shuts down on SIGINT.

## Unreleased — Day 2 features

Stream + outbox. Test suite: 9 Rust + 51 Python (~2.5 s parallel).

### Additions
- **joblite.stream**: durable pub/sub. `db.stream(name).publish(payload, tx=?)`
  inserts into `_joblite_stream` with an auto-incrementing offset and notifications
  `joblite:stream:{name}`. `subscribe(from_offset=?, consumer=?)` yields
  `Event` objects: replays rows with `offset > from_offset` in batches,
  transitions to live NOTIFY delivery when caught up. Named consumers can
  resume via `save_offset(consumer, offset)` / `get_offset(consumer)`; offset
  saves are monotonic (lower values ignored).
- **joblite.outbox**: transactional side-effect delivery built on `Queue`.
  `db.outbox(name, delivery=fn)` takes a user-supplied sync or async handler.
  `outbox.enqueue(payload, tx=?)` couples the side effect to the business
  write. `outbox.run_worker(worker_id)` drives delivery; failures retry with
  exponential backoff (`base_backoff_s * 2^(attempts-1)`) up to
  `max_attempts`, then land in `dead`.
- `Database.stream(name)` and `Database.outbox(name, delivery=)` are memoized
  like `Database.queue(name)`.

### Tests (14 new)
- Stream: publish/read-back, in-tx atomicity, rollback drops event, monotonic
  offset save, replay→live iterator, `from_offset` skip, named-consumer
  resume, two consumers at different offsets, memoization.
- Outbox: delivery called + acked, retry on exception then success, rollback
  atomicity, in-tx atomicity with business write, memoization.

## Unreleased — Day 1 stabilization

Fixes and tests that close out the Day 1 scope (notifier, litenotify, joblite,
joblite-fastapi). Test suite: 9 Rust + 37 Python (~2 s end to end, parallel).

### Fixes
- **litenotify**: convert Python params to typed `rusqlite::Value`
  (int/float/None/bool/bytes/str) instead of `.str()`-stringifying everything.
  Prior behavior silently broke numeric comparisons, `run_at`/`priority`
  ordering, and any integer math on inserted values.
- **litenotify**: always return the writer connection to the pool in
  `Transaction.__exit__`, even when the body raised, `COMMIT` failed, or the
  object is dropped unclosed. Previously the writer slot leaked on failures.
- **litenotify**: reader/writer pool split. A single dedicated writer
  serializes via `BEGIN IMMEDIATE`; a bounded reader pool
  (`max_readers`, default 8) handles `db.query()` concurrently under WAL.
- **litenotify**: `tx.notify(channel, payload)` now accepts `dict`/`list`/`str`
  and `json.dumps`-encodes non-string payloads to match the plan's API.
- **notifier**: documented inline that `commit_hook` does NOT fire for
  `BEGIN DEFERRED` transactions with no writes (SQLite fast-paths them).
  Library contract requires `BEGIN IMMEDIATE`; regression test locks it in.
- **litenotify Listener bridge**: replaced `pyo3_async_runtimes::future_into_py`
  with a `std::thread` that does `broadcast::blocking_recv` and then
  `loop.call_soon_threadsafe(queue.put_nowait, notif)` at delivery time. The
  previous bridge was statically bound to pyo3-async-runtimes' runtime, so
  any embedder running on a different asyncio loop (starlette's TestClient
  portal, anyio portals, Jupyter kernels) saw listeners silently hang. The
  new bridge captures the running loop at `__aiter__` time and works on any
  asyncio event loop. Drops the `pyo3-async-runtimes` dependency.
- **joblite**: `Queue.heartbeat(job_id, worker_id, extend_s)` extends the
  claim only when `worker_id` matches and state is `processing`.
- **joblite**: `joblite.open(path)` now returns a wrapper with
  `db.queue(name, ...)` (memoized) so the plan's advertised API actually
  works.
- **joblite-fastapi**: `JobliteApp(authorize=fn, subscribe_path=...,
  user_dependency=...)` plus a `GET {subscribe_path}` SSE endpoint that
  bridges `db.listen()` and honors the authorize callable.
- **joblite-fastapi**: worker loops now distinguish `Retryable` (scheduled
  retry) from other exceptions (also retry, but with a generic 60 s delay
  and the traceback in `last_error`).

### Tests

**Rust (notifier)** — 9 tests:
- rollback drops pending notifications
- commit fans out one copy per subscriber
- multiple subscribers each receive every notification
- multiple notifications in one tx deliver in order
- savepoint partial rollback behavior is documented
- full rollback drops everything, even after savepoints
- unicode + 1 MB payload round-trip
- subscribe-before-attach is safe
- `BEGIN DEFERRED` read-only transaction loses notifications (locked-in contract)

**Python litenotify** — 16 tests covering param type fidelity, numeric
comparisons, unsupported types, listener channel isolation, multi-listener
fanout, rollback-drops-notification, dict/list payloads, connection pool release on
success/rollback/body-exception/commit-error, slow listener not blocking the
commit hook, `BEGIN IMMEDIATE` under concurrent writers (3 threads × 20
writes), readers concurrent with a held writer, reader pool for `db.query()`,
and PRAGMA verification.

**Python joblite** — 15 tests. All six PLAN must-pass cases covered:
two workers racing on `claim()` → one winner; expired claim loses ack and is
reclaimable; heartbeat rejects mismatched `worker_id`; `BEGIN IMMEDIATE`
under concurrent readers; plus priority, delayed `run_at`, max_attempts →
dead, fail → dead, rollback drops enqueue, worker wakes on NOTIFY (< 2 s vs
5 s polling fallback), and queue memoization.

**Python joblite-fastapi** — 6 tests: worker boot/shutdown, request-tx
atomicity (business rollback drops the job), `Retryable` flow retries then
dies, SSE auth rejects (403), user-dependency is forwarded to the authorize
callable, and an end-to-end SSE delivery test against a real uvicorn
subprocess (sync TestClient + httpx ASGI transports both buffer streaming
chunks in a way that never delivers the first event — this is a harness
limitation, not a library one, so we run a real HTTP server).
