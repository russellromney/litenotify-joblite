# ROADMAP

## Shipped

- Cross-process NOTIFY/LISTEN via a `_litenotify_notifications` table
  and a 1 ms stat-polling WAL-file watcher.
- `joblite.Queue` (at-least-once with visibility timeout, partial-index
  claim, `claim_batch` / `ack_batch`), `joblite.Stream` (durable
  pub/sub with per-consumer offsets), `joblite.Outbox`.
- `joblite.Database.prune_notifications(older_than_s, max_keep)` —
  user-invoked; no magic background timer.
- Framework plugins: `joblite_fastapi`, `joblite_django`, `joblite_flask`.
- SQLite loadable extension (`liblitenotify_ext.dylib`/`.so`) with
  `jl_bootstrap()`, `jl_claim_batch()`, `jl_ack_batch()` SQL
  functions for callers that don't go through the Python API.
- Node.js binding via napi-rs (`litenotify-node`). Cross-language
  interop tested: Python subprocess fires notifications, Node
  subscriber receives them via `walEvents.next()` + `SELECT`.
- Independently buildable Python wheels: `joblite`, `joblite-fastapi`,
  `joblite-django`, `joblite-flask`.
- `litenotify-core` rlib shared by all three bindings (PyO3 / SQLite
  extension / napi-rs). Eliminated ~200 lines of duplicated
  writer/readers/watcher/notify-install code.
- **Adversarial-review fix-up** (one commit). Surfaced and fixed:
  - Extension `jl_ack_batch` now DELETEs (matches Python). Schema DDL
    moved to `litenotify-core::bootstrap_joblite_schema` so the
    Python binding and extension can't drift on column counts.
    `tests/test_extension_interop.py` asserts cross-binding behavior.
  - `tx.notify` payload serialization normalized to unconditional
    `json.dumps` across PyO3 + Node. PyO3 `tx.notify` returns the
    inserted id. Node `Transaction::notify` accepts any
    JSON-serializable value. Round-trip tests in all three bindings.
  - Shared WAL watcher: one stat-poll thread per `Database`, N
    subscribers. Auto-unsubscribe via Drop on `WalEvents` (PyO3 +
    Node). 100 listeners now use 1 stat thread, not 200.
  - Flask SSE disconnect hardening. `_StreamBridge` catches
    `GeneratorExit` via `try/finally`, cancels the drive task,
    joins the thread. `JobliteFlask._active_streams` (WeakSet)
    tracks live bridges for test assertions.
  - Node binding: `Writer::try_acquire` fast path for transactions
    (matches PyO3 parity). `WalEvents` Drop stops its subscription.
  - `Listener` docstring rewritten to remove the auto-pruning lie.
  - `prune_notifications` pre-computes `MAX(id)` in Python instead
    of a correlated subquery. OR-semantics documented.
  - Django plugin: `user_factory(request)` optional; default reads
    `request.user` with a clear error if auth middleware missing.
  - `build_worker_id(framework, instance_id, queue, i)` helper in
    joblite; plugins no longer duplicate the format string.
  - `bench/wake_latency_bench.py` + `tests/test_cross_process_wake_latency.py`
    pin the wake-latency claim (measured 1.2 ms p50 / 2.4 ms p90 on
    M-series). README rewritten around what's actually bounded:
    trigger sub-ms, consumer wake = 1 ms stat-poll interval.
  - Silent-pass tests (`test_slow_listener_does_not_block_writer`,
    `test_cross_channel_starvation_immune`) rewritten against the
    current architecture's equivalent invariants.
  - Stale comments referencing the removed Notifier / commit-hook
    broadcast swept across joblite, tests, and bench.

## Next (post-correctness-push)

### Repo layout → packages as git submodules

Done: all language / framework packages moved into `packages/` —
each is a self-contained subdirectory with its own
`Cargo.toml` / `pyproject.toml` / `package.json`, ready to be split
into its own GitHub repo and re-added as a git submodule.

To do (requires authenticated repo creation on GitHub):

- [ ] For each of `packages/litenotify`, `packages/litenotify-node`,
  `packages/joblite`, `packages/joblite_fastapi`,
  `packages/joblite_django`, `packages/joblite_flask`:
  1. Create a new GitHub repo under `russellromney/<package-name>`.
  2. `git subtree split --prefix=packages/<name> -b split-<name>`
     to produce a branch containing only that subtree's history.
  3. Push the branch to the new repo.
  4. `git rm -r packages/<name>` in the main repo.
  5. `git submodule add <repo-url> packages/<name>`.
  6. Verify `cargo test -p litenotify-core`, `pytest tests/`,
     `npm test` still pass.
- [ ] Update CI once it exists to `git submodule update --init --recursive`
  before running the test matrix.
- [ ] Each package's own repo gets its own release / versioning /
  publishing flow (PyPI, npm). The umbrella repo pins specific
  submodule commits for "the official stack".

### Task queue features (huey parity, minus pipelines)

Ordered by value / effort. The "value" column is my best guess at
how often the feature actually matters in real workloads.

- [x] **Handler timeout.** `@task(timeout=N)` wraps handler calls in
  `asyncio.wait_for` via the shared `joblite._worker.run_task` helper.
  Closed the reclaim-while-still-running correctness hole.
- [x] **`delay=` kwarg on `enqueue`.** `Queue.enqueue(..., delay=60)`
  sugar for `run_at=time.time() + 60`.
- [x] **Declarative retries.** `@task(retries=3, retry_delay=60,
  backoff=2.0)` in plugin decorators. Worker catches exceptions and
  applies the exponential-backoff formula via `run_task`. Retryable
  exceptions honor the caller's own `delay_s`.
- [x] **Task expiration.** `Queue.enqueue(expires=60)` sets
  `expires_at`. Claim path filters expired rows.
  `queue.sweep_expired()` moves them to `_joblite_dead`.
- [x] **Task locking.** `with db.lock(name, ttl=60): ...` via a new
  `_joblite_locks` table. Raises `joblite.LockHeld` if the lock is
  held. TTL bounds how long a crashed holder can block others.
- [x] **Rate-limiting.** `db.try_rate_limit(name, limit, per)`
  returns True/False. Fixed-window counter in `_joblite_rate_limits`.
  `db.sweep_rate_limits()` reclaims stale windows.
- [ ] **Crontab / periodic tasks.** `@periodic_task(crontab(minute='0',
  hour='3'))`. A scheduler process (dedicated CLI or in-process
  background task) enqueues periodic tasks at their cron boundaries.
  Needs: a crontab parser (either vendor from croniter or a minimal
  built-in), a scheduler loop that wakes at the next boundary and
  calls `enqueue`, and a way to avoid double-firing across multiple
  scheduler processes (simplest: leader election via
  `db.lock('scheduler', ttl=N)` — which we now have). ~4 hours.
- [ ] **Task result storage.** `job.result(timeout=...)` returns the
  handler's return value. New `_joblite_results(id, value, expires_at)`
  table, worker UPSERTs on success, caller polls (or awaits a WAL
  wake, then SELECTs). TTL prune. Opens the door to
  pipelines/chains/groups later if anyone asks, but keep those
  **out of scope for v1** — they add real complexity for limited
  real-world use. ~1 day.

### Bindings + framework plugins

- [ ] **Refactor `litenotify` loadable extension toward a pure
  `sqlite-loadable-rs` build exposing `litenotify_get_fd()`** — a
  host-language-async-friendly alternative to stat-polling, letting
  runtimes `await` on a commit-hook fd without any poll loop.
  Separate track from the current stat-poll watcher (which stays
  as the cross-platform baseline).
- [ ] **`joblite-node`**: TypeScript port of `joblite.Queue` /
  `Stream` / `Outbox` built on `@litenotify/node`. Symmetrizes the
  cross-language story.
- [ ] **`joblite-express`**: Express middleware wrapping
  `@litenotify/node` + a TypeScript `Queue`. SSE endpoint, worker
  pool, `authorize` hook.
- [ ] **Go and Ruby bindings**. Go via cgo over a C ABI that
  `litenotify-core` would export; Ruby via magnus.
- [ ] **Rails plugin** once the Ruby binding lands. Same shape as
  FastAPI/Django/Flask: SSE endpoints, `authorize` hook, task
  decorator.

## 1.0 release prep (separate milestone)

- **GitHub Actions CI**: Linux + macOS matrix (`cargo test -p
  litenotify-core`, `pytest tests/`, `npm test`). Land first.
- **Windows test run.** Expect WAL-file-locking edge cases — real
  work if it breaks.
- **Maturin wheels**: `joblite` × Python 3.11 / 3.12 / 3.13 × Linux
  / macOS / Windows × x86_64 / arm64.
- **Pure-Python wheels**: `joblite-fastapi`, `joblite-django`,
  `joblite-flask`.
- **npm publish** with napi-rs cross-compile prebuilds.
- **Health / observability primitives**: claim depth, DLQ rate,
  WAL-watcher firing rate. Integration with OpenTelemetry.
- Crash-recovery tests beyond writer kills: listener-process kills,
  bridge-thread panics, mid-checkpoint disk-full.

## Perf

- **Shave PyO3 / mutex / GIL overhead off single-tx.** Measured
  gap to raw `sqlite3` is being re-measured as part of the
  correctness push; current ROADMAP says 3×, README says 4×.
  `prepare_cached` and `try_acquire` already applied; further gains
  would need reducing PyO3 call count per tx (e.g. a combined
  `execute_tx(stmts)` that batches BEGIN + body + COMMIT in one
  call). Low priority — batched workloads already clear 100 k/s.
- **Stream consumer groups**. Kafka-style "competing consumers
  within a named group, each with a shared advancing offset":
  group-scoped atomic UPDATE on `_joblite_stream_consumers` on top
  of the existing stream. No schema change required. Not needed
  until someone asks.
- **`litenotify-node` direct claim/ack**. Node binding already
  exposes the primitives; a `joblite-node` Queue wrapping them is
  the natural next step.

## Docs

- Publish benchmark baselines for reference hardware beyond the
  M-series + release build numbers in bench/README.
- A small `docs/` site with runnable snippets per binding and
  framework plugin.
