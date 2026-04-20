# ROADMAP

## Shipped

- Cross-process NOTIFY/LISTEN via a `_litenotify_notifications` table
  and a 1 ms stat-polling WAL-file watcher.
- `joblite.Queue` (at-least-once with visibility timeout, partial-index
  claim, `claim_batch` / `ack_batch`), `joblite.Stream` (durable
  pub/sub with per-consumer offsets), `joblite.Outbox`.
- `joblite.Database.prune_notifications(older_than_s, max_keep)` —
  user-invoked; no magic background timer.
- Framework plugins for FastAPI / Django / Flask (since cut — see
  "Framework plugins (cut for now)" below).
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
  `packages/joblite`:
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

All task-queue Batch items (Batch 1 + 2 + crontab + task result
storage) have shipped. See CHANGELOG for the summary.

Pipelines / chains / groups / chords remain **out of scope for v1**
— the build-it-yourself pattern (task A enqueues task B on success)
is good enough and avoids the complexity.

### Maximize Rust / minimize per-language code (in flight)

Thesis: every new language binding (Go, Ruby, TypeScript joblite-node)
would otherwise re-implement the same SQL + cron parsing + scheduler
logic. The less code that lives in the language wrapper, the easier
each new binding is. Six-commit refactor:

- [x] **1. Move Rust helpers from `litenotify-extension/src/lib.rs`
  to `litenotify-core::joblite_ops`.** Add
  `attach_joblite_functions(conn)`. Extension collapses to
  `attach_notify + attach_joblite_functions`. PyO3 `Database.new()`
  also calls `attach_joblite_functions` so Python can use
  `SELECT jl_*(...)` without loading the `.dylib`.
- [x] **2. Fill the gap.** Add `jl_enqueue`, `jl_retry`, `jl_fail`,
  `jl_heartbeat`, `jl_ack` (the singular, not just `jl_ack_batch`).
  These are currently Python-only; every other binding would have to
  reinvent them. Enqueue param coercion (delay / run_at / expires)
  lives in Rust.
- [x] **3. Python inline SQL → `SELECT jl_*()`.** Replace every inline
  SQL string in `joblite.py` (roughly `Queue.ack`, `Queue.retry`,
  `Queue.fail`, `Queue.heartbeat`, `Queue.claim_batch`,
  `Queue.ack_batch`, `Queue.sweep_expired`, `Database.lock`,
  `Database.try_rate_limit`, `Database.sweep_rate_limits`,
  `Scheduler._record_fire`, `Queue.save_result`, `Queue.get_result`,
  `Queue.sweep_results`) with calls to the corresponding `jl_*`
  function. ~200 LOC reduction.
- [x] **4. Cron parser to Rust.** `jl_cron_next_after(expr, from_unix)
  -> unix_ts`. Python's `CronSchedule` class collapses to a marker
  holding the expression string; `next_after()` is one SQL call. 100
  lines of Python parsing deleted.
- [x] **5. Scheduler state + fire-due to Rust.** New SQL functions
  `jl_scheduler_register(name, queue, cron_expr, payload, priority,
  expires)` and `jl_scheduler_tick() -> JSON of due fires`. Python
  `Scheduler.run()` collapses to ~40 lines of asyncio glue around
  lock + tick + enqueue + sleep + heartbeat. Tasks stored in DB,
  not in a Python dict.
- [x] **6. Stream ops to Rust.** `jl_stream_publish`,
  `jl_stream_read_since`, `jl_stream_save_offset`,
  `jl_stream_get_offset`. `_StreamIter` becomes a thin iterator
  around `SELECT jl_stream_read_since(...)`. Auto-save threshold
  logic stays in Python (language-specific async control flow) but
  delegates all SQL to core.

After the six: per-language wrapper is ~300 LOC of context-manager /
iterator / timer glue + payload wrapping. Anything SQL-shaped lives
in `litenotify-core`. New bindings inherit the feature set by
loading the extension + writing that glue.

### Bindings

- [ ] **Refactor `litenotify` loadable extension toward a pure
  `sqlite-loadable-rs` build exposing `litenotify_get_fd()`** — a
  host-language-async-friendly alternative to stat-polling, letting
  runtimes `await` on a commit-hook fd without any poll loop.
  Separate track from the current stat-poll watcher (which stays
  as the cross-platform baseline).
- [ ] **`joblite-node`**: TypeScript port of `joblite.Queue` /
  `Stream` / `Outbox` built on `@litenotify/node`. Symmetrizes the
  cross-language story.
- [ ] **Go and Ruby bindings**. Go via cgo over a C ABI that
  `litenotify-core` would export; Ruby via magnus.

### Framework plugins (cut for now)

FastAPI / Django / Flask plugins were dropped: the core API is small
enough that wiring joblite into a web framework is ~20 lines, which
we can show as a cookbook example in the README rather than ship as
three maintained packages. If a real user asks for a packaged
version, bring them back as their own repositories. Likewise for
Express, Rails, and any future framework.

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
- A small `docs/` site with runnable snippets per binding plus
  wire-it-in recipes for FastAPI / Django / Flask / Express / Rails.
