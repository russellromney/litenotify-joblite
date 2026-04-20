# ROADMAP

## Phase naming

Phases use the format `Phase <Name>`, not numbers — avoids renumbering
when inserting new phases. Names are unique, picked from space missions,
battles, mountains, or epic-sounding words. Each phase header includes
adjacency links (`After: · Before:`). Subphases use letters.

## Phase Shakedown — Pre-launch correctness sweep

> After: (first named phase) · Before: Phase Submodule

Cleanup pass found during the pre-HN-launch review. Each item is a
semi-hidden race or footgun: the code appears to work because the
happy path masks the window where a commit, registration, or
configuration choice silently doesn't do what the caller expects.

### a. Subscribe-before-first-read race (DONE)

`_WorkerQueueIter`, `Queue.wait_result`, and `Listener` previously
snapshotted some state (claim result / get_result / MAX(id)) and then
subscribed to `wal_events()`. A commit landing between snapshot and
subscribe fired its WAL tick to no subscriber, leaving the consumer
parked on its paranoia timeout (15s / idle_poll_s) instead of the
stat-poll cadence.

Fixed by subscribing eagerly in `__init__` (or before the first check
in `wait_result`). `_StreamIter` was already correct and unchanged.
Regression guard: `tests/test_subscribe_race.py` — 6 tests covering
the structural invariant (`_wal` set at construction) and the
behavioral bound (wake in <1s, not 15s/5s fallback).

### b. Scheduler silent no-op when `_registered` is empty (DONE)

`Scheduler.run()` previously returned immediately without acquiring
the leader lock if the current process hadn't called `.add()`. A
deploy with a dedicated "runner" process (tasks registered by a
different process / CLI / migration) silently no-opped — no logs, no
lock contention, no fires.

Fixed in `_scheduler.py`: `run()` now checks
`_honker_scheduler_tasks` when local `_registered` is empty. If rows
exist, proceed. If none, raise `RuntimeError` with a clear message.
Regression guards: `test_scheduler_run_raises_without_tasks` and
`test_scheduler_run_proceeds_if_another_process_registered` in
`tests/test_scheduler.py`.

### c. Scheduler does not wake on new registrations mid-sleep (TODO)

Main loop sleeps until `honker_scheduler_soonest()`. If another
process registers a new task whose `next_fire_at` is earlier than the
current sleep target, the leader doesn't notice until the current
sleep ends.

**Fix:** have `honker_scheduler_register` fire a notify on a reserved
channel (e.g. `honker:scheduler`). Main loop races its timer sleep
against `wal_events()` so a new registration kicks it out of the
sleep early.

### d. `honker_bootstrap` should assert WAL mode (TODO)

The loadable extension is loaded into any client's connection,
inheriting whatever PRAGMAs the user set. A client in
`journal_mode=DELETE` gets tables that work (INSERTs into
`_honker_*`) but no other process can wake on the WAL because there
isn't one. The setup looks fine until nothing fires.

**Fix:** in the `honker_bootstrap` SQL function, query
`PRAGMA journal_mode` and raise a SQL error if it's not `wal`. The
Python binding already opens in WAL (`open_conn`) so it's unaffected;
the check catches extension users with mis-PRAGMA'd connections.
Skip the check if the DB is `:memory:` (in-memory can't be WAL —
used by Rust unit tests).

### e. Listener + `prune_notifications` interaction (DONE, docs-only)

Pruning deletes rows. A listener offline (or slow) during prune loses
events pruned past its `last_seen`. Was documented on `Listener` but
not cross-referenced from `prune_notifications` — easy footgun for
users who set up an hourly cron to trim the table and wonder why
subscribers occasionally miss events.

Fixed: `prune_notifications` docstring now includes an explicit
warning pointing offline-tolerant consumers at `db.stream(...)`.

### f. WalEvents thread-leak regression test (DONE, already covered)

Existing `tests/test_resource_bounds.py::test_listener_churn_does_not_leak_threads`
and `test_many_simultaneous_listeners_bounded_thread_count` exercise
the `Drop → unsubscribe → bridge-thread-exit` chain (300× and 100×
respectively), since `Listener` uses `db.wal_events()` internally.
No new test needed.

### g. Scheduler lock pause-tolerance (DONE, docs-only)

`LOCK_TTL=60`, `HEARTBEAT_INTERVAL=30`. If the leader pauses >60s
(GC, laptop sleep, kernel OOM pressure), another process can acquire
the lock and both fire the same tasks. Acceptable for idempotent
cron work but not for long-running exclusive tasks.

Fixed: `Scheduler` class docstring now has an explicit caveat
paragraph recommending a second `db.lock('task-name', ttl=...)`
inside the task body for exclusive work. No code change — the right
design, just needs to be said.

### Remaining (post-launch candidates)

- **(c) Scheduler wake-on-new-registration** — needs Rust change
  (`honker_scheduler_register` emits a wake on `honker:scheduler`)
  plus Python scheduler loop racing its timer against `wal_events()`.
  Not blocking: late-bound registrations still fire on the next tick.
- **(d) `honker_bootstrap` WAL mode assertion** — needs Rust change
  with a `:memory:` carve-out for in-memory tests. Not blocking:
  mis-PRAGMA'd extension users are a narrow edge case, and the
  Python binding is unaffected.

## Shipped

- Cross-process NOTIFY/LISTEN via a `_honker_notifications` table
  and a 1 ms stat-polling WAL-file watcher.
- `honker.Queue` (at-least-once with visibility timeout, partial-index
  claim, `claim_batch` / `ack_batch`), `honker.Stream` (durable
  pub/sub with per-consumer offsets), `honker.Outbox`.
- `honker.Database.prune_notifications(older_than_s, max_keep)` —
  user-invoked; no magic background timer.
- Framework plugins for FastAPI / Django / Flask (since cut — see
  "Framework plugins (cut for now)" below).
- SQLite loadable extension (`libhonker_ext.dylib`/`.so`) with
  `honker_bootstrap()`, `honker_claim_batch()`, `honker_ack_batch()` SQL
  functions for callers that don't go through the Python API.
- Node.js binding via napi-rs (`honker-node`). Cross-language
  interop tested: Python subprocess fires notifications, Node
  subscriber receives them via `walEvents.next()` + `SELECT`.
- Independently buildable Python wheels: `honker`, `honker-fastapi`,
  `honker-django`, `honker-flask`.
- `honker-core` rlib shared by all three bindings (PyO3 / SQLite
  extension / napi-rs). Eliminated ~200 lines of duplicated
  writer/readers/watcher/notify-install code.
- **Adversarial-review fix-up** (one commit). Surfaced and fixed:
  - Extension `honker_ack_batch` now DELETEs (matches Python). Schema DDL
    moved to `honker-core::bootstrap_honker_schema` so the
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
    honker; plugins no longer duplicate the format string.
  - `bench/wake_latency_bench.py` + `tests/test_cross_process_wake_latency.py`
    pin the wake-latency claim (measured 1.2 ms p50 / 2.4 ms p90 on
    M-series). README rewritten around what's actually bounded:
    trigger sub-ms, consumer wake = 1 ms stat-poll interval.
  - Silent-pass tests (`test_slow_listener_does_not_block_writer`,
    `test_cross_channel_starvation_immune`) rewritten against the
    current architecture's equivalent invariants.
  - Stale comments referencing the removed Notifier / commit-hook
    broadcast swept across honker, tests, and bench.

## Next (post-correctness-push)

### Repo layout → packages as git submodules

Done: all language / framework packages moved into `packages/` —
each is a self-contained subdirectory with its own
`Cargo.toml` / `pyproject.toml` / `package.json`, ready to be split
into its own GitHub repo and re-added as a git submodule.

To do (requires authenticated repo creation on GitHub):

- [ ] For each of `packages/honker`, `packages/honker-node`,
  `packages/honker`:
  1. Create a new GitHub repo under `russellromney/<package-name>`.
  2. `git subtree split --prefix=packages/<name> -b split-<name>`
     to produce a branch containing only that subtree's history.
  3. Push the branch to the new repo.
  4. `git rm -r packages/<name>` in the main repo.
  5. `git submodule add <repo-url> packages/<name>`.
  6. Verify `cargo test -p honker-core`, `pytest tests/`,
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

Thesis: every new language binding (Go, Ruby, TypeScript honker-node)
would otherwise re-implement the same SQL + cron parsing + scheduler
logic. The less code that lives in the language wrapper, the easier
each new binding is. Six-commit refactor:

- [x] **1. Move Rust helpers from `honker-extension/src/lib.rs`
  to `honker-core::honker_ops`.** Add
  `attach_honker_functions(conn)`. Extension collapses to
  `attach_notify + attach_honker_functions`. PyO3 `Database.new()`
  also calls `attach_honker_functions` so Python can use
  `SELECT honker_*(...)` without loading the `.dylib`.
- [x] **2. Fill the gap.** Add `honker_enqueue`, `honker_retry`, `honker_fail`,
  `honker_heartbeat`, `honker_ack` (the singular, not just `honker_ack_batch`).
  These are currently Python-only; every other binding would have to
  reinvent them. Enqueue param coercion (delay / run_at / expires)
  lives in Rust.
- [x] **3. Python inline SQL → `SELECT honker_*()`.** Replace every inline
  SQL string in `_honker.py` (roughly `Queue.ack`, `Queue.retry`,
  `Queue.fail`, `Queue.heartbeat`, `Queue.claim_batch`,
  `Queue.ack_batch`, `Queue.sweep_expired`, `Database.lock`,
  `Database.try_rate_limit`, `Database.sweep_rate_limits`,
  `Scheduler._record_fire`, `Queue.save_result`, `Queue.get_result`,
  `Queue.sweep_results`) with calls to the corresponding `honker_*`
  function. ~200 LOC reduction.
- [x] **4. Cron parser to Rust.** `honker_cron_next_after(expr, from_unix)
  -> unix_ts`. Python's `CronSchedule` class collapses to a marker
  holding the expression string; `next_after()` is one SQL call. 100
  lines of Python parsing deleted.
- [x] **5. Scheduler state + fire-due to Rust.** New SQL functions
  `honker_scheduler_register(name, queue, cron_expr, payload, priority,
  expires)` and `honker_scheduler_tick() -> JSON of due fires`. Python
  `Scheduler.run()` collapses to ~40 lines of asyncio glue around
  lock + tick + enqueue + sleep + heartbeat. Tasks stored in DB,
  not in a Python dict.
- [x] **6. Stream ops to Rust.** `honker_stream_publish`,
  `honker_stream_read_since`, `honker_stream_save_offset`,
  `honker_stream_get_offset`. `_StreamIter` becomes a thin iterator
  around `SELECT honker_stream_read_since(...)`. Auto-save threshold
  logic stays in Python (language-specific async control flow) but
  delegates all SQL to core.

After the six: per-language wrapper is ~300 LOC of context-manager /
iterator / timer glue + payload wrapping. Anything SQL-shaped lives
in `honker-core`. New bindings inherit the feature set by
loading the extension + writing that glue.

### Bindings

- [ ] **Refactor `honker` loadable extension toward a pure
  `sqlite-loadable-rs` build exposing `honker_get_fd()`** — a
  host-language-async-friendly alternative to stat-polling, letting
  runtimes `await` on a commit-hook fd without any poll loop.
  Separate track from the current stat-poll watcher (which stays
  as the cross-platform baseline).
- [ ] **`honker-node`**: TypeScript port of `honker.Queue` /
  `Stream` / `Outbox` built on `@honker/node`. Symmetrizes the
  cross-language story.
- [ ] **Go and Ruby bindings**. Go via cgo over a C ABI that
  `honker-core` would export; Ruby via magnus.

### Framework plugins (cut for now)

FastAPI / Django / Flask plugins were dropped: the core API is small
enough that wiring honker into a web framework is ~20 lines, which
we can show as a cookbook example in the README rather than ship as
three maintained packages. If a real user asks for a packaged
version, bring them back as their own repositories. Likewise for
Express, Rails, and any future framework.

## 1.0 release prep (separate milestone)

- **GitHub Actions CI**: Linux + macOS matrix (`cargo test -p
  honker-core`, `pytest tests/`, `npm test`). Land first.
- **Windows test run.** Expect WAL-file-locking edge cases — real
  work if it breaks.
- **Maturin wheels**: `honker` × Python 3.11 / 3.12 / 3.13 × Linux
  / macOS / Windows × x86_64 / arm64.
- **Pure-Python wheels**: `honker-fastapi`, `honker-django`,
  `honker-flask`.
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
  group-scoped atomic UPDATE on `_honker_stream_consumers` on top
  of the existing stream. No schema change required. Not needed
  until someone asks.
- **`honker-node` direct claim/ack**. Node binding already
  exposes the primitives; a `honker-node` Queue wrapping them is
  the natural next step.

## Docs

- Publish benchmark baselines for reference hardware beyond the
  M-series + release build numbers in bench/README.
- A small `docs/` site with runnable snippets per binding plus
  wire-it-in recipes for FastAPI / Django / Flask / Express / Rails.
