# ROADMAP

Roadmap items are future work. Completed phases move to
`CHANGELOG.md`; this file should not carry shipped history.

## Phase naming

Phases use the format `Phase <Name>`, not numbers, so we can insert
new work without renumbering. Names are unique. Each phase header
should include adjacency links (`After: ... · Before: ...`) when the
ordering matters.

## Phase Ranger — Delegate Locks To Bouncer

> After: pre-1.0 cleanup · Before: 1.0 release prep

Replace Honker's internal named-lock lease implementation with
`bouncer-core` while preserving Honker's public lock APIs.

### Scope

- Add a versioned `bouncer-core` dependency to `honker-core`.
- Bootstrap `bouncer_resources` as part of Honker bootstrap.
- Reimplement `honker_lock_acquire(name, owner, ttl_s)` using
  `bouncer_core::claim` / `claim_in_tx`.
- Reimplement `honker_lock_release(name, owner)` using
  `bouncer_core::release` / `release_in_tx`.
- Add a Honker renew path for scheduler and binding heartbeats,
  backed by `bouncer_core::renew` / `renew_in_tx`.
- Preserve Python `db.lock(...)`, Rust `db.try_lock(...)`, and SQL
  `honker_lock_*` return shapes.
- Keep Bouncer fencing tokens internal for now; Honker's lock API can
  stay boolean.

### Non-goals

- Do not expose Bouncer's full public API through Honker.
- Do not rename `honker_lock_acquire` / `honker_lock_release`.
- Do not migrate live `_honker_locks` rows across upgrade; these are
  ephemeral TTL rows and can expire naturally.
- Do not move queue job visibility claims to Bouncer. Job claims are
  queue semantics, not named resource ownership.
- Do not extract rate limiting here.

### Verification

- Existing Python lock tests pass unchanged except assertions that
  reached into `_honker_locks` internals.
- Existing Rust `honker-rs` advisory-lock tests pass.
- Existing scheduler leader tests pass.
- New SQL interop test proves Honker lock acquire and Bouncer
  inspection observe the same resource row.
- New regression test proves losing the Bouncer lease causes the
  scheduler leader loop to exit before firing again.

## Phase Submodule — Binding CI And Interop

> After: current PR #13 cleanup · Before: 1.0 release prep

The remaining test-regime gaps are cross-binding and cross-platform.
Keep them split into reviewable slices instead of growing one giant CI
change.

### Smoke-build each binding

- `packages/honker-bun`
- `packages/honker-cpp`
- `packages/honker-ex`
- `packages/honker-go`
- `packages/honker-rs`
- `packages/honker-ruby`

Start with build-only jobs on Linux. Add full test jobs only where the
per-binding setup is cheap and deterministic.

### Cross-binding interop

Add at least one more pair beyond Python <-> Node. Ruby <-> Python is
the likely next pair because both have compact setup and direct access
to the same `.db` file.

### Windows follow-ups

Tracked by issue #11:

- Re-enable Windows for `rust-extension` after the loadable-extension
  FFI panic is understood.
- Re-enable Windows for Python after the watcher/database close path
  stops tempdir cleanup from hitting locked files.
- Re-enable Windows for Node after the same close-path fix and after
  `cross_lang.js` stops hard-coding Unix venv paths.

## Phase Ballmer — .NET C# Binding

> After: Phase Submodule · Before: Phase Wake Parity

Track issue #28 by adding an idiomatic .NET binding as a thin wrapper
around the SQLite loadable extension. The intent is to let LLM-assisted
porting do most of the mechanical work from the existing Go, Ruby,
Node, and Rust binding patterns, while keeping human review focused on
native packaging, cancellation semantics, and cross-platform behavior.

### Shape

- Create a standalone `honker-dotnet` / `Honker` package repo and pin it
  here as a submodule once the first parity slice is usable.
- Target modern .NET first, using `Microsoft.Data.Sqlite` as the default
  managed SQLite layer.
- Load `honker-extension` on each opened connection and call
  `honker_bootstrap()` during `Honker.Database.Open(...)`.
- Package native `honker-extension` binaries under NuGet
  `runtimes/<rid>/native/` paths, with explicit resolver tests for
  Linux, macOS, and Windows.
- Prefer typed wrappers over a Rust/C ABI: `Database`, `Transaction`,
  `Queue`, `Job`, `Stream`, `Scheduler`, `Lock`, and result helpers all
  call `SELECT honker_*(...)`.

### First parity slice

- Queue enqueue / claim / ack / retry / fail / heartbeat.
- `IAsyncEnumerable<Job>` claim loop with `CancellationToken`.
- Deadline-aware worker sleep using `honker_queue_next_claim_at(queue)`.
- Scheduler add / remove / tick / soonest / run.
- Canonical `schedule` naming with `cron` kept as a compatibility alias.
- `@every <n><unit>`, 6-field cron, and delayed `run_at` tests matching
  the shipped parity bar from PR #29.

### Follow-up parity

- Durable streams with per-consumer offsets.
- Ephemeral listen / notify once an update-event bridge exists.
- Rate limits, locks, task results, and batch helpers.
- EF Core recipe showing how to load the extension on an application
  connection and enqueue inside an existing transaction.

### Non-goals

- Do not reimplement Honker queue or scheduler logic in C#.
- Do not make .NET the source of truth for schema or SQL behavior.
- Do not block the first package on full EF Core integration, AOT,
  Unity, Xamarin, or mobile support.
- Do not promise every binding surface in the first release; mark any
  missing wrappers clearly and keep raw SQL access available.

### Verification

- .NET unit tests cover the same six must-pass queue cases as the other
  bindings.
- Cross-process delayed `run_at` and reclaim-deadline tests prove the
  async worker does not wait for a fallback poll.
- Scheduler tests prove `@every 1s`, 6-field cron, `schedule`, and
  legacy `cron` alias behavior.
- Cross-language interop test proves Python writes can be claimed by C#
  and C# writes can be claimed by Python.
- CI builds and tests on Linux, macOS, and Windows for the supported RIDs
  included in the NuGet package.

## Phase Herd — Bindings Source Of Truth

> After: Phase Ballmer · Before: 1.0 release prep

The current "one repo per binding plus git submodules in the main repo"
shape made parity work much more expensive than it should be. Future
binding work should move to a single source-of-truth repo layout, with
language packages published from one tree.

### Shape

- Keep `honker-core` and `honker-extension` in the main repo.
- Move maintained bindings back into `packages/` as normal directories,
  not git submodules.
- Treat the main repo as the place where cross-binding feature work,
  docs, parity tests, and releases are coordinated.
- Publish language packages from the monorepo by subdirectory.
- If separate public binding repos still matter, make them mirrors or
  split artifacts, not the primary place humans edit.

### Why

- One feature should be one PR, not a convoy of PRs across many repos.
- Parity tests belong next to the shared fixtures and shared extension.
- Release prep becomes much less fragile when submodule SHAs are not part
  of the work.
- The current arrangement makes it too easy for one binding to drift in
  names, docs, CI, or packaging.

### Notes

- "Parity" today mostly means the core database surface: queues,
  streams, scheduler, `run_at`, deadline wake, and recurring `schedule`
  naming.
- Python is still ahead on the higher-level task-runtime layer
  (`@task`, `@periodic_task`, `TaskResult`, `wait_result`, worker/task
  registry ergonomics). Other bindings mostly expose the underlying
  primitives, not that whole product layer yet.

## Completed — Time-trigger scheduler and wake parity

Shipped in PR #29, with follow-up release prep in PR #33.

- `run_at` jobs now wake workers at their deadline instead of waiting
  for a later fallback poll.
- Reclaim deadlines now wake sleeping workers on time too.
- Scheduler expressions now support 5-field cron, 6-field cron, and
  `@every <n><unit>`.
- Maintained bindings converged on the same basic time-trigger shape:
  update wake or next deadline, with fallback polling only as backup.
- Canonical recurring name is now `schedule`, with legacy `cron` kept
  as a compatibility alias where needed.
- Ruby and Elixir expose extension-backed `notify` and table APIs but do
  not yet expose async listen/update-watcher APIs.

### Scope

- Add binding docs that name whether each package uses update events or
  timer polling.
- Add Bun `updateEvents()` / listen bridge, or explicitly defer it with
  tests proving current poll behavior.
- Add Ruby and Elixir listener APIs only if their runtime integrations
  can support a clean cancellation story.
- Decide whether Go and C++ should keep local watcher implementations or
  grow a shared C ABI around the core watcher.
- Add parity tests that exercise a cross-process notification wake in
  every binding with a listener API.

### Non-goals

- Do not make the SQLite loadable extension itself push events. Plain SQL
  clients can write/read the shared tables, but need a host-language
  watcher to sleep efficiently.
- Do not block 1.0 on bindings that are explicitly marked poll-based or
  partial, as long as the docs and tests say so.

## Phase Cadence — Time-Based Watcher Ticks

> After: Phase Wake Parity · Before: 1.0 release prep

The update watcher currently performs the file-identity dead-man check
every N poll-loop iterations. On Windows, 1 ms sleeps round up toward
the system timer granularity, so tick-count timing drifts.

Switch identity checks to `Instant`-based timing so the check cadence
is time-based on every platform.

## 1.0 Release Prep

- Maturin wheels: Python 3.11 / 3.12 / 3.13 across Linux, macOS, and
  Windows where supported.
- npm publish with napi-rs prebuilds.
- Crate publish flow for `honker-core`, `honker-extension`, and
  `honker-rs`.
- Health / observability primitives: claim depth, DLQ rate, update
  watcher firing rate, and optional OpenTelemetry integration.
- Crash-recovery tests beyond writer kills: listener-process kills,
  bridge-thread panics, mid-checkpoint disk-full.

## Perf

- Shave PyO3 / mutex / GIL overhead off single-tx paths only if new
  measurements show it matters. Current batched workloads already
  clear the target throughput.
- Consider cached `PRAGMA data_version` polling if the current
  update watcher ever shows up in profiles.
- Keep the `mmap` wal-index reader as a research path, not a default
  runtime path, until cross-platform correctness is proven.
- Stream consumer groups: competing consumers within a named group
  with a shared advancing offset. No schema change expected.

## Docs

- Publish benchmark baselines for reference hardware beyond the
  M-series release-build numbers in `bench/README.md`.
- Keep the docs site aligned with the package README wake-path text:
  update watcher, `PRAGMA data_version`, WAL recommended but not a
  correctness requirement.
- Add cookbook recipes for FastAPI / Django / Flask / Express /
  Rails instead of reviving framework packages by default.
