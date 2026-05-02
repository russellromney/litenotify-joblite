# ROADMAP

Roadmap items are future work. Completed phases move to
`CHANGELOG.md`; this file should not carry shipped history.

## Phase naming

Phases use the format `Phase <Name>`, not numbers, so we can insert
new work without renumbering. Names are unique. Each phase header
should include adjacency links (`After: ... · Before: ...`) when the
ordering matters.

## Phase Ranger — Delegate Locks To Bouncer

> Later architecture work

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

## Phase Test Depth And Interop

> Near-term hardening

The maintained bindings now live in-tree, and root CI owns the default
checks. The remaining test-regime gaps are about depth, not basic
"does this binding build?" coverage.

Keep this split into reviewable slices instead of growing one giant CI
change.

### Cross-binding interop

- Add at least one more pair beyond Python <-> Node and Ruby <->
  Python. Go <-> Python is the likely next pair because both can share
  a plain `.db` file through the extension.
- Add a tiny matrix that proves a job enqueued in one binding can be
  claimed and acked in another.
- Keep slow or expensive combinations out of default CI unless they
  catch real bugs.

### Stress and soak

- Add higher-pressure multi-writer / multi-reader tests.
- Add many-subscriber listener churn coverage beyond the focused
  regression tests.
- Add a manual or scheduled soak workflow that watches FD, thread, and
  memory growth over time.

### Compatibility surface

- Add a SQLite version matrix where it matters. Default CI mostly proves
  the versions on GitHub runners.
- Add coverage reporting if it starts guiding useful decisions.
- Add follow-up watcher timing tests if Windows still shows
  platform-specific drift.

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

## Release Automation

This is not 1.0 prep. The goal is simpler: make normal releases boring.

- One tag should drive extension artifacts and package publishes.
- Build and attach loadable extension binaries for supported platforms.
- Publish Python wheels with maturin for the supported Python/platform
  matrix.
- Publish npm/Bun packages with native artifacts where needed.
- Publish crates for `honker-core`, `honker-extension`, and
  `honker-rs`.
- Publish NuGet, Ruby, and other maintained binding packages from the
  in-tree `packages/` directories.
- Keep release notes tied to `CHANGELOG.md`.

## Later 1.0 Prep

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
