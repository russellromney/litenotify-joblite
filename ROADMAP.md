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


## Phase Echo — Experimental Watcher Backends Across All Bindings

> After: Phase Wake Parity · Before: 1.0 release prep

The experimental `kernel` and `shm` watcher backends ship in
`honker-core` and are wired through Python and Node only. Bring the
remaining bindings to parity so users can opt in from any language.

### Scope

For each of `honker-bun`, `honker-go`, `honker-rs`, `honker-ruby`,
`honker-cpp`, `honker-ex`:

- Add Cargo features `kernel-watcher` and `shm-fast-path` that forward
  to `honker-core/<feature>` (where the binding has a Cargo.toml).
- Accept a `watcher_backend` (or language-idiomatic equivalent) string
  parameter on the binding's `open()`.
- Parse via `honker_core::WatcherBackend::parse` so the accepted
  aliases (`"polling"` / `"poll"`, `"kernel"` / `"kernel-watcher"`,
  `"shm"` / `"shm-fast-path"`) stay in lockstep with Python/Node.
- Call `WatcherBackend::probe(&db_path)` at `open()` time and surface
  failures as the language's idiomatic error type. **No silent
  fallback.**
- Pass the `WatcherConfig` through to `SharedUpdateWatcher::new_with_config`.

### Tests per binding

- Direct API test: `open(backend=...)` for each backend; one returns
  the polling default, kernel/shm raise iff the feature isn't
  compiled into that binding's build.
- Cross-process listener: writer subprocess + parent listener under
  each backend. Every commit must surface to the listener.
- Cross-process queue worker: 1×1, 1×N (workers), N×1 (writers)
  topologies × each backend. Mirrors the existing
  `tests/test_watcher_backends_queue_e2e.py` and
  `packages/honker-node/test/watcher_backends_queue_e2e.js`.

### Non-goals

- Don't add new wire formats or per-binding watcher logic. All
  backends live in `honker-core`; bindings only thread the param.
- Don't auto-detect / silently substitute backends — the experimental
  contract is "opt in, fail loud, restart".
- Don't backport to bindings without an `update_events()` equivalent
  (some have polling-only consumer APIs); document those as
  polling-only and skip.

### Verification

- Per binding: tests above pass on Linux and macOS in CI.
- Windows is opt-in; document expected behavior per binding.
- All tests in `tests/test_watcher_backends.py`,
  `tests/test_watcher_backends_e2e.py`, and
  `tests/test_watcher_backends_queue_e2e.py` continue to pass for
  Python; equivalents continue to pass for Node.

## Phase Atlas — Map Experimental Backend Edge-Case Behavior

> After: Phase Echo · Before: 1.0 release prep

The experimental `kernel` and `shm` backends ship with a documented
contract ("spurious wakes possible, missed wakes possible") plus a
dead-man's switch for file replacement. That covers the headline
correctness story but leaves a long tail of edge cases where the
three backends behave differently. Users opt in to "experimental";
they deserve tests that characterize *exactly* what they're opting
in to.

This phase doesn't fix anything — it pins down behavior so users
know what to expect, and so future regressions are visible.

### Scope: behavioral characterization tests

For each scenario, write a test per backend that asserts the actual
observed behavior. Document the results in
`honker-core/src/{kernel,shm}_watcher.rs` module docs + the README
"Experimental wake backends" section.

1. **Rollback handling** — `BEGIN IMMEDIATE; INSERT; ROLLBACK`.
   Polling: 0 wakes. SHM: 0 wakes. Kernel: ≥ 1 wake (DELETE mode
   creates+deletes -journal; WAL mode may write to -wal even on
   rollback).

2. **Checkpoint wake counts** — `wal_checkpoint(TRUNCATE)` after
   N committed writes. Count distinct wakes. Polling: 1 (data_version
   bump). Kernel: ≥ 1 (-wal truncated, -shm modified). SHM: 1
   (iChange advances).

3. **External non-SQLite writes** — `dd` or `truncate` against the
   db file. Polling: 0 wakes (data_version unchanged). SHM: 0 wakes
   (iChange unchanged). Kernel: ≥ 1 wake (filesystem event fires).

4. **Multiple databases in the same directory** — open `db1.db` with
   kernel watcher, commit to `db2.db` in the same dir. Kernel: spurious
   wake on db1's watcher. Polling/SHM: no spurious wake.

5. **VACUUM** — VACUUM rewrites the db with a new inode. All three
   backends should panic via the dead-man's switch ("Restart required").
   Verify the panic message is identical across backends.

6. **Mid-flight `journal_mode` change** — open in WAL with shm
   backend, change `PRAGMA journal_mode=DELETE` from another connection.
   SHM: -shm unlinked → dead-man's switch panics. Polling: keeps
   working. Kernel: keeps working.

7. **System suspend / hibernate** — hard to test in CI but possible
   on a developer machine. Document expected behavior:
   - Polling: resumes correctly.
   - Kernel: events queued during suspend may be dropped by the OS;
     missed wakes possible.
   - SHM: resumes correctly.

8. **Symlink for the db path** — open `/tmp/symlink.db` →
   `/var/data/real.db`. Polling: works. Kernel: behavior varies by
   platform — Linux follows by default, macOS may not. SHM: follows
   to target.

9. **Fork without exec** — spawn a watcher, then `fork()`. Document:
   not supported; child process should re-open the database with its
   own watcher. Test that the parent's watcher continues to work.

10. **NFS / SMB / FUSE filesystem** — in CI, mount tmpfs or use a
    `bindfs` shim to simulate. Polling: works (slow but correct).
    Kernel: notifications don't fire on remote modifications from
    other clients — missed wakes from non-local writers. SHM:
    SQLite WAL is unsupported on network FS, the probe at `open()`
    should refuse rather than start a broken backend. Add the refusal
    if not already there.

11. **Crash recovery** — kill the writer mid-INSERT (SIGKILL).
    Reopen, drive new commits. All three backends should detect
    post-restart commits. Polling: works (trivially). Kernel/SHM:
    after the dead-man's switch panic, the watcher needs restart;
    test that restarting recovers cleanly.

12. **Litestream-style restore** — a successful litestream restore
    atomically replaces the db file. All three backends panic via the
    dead-man's switch (same path as VACUUM #5). Verify:
    - Subscribers' `update_events()` returns `Err` (Disconnected)
      within ≤ 200 ms of the replacement, via WatcherDeathGuard.
    - After the consumer recreates `Database`, wakes resume normally.
    - Document the "recreate after restore" recovery pattern in
      `docs/litestream.md` (new) so users know what to expect.
    Optional follow-up: a "tolerate file replacement" config flag that
    re-attaches watches/mmaps instead of panicking. Out of scope for
    Atlas; track separately if there's actual demand.

### Non-goals

- Don't *fix* the differences. The contract says "experimental";
  the tests *characterize* what experimental means. If a behavior
  is unacceptable, that's a separate phase.
- Don't add backend-specific de-duplication to make kernel match
  polling on rollback wakes. That's just rebuilding the conservative
  version we already nuked.
- Don't suppress the panic on VACUUM. The user really does need to
  restart.

### Verification

- One Rust unit test per (backend, scenario) cell, gated on the
  appropriate Cargo feature. ~30 new tests total.
- `tests/test_watcher_backends.py` and the Node equivalent get a
  short cross-language test for the most user-facing differences
  (rollback, external writes, multi-db).
- README + module docs updated with a "behavior matrix" table that
  cites the test name for each cell, so docs and tests can't drift.

## Phase Boundary — Wake-Source Isolation And Independence

> After: Phase Atlas · Before: 1.0 release prep

As honker grows multiple wake/signal sources (DB-update wake via
`SharedUpdateWatcher`, deadline wake from #29, fallback poll, push
signals via `notify` rows), each consumer ends up listening on a
union of them. They should be **independent** — disabling any one
should leave the others able to carry the load — and there should
be no double-fire that produces spurious work.

Today's mechanisms appear independent by inspection but no test
proves it. This phase pins it down with isolation tests.

### Wake topology (current + post-#29)

| source | fires when | mechanism | who consumes |
|---|---|---|---|
| DB-update wake | any commit detected | `SharedUpdateWatcher` → bounded channel | every `update_events()` subscriber (Listener, Queue.claim, Stream.subscribe) |
| Deadline wake (#29) | a known future deadline arrives (delayed `run_at`, expired `claim_expires_at`) | `asyncio.wait_for(timeout=until_deadline)` | Queue.claim worker loop |
| Fallback poll | nothing else fired in `idle_poll_s` | timer | every async iterator consumer |
| Push signal | `notify(channel, payload)` was committed | rides on DB-update wake; rows in `_honker_notifications` | `db.listen(channel)` |

### Invariants to prove

1. **No source fires for the wrong reason.** Deadline wake only on
   deadline expiry, not on commits. DB-update wake only on commits,
   not on deadlines.

2. **Missing one source doesn't go undetected.** A worker sleeping
   to deadline T will wake at T even if a commit at T-1 ms's
   DB-update wake was lost — the deadline wake "masks" the missed
   wake silently. Correctness is preserved (re-query covers both),
   but a regression in DB-update wake delivery would be invisible
   without dedicated tests.

3. **No double-fire causes spurious work.** A commit that lands
   exactly at a deadline shouldn't run two re-queries (one per
   source). Cap-1 channel + asyncio.wait_for racing semantics
   currently behave correctly; pin it with a test.

4. **Sources don't deadlock or starve each other.** A flood of
   DB-update wakes shouldn't prevent deadline wakes from firing,
   and vice versa.

### Test approach: isolate one source, prove others carry the load

For each source, mock or disable it and assert the consumer still
makes progress on a representative workload:

- **DB-update only** — disable deadline wake (no delayed jobs / no
  pending claims). Worker drains commits via DB-update wake alone.
  Already covered by existing queue tests in WAL mode; just label
  them as the "DB-update-only" case.

- **Deadline-only** — mock `SharedUpdateWatcher` to never fire.
  Workers should wake at delayed-job `run_at` and reclaim
  `claim_expires_at` deadlines and process work. Uses the existing
  `idle_poll_s` paranoia poll as the only DB-update fallback.

- **Fallback-poll-only** — mock both DB-update wake AND deadline
  timer. Worker should still drain queued work, just at
  `idle_poll_s` cadence. Slow but correct.

- **Push-signal isolation** — `db.listen(channel)` consumer should
  see notifications without depending on the deadline wake (no
  deadline applies to listen).

### Verification

- Per-isolation tests pass on Linux and macOS.
- New cross-process test: writer subprocess + worker subprocess where
  the worker has each wake source disabled in turn. Worker still
  drains in every config (with appropriate latency expectations).
- Documentation in `docs/wake-topology.md` (new) — the table above
  plus a per-consumer note on which sources they listen to.

### Non-goals

- Don't try to *unify* the sources. They serve different purposes
  (event-driven vs deadline-driven vs paranoia poll vs in-band
  notification data). Independence is a feature.
- Don't add a "source priority" mechanism. Every source can fire;
  consumers always re-query state on wake; idempotent.

## Phase Lighthouse — Ship Experimental Wake Backends In Published Wheels

> After: Phase Atlas · Before: 1.0 release prep

The experimental `kernel` and `shm` watcher backends shipped in source
in PR #30, gated behind Cargo features. Published wheels (`pip install
honker`, `npm install @russellthehippo/honker-node`, the future
`crates.io` releases of `honker`) currently build **without** those
features so users get the proven polling backend by default.

This phase decides when to flip the default and ship them in wheels.

### Prerequisites (must all hold before flipping)

1. **Linux + Windows CI matrix builds and tests with both features
   enabled.** Today CI only exercises the default polling path on
   the experimental backends' code. Add jobs that:
   - Build with `--features kernel-watcher,shm-fast-path` on Linux
     (inotify) and Windows (ReadDirectoryChangesW).
   - Run the full Rust test suite under both features on each OS.
   - Run the cross-process Python and Node e2e suites with the
     experimental backends selected at open() time.

2. **Phase Atlas characterization tests pass on every CI platform.**
   At minimum the highest-risk subset:
   - Rollback handling (kernel may produce spurious wakes per OS)
   - Multi-database in same dir (kernel cross-pollination)
   - VACUUM (dead-man's switch panic message)
   - NFS detection in `probe()` (refuse fast path on network FS)

3. **Real-world dogfooding.** At least one extended workload (days,
   not minutes) running with each backend selected. Catches things
   tests can't: long-tail event delivery, OS quirks under load,
   resource leaks.

### Scope (when prereqs are met)

- Update wheel-build CI jobs to pass `--features kernel-watcher,shm-fast-path`
  on Linux and macOS (skip Windows if Windows CI shows specific issues
  the moment it lights up).
- Update README to drop the "source-only" notice; add a "use these only
  if you have a specific reason" note alongside the comparison table.
- Bump versions on the bindings to signal the new behavior.
- Optional: a separate "experimental wheels" channel (e.g. a `*-rc`
  suffix) for users who want the backends without committing to source
  builds, before flipping defaults.

### Non-goals

- Don't make any experimental backend the new default. Polling stays
  the default. The flip is "available in wheels" not "selected by
  default".
- Don't ship features that fail any platform's CI. If kqueue on
  macOS works but inotify on Linux doesn't, ship neither in wheels
  until both pass.

### Verification

- All prereqs above documented as passing in the PR that flips wheels.
- Release notes call out the experimental status, link the contract,
  link the roadmap.

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
