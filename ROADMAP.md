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

## Phase Wake Parity — Binding Update Events

> After: Phase Submodule · Before: Phase Cadence

Make every maintained binding explicit about its wake/listen behavior,
then converge them on the shared update-event semantics where the host
runtime can support it.

### Current split

- Python, Node, and Rust use `honker-core::SharedUpdateWatcher`.
- Go and C++ reimplement the same `PRAGMA data_version` watcher locally.
- Bun async iterators poll target tables on short timers.
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
