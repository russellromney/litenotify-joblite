# ROADMAP

## Now

- **Perf pass 4.** Cheap wins before schema refactor: pipeline
  ack-with-next-claim in `_WorkerQueueIter`, narrow `RETURNING` to
  (id, worker_id, payload, attempts), PRAGMA tuning (cache_size=-32000,
  wal_autocheckpoint=10000).
- **Perf pass 5: tables-per-state.** Split `_joblite_jobs` into
  `_joblite_jobs_pending`, `_joblite_jobs_processing`, `_joblite_jobs_dead`.
  Claim = DELETE RETURNING from pending + INSERT into processing. Ack =
  DELETE from processing. Pending stays small and cache-hot. Expected
  3-5x on claim+ack.

## Next: loadable extension architecture

Split the Rust workspace so the project ships as:

- `litenotify` (pure Rust lib) — Notifier + commit-hook plumbing.
- `litenotify-extension` (cdylib) — SQLite loadable extension. Users
  `conn.load_extension("litenotify.so")` on any SQLite connection and
  get the `notify(channel, payload)` SQL function + commit hook. No
  wrapper DB class required.
- `litenotify-python` (PyO3 wrapper) — thin bindings for async listen;
  relies on a process-global Notifier keyed by DB path so it can
  subscribe to notifications fired by any other connection in the
  process (including ones opened through Django/SQLAlchemy/etc.).

## Multi-language bindings

Once the loadable extension ships:

- `litenotify-node` (napi-rs) — proof of the pattern.
- `litenotify-go` (cgo over a C ABI) — note: cgo call cost is
  non-trivial, so Go bindings may reimplement hot paths in pure Go
  using the extension only for notifications.
- `litenotify-ruby` (magnus) and friends, as demand warrants.

## Framework plugins (once bindings settle)

- `joblite-flask`, `joblite-rails` — same shape as `joblite-fastapi`
  and `joblite-django` today.
- `joblite-express` — SSE + Last-Event-ID + worker pool.

## Single-tx perf ceiling

Bench shows `litenotify.tx` at ~14k/s vs raw Python `sqlite3` at ~47k/s
on the same WAL+`synchronous=NORMAL` file. 3-4x gap is PyO3 boundary
crossings, writer-mutex acquire/release, GIL detach/reacquire.
`prepare_cached` and `try_acquire` already applied. Further gains would
require reducing PyO3 calls per transaction (for example, a single
`execute_tx(stmts)` that batches BEGIN + body + COMMIT in one PyO3
call). Low priority; batched workloads already clear 100k/s.

## Docs

- `docs/` site with runnable snippets per binding.
- Publish benchmark baselines for reference hardware.
- Document the "notifier is per-process" story (cross-process wake-ups
  fall back to `idle_poll_s` polling in `Queue.claim` — by design).
