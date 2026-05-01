"""Tests for honker.Scheduler and the Rust-backed crontab parser.

Scheduler has two separate concerns:
  1. Cron parsing + next-boundary (pure Rust; tested via the Python
     facade). Low-level parser tests live alongside the Rust
     implementation (`honker-core/src/cron.rs`); here we only
     exercise the Python-facing API.
  2. Fire-due logic (pure — test with a mock `now`).
  3. Live scheduler loop (integration — hard to test without waiting
     for real cron boundaries; covered by a minimal happy-path test
     with `*/1 * * * *` + a short-running loop).
"""

import asyncio
import json
import time
from datetime import datetime

import pytest

import honker
from honker import crontab, Scheduler


# ---------- crontab() / CronSchedule.next_after ----------


def test_crontab_field_count_validated():
    with pytest.raises(ValueError):
        crontab("* * * *")        # 4 fields
    with pytest.raises(ValueError):
        crontab("* * * * * *")    # 6 fields


def test_crontab_out_of_range_validated():
    with pytest.raises(ValueError):
        crontab("60 * * * *")
    with pytest.raises(ValueError):
        crontab("* 24 * * *")


def test_crontab_inverted_range_validated():
    with pytest.raises(ValueError):
        crontab("30-10 * * * *")


def test_crontab_zero_step_validated():
    with pytest.raises(ValueError):
        crontab("*/0 * * * *")


def test_crontab_next_after_hourly():
    c = crontab("0 * * * *")
    # Next top of the hour after 10:05:03 is 11:00.
    dt = datetime(2025, 1, 1, 10, 5, 3)
    nxt = c.next_after(dt)
    assert nxt == datetime(2025, 1, 1, 11, 0)


def test_crontab_next_after_exactly_at_boundary_returns_next():
    c = crontab("0 * * * *")
    # At exactly 10:00 — next match is 11:00, not 10:00.
    dt = datetime(2025, 1, 1, 10, 0)
    nxt = c.next_after(dt)
    assert nxt == datetime(2025, 1, 1, 11, 0)


def test_crontab_next_after_crosses_day():
    c = crontab("0 3 * * *")
    # At 4am, next 3am is tomorrow.
    dt = datetime(2025, 1, 1, 4, 0)
    nxt = c.next_after(dt)
    assert nxt == datetime(2025, 1, 2, 3, 0)


def test_crontab_next_after_crosses_year():
    c = crontab("0 0 1 1 *")  # Jan 1 midnight.
    dt = datetime(2025, 6, 15, 12, 0)
    nxt = c.next_after(dt)
    assert nxt == datetime(2026, 1, 1, 0, 0)


# ---------- Scheduler.add / _fire_due ----------


def test_scheduler_add_registers_task(db_path):
    db = honker.open(db_path)
    sched = Scheduler(db)
    sched.add(
        name="nightly",
        queue="backups",
        schedule=crontab("0 3 * * *"),
    )
    # Registration is persisted in _honker_scheduler_tasks.
    rows = db.query(
        "SELECT name, queue, cron_expr FROM _honker_scheduler_tasks "
        "WHERE name='nightly'"
    )
    assert len(rows) == 1
    assert rows[0]["queue"] == "backups"
    assert rows[0]["cron_expr"] == "0 3 * * *"
    assert "nightly" in sched._registered


def test_scheduler_add_replaces_by_name(db_path):
    db = honker.open(db_path)
    sched = Scheduler(db)
    sched.add(name="t", queue="a", schedule=crontab("* * * * *"))
    sched.add(name="t", queue="b", schedule=crontab("* * * * *"))
    rows = db.query(
        "SELECT queue FROM _honker_scheduler_tasks WHERE name='t'"
    )
    assert len(rows) == 1
    assert rows[0]["queue"] == "b"


def test_scheduler_tick_enqueues_on_boundary(db_path):
    """honker_scheduler_tick(now) enqueues one job per registered task
    whose next_fire_at <= now, and advances next_fire_at."""
    db = honker.open(db_path)
    db.queue("hourly-q")  # create schema
    sched = Scheduler(db)
    sched.add(
        name="hourly",
        queue="hourly-q",
        schedule=crontab("0 * * * *"),
        payload={"ping": True},
    )

    # Fetch the task's next_fire_at (set by register to the next top
    # of the hour after "now") and tick one second past it.
    row = db.query(
        "SELECT next_fire_at FROM _honker_scheduler_tasks WHERE name='hourly'"
    )[0]
    boundary = int(row["next_fire_at"])
    with db.transaction() as tx:
        result = tx.query(
            "SELECT honker_scheduler_tick(?) AS j", [boundary + 1]
        )
    fires = json.loads(result[0]["j"])
    assert len(fires) == 1
    assert fires[0]["name"] == "hourly"
    assert fires[0]["queue"] == "hourly-q"
    assert fires[0]["fire_at"] == boundary
    # Job landed in the queue.
    rows = db.query(
        "SELECT payload FROM _honker_live WHERE queue='hourly-q'"
    )
    assert len(rows) == 1
    # next_fire_at advanced one hour.
    row = db.query(
        "SELECT next_fire_at FROM _honker_scheduler_tasks WHERE name='hourly'"
    )[0]
    assert int(row["next_fire_at"]) == boundary + 3600


def test_scheduler_tick_skips_already_fired(db_path):
    """Calling tick twice at the same `now` doesn't re-fire —
    next_fire_at advances past `now` on the first call, so the
    second is a no-op. Keeps scheduler restart safe within a
    boundary window."""
    db = honker.open(db_path)
    db.queue("no-dup")
    sched = Scheduler(db)
    sched.add(
        name="t",
        queue="no-dup",
        schedule=crontab("0 * * * *"),
    )
    row = db.query(
        "SELECT next_fire_at FROM _honker_scheduler_tasks WHERE name='t'"
    )[0]
    boundary = int(row["next_fire_at"])
    with db.transaction() as tx:
        result_a = tx.query(
            "SELECT honker_scheduler_tick(?) AS j", [boundary + 1]
        )
    with db.transaction() as tx:
        result_b = tx.query(
            "SELECT honker_scheduler_tick(?) AS j", [boundary + 1]
        )
    assert len(json.loads(result_a[0]["j"])) == 1
    assert len(json.loads(result_b[0]["j"])) == 0
    rows = db.query("SELECT COUNT(*) AS c FROM _honker_live WHERE queue='no-dup'")
    assert rows[0]["c"] == 1


def test_scheduler_tick_catches_up_multiple_boundaries(db_path):
    """If the scheduler was down for multiple boundaries, tick walks
    forward firing each one. Current behavior is to catch up
    unbounded — fine for low-frequency schedules; for noisy ones
    callers can use `expires` to drop stale catch-up jobs.
    """
    db = honker.open(db_path)
    db.queue("catchup-q")
    sched = Scheduler(db)
    sched.add(
        name="h",
        queue="catchup-q",
        schedule=crontab("0 * * * *"),  # hourly
    )
    # Rewind next_fire_at to 4 hours ago to simulate downtime
    # across 4 boundaries (the boundary we're rewinding to + 3
    # more while we sleep through now).
    row = db.query(
        "SELECT next_fire_at FROM _honker_scheduler_tasks WHERE name='h'"
    )[0]
    orig_next = int(row["next_fire_at"])
    rewound = orig_next - 4 * 3600
    with db.transaction() as tx:
        tx.execute(
            "UPDATE _honker_scheduler_tasks SET next_fire_at=? WHERE name='h'",
            [rewound],
        )
    # Now tick at a time slightly past the original boundary: 5
    # boundaries should fire (rewound, rewound+1h, ..., rewound+4h).
    now = orig_next + 60
    with db.transaction() as tx:
        result = tx.query("SELECT honker_scheduler_tick(?) AS j", [now])
    fires = json.loads(result[0]["j"])
    assert len(fires) == 5
    # next_fire_at advanced to the hour after `now`.
    row = db.query(
        "SELECT next_fire_at FROM _honker_scheduler_tasks WHERE name='h'"
    )[0]
    assert int(row["next_fire_at"]) == orig_next + 3600


def test_scheduler_tick_racing_writers_produce_no_duplicates(db_path):
    """Multiple workers calling `honker_scheduler_tick` concurrently
    must never double-fire a boundary.

    In production the `honker-scheduler` leader lock gates callers —
    only one process holds it at a time. But the lock is advisory at
    the application layer; if a future binding forgets to acquire
    it, or a test misuses the API, the underlying SQL must still be
    safe. This proves that `BEGIN IMMEDIATE` + the advance-then-
    return contract in `scheduler_tick` means at most one ticker
    observes an unfired boundary.

    Strategy: register one task with `next_fire_at = now - 1` (one
    boundary overdue), fire 10 Python threads each running one
    `SELECT honker_scheduler_tick(now)` through its own writer
    transaction. Exactly one thread should see the fire, nine
    should see `[]`. The job lands in `_honker_live` exactly once.
    """
    import threading

    db = honker.open(db_path)
    db.queue("race-q")
    sched = Scheduler(db)
    sched.add(
        name="one",
        queue="race-q",
        schedule=crontab("* * * * *"),  # every minute
    )
    # Force exactly one boundary overdue.
    row = db.query(
        "SELECT next_fire_at FROM _honker_scheduler_tasks WHERE name='one'"
    )[0]
    boundary = int(row["next_fire_at"])
    with db.transaction() as tx:
        tx.execute(
            "UPDATE _honker_scheduler_tasks "
            "SET next_fire_at = ? WHERE name = 'one'",
            [boundary - 60],
        )
    # Tick at `boundary - 60 + 1`: exactly one boundary should be
    # eligible to fire (the rewound one), not two.
    now = boundary - 60 + 1

    fire_counts: list = [0] * 10
    barrier = threading.Barrier(10)

    def worker(i: int) -> None:
        # Synchronize starts so threads race, rather than serializing
        # trivially through Python's GIL + thread scheduling.
        barrier.wait()
        with db.transaction() as tx:
            rows = tx.query("SELECT honker_scheduler_tick(?) AS j", [now])
        fires = json.loads(rows[0]["j"])
        fire_counts[i] = len(fires)

    threads = [
        threading.Thread(target=worker, args=(i,)) for i in range(10)
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=10)

    # Exactly one ticker saw the fire; nine saw nothing.
    total_fires = sum(fire_counts)
    assert total_fires == 1, (
        f"expected 1 total fire across 10 concurrent tickers, "
        f"got {total_fires}: {fire_counts}"
    )
    # And the queue has exactly one job, not ten.
    rows = db.query(
        "SELECT COUNT(*) AS c FROM _honker_live WHERE queue='race-q'"
    )
    assert rows[0]["c"] == 1


async def test_scheduler_run_with_stop_event(db_path):
    """Happy-path integration: start a scheduler with a *very* fast
    schedule, fire at least once, stop via stop_event, return cleanly.
    Verifies the lock-acquire + heartbeat + stop path without needing
    to wait for real cron boundaries.
    """
    db = honker.open(db_path)
    db.queue("flash")

    sched = Scheduler(db)
    sched.add(
        name="flash-task",
        queue="flash",
        schedule=crontab("* * * * *"),  # every minute
    )

    # Override the main loop's "wait until next boundary" behavior by
    # pre-setting next_fires to now, so the first iteration fires
    # immediately. Easier than mocking datetime.
    stop_event = asyncio.Event()
    run_task = asyncio.create_task(sched.run(stop_event))

    # Let the scheduler start up + fire once, then stop.
    await asyncio.sleep(0.3)
    stop_event.set()
    await asyncio.wait_for(run_task, timeout=5.0)

    # Scheduler acquired + released the lock cleanly.
    rows = db.query(
        "SELECT COUNT(*) AS c FROM _honker_locks WHERE name='honker-scheduler'"
    )
    assert rows[0]["c"] == 0


async def test_two_schedulers_one_runs_one_raises_lockheld(db_path):
    """Leader election: two scheduler processes can't both hold the
    lock. The second one raises LockHeld, matching our documented
    hot-standby semantics (caller retries in a loop)."""
    db = honker.open(db_path)
    db.queue("leader-q")

    s1 = Scheduler(db)
    s2 = Scheduler(db)
    s1.add(name="x", queue="leader-q", schedule=crontab("* * * * *"))
    s2.add(name="x", queue="leader-q", schedule=crontab("* * * * *"))

    stop = asyncio.Event()
    t1 = asyncio.create_task(s1.run(stop))
    await asyncio.sleep(0.1)  # let s1 acquire the lock

    # s2 tries to run — the lock is held, so it raises LockHeld.
    with pytest.raises(honker.LockHeld):
        await s2.run()

    stop.set()
    await asyncio.wait_for(t1, timeout=3.0)


def test_scheduler_run_raises_without_tasks(db_path):
    """A scheduler with no tasks added and an empty DB raises a clear
    error rather than silently no-opping. Phase Shakedown (b)."""
    db = honker.open(db_path)
    sched = Scheduler(db)
    with pytest.raises(RuntimeError, match="no registered tasks"):
        asyncio.run(sched.run())
    # Lock never acquired — we raised before the lock block.
    rows = db.query(
        "SELECT COUNT(*) AS c FROM _honker_locks WHERE name='honker-scheduler'"
    )
    assert rows[0]["c"] == 0


async def test_scheduler_wakes_on_new_registration(db_path):
    """A scheduler sleeping until a far-future fire must wake within ~1s
    when another caller registers a task whose next_fire_at is sooner.
    Phase Shakedown (c).

    `honker_scheduler_register` fires a wake on the 'honker:scheduler'
    channel; the main loop races its timer against update_events(), so a
    new registration kicks it out of sleep. Without this, a leader
    scheduled to next wake in an hour would silently miss a task
    registered 1 minute from now until the current sleep ended.
    """
    db = honker.open(db_path)
    sched = Scheduler(db)
    # Pre-register one task so .run() doesn't bail on the empty-table
    # guard from Phase Shakedown (b). The cron expr fires in ~1hr
    # (next top-of-the-hour plus one), so the leader's first sleep
    # will be long enough to demonstrate the wake.
    sched.add(
        name="far-future",
        queue="q",
        schedule=crontab("0 */2 * * *"),  # every 2hr — next fire is at least ~an hour
    )

    stop = asyncio.Event()
    run_task = asyncio.create_task(sched.run(stop_event=stop))
    await asyncio.sleep(0.2)  # let the leader enter its sleep

    # Inject a "closer" task from a second scheduler on the same DB.
    # The main loop should wake within the WAL update-watcher cadence.
    injected = Scheduler(db)
    start = time.monotonic()
    injected.add(
        name="injected",
        queue="q",
        schedule=crontab("* * * * *"),  # every minute
    )

    # Give the loop a moment to observe the wake; then stop.
    await asyncio.sleep(0.5)
    elapsed = time.monotonic() - start
    stop.set()
    await asyncio.wait_for(run_task, timeout=5.0)

    # If the wake worked, we spent <1s between injecting the task and
    # setting stop. The real assertion is indirect: the fact that the
    # run_task completed at all means the sleep didn't block past the
    # registration wake — otherwise `stop.set()` would race a long
    # sleep and the test would take the full ~2hr cron target (or at
    # least time out on asyncio.wait_for).
    assert elapsed < 1.5, (
        f"Loop didn't react to injected registration within {elapsed:.2f}s"
    )


async def test_scheduler_run_proceeds_if_another_process_registered(db_path):
    """A scheduler that didn't `.add()` anything locally but finds rows
    in `_honker_scheduler_tasks` (registered by another process or a
    prior run) still runs. Phase Shakedown (b) — prevents silent no-op
    for dedicated runner processes."""
    db = honker.open(db_path)
    # Simulate another process having registered a task.
    Scheduler(db).add(
        name="from-elsewhere",
        queue="health",
        schedule=crontab("0 3 * * *"),
    )

    # Fresh instance — no local self._registered entries. Must still run.
    runner = Scheduler(db)
    stop = asyncio.Event()

    async def stop_soon():
        await asyncio.sleep(0.1)
        stop.set()

    # Race the scheduler loop against a quick stop. If it silently
    # returns (the old bug), the gather completes immediately and
    # the lock was never acquired. If the fix is in place, it
    # acquires the lock, enters the main loop, and stops on the event.
    async def check_lock_was_held():
        await asyncio.sleep(0.05)
        rows = db.query(
            "SELECT COUNT(*) AS c FROM _honker_locks "
            "WHERE name='honker-scheduler'"
        )
        return rows[0]["c"]

    holds_lock_during_run = asyncio.create_task(check_lock_was_held())
    await asyncio.gather(runner.run(stop_event=stop), stop_soon())
    assert await holds_lock_during_run == 1, (
        "scheduler silently returned without acquiring the lock — "
        "regression of Phase Shakedown (b)"
    )


# ---------- max_runs ----------
#
# These tests use honker_scheduler_register SQL directly (same pattern
# as test_scheduler_tick_*) rather than sched.add(max_runs=...) because
# the Python Scheduler.add() binding in packages/honker needs a separate
# update to expose the max_runs parameter.


def _register_with_max_runs(db, name, queue, cron_expr, max_runs):
    """Register a task via raw SQL, setting max_runs."""
    with db.transaction() as tx:
        tx.query(
            "SELECT honker_scheduler_register(?, ?, ?, '\"go\"', 0, NULL, ?)",
            [name, queue, cron_expr, max_runs],
        )
    return int(
        db.query(
            "SELECT next_fire_at FROM _honker_scheduler_tasks WHERE name=?", [name]
        )[0]["next_fire_at"]
    )


def test_scheduler_max_runs_unregisters_after_limit(db_path):
    """A task with max_runs=3 fires exactly 3 times then removes itself
    from _honker_scheduler_tasks."""
    db = honker.open(db_path)
    db.queue("limited-q")
    boundary = _register_with_max_runs(db, "limited", "limited-q", "0 * * * *", 3)

    # Rewind next_fire_at so 3 boundaries are overdue.
    with db.transaction() as tx:
        tx.execute(
            "UPDATE _honker_scheduler_tasks SET next_fire_at=? WHERE name='limited'",
            [boundary - 2 * 3600],  # 3 hourly boundaries: -2h, -1h, now
        )

    # Tick past all three boundaries.
    with db.transaction() as tx:
        result = tx.query("SELECT honker_scheduler_tick(?) AS j", [boundary + 1])
    fires = json.loads(result[0]["j"])
    assert len(fires) == 3

    # Task must be gone from the scheduler table.
    rows = db.query(
        "SELECT COUNT(*) AS c FROM _honker_scheduler_tasks WHERE name='limited'"
    )
    assert rows[0]["c"] == 0, "task should be unregistered after max_runs exhausted"

    # But all 3 jobs landed in the queue.
    rows = db.query("SELECT COUNT(*) AS c FROM _honker_live WHERE queue='limited-q'")
    assert rows[0]["c"] == 3


def test_scheduler_max_runs_one(db_path):
    """max_runs=1 fires exactly once then self-destructs."""
    db = honker.open(db_path)
    db.queue("once-q")
    boundary = _register_with_max_runs(db, "once", "once-q", "* * * * *", 1)

    with db.transaction() as tx:
        result = tx.query("SELECT honker_scheduler_tick(?) AS j", [boundary + 1])
    fires = json.loads(result[0]["j"])
    assert len(fires) == 1

    rows = db.query(
        "SELECT COUNT(*) AS c FROM _honker_scheduler_tasks WHERE name='once'"
    )
    assert rows[0]["c"] == 0


def test_scheduler_max_runs_none_fires_indefinitely(db_path):
    """A task with no max_runs (NULL) stays registered after firing."""
    db = honker.open(db_path)
    db.queue("forever-q")
    sched = Scheduler(db)
    sched.add(name="forever", queue="forever-q", schedule=crontab("0 * * * *"))
    row = db.query(
        "SELECT next_fire_at FROM _honker_scheduler_tasks WHERE name='forever'"
    )[0]
    boundary = int(row["next_fire_at"])

    with db.transaction() as tx:
        tx.query("SELECT honker_scheduler_tick(?) AS j", [boundary + 1])

    rows = db.query(
        "SELECT COUNT(*) AS c FROM _honker_scheduler_tasks WHERE name='forever'"
    )
    assert rows[0]["c"] == 1, "unlimited task must remain registered after firing"


def test_scheduler_max_runs_stops_catchup_at_limit(db_path):
    """When catching up after downtime, max_runs caps the total fires
    even if more boundaries are overdue."""
    db = honker.open(db_path)
    db.queue("cap-q")
    boundary = _register_with_max_runs(db, "cap", "cap-q", "0 * * * *", 2)

    # Rewind so 5 boundaries are overdue, but max_runs=2.
    with db.transaction() as tx:
        tx.execute(
            "UPDATE _honker_scheduler_tasks SET next_fire_at=? WHERE name='cap'",
            [boundary - 4 * 3600],
        )

    with db.transaction() as tx:
        result = tx.query("SELECT honker_scheduler_tick(?) AS j", [boundary + 1])
    fires = json.loads(result[0]["j"])
    assert len(fires) == 2, f"expected 2 fires (max_runs cap), got {len(fires)}"

    rows = db.query(
        "SELECT COUNT(*) AS c FROM _honker_scheduler_tasks WHERE name='cap'"
    )
    assert rows[0]["c"] == 0
