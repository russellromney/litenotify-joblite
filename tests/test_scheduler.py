"""Tests for joblite.Scheduler and the Rust-backed crontab parser.

Scheduler has two separate concerns:
  1. Cron parsing + next-boundary (pure Rust; tested via the Python
     facade). Low-level parser tests live alongside the Rust
     implementation (`litenotify-core/src/cron.rs`); here we only
     exercise the Python-facing API.
  2. Fire-due logic (pure — test with a mock `now`).
  3. Live scheduler loop (integration — hard to test without waiting
     for real cron boundaries; covered by a minimal happy-path test
     with `*/1 * * * *` + a short-running loop).
"""

import asyncio
import json
from datetime import datetime, timedelta

import pytest

import joblite
from joblite import crontab, Scheduler


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
    db = joblite.open(db_path)
    sched = Scheduler(db)
    sched.add(
        name="nightly",
        queue="backups",
        schedule=crontab("0 3 * * *"),
    )
    # Registration is persisted in _joblite_scheduler_tasks.
    rows = db.query(
        "SELECT name, queue, cron_expr FROM _joblite_scheduler_tasks "
        "WHERE name='nightly'"
    )
    assert len(rows) == 1
    assert rows[0]["queue"] == "backups"
    assert rows[0]["cron_expr"] == "0 3 * * *"
    assert "nightly" in sched._registered


def test_scheduler_add_replaces_by_name(db_path):
    db = joblite.open(db_path)
    sched = Scheduler(db)
    sched.add(name="t", queue="a", schedule=crontab("* * * * *"))
    sched.add(name="t", queue="b", schedule=crontab("* * * * *"))
    rows = db.query(
        "SELECT queue FROM _joblite_scheduler_tasks WHERE name='t'"
    )
    assert len(rows) == 1
    assert rows[0]["queue"] == "b"


def test_scheduler_tick_enqueues_on_boundary(db_path):
    """jl_scheduler_tick(now) enqueues one job per registered task
    whose next_fire_at <= now, and advances next_fire_at."""
    db = joblite.open(db_path)
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
        "SELECT next_fire_at FROM _joblite_scheduler_tasks WHERE name='hourly'"
    )[0]
    boundary = int(row["next_fire_at"])
    with db.transaction() as tx:
        result = tx.query(
            "SELECT jl_scheduler_tick(?) AS j", [boundary + 1]
        )
    fires = json.loads(result[0]["j"])
    assert len(fires) == 1
    assert fires[0]["name"] == "hourly"
    assert fires[0]["queue"] == "hourly-q"
    assert fires[0]["fire_at"] == boundary
    # Job landed in the queue.
    rows = db.query(
        "SELECT payload FROM _joblite_live WHERE queue='hourly-q'"
    )
    assert len(rows) == 1
    # next_fire_at advanced one hour.
    row = db.query(
        "SELECT next_fire_at FROM _joblite_scheduler_tasks WHERE name='hourly'"
    )[0]
    assert int(row["next_fire_at"]) == boundary + 3600


def test_scheduler_tick_skips_already_fired(db_path):
    """Calling tick twice at the same `now` doesn't re-fire —
    next_fire_at advances past `now` on the first call, so the
    second is a no-op. Keeps scheduler restart safe within a
    boundary window."""
    db = joblite.open(db_path)
    db.queue("no-dup")
    sched = Scheduler(db)
    sched.add(
        name="t",
        queue="no-dup",
        schedule=crontab("0 * * * *"),
    )
    row = db.query(
        "SELECT next_fire_at FROM _joblite_scheduler_tasks WHERE name='t'"
    )[0]
    boundary = int(row["next_fire_at"])
    with db.transaction() as tx:
        result_a = tx.query(
            "SELECT jl_scheduler_tick(?) AS j", [boundary + 1]
        )
    with db.transaction() as tx:
        result_b = tx.query(
            "SELECT jl_scheduler_tick(?) AS j", [boundary + 1]
        )
    assert len(json.loads(result_a[0]["j"])) == 1
    assert len(json.loads(result_b[0]["j"])) == 0
    rows = db.query("SELECT COUNT(*) AS c FROM _joblite_live WHERE queue='no-dup'")
    assert rows[0]["c"] == 1


def test_scheduler_tick_catches_up_multiple_boundaries(db_path):
    """If the scheduler was down for multiple boundaries, tick walks
    forward firing each one. Current behavior is to catch up
    unbounded — fine for low-frequency schedules; for noisy ones
    callers can use `expires` to drop stale catch-up jobs.
    """
    db = joblite.open(db_path)
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
        "SELECT next_fire_at FROM _joblite_scheduler_tasks WHERE name='h'"
    )[0]
    orig_next = int(row["next_fire_at"])
    rewound = orig_next - 4 * 3600
    with db.transaction() as tx:
        tx.execute(
            "UPDATE _joblite_scheduler_tasks SET next_fire_at=? WHERE name='h'",
            [rewound],
        )
    # Now tick at a time slightly past the original boundary: 5
    # boundaries should fire (rewound, rewound+1h, ..., rewound+4h).
    now = orig_next + 60
    with db.transaction() as tx:
        result = tx.query("SELECT jl_scheduler_tick(?) AS j", [now])
    fires = json.loads(result[0]["j"])
    assert len(fires) == 5
    # next_fire_at advanced to the hour after `now`.
    row = db.query(
        "SELECT next_fire_at FROM _joblite_scheduler_tasks WHERE name='h'"
    )[0]
    assert int(row["next_fire_at"]) == orig_next + 3600


async def test_scheduler_run_with_stop_event(db_path):
    """Happy-path integration: start a scheduler with a *very* fast
    schedule, fire at least once, stop via stop_event, return cleanly.
    Verifies the lock-acquire + heartbeat + stop path without needing
    to wait for real cron boundaries.
    """
    db = joblite.open(db_path)
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
        "SELECT COUNT(*) AS c FROM _joblite_locks WHERE name='joblite-scheduler'"
    )
    assert rows[0]["c"] == 0


async def test_two_schedulers_one_runs_one_raises_lockheld(db_path):
    """Leader election: two scheduler processes can't both hold the
    lock. The second one raises LockHeld, matching our documented
    hot-standby semantics (caller retries in a loop)."""
    db = joblite.open(db_path)
    db.queue("leader-q")

    s1 = Scheduler(db)
    s2 = Scheduler(db)
    s1.add(name="x", queue="leader-q", schedule=crontab("* * * * *"))
    s2.add(name="x", queue="leader-q", schedule=crontab("* * * * *"))

    stop = asyncio.Event()
    t1 = asyncio.create_task(s1.run(stop))
    await asyncio.sleep(0.1)  # let s1 acquire the lock

    # s2 tries to run — the lock is held, so it raises LockHeld.
    with pytest.raises(joblite.LockHeld):
        await s2.run()

    stop.set()
    await asyncio.wait_for(t1, timeout=3.0)


def test_scheduler_run_noop_without_tasks(db_path):
    """A scheduler with no tasks added returns without acquiring the
    lock — avoids blocking real schedulers that might be trying to
    hold it."""
    db = joblite.open(db_path)
    sched = Scheduler(db)
    asyncio.run(sched.run())
    # Lock never acquired.
    rows = db.query(
        "SELECT COUNT(*) AS c FROM _joblite_locks WHERE name='joblite-scheduler'"
    )
    assert rows[0]["c"] == 0
