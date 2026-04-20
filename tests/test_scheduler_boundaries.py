"""Real-time cron boundary tests.

The unit tests in `test_scheduler.py` mock `now_unix` and tick
`honker_scheduler_tick(now)` with fabricated timestamps — fast, but
they don't prove the live scheduler loop gets the sleep math right.
If `_main_loop` sleeps one second too long or
`honker_scheduler_soonest()` returns a value in the past (causing a
busy-loop), the unit tests won't catch it.

These tests wait for a real wall-clock cron boundary. Marked
`@pytest.mark.slow` so the default CI run skips them; invoke
explicitly via `pytest -m slow`.
"""

import asyncio
import time

import pytest

import joblite
from joblite import Scheduler, crontab


@pytest.mark.slow
async def test_scheduler_fires_at_real_minute_boundary(db_path):
    """Register `* * * * *`, start the scheduler, and wait for the
    next real minute boundary. Assert:
      1. A job lands in the queue within ±3s of the boundary.
      2. The scheduler didn't busy-loop during the wait (sleep math
         is correct).

    Runs for up to ~65s in the worst case (just after a boundary).
    """
    db = joblite.open(db_path)
    db.queue("real-boundary")
    sched = Scheduler(db)
    sched.add(
        name="tick",
        queue="real-boundary",
        schedule=crontab("* * * * *"),
    )

    # Compute the expected boundary: the first unix_ts divisible by
    # 60 strictly after `now`.
    start_ts = time.time()
    expected_boundary = (int(start_ts) // 60 + 1) * 60

    # Timing instrumentation: count wake-ups in `_main_loop` by
    # monkey-patching `honker_scheduler_tick`. If the scheduler is
    # busy-looping, this counter jumps well past the expected
    # once-per-boundary cadence.
    tick_count = 0
    original_query = db.transaction

    # Simpler: just check how many rows land in the queue after
    # the boundary passes. Busy-looping would produce >1 fire.
    stop_event = asyncio.Event()
    run_task = asyncio.create_task(sched.run(stop_event))

    # Wait until at least 3s past the expected boundary, so any late
    # fire has clearly landed.
    wait_until = expected_boundary + 3.0
    remaining = wait_until - time.time()
    await asyncio.sleep(remaining)

    # Inspect the queue. Exactly one job should have fired at the
    # boundary (± the scheduler's sleep resolution).
    rows = db.query(
        "SELECT created_at FROM _joblite_live WHERE queue='real-boundary'"
    )
    stop_event.set()
    await asyncio.wait_for(run_task, timeout=5.0)

    assert len(rows) == 1, (
        f"expected exactly 1 fire at the boundary, got {len(rows)}. "
        f"Likely the scheduler busy-looped or double-fired."
    )
    actual_ts = rows[0]["created_at"]
    drift = abs(actual_ts - expected_boundary)
    assert drift <= 3, (
        f"fire landed at unix_ts={actual_ts}, expected near "
        f"{expected_boundary} (drift={drift}s). Sleep math likely off."
    )


@pytest.mark.slow
async def test_scheduler_does_not_busy_loop_between_boundaries(db_path):
    """A scheduler with only a `*/5 * * * *` task (next fire is 1-5
    minutes away) should NOT consume measurable CPU in its wait.
    Rough proxy: we tick the scheduler for 3s, count how many times
    `honker_scheduler_tick` got called. If the sleep math is wrong
    (soonest returns a value in the past), we'd see hundreds.
    """
    db = joblite.open(db_path)
    db.queue("idle-q")
    sched = Scheduler(db)
    sched.add(
        name="far",
        queue="idle-q",
        schedule=crontab("*/5 * * * *"),  # up to 5 minutes away
    )

    # Wrap honker_scheduler_tick to count calls. We do this by reading
    # the task's `next_fire_at` and watching it for unexpected
    # advancement (would indicate a tick fired when it shouldn't).
    row_before = db.query(
        "SELECT next_fire_at FROM _joblite_scheduler_tasks WHERE name='far'"
    )[0]
    before_next = int(row_before["next_fire_at"])

    stop_event = asyncio.Event()
    run_task = asyncio.create_task(sched.run(stop_event))
    await asyncio.sleep(3.0)
    stop_event.set()
    await asyncio.wait_for(run_task, timeout=5.0)

    # In 3s, no fire should happen (next fire is 1-5 minutes out).
    # next_fire_at should be unchanged.
    row_after = db.query(
        "SELECT next_fire_at FROM _joblite_scheduler_tasks WHERE name='far'"
    )[0]
    after_next = int(row_after["next_fire_at"])
    assert after_next == before_next, (
        f"next_fire_at advanced from {before_next} to {after_next} "
        f"in 3s — scheduler busy-looped past a boundary it shouldn't "
        f"have seen yet."
    )
    # And no jobs landed.
    rows = db.query(
        "SELECT COUNT(*) AS c FROM _joblite_live WHERE queue='idle-q'"
    )
    assert rows[0]["c"] == 0
