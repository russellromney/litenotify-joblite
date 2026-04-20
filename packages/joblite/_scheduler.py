"""Crontab-style periodic-task scheduler for joblite.

A scheduler process holds a set of named schedules (cron expressions
→ queue + payload). On each cron boundary, it enqueues the payload
into the named queue. Regular workers claim and execute it. The
scheduler itself doesn't run handlers — it just dispatches.

Leader election via `db.lock('joblite-scheduler', ttl=60)` ensures at
most one scheduler fires across all scheduler processes. A periodic
heartbeat refreshes the lock's TTL during long sleeps between fires.
If the leader crashes, the TTL elapses and a standby can take over.

Per-task `last_fire_at` is persisted in `_joblite_scheduler_state`,
so scheduler restart within the same boundary window doesn't
double-fire.

Missed fires (scheduler down during a boundary) are NOT caught up —
when the scheduler comes back, it computes the next boundary after
now and sleeps. Users who want catch-up semantics can layer that on
top; an unbounded catch-up after a long outage is usually worse than
skipping.

Usage:

    import asyncio
    import joblite
    from joblite import Scheduler, crontab

    db = joblite.open("app.db")
    scheduler = Scheduler(db)
    scheduler.add(
        name="nightly-backup",
        queue="backups",
        schedule=crontab("0 3 * * *"),
        payload={"target": "s3"},
        expires=3600,  # fired job drops out of claim after 1 hour
    )
    scheduler.add(
        name="every-five",
        queue="health",
        schedule=crontab("*/5 * * * *"),
    )
    asyncio.run(scheduler.run())
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Dict, Optional


def _parse_field(field: str, lo: int, hi: int) -> frozenset[int]:
    """Parse one cron field into the set of valid integer values.

    Supports:
      - `*` (any value in [lo, hi])
      - `N` (single value)
      - `N-M` (inclusive range)
      - `*/K` (every K starting at lo)
      - `N-M/K` (range with step)
      - `N,M,P,...` (list; each part can use the above syntax)
    """
    values: set[int] = set()
    for part in field.split(","):
        step = 1
        if "/" in part:
            range_part, step_str = part.split("/", 1)
            step = int(step_str)
            if step <= 0:
                raise ValueError(f"cron step must be positive: {part!r}")
        else:
            range_part = part
        if range_part == "*":
            start, end = lo, hi
        elif "-" in range_part:
            a, b = range_part.split("-", 1)
            start, end = int(a), int(b)
        else:
            start = end = int(range_part)
        if start < lo or end > hi or start > end:
            raise ValueError(
                f"cron field {part!r} out of range [{lo},{hi}] or inverted"
            )
        for v in range(start, end + 1, step):
            values.add(v)
    return frozenset(values)


class CronSchedule:
    """A parsed 5-field cron expression.

    Fields (standard Unix cron):
      - minute       (0-59)
      - hour         (0-23)
      - day-of-month (1-31)
      - month        (1-12)
      - day-of-week  (0-6, Sunday=0)

    Datetime arithmetic uses the system's local time zone — same as
    standard cron. Set `TZ=UTC` in the scheduler's environment if you
    want UTC boundaries.
    """

    __slots__ = ("_minutes", "_hours", "_days", "_months", "_dows", "_expr")

    def __init__(self, expr: str):
        parts = expr.split()
        if len(parts) != 5:
            raise ValueError(
                f"crontab requires 5 fields "
                f"(minute hour dom month dow); got {len(parts)}: {expr!r}"
            )
        self._expr = expr
        self._minutes = _parse_field(parts[0], 0, 59)
        self._hours = _parse_field(parts[1], 0, 23)
        self._days = _parse_field(parts[2], 1, 31)
        self._months = _parse_field(parts[3], 1, 12)
        self._dows = _parse_field(parts[4], 0, 6)

    def __repr__(self) -> str:
        return f"crontab({self._expr!r})"

    def matches(self, dt: datetime) -> bool:
        """True iff `dt` (minute precision) matches all 5 fields."""
        # Python weekday(): Monday=0 .. Sunday=6.
        # Cron dow:         Sunday=0 .. Saturday=6.
        cron_dow = (dt.weekday() + 1) % 7
        return (
            dt.minute in self._minutes
            and dt.hour in self._hours
            and dt.day in self._days
            and dt.month in self._months
            and cron_dow in self._dows
        )

    def next_after(self, dt: datetime) -> datetime:
        """Return the next datetime strictly after `dt` matching this
        schedule, at minute precision. Raises `ValueError` if no match
        exists within ~5 years (catches degenerate schedules).
        """
        next_dt = dt.replace(second=0, microsecond=0) + timedelta(minutes=1)
        # Cap iterations at ~5 years' worth of minutes.
        for _ in range(5 * 366 * 24 * 60):
            if self.matches(next_dt):
                return next_dt
            next_dt += timedelta(minutes=1)
        raise ValueError(
            f"no cron match found within 5 years after {dt}: {self!r}"
        )


def crontab(expr: str) -> CronSchedule:
    """Parse a 5-field cron expression into a `CronSchedule`."""
    return CronSchedule(expr)


@dataclass
class _ScheduledTask:
    name: str
    queue: str
    schedule: CronSchedule
    payload: Any = None
    priority: int = 0
    expires: Optional[float] = None


class Scheduler:
    """Periodic-task dispatcher. Run one process worth of it per app;
    multiple scheduler processes compete for the leader lock and only
    one fires.

    The scheduler enqueues into named queues on cron boundaries. It
    doesn't run handlers — regular workers consume the enqueued jobs
    like any other job.
    """

    LOCK_NAME = "joblite-scheduler"
    LOCK_TTL = 60
    HEARTBEAT_INTERVAL = 30

    def __init__(self, db, lock_name: Optional[str] = None):
        self.db = db
        self.lock_name = lock_name or self.LOCK_NAME
        self._tasks: Dict[str, _ScheduledTask] = {}

    def add(
        self,
        name: str,
        queue: str,
        schedule: CronSchedule,
        payload: Any = None,
        priority: int = 0,
        expires: Optional[float] = None,
    ) -> None:
        """Register a periodic task.

        - `name`: unique per-scheduler identifier. Used as the key in
          `_joblite_scheduler_state` to track `last_fire_at`. A
          second `add` with the same name replaces the first.
        - `queue`: the queue to enqueue into on each boundary.
        - `schedule`: a `CronSchedule` from `crontab(expr)`.
        - `payload`: the payload for enqueued jobs. Default None.
        - `priority`: enqueue priority for fired jobs.
        - `expires`: how many seconds a fired job stays claimable. If
          workers are down longer than this, the job expires and
          `queue.sweep_expired()` will move it to `_joblite_dead`.
          Useful to avoid piling up a backlog.
        """
        self._tasks[name] = _ScheduledTask(
            name=name,
            queue=queue,
            schedule=schedule,
            payload=payload,
            priority=priority,
            expires=expires,
        )

    # --- persistence helpers ------------------------------------------

    def _load_last_fires(self) -> Dict[str, int]:
        rows = self.db.query(
            "SELECT name, last_fire_at FROM _joblite_scheduler_state"
        )
        return {r["name"]: r["last_fire_at"] for r in rows}

    def _record_fire(self, name: str, fire_at_unix: int) -> None:
        with self.db.transaction() as tx:
            tx.query(
                "SELECT jl_scheduler_record_fire(?, ?)",
                [name, fire_at_unix],
            )

    # --- pure fire logic, testable without sleeps --------------------

    def _fire_due(
        self,
        now: datetime,
        next_fires: Dict[str, datetime],
        last_fires: Dict[str, int],
    ) -> int:
        """Fire all tasks whose next boundary is at or before `now`.
        Updates `next_fires` and `last_fires` in place. Returns the
        number of tasks fired.

        Pure function over (now, tasks, state) — no sleeps, no lock.
        Unit-testable without waiting for real cron boundaries.
        """
        fired = 0
        for name, task in self._tasks.items():
            nf = next_fires[name]
            while nf <= now:
                boundary_ts = int(nf.timestamp())
                prev = last_fires.get(name, 0)
                if boundary_ts > prev:
                    self.db.queue(task.queue).enqueue(
                        task.payload,
                        priority=task.priority,
                        expires=task.expires,
                    )
                    self._record_fire(name, boundary_ts)
                    last_fires[name] = boundary_ts
                    fired += 1
                nf = task.schedule.next_after(nf)
            next_fires[name] = nf
        return fired

    # --- scheduler main loop -----------------------------------------

    async def run(
        self,
        stop_event: Optional[asyncio.Event] = None,
    ) -> None:
        """Acquire the leader lock and run the scheduler loop until
        `stop_event` is set or the enclosing task is cancelled.

        Raises `joblite.LockHeld` if another scheduler already holds
        the lock. Callers that want hot-standby semantics should wrap
        in a retry loop:

            while True:
                try:
                    await scheduler.run(stop_event)
                    break
                except joblite.LockHeld:
                    await asyncio.sleep(5)
        """
        stop_event = stop_event or asyncio.Event()

        if not self._tasks:
            # Nothing to do; return without acquiring the lock so a
            # misconfigured scheduler doesn't block out other processes.
            return

        with self.db.lock(self.lock_name, ttl=self.LOCK_TTL):
            hb = asyncio.create_task(self._heartbeat_loop(stop_event))
            try:
                await self._main_loop(stop_event)
            finally:
                hb.cancel()
                try:
                    await hb
                except asyncio.CancelledError:
                    pass

    async def _heartbeat_loop(self, stop_event: asyncio.Event) -> None:
        """Refresh the leader lock's `expires_at` every
        HEARTBEAT_INTERVAL seconds so the TTL doesn't elapse during
        long sleeps between cron boundaries.
        """
        while not stop_event.is_set():
            try:
                await asyncio.wait_for(
                    stop_event.wait(), timeout=self.HEARTBEAT_INTERVAL
                )
                return  # stop_event set
            except asyncio.TimeoutError:
                pass
            with self.db.transaction() as tx:
                tx.execute(
                    "UPDATE _joblite_locks "
                    "SET expires_at = unixepoch() + ? "
                    "WHERE name = ?",
                    [self.LOCK_TTL, self.lock_name],
                )

    async def _main_loop(self, stop_event: asyncio.Event) -> None:
        last_fires = self._load_last_fires()
        now = datetime.now()
        next_fires = {
            name: task.schedule.next_after(now)
            for name, task in self._tasks.items()
        }
        while not stop_event.is_set():
            now = datetime.now()
            self._fire_due(now, next_fires, last_fires)
            if not next_fires:
                return
            soonest = min(next_fires.values())
            sleep_s = max(
                0.1, (soonest - datetime.now()).total_seconds()
            )
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=sleep_s)
                return  # stop_event set
            except asyncio.TimeoutError:
                continue
