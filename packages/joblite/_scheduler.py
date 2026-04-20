"""Crontab-style periodic-task scheduler for joblite.

A scheduler process holds a set of named schedules (cron expressions
→ queue + payload). On each cron boundary, it enqueues the payload
into the named queue. Regular workers claim and execute it. The
scheduler itself doesn't run handlers — it just dispatches.

Registration + fire-due logic live in Rust via `honker_scheduler_register`
and `honker_scheduler_tick`; this module is a thin asyncio wrapper. Tasks
persist in `_joblite_scheduler_tasks`, so any process (Python, a `sqlite3
.load` session, a future Node/Go binding) sees the same registrations.

Leader election via `db.lock('joblite-scheduler', ttl=60)` ensures at
most one scheduler fires across all scheduler processes. A periodic
heartbeat refreshes the lock's TTL during long sleeps between fires.
If the leader crashes, the TTL elapses and a standby can take over.

Boundaries that were missed while the leader was down are caught up
on the next tick — `honker_scheduler_tick` advances `next_fire_at`
minute-by-minute until it's past `now`. For noisy schedules that
shouldn't backfill, set `expires=` so stale jobs get swept instead
of executed.

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
import json
import time
from datetime import datetime
from typing import Any, Optional

import litenotify


class CronSchedule:
    """Thin marker around a 5-field cron expression. All parsing and
    next-boundary computation lives in Rust (`litenotify.cron_next_after`
    / `honker_cron_next_after`) so every language binding shares one
    implementation.

    Fields (standard Unix cron):
      - minute       (0-59)
      - hour         (0-23)
      - day-of-month (1-31)
      - month        (1-12)
      - day-of-week  (0-6, Sunday=0)

    Calendar arithmetic runs in the system local time zone — same as
    standard cron. Set `TZ=UTC` in the scheduler's environment if you
    want UTC boundaries.
    """

    __slots__ = ("expr",)

    def __init__(self, expr: str):
        # Validate eagerly by asking Rust to compute one boundary from
        # a known timestamp. Raises ValueError on malformed input
        # (field count, out-of-range, inverted range, bad step).
        litenotify.cron_next_after(expr, 0)
        self.expr = expr

    def __repr__(self) -> str:
        return f"crontab({self.expr!r})"

    def next_after(self, dt: datetime) -> datetime:
        """Return the next datetime strictly after `dt` matching this
        schedule, at minute precision. Pure function — no db needed.
        Raises `ValueError` if no match exists within ~5 years.
        """
        return datetime.fromtimestamp(
            litenotify.cron_next_after(self.expr, int(dt.timestamp()))
        )


def crontab(expr: str) -> CronSchedule:
    """Parse a 5-field cron expression into a `CronSchedule`."""
    return CronSchedule(expr)


class Scheduler:
    """Periodic-task dispatcher. Run one process worth of it per app;
    multiple scheduler processes compete for the leader lock and only
    one fires.

    Task registration + fire-due logic live in Rust
    (`honker_scheduler_register` / `honker_scheduler_tick`); this class is
    ~40 lines of asyncio glue around lock + tick + sleep + heartbeat.
    """

    LOCK_NAME = "joblite-scheduler"
    LOCK_TTL = 60
    HEARTBEAT_INTERVAL = 30

    def __init__(self, db, lock_name: Optional[str] = None):
        self.db = db
        self.lock_name = lock_name or self.LOCK_NAME
        # Names registered via this Scheduler instance. The
        # authoritative registration lives in
        # `_joblite_scheduler_tasks` — this set only exists so a
        # process with no tasks to add doesn't acquire the lock in
        # `run()`.
        self._registered: set[str] = set()

    def add(
        self,
        name: str,
        queue: str,
        schedule: CronSchedule,
        payload: Any = None,
        priority: int = 0,
        expires: Optional[float] = None,
    ) -> None:
        """Register a periodic task in `_joblite_scheduler_tasks`.

        - `name`: unique per-scheduler identifier. A second `add`
          with the same name replaces the first registration
          entirely (including cron expr, queue, payload).
        - `queue`: the queue to enqueue into on each boundary.
        - `schedule`: a `CronSchedule` from `crontab(expr)`.
        - `payload`: the payload for enqueued jobs. Default None.
        - `priority`: enqueue priority for fired jobs.
        - `expires`: how many seconds a fired job stays claimable.
          `queue.sweep_expired()` moves expired rows into
          `_joblite_dead`.
        """
        with self.db.transaction() as tx:
            tx.query(
                "SELECT honker_scheduler_register(?, ?, ?, ?, ?, ?)",
                [
                    name,
                    queue,
                    schedule.expr,
                    json.dumps(payload),
                    int(priority),
                    int(expires) if expires is not None else None,
                ],
            )
        self._registered.add(name)

    def remove(self, name: str) -> bool:
        """Unregister a task. Returns True iff a row was removed."""
        with self.db.transaction() as tx:
            rows = tx.query(
                "SELECT honker_scheduler_unregister(?) AS n", [name]
            )
        self._registered.discard(name)
        return rows[0]["n"] > 0

    # --- scheduler main loop -----------------------------------------

    async def run(
        self,
        stop_event: Optional[asyncio.Event] = None,
    ) -> None:
        """Acquire the leader lock and run the scheduler loop until
        `stop_event` is set or the enclosing task is cancelled.

        Raises `joblite.LockHeld` if another scheduler already holds
        the lock. Callers that want hot-standby semantics should wrap
        in a retry loop.
        """
        stop_event = stop_event or asyncio.Event()

        if not self._registered:
            # Nothing to do; return without acquiring the lock so a
            # misconfigured scheduler doesn't block out other
            # processes.
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
        while not stop_event.is_set():
            now = int(time.time())
            # tick + soonest share a writer transaction: honker_* scalars
            # are registered on the writer slot only (one copy, lowest
            # memory), so reads through reader connections wouldn't
            # find them. The soonest query also serializes behind the
            # tick, so we can't miss a freshly registered task.
            with self.db.transaction() as tx:
                tx.query("SELECT honker_scheduler_tick(?)", [now])
                rows = tx.query(
                    "SELECT honker_scheduler_soonest() AS t"
                )
            soonest = int(rows[0]["t"])
            if soonest == 0:
                return
            sleep_s = max(0.1, soonest - time.time())
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=sleep_s)
                return  # stop_event set
            except asyncio.TimeoutError:
                continue
