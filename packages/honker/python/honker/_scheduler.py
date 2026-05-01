"""Time-trigger scheduler for honker.

A scheduler process holds a set of named schedules (cron expressions
→ queue + payload). On each cron boundary, it enqueues the payload
into the named queue. Regular workers claim and execute it. The
scheduler itself doesn't run handlers — it just dispatches.

Registration + fire-due logic live in Rust via `honker_scheduler_register`
and `honker_scheduler_tick`; this module is a thin asyncio wrapper. Tasks
persist in `_honker_scheduler_tasks`, so any process (Python, a `sqlite3
.load` session, a future Node/Go binding) sees the same registrations.

Leader election via `db.lock('honker-scheduler', ttl=60)` ensures at
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
    import honker
    from honker import Scheduler, crontab, every_s

    db = honker.open("app.db")
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
        schedule=every_s(5),
    )
    asyncio.run(scheduler.run())
"""

from __future__ import annotations

import asyncio
import json
import time
from datetime import datetime
from typing import Any, Optional

from honker import _honker_native


class CronSchedule:
    """Thin marker around a scheduler expression.

    All parsing and next-boundary computation lives in Rust
    (`honker.cron_next_after` / `honker_cron_next_after`) so every
    language binding shares one implementation.

    Valid expression shapes:
      - 5-field cron: `minute hour dom month dow`
      - 6-field cron: `second minute hour dom month dow`
      - interval expression: `@every <n><unit>` (e.g. `@every 1s`)
    """

    __slots__ = ("expr",)

    def __init__(self, expr: str):
        # Validate eagerly by asking Rust to compute one boundary from
        # a known timestamp. Raises ValueError on malformed input.
        _honker_native.cron_next_after(expr, 0)
        self.expr = expr

    def __repr__(self) -> str:
        return f"crontab({self.expr!r})"

    def next_after(self, dt: datetime) -> datetime:
        """Return the next datetime strictly after `dt` matching this
        schedule expression. Pure function — no db needed.
        """
        return datetime.fromtimestamp(
            _honker_native.cron_next_after(self.expr, int(dt.timestamp()))
        )


def crontab(expr: str) -> CronSchedule:
    """Parse a 5-field or 6-field cron expression into a
    `CronSchedule`.
    """
    if expr.strip().startswith("@every"):
        raise ValueError("use every_s(...) for interval schedules")
    return CronSchedule(expr)


def every_s(seconds: int) -> CronSchedule:
    """Build a fixed-interval schedule expression persisted as
    `@every <n>s`.
    """
    seconds = int(seconds)
    if seconds <= 0:
        raise ValueError("every_s(seconds) requires a positive integer")
    return CronSchedule(f"@every {seconds}s")


class Scheduler:
    """Periodic-task dispatcher. Run one process worth of it per app;
    multiple scheduler processes compete for the leader lock and only
    one fires.

    Task registration + fire-due logic live in Rust
    (`honker_scheduler_register` / `honker_scheduler_tick`); this class is
    ~40 lines of asyncio glue around lock + tick + sleep + heartbeat.

    Leader-lock caveat: the scheduler lock is TTL-based and
    best-effort, not true mutual exclusion. If a leader process pauses
    for longer than `LOCK_TTL` (60s, e.g. GC pause, laptop sleep,
    kernel OOM pressure), another process can acquire the lock while
    the "dead" leader is still running. Both would fire scheduled
    tasks until the original wakes and notices its lock is gone.

    For idempotent cron work (health checks, metrics rollups) this is
    fine. For work that must run exactly once per fire (nightly
    backup, invoice generation), wrap the task body in a second
    `db.lock('task-name', ttl=...)` and have it exit early on
    `LockHeld`.
    """

    LOCK_NAME = "honker-scheduler"
    LOCK_TTL = 60
    HEARTBEAT_INTERVAL = 30

    def __init__(self, db, lock_name: Optional[str] = None):
        self.db = db
        self.lock_name = lock_name or self.LOCK_NAME
        # Names registered via this Scheduler instance. The
        # authoritative registration lives in
        # `_honker_scheduler_tasks` — this set only exists so a
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
        """Register a periodic task in `_honker_scheduler_tasks`.

        - `name`: unique per-scheduler identifier. A second `add`
          with the same name replaces the first registration
          entirely (including cron expr, queue, payload).
        - `queue`: the queue to enqueue into on each boundary.
        - `schedule`: a `CronSchedule` from `crontab(expr)` or
          `every_s(n)`.
        - `payload`: the payload for enqueued jobs. Default None.
        - `priority`: enqueue priority for fired jobs.
        - `expires`: how many seconds a fired job stays claimable.
          `queue.sweep_expired()` moves expired rows into
          `_honker_dead`.
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

        Raises `honker.LockHeld` if another scheduler already holds
        the lock. Callers that want hot-standby semantics should wrap
        in a retry loop.
        """
        stop_event = stop_event or asyncio.Event()

        if not self._registered:
            # This process didn't call .add(), but tasks may have been
            # registered by another process (separate registrar, CLI,
            # migration, prior run persisted to disk). Check the
            # authoritative table before bailing. Raising is friendlier
            # than silent no-op — a dedicated runner process that
            # loads tasks from elsewhere would otherwise log nothing
            # and fire nothing.
            rows = self.db.query(
                "SELECT COUNT(*) AS n FROM _honker_scheduler_tasks"
            )
            if not rows or rows[0]["n"] == 0:
                raise RuntimeError(
                    "Scheduler.run() called with no registered tasks. "
                    "Call scheduler.add(...) first, or ensure another "
                    "process has populated _honker_scheduler_tasks."
                )

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
                    "UPDATE _honker_locks "
                    "SET expires_at = unixepoch() + ? "
                    "WHERE name = ?",
                    [self.LOCK_TTL, self.lock_name],
                )

    async def _main_loop(self, stop_event: asyncio.Event) -> None:
        # Subscribe to WAL eagerly so a register/unregister landing
        # during the tick transaction is buffered. `honker_scheduler_
        # register` and `_unregister` emit a wake on the
        # 'honker:scheduler' channel precisely to kick us out of a
        # sleep — otherwise we'd oversleep past a freshly-registered
        # task whose next_fire_at is earlier than the previously
        # computed soonest.
        updates = self.db.update_events()
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
            # Race three wake sources against the timer:
            #   - stop_event   → caller asked us to shut down
            #   - update tick     → a register/unregister (or any other
            #                    commit) happened; re-evaluate soonest
            #   - timeout      → the originally-computed soonest fired
            # Any of the three just falls through to the top of the loop.
            stop_task = asyncio.ensure_future(stop_event.wait())
            update_task = asyncio.ensure_future(updates.__anext__())
            try:
                await asyncio.wait(
                    {stop_task, update_task},
                    timeout=sleep_s,
                    return_when=asyncio.FIRST_COMPLETED,
                )
            finally:
                for t in (stop_task, update_task):
                    if not t.done():
                        t.cancel()
                # Surface task exceptions (other than CancelledError)
                # so a broken update iterator doesn't silently hang the
                # scheduler. Done tasks that we didn't await would
                # otherwise warn at GC.
                for t in (stop_task, update_task):
                    try:
                        await t
                    except (asyncio.CancelledError, StopAsyncIteration):
                        pass
