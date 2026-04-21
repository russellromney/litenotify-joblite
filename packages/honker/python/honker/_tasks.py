"""Huey-style task decorators on top of honker.Queue.

    import honker

    db = honker.open("app.db")
    default = db.queue("default")

    @default.task(retries=3)
    def send_email(to: str, subject: str) -> dict:
        ...
        return {"sent_at": time.time()}

    # Caller side — looks synchronous, isn't.
    r = send_email("alice@example.com", "Hi")  # enqueues
    print(r.get(timeout=10))                   # blocks until worker runs it

    # Worker side — from a different process (or the same):
    #   python -m honker worker myapp.tasks:db --queue=default
    # or inline:
    await db.run_workers(queue="default", concurrency=4)

Design:
  * `@queue.task()` wraps the callable. Registry: name → (fn, opts).
  * Default name is `f"{fn.__module__}.{fn.__qualname__}"` — same as
    Huey/Celery. Override via `@q.task(name="...")`. Rename = orphaned
    jobs in dead-letter, same footgun as Celery — see docs.
  * Calling a wrapped task returns a `TaskResult` (a future-ish). Under
    the hood it `json.dumps`'s args+kwargs into a job payload with a
    `__honker_task__` envelope: {task, args, kwargs}.
  * Result storage is ON by default with 1h TTL — mirrors Huey.
    `@q.task(store_result=False)` opts out for fire-and-forget.
  * `@q.periodic_task(crontab)` wraps `Scheduler.add()` — still runs
    through the task registry.
  * Worker loop resolves task_name → fn from the registry. If a worker
    sees a task it doesn't have registered (e.g. renamed task, worker
    imported different module), the job goes to dead-letter with
    "unknown task: ..." — visible and diagnosable.
"""

from __future__ import annotations

import asyncio
import functools
import importlib
import inspect
import json
import os
import time
import traceback
from dataclasses import dataclass, field
from typing import Any, Callable, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from ._honker import Database, Job, Queue
    from ._scheduler import CronSchedule

# Envelope key inside the job payload — distinguishes decorated-task
# jobs from raw enqueued payloads. Raw payloads from `q.enqueue({...})`
# don't have this key, so the worker dispatches them differently (or
# skips; see Worker.dispatch).
_ENVELOPE = "__honker_task__"

# Default TTL for stored results (1 hour). Long enough for any sane
# .get(), short enough that `_honker_results` doesn't balloon.
_DEFAULT_RESULT_TTL_S = 3600


# -----------------------------------------------------------------------
# Task registry (per-process)
# -----------------------------------------------------------------------

@dataclass
class TaskSpec:
    """Metadata for a decorated task. Stored in the per-process
    registry that both the enqueue-side wrapper and the worker-side
    dispatch consult.
    """
    name: str
    fn: Callable[..., Any]
    queue_name: str
    retries: int = 3
    retry_delay_s: int = 60
    timeout_s: Optional[float] = None
    priority: int = 0
    expires_s: Optional[int] = None
    store_result: bool = True
    result_ttl_s: int = _DEFAULT_RESULT_TTL_S


class TaskRegistry:
    """Per-process map of task name → TaskSpec. Used by both the
    enqueue-side decorator and the worker-side dispatcher.

    Tasks are registered at import time via the `@q.task()` decorator.
    The CLI and the in-process worker both walk this registry.
    """

    def __init__(self):
        self._by_name: dict[str, TaskSpec] = {}

    def register(self, spec: TaskSpec) -> None:
        existing = self._by_name.get(spec.name)
        if existing is not None and existing.fn is not spec.fn:
            raise ValueError(
                f"duplicate task name {spec.name!r} — two functions "
                f"registered under the same name: "
                f"{existing.fn.__module__}.{existing.fn.__qualname__} and "
                f"{spec.fn.__module__}.{spec.fn.__qualname__}. "
                f"Pass an explicit name=... to disambiguate."
            )
        self._by_name[spec.name] = spec

    def get(self, name: str) -> Optional[TaskSpec]:
        return self._by_name.get(name)

    def names(self) -> list[str]:
        return sorted(self._by_name)

    def queues(self) -> set[str]:
        return {s.queue_name for s in self._by_name.values()}


# Single process-wide registry. All Queues on the same Database share
# it so the CLI can discover every task regardless of which Queue
# instance the decorator ran on.
_GLOBAL_REGISTRY = TaskRegistry()


def registry() -> TaskRegistry:
    """Return the process-wide TaskRegistry. Useful for introspection
    and for tests that need to clear state."""
    return _GLOBAL_REGISTRY


# -----------------------------------------------------------------------
# TaskResult — what a wrapped-task call returns on the caller side
# -----------------------------------------------------------------------

class TaskResult:
    """Return value from calling a decorated task. Wraps a job id;
    `.get(timeout)` blocks until the worker finishes and saves the
    result. `.aget()` is the async variant.

    If the task was registered with `store_result=False`, `.get()`
    will time out — no result was ever saved. Check `.id` against
    `_honker_live` / `_honker_dead` for progress visibility.
    """
    __slots__ = ("_db", "_queue", "_job_id")

    def __init__(self, db: "Database", queue: "Queue", job_id: int):
        self._db = db
        self._queue = queue
        self._job_id = job_id

    @property
    def id(self) -> int:
        return self._job_id

    def get(self, timeout: Optional[float] = None) -> Any:
        """Block until the worker saves a result, then return the
        deserialized value. Raises `TimeoutError` on expiry.
        """
        return asyncio.run(self.aget(timeout=timeout))

    async def aget(self, timeout: Optional[float] = None) -> Any:
        return await self._queue.wait_result(self._job_id, timeout=timeout)

    def __repr__(self) -> str:
        return f"TaskResult(queue={self._queue.name!r}, id={self._job_id})"


# -----------------------------------------------------------------------
# Decorator plumbing — what @q.task() returns
# -----------------------------------------------------------------------

def _default_task_name(fn: Callable[..., Any]) -> str:
    """Stable auto-name — matches Huey/Celery convention."""
    module = fn.__module__ or "__main__"
    qualname = fn.__qualname__
    return f"{module}.{qualname}"


class _TaskWrapper:
    """Callable returned by `@q.task()`. Calling it enqueues; the
    original function is still available as `.fn` for direct calls
    (useful in tests).
    """
    def __init__(self, spec: TaskSpec, queue: "Queue"):
        self.spec = spec
        self.fn = spec.fn
        self._queue = queue
        functools.update_wrapper(self, spec.fn)

    @property
    def name(self) -> str:
        return self.spec.name

    def __call__(self, *args: Any, **kwargs: Any) -> TaskResult:
        """Enqueue the task with these args. Does NOT run the function
        in the caller's process."""
        envelope = {
            _ENVELOPE: {
                "task": self.spec.name,
                "args": list(args),
                "kwargs": kwargs,
            }
        }
        job_id = self._queue.enqueue(
            envelope,
            priority=self.spec.priority,
            expires=self.spec.expires_s,
        )
        return TaskResult(self._queue.db, self._queue, job_id)

    def call_local(self, *args: Any, **kwargs: Any) -> Any:
        """Invoke the underlying function directly in the caller's
        process (bypasses the queue). Useful in tests."""
        return self.fn(*args, **kwargs)


def task_decorator(
    queue: "Queue",
    *,
    name: Optional[str] = None,
    retries: Optional[int] = None,
    retry_delay_s: int = 60,
    timeout_s: Optional[float] = None,
    priority: int = 0,
    expires_s: Optional[int] = None,
    store_result: bool = True,
    result_ttl_s: int = _DEFAULT_RESULT_TTL_S,
) -> Callable[[Callable[..., Any]], _TaskWrapper]:
    """Return a decorator that registers `fn` as a task on `queue`.

    See `Queue.task()` in _honker.py for the user-facing docstring.
    """
    def decorate(fn: Callable[..., Any]) -> _TaskWrapper:
        spec = TaskSpec(
            name=name or _default_task_name(fn),
            fn=fn,
            queue_name=queue.name,
            retries=retries if retries is not None else queue.max_attempts,
            retry_delay_s=retry_delay_s,
            timeout_s=timeout_s,
            priority=priority,
            expires_s=expires_s,
            store_result=store_result,
            result_ttl_s=result_ttl_s,
        )
        _GLOBAL_REGISTRY.register(spec)
        return _TaskWrapper(spec, queue)

    return decorate


# -----------------------------------------------------------------------
# Periodic tasks — same registry, registered as scheduler tasks too
# -----------------------------------------------------------------------

def periodic_task_decorator(
    queue: "Queue",
    schedule: "CronSchedule",
    *,
    name: Optional[str] = None,
    scheduler_name: Optional[str] = None,
    expires_s: Optional[int] = None,
    priority: int = 0,
    store_result: bool = False,
    result_ttl_s: int = _DEFAULT_RESULT_TTL_S,
) -> Callable[[Callable[..., Any]], _TaskWrapper]:
    """Wrap a function as a periodic task. Registers BOTH in the task
    registry (so workers can dispatch it) AND in the scheduler (so it
    fires on cron boundaries).

    scheduler_name defaults to the task name — keep both aligned so
    the scheduler's `_honker_scheduler_tasks` row and the task
    registry entry are findable together.
    """
    def decorate(fn: Callable[..., Any]) -> _TaskWrapper:
        resolved_name = name or _default_task_name(fn)
        # Periodic tasks take no args — the scheduler enqueues a fixed
        # payload. Wrap so the envelope has empty args/kwargs.
        spec = TaskSpec(
            name=resolved_name,
            fn=fn,
            queue_name=queue.name,
            priority=priority,
            expires_s=expires_s,
            store_result=store_result,
            result_ttl_s=result_ttl_s,
        )
        _GLOBAL_REGISTRY.register(spec)
        wrapper = _TaskWrapper(spec, queue)

        # Register the scheduler side. `Scheduler.add()` is a thin
        # wrapper over `honker_scheduler_register`, which UPSERTs,
        # so creating a fresh Scheduler each time is harmless.
        from ._scheduler import Scheduler  # lazy to avoid cycle
        Scheduler(queue.db).add(
            name=scheduler_name or resolved_name,
            queue=queue.name,
            schedule=schedule,
            payload={_ENVELOPE: {"task": resolved_name, "args": [], "kwargs": {}}},
            priority=priority,
            expires=expires_s,
        )
        return wrapper

    return decorate


# -----------------------------------------------------------------------
# Worker loop — resolves job → task → calls → saves result
# -----------------------------------------------------------------------

class UnknownTaskError(Exception):
    """Raised when the worker sees a task name it hasn't registered.
    Most common cause: the function was renamed or the worker imports
    a different module than the producer.
    """


async def _run_one(job: "Job", spec: TaskSpec) -> None:
    """Core worker-side dispatch: extract args, call the function,
    save the result (if configured), ack on success. On failure,
    bump retry with the task's configured delay.

    Sync tasks are dispatched onto a background thread via
    `asyncio.to_thread` so a blocking def doesn't freeze the event
    loop (and every other worker sharing it). Async tasks run on
    the event loop directly. Timeouts wrap either uniformly via
    `asyncio.wait_for`.
    """
    from ._honker import Retryable  # lazy cycle break

    payload = job.payload
    envelope = payload.get(_ENVELOPE) if isinstance(payload, dict) else None
    if envelope is None:
        # Raw `q.enqueue(...)` payload, no task name. Treat as a
        # non-task job: ack and move on. A future enhancement could
        # route these to a user-supplied fallback handler.
        job.ack()
        return

    args = envelope.get("args", [])
    kwargs = envelope.get("kwargs", {})

    try:
        if inspect.iscoroutinefunction(spec.fn):
            coro = spec.fn(*args, **kwargs)
        else:
            # Push the sync call off the event loop onto a thread so
            # it doesn't block peer workers. asyncio can't interrupt
            # the thread on timeout — the task is marked timed-out
            # but the thread runs to completion — accept that tradeoff.
            coro = asyncio.to_thread(spec.fn, *args, **kwargs)

        if spec.timeout_s is not None:
            result = await asyncio.wait_for(coro, timeout=spec.timeout_s)
        else:
            result = await coro
    except Retryable as e:
        job.retry(delay_s=e.delay_s, error=str(e))
        return
    except asyncio.TimeoutError:
        job.retry(
            delay_s=spec.retry_delay_s,
            error=f"timeout after {spec.timeout_s}s",
        )
        return
    except Exception as e:
        job.retry(
            delay_s=spec.retry_delay_s,
            error=f"{type(e).__name__}: {e}\n{traceback.format_exc()}",
        )
        return

    # Success path. Save result (if configured) then ack.
    if spec.store_result:
        try:
            job.queue.save_result(job.id, result, ttl=spec.result_ttl_s)
        except Exception:
            # Result save failure shouldn't retry the task; log.
            traceback.print_exc()
    job.ack()


async def run_workers(
    db: "Database",
    queue: Optional[str] = None,
    concurrency: Optional[int] = None,
    registry_: Optional[TaskRegistry] = None,
    stop_event: Optional[asyncio.Event] = None,
) -> None:
    """In-process worker loop. Runs `concurrency` async workers on
    each queue, drains jobs forever (or until `stop_event` is set).

    If `queue` is None, runs workers for every queue in the registry.
    """
    reg = registry_ or _GLOBAL_REGISTRY
    if concurrency is None:
        concurrency = os.cpu_count() or 1
    if stop_event is None:
        stop_event = asyncio.Event()

    queues_to_run = {queue} if queue else reg.queues()
    if not queues_to_run:
        raise RuntimeError(
            "run_workers() called with no tasks registered. "
            "Import the module that defines your @q.task() functions "
            "before calling run_workers()."
        )

    worker_id_prefix = f"honker-worker-{os.getpid()}"
    tasks: list[asyncio.Task] = []

    for qname in queues_to_run:
        q = db.queue(qname)
        for i in range(concurrency):
            worker_id = f"{worker_id_prefix}-{qname}-{i}"
            tasks.append(asyncio.create_task(
                _drain_loop(q, worker_id, reg, stop_event),
            ))

    try:
        await stop_event.wait()
    finally:
        for t in tasks:
            t.cancel()
        for t in tasks:
            try:
                await t
            except (asyncio.CancelledError, Exception):
                pass


async def _drain_loop(
    queue: "Queue",
    worker_id: str,
    reg: TaskRegistry,
    stop_event: asyncio.Event,
) -> None:
    """Inner per-worker loop: claim one job at a time, dispatch,
    repeat until stop_event."""
    async for job in queue.claim(worker_id):
        if stop_event.is_set():
            # Return the claim instead of processing it.
            job.retry(delay_s=0, error="worker shutting down")
            return

        envelope = job.payload.get(_ENVELOPE) if isinstance(job.payload, dict) else None
        if envelope is None:
            # Non-decorated job on a queue that has decorators. We
            # don't know how to run it — dead-letter so it's visible.
            job.fail(error="raw (non-decorated) payload on a decorated-task queue")
            continue

        task_name = envelope.get("task")
        spec = reg.get(task_name) if task_name else None
        if spec is None:
            # Task name the worker doesn't know about. Common cause:
            # rename + deploy of producer without updating worker.
            job.fail(
                error=f"unknown task: {task_name!r}. "
                f"Registered tasks: {reg.names()}"
            )
            continue

        await _run_one(job, spec)


# -----------------------------------------------------------------------
# CLI helpers — used by `python -m honker worker ...`
# -----------------------------------------------------------------------

def load_app(target: str) -> "Database":
    """Parse a CLI target like `myapp.tasks:db` into an imported
    Database instance. Importing the module fires the `@task`
    decorators as a side effect, populating the registry.
    """
    if ":" not in target:
        raise ValueError(
            f"target must be 'module.path:variable', got {target!r}"
        )
    module_path, attr = target.rsplit(":", 1)
    module = importlib.import_module(module_path)
    obj = getattr(module, attr, None)
    if obj is None:
        raise AttributeError(
            f"{module_path} has no attribute {attr!r}"
        )
    return obj
