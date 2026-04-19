"""Shared worker-side task execution.

`run_task` is called by every framework plugin's worker loop to
execute one claimed job. It centralizes the behaviors that the
`@task(...)` decorator knobs configure:

  - `timeout=N`     : wall-clock bound on handler execution.
  - `retries=N`     : max attempt count before the job moves to
                      `_joblite_dead`. If None, retry forever (up to
                      the Queue's `max_attempts`).
  - `retry_delay=S` : base delay between retries, in seconds.
  - `backoff=B`     : multiplier applied per attempt — delay on attempt
                      N is `retry_delay * backoff**(N-1)`. Set to 1.0
                      for constant delay.

Plugins call `run_task` inside their `async for job in queue.claim(...)`
loop. One implementation, three callers — plugin-specific concurrency
(in-process async tasks for FastAPI; CLI worker loops for Django /
Flask) wraps the call, but the handler-execution semantics are
identical.
"""

import asyncio
import traceback
from typing import Any, Callable, Optional

from joblite.joblite import Retryable


async def run_task(
    job,
    handler: Callable,
    *,
    timeout: Optional[float] = None,
    retries: Optional[int] = None,
    retry_delay: float = 60.0,
    backoff: float = 1.0,
    save_result: bool = False,
    result_ttl: Optional[float] = 3600.0,
) -> None:
    """Execute `handler(job.payload)` with the given retry policy.

    On success: job.ack(). If `save_result=True`, also persists the
    handler's return value to `_joblite_results` via
    `queue.save_result(job.id, value, ttl=result_ttl)` before the
    ack, so a caller awaiting `queue.wait_result(job.id)` will
    receive the value on their next WAL wake.

    On timeout or exception: job.retry(delay) unless `retries` is set
    and the job has reached that attempt count, in which case
    job.fail() moves the row to _joblite_dead. Results are not saved
    on failure — callers should treat a missing result as "did not
    complete successfully."

    Retryable is honored as a handler-driven retry with a
    caller-chosen delay, bypassing `retry_delay` / `backoff`.

    asyncio.CancelledError is re-raised (the worker-loop task was
    cancelled; let it unwind).
    """
    try:
        if asyncio.iscoroutinefunction(handler):
            if timeout is not None:
                result = await asyncio.wait_for(
                    handler(job.payload), timeout=timeout
                )
            else:
                result = await handler(job.payload)
        else:
            # Sync handlers don't get wall-clock timeout enforcement —
            # asyncio can't interrupt a blocking sync call. We run and
            # hope. Users who want a hard deadline on a sync handler
            # should wrap it themselves or switch to async.
            result = handler(job.payload)
        if save_result:
            job.queue.save_result(job.id, result, ttl=result_ttl)
        job.ack()
    except asyncio.CancelledError:
        # Let the worker-loop unwind. Don't ack / retry — the job's
        # visibility timeout will reclaim it for the next worker.
        raise
    except asyncio.TimeoutError:
        delay = _compute_delay(retry_delay, backoff, job.attempts)
        _retry_or_fail(job, retries, delay, "handler timeout")
    except Retryable as r:
        # Handler explicitly asked for a retry with a caller-chosen
        # delay. Don't apply the default backoff formula.
        job.retry(delay_s=r.delay_s, error=str(r))
    except Exception as e:
        err = f"{e}\n{traceback.format_exc()}"
        delay = _compute_delay(retry_delay, backoff, job.attempts)
        _retry_or_fail(job, retries, delay, err)


def _compute_delay(retry_delay: float, backoff: float, attempts: int) -> int:
    """Exponential backoff. `attempts` is post-increment (the just-
    finished attempt number), so backoff**(attempts-1) gives us
    N=1 → 1x, N=2 → backoff, N=3 → backoff**2, ...
    """
    n = max(0, int(attempts) - 1)
    return int(retry_delay * (backoff ** n))


def _retry_or_fail(job, retries: Optional[int], delay_s: int, error: str) -> None:
    if retries is not None and job.attempts >= retries:
        job.fail(error=error)
    else:
        job.retry(delay_s=delay_s, error=error)
