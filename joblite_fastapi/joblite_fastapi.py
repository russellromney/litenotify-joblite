import asyncio
import json
import traceback
import uuid
from typing import Any, Callable, Optional

from fastapi import HTTPException, Request
from fastapi.responses import StreamingResponse

import joblite
from joblite import Retryable  # re-export for backward-compatible import path


class JobliteApp:
    """FastAPI integration for joblite.

    The `authorize` callable may be sync or async; a coroutine return
    value is awaited. If it raises, the exception propagates and FastAPI
    returns HTTP 500; the SSE stream is never opened (no hang, no
    partial response).
    """

    def __init__(
        self,
        app,
        db: joblite.Database,
        authorize: Optional[Callable[[Any, str], bool]] = None,
        subscribe_path: str = "/joblite/subscribe/{channel}",
        stream_path: str = "/joblite/stream/{name}",
        user_dependency: Optional[Callable] = None,
    ):
        self.app = app
        self.db = db
        self.authorize = authorize
        self.subscribe_path = subscribe_path
        self.stream_path = stream_path
        self.user_dependency = user_dependency
        self.tasks: dict = {}
        self._worker_tasks: list = []
        self._instance_id = uuid.uuid4().hex[:8]

        app.router.add_event_handler("startup", self._start_workers)
        app.router.add_event_handler("shutdown", self._stop_workers)
        self._register_subscribe_route()
        self._register_stream_route()

    def task(
        self,
        queue_name: str,
        concurrency: int = 1,
        visibility_timeout_s: int = 300,
        max_attempts: int = 3,
    ):
        def decorator(func: Callable):
            q = self.db.queue(
                queue_name,
                visibility_timeout_s=visibility_timeout_s,
                max_attempts=max_attempts,
            )
            self.tasks[queue_name] = {
                "func": func,
                "queue": q,
                "concurrency": concurrency,
            }
            return func

        return decorator

    async def _start_workers(self):
        for queue_name, info in self.tasks.items():
            for i in range(info["concurrency"]):
                worker_id = f"fastapi-{self._instance_id}-{queue_name}-{i}"
                t = asyncio.create_task(
                    self._worker_loop(info["queue"], info["func"], worker_id)
                )
                self._worker_tasks.append(t)

    async def _stop_workers(self):
        for t in self._worker_tasks:
            t.cancel()
        if self._worker_tasks:
            await asyncio.gather(*self._worker_tasks, return_exceptions=True)
        self._worker_tasks = []

    async def _worker_loop(
        self, queue: joblite.Queue, func: Callable, worker_id: str
    ):
        try:
            async for job in queue.claim(worker_id):
                try:
                    if asyncio.iscoroutinefunction(func):
                        await func(job.payload)
                    else:
                        func(job.payload)
                    job.ack()
                except Retryable as r:
                    job.retry(delay_s=r.delay_s, error=str(r))
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    err = f"{e}\n{traceback.format_exc()}"
                    job.retry(delay_s=60, error=err)
        except asyncio.CancelledError:
            raise

    def _register_subscribe_route(self):
        path = self.subscribe_path
        user_dep = self.user_dependency
        authorize = self.authorize
        db = self.db

        if user_dep is None:

            async def subscribe(request: Request, channel: str):
                return await _sse_response(db, request, channel, None, authorize)

        else:
            from fastapi import Depends

            async def subscribe(
                request: Request, channel: str, user=Depends(user_dep)
            ):
                return await _sse_response(db, request, channel, user, authorize)

        self.app.get(path)(subscribe)

    def _register_stream_route(self):
        path = self.stream_path
        user_dep = self.user_dependency
        authorize = self.authorize
        db = self.db

        if user_dep is None:

            async def stream_route(request: Request, name: str):
                return await _sse_stream_response(
                    db, request, name, None, authorize
                )

        else:
            from fastapi import Depends

            async def stream_route(
                request: Request, name: str, user=Depends(user_dep)
            ):
                return await _sse_stream_response(
                    db, request, name, user, authorize
                )

        self.app.get(path)(stream_route)


async def _run_authorize(
    authorize: Optional[Callable[[Any, str], Any]], user: Any, target: str
) -> bool:
    """Evaluate an authorize callable.

    Policy:
      * `authorize` may be sync or async. Async is detected by looking at
        the return value: if it's a coroutine we `await` it. This also
        covers callable objects whose `__call__` is async.
      * If `authorize` raises (sync or async), the exception is propagated
        unchanged. Framework glue (FastAPI/Starlette, Django) is expected
        to turn that into an HTTP 500 via its normal exception handling.
        We never swallow the exception or keep the SSE stream open in an
        ambiguous state.
    """
    if authorize is None:
        return True
    result = authorize(user, target)
    if asyncio.iscoroutine(result):
        result = await result
    return bool(result)


async def _sse_response(
    db: joblite.Database,
    request: Request,
    channel: str,
    user: Any,
    authorize: Optional[Callable[[Any, str], bool]],
):
    if not await _run_authorize(authorize, user, channel):
        raise HTTPException(status_code=403, detail="forbidden")

    listener = db.listen(channel)

    async def event_stream():
        try:
            while True:
                if await request.is_disconnected():
                    break
                try:
                    notif = await asyncio.wait_for(
                        listener.__anext__(), timeout=15.0
                    )
                except asyncio.TimeoutError:
                    yield ": keepalive\n\n"
                    continue
                except StopAsyncIteration:
                    break
                data = {"channel": notif.channel, "payload": notif.payload}
                yield f"data: {json.dumps(data)}\n\n"
        except asyncio.CancelledError:
            return

    return StreamingResponse(event_stream(), media_type="text/event-stream")


def _parse_last_event_id(request: Request) -> int:
    """Parse the SSE Last-Event-ID header as an integer offset. Return 0 on
    missing or malformed header (the browser will re-request from scratch).
    """
    raw = request.headers.get("last-event-id")
    if not raw:
        return 0
    try:
        return max(0, int(raw))
    except ValueError:
        return 0


async def _sse_stream_response(
    db: joblite.Database,
    request: Request,
    name: str,
    user: Any,
    authorize: Optional[Callable[[Any, str], bool]],
):
    if not await _run_authorize(authorize, user, name):
        raise HTTPException(status_code=403, detail="forbidden")

    from_offset = _parse_last_event_id(request)
    stream = db.stream(name)
    iterator = stream.subscribe(from_offset=from_offset).__aiter__()

    async def event_stream():
        try:
            while True:
                if await request.is_disconnected():
                    break
                try:
                    event = await asyncio.wait_for(
                        iterator.__anext__(), timeout=15.0
                    )
                except asyncio.TimeoutError:
                    yield ": keepalive\n\n"
                    continue
                except StopAsyncIteration:
                    break
                data_json = json.dumps(event.payload)
                # SSE `id:` is what the browser echoes back in
                # Last-Event-ID on reconnect — we use the stream offset.
                yield (
                    f"id: {event.offset}\n"
                    f"data: {data_json}\n\n"
                )
        except asyncio.CancelledError:
            return

    return StreamingResponse(event_stream(), media_type="text/event-stream")
