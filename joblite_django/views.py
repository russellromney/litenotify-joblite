import asyncio
import json

from django.http import HttpResponseForbidden, StreamingHttpResponse

import joblite_django


async def _run_authorize(authorize, user, target: str) -> bool:
    """Evaluate the authorize callable.

    Policy:
      * `authorize` may be sync or async. Async is detected by looking at
        the return value: if it's a coroutine we `await` it. This also
        covers callable objects with an async `__call__`.
      * If `authorize` raises (sync or async), the exception is propagated
        unchanged. Django's async view machinery turns that into a 500;
        we never swallow the error or keep the SSE stream open in an
        ambiguous state.
    """
    if authorize is None:
        return True
    result = authorize(user, target)
    if asyncio.iscoroutine(result):
        result = await result
    return bool(result)


async def subscribe_sse(request, channel: str):
    """Async SSE view bridging db.listen(channel) over a raw NOTIFY channel."""
    authorize = joblite_django.get_authorize()
    user = joblite_django.get_user_factory()(request)
    if not await _run_authorize(authorize, user, channel):
        return HttpResponseForbidden("forbidden")

    db = joblite_django.db()
    listener = db.listen(channel)

    async def gen():
        try:
            while True:
                try:
                    notif = await asyncio.wait_for(
                        listener.__anext__(), timeout=15.0
                    )
                except asyncio.TimeoutError:
                    yield b": keepalive\n\n"
                    continue
                except StopAsyncIteration:
                    break
                data = {"channel": notif.channel, "payload": notif.payload}
                yield f"data: {json.dumps(data)}\n\n".encode()
        except asyncio.CancelledError:
            return

    return StreamingHttpResponse(gen(), content_type="text/event-stream")


async def stream_sse(request, name: str):
    """Async SSE view for a durable stream with Last-Event-ID replay."""
    authorize = joblite_django.get_authorize()
    user = joblite_django.get_user_factory()(request)
    if not await _run_authorize(authorize, user, name):
        return HttpResponseForbidden("forbidden")

    try:
        from_offset = int(request.headers.get("Last-Event-ID") or "0")
        if from_offset < 0:
            from_offset = 0
    except ValueError:
        from_offset = 0

    db = joblite_django.db()
    iterator = db.stream(name).subscribe(from_offset=from_offset).__aiter__()

    async def gen():
        try:
            while True:
                try:
                    event = await asyncio.wait_for(
                        iterator.__anext__(), timeout=15.0
                    )
                except asyncio.TimeoutError:
                    yield b": keepalive\n\n"
                    continue
                except StopAsyncIteration:
                    break
                payload = json.dumps(event.payload)
                yield f"id: {event.offset}\ndata: {payload}\n\n".encode()
        except asyncio.CancelledError:
            return

    return StreamingHttpResponse(gen(), content_type="text/event-stream")
