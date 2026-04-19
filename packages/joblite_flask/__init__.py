"""joblite integration for Flask 3+.

Minimal shape, same as joblite_fastapi:

    from flask import Flask
    import joblite
    from joblite_flask import JobliteFlask

    app = Flask(__name__)
    db = joblite.open("app.db")
    jl = JobliteFlask(app, db, authorize=lambda user, target: True)

    @jl.task("emails")
    async def send_email(payload):
        await mailer.send(payload["to"])

    # Clients GET /joblite/subscribe/<channel>  -> SSE stream of notifications
    # Clients GET /joblite/stream/<name>        -> SSE stream of stream events
    # Supports Last-Event-ID replay for the durable stream endpoint.

    # Workers run via the CLI:
    #   flask --app app joblite_worker

The plugin wires Flask's async-view support to joblite's async
iterators. It does NOT spawn workers inside the Flask process by
default — Flask's sync/async story doesn't have the lifespan hooks
FastAPI does. Use the CLI in a dedicated process, same shape as
joblite_django's `python manage.py joblite_worker`.
"""

import asyncio
import json
import queue as _queue
import threading
import traceback
import uuid
import weakref
from typing import Any, AsyncIterator, Callable, Dict, Optional

from flask import Response, abort, current_app, request
import click

import joblite
from joblite import Retryable


_SENTINEL = object()
_EXC_TAG = object()


class _StreamBridge:
    """Drives an async generator in a dedicated thread's event loop and
    exposes a sync iterator. The only disconnect signal WSGI gives us
    is `GeneratorExit` raised when the server calls `iterator.close()`,
    so the outer sync generator's `finally` block must tear this down.
    Without that teardown a dropped SSE client leaks: the bridge thread
    keeps driving the async gen, `out.put(v)` eventually blocks on a
    full queue, and the listener + its WAL subscription stay alive
    forever.
    """

    __slots__ = (
        "_make_agen",
        "_loop",
        "_drive_task",
        "_out",
        "_thread",
        "_started",
        "__weakref__",
    )

    def __init__(self, make_agen: Callable[[], AsyncIterator[bytes]]):
        self._make_agen = make_agen
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._drive_task: Optional[asyncio.Task] = None
        self._out: _queue.Queue = _queue.Queue(maxsize=16)
        self._thread: Optional[threading.Thread] = None
        self._started = threading.Event()

    def start(self) -> None:
        self._thread = threading.Thread(
            target=self._run, name="joblite-flask-sse-bridge", daemon=True
        )
        self._thread.start()
        # Wait until the loop + drive task are created so close() has
        # a valid task to cancel.
        self._started.wait(timeout=5.0)

    def _run(self) -> None:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        self._loop = loop

        async def drive():
            agen = self._make_agen()
            try:
                async for v in agen:
                    # Fixed-timeout put so we don't wedge on a gone
                    # consumer. 5 s is ample for a keepalive cadence.
                    try:
                        self._out.put(v, timeout=5.0)
                    except _queue.Full:
                        break
            except asyncio.CancelledError:
                pass
            except Exception as e:  # pragma: no cover
                try:
                    self._out.put_nowait((_EXC_TAG, e))
                except _queue.Full:
                    pass
            finally:
                try:
                    await agen.aclose()
                except Exception:
                    pass
                try:
                    self._out.put_nowait(_SENTINEL)
                except _queue.Full:
                    pass

        task = loop.create_task(drive())
        self._drive_task = task
        self._started.set()
        try:
            loop.run_until_complete(task)
        except asyncio.CancelledError:
            pass
        finally:
            loop.close()

    def close(self) -> None:
        """Signal the bridge to shut down. Idempotent."""
        loop = self._loop
        task = self._drive_task
        if loop is not None and task is not None and not loop.is_closed():
            # Cancel the drive task from our thread; this raises
            # CancelledError inside the `async for`, which unwinds to
            # the finally block → agen.aclose() → put SENTINEL.
            try:
                loop.call_soon_threadsafe(task.cancel)
            except RuntimeError:
                pass

        # Drain anything in-flight so the bridge thread's
        # `self._out.put(..., timeout=5.0)` doesn't block.
        try:
            while True:
                self._out.get_nowait()
        except _queue.Empty:
            pass

        t = self._thread
        if t is not None and t.is_alive():
            t.join(timeout=2.0)

    def __iter__(self):
        while True:
            v = self._out.get()
            if v is _SENTINEL:
                return
            if isinstance(v, tuple) and len(v) == 2 and v[0] is _EXC_TAG:
                raise v[1]
            yield v


def _async_to_sync_gen(
    make_agen: Callable[[], AsyncIterator[bytes]],
    active_streams: Optional["weakref.WeakSet"] = None,
):
    """Drive an async generator in a dedicated thread's event loop and
    yield synchronously. WSGI's only disconnect signal is
    `GeneratorExit` raised when the server calls `close()` on our
    iterator — so the try/finally here is load-bearing, not a nicety.
    """
    bridge = _StreamBridge(make_agen)
    if active_streams is not None:
        active_streams.add(bridge)
    bridge.start()
    try:
        for chunk in bridge:
            yield chunk
    finally:
        bridge.close()


async def _run_authorize(
    authorize: Optional[Callable[[Any, str], Any]],
    user: Any,
    target: str,
) -> bool:
    if authorize is None:
        return True
    result = authorize(user, target)
    if asyncio.iscoroutine(result):
        result = await result
    return bool(result)


class JobliteFlask:
    """Flask integration for joblite.

    `authorize(user, target)` may be sync or async. If it raises, Flask
    returns 500 via its normal error-handling machinery. User passing
    is left to the app — pass something derived from the request in
    `user_factory(request) -> user`.
    """

    def __init__(
        self,
        app,
        db: joblite.Database,
        authorize: Optional[Callable[[Any, str], bool]] = None,
        user_factory: Optional[Callable] = None,
        subscribe_rule: str = "/joblite/subscribe/<string:channel>",
        stream_rule: str = "/joblite/stream/<string:name>",
    ):
        self.app = app
        self.db = db
        self.authorize = authorize
        self.user_factory = user_factory
        self.tasks: Dict[str, Dict[str, Any]] = {}
        self._instance_id = uuid.uuid4().hex[:8]
        # Weak-ref set of live SSE bridges. Tests assert that a dropped
        # client tears its bridge down within a bounded window.
        self._active_streams: "weakref.WeakSet[_StreamBridge]" = (
            weakref.WeakSet()
        )

        app.add_url_rule(
            subscribe_rule,
            endpoint="joblite_subscribe",
            view_func=self._subscribe_view,
        )
        app.add_url_rule(
            stream_rule,
            endpoint="joblite_stream",
            view_func=self._stream_view,
        )

        # Attach the CLI worker command to the Flask app.
        @app.cli.command("joblite_worker")
        @click.option("--queues", multiple=True, help="Only run these queues.")
        def joblite_worker(queues):
            asyncio.run(self._worker_loop(list(queues) or None))

    def task(
        self,
        queue_name: str,
        concurrency: int = 1,
        visibility_timeout_s: int = 300,
        max_attempts: int = 3,
    ):
        """Register a handler for a queue. The CLI worker picks it up."""

        def decorator(func: Callable) -> Callable:
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

    async def _worker_loop(self, selected: Optional[list]):
        if not self.tasks:
            click.echo("no tasks registered; nothing to do", err=True)
            return
        names = selected or list(self.tasks.keys())
        workers = []
        for name in names:
            info = self.tasks.get(name)
            if info is None:
                click.echo(f"no task registered for queue {name!r}", err=True)
                continue
            for i in range(info["concurrency"]):
                worker_id = joblite.build_worker_id(
                    "flask", self._instance_id, name, i
                )
                workers.append(
                    asyncio.create_task(
                        _run_worker(info["queue"], info["func"], worker_id)
                    )
                )
        try:
            await asyncio.gather(*workers)
        except asyncio.CancelledError:
            for w in workers:
                w.cancel()
            await asyncio.gather(*workers, return_exceptions=True)

    def _resolve_user(self):
        if self.user_factory is None:
            return None
        return self.user_factory(request)

    def _authorize_sync(self, user: Any, target: str) -> bool:
        """Sync wrapper over the authorize callable. Handles both sync
        and async callables — uses a temporary event loop for async ones
        so we can return a plain bool from a WSGI view."""
        if self.authorize is None:
            return True
        result = self.authorize(user, target)
        if asyncio.iscoroutine(result):
            loop = asyncio.new_event_loop()
            try:
                result = loop.run_until_complete(result)
            finally:
                loop.close()
        return bool(result)

    def _subscribe_view(self, channel: str):
        user = self._resolve_user()
        if not self._authorize_sync(user, channel):
            abort(403)

        def make_agen():
            listener = self.db.listen(channel)

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
                        data = {
                            "channel": notif.channel,
                            "payload": notif.payload,
                        }
                        yield f"data: {json.dumps(data)}\n\n".encode()
                except asyncio.CancelledError:
                    return

            return gen()

        return Response(
            _async_to_sync_gen(make_agen, self._active_streams),
            mimetype="text/event-stream",
        )

    def _stream_view(self, name: str):
        user = self._resolve_user()
        if not self._authorize_sync(user, name):
            abort(403)

        try:
            from_offset = int(request.headers.get("Last-Event-ID") or "0")
            if from_offset < 0:
                from_offset = 0
        except ValueError:
            from_offset = 0

        def make_agen():
            iterator = (
                self.db.stream(name).subscribe(from_offset=from_offset).__aiter__()
            )

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

            return gen()

        return Response(
            _async_to_sync_gen(make_agen, self._active_streams),
            mimetype="text/event-stream",
        )


async def _run_worker(queue: joblite.Queue, func: Callable, worker_id: str):
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


__all__ = ["JobliteFlask", "Retryable"]
