"""Tests for joblite_flask.

Exercises worker registration + the SSE endpoints via Flask's test
client. Uses a fresh Flask app per test because Flask doesn't have
lifespan hooks like FastAPI, so we can't tear down / re-register cleanly.
"""

import asyncio
import os
import tempfile
import threading

import pytest


@pytest.fixture
def app_db():
    d = tempfile.mkdtemp()
    yield os.path.join(d, "app.db")


def test_task_decorator_registers(app_db):
    """`@jl.task(name)` puts the handler on the plugin's task registry."""
    from flask import Flask
    import joblite
    from joblite_flask import JobliteFlask

    app = Flask(__name__)
    db = joblite.open(app_db)
    jl = JobliteFlask(app, db)

    @jl.task("emails", concurrency=2, max_attempts=5)
    async def send_email(payload):
        pass

    info = jl.tasks["emails"]
    assert info["func"] is send_email
    assert info["concurrency"] == 2
    # Queue should be memoized on the Database.
    assert info["queue"] is db.queue("emails")


def test_sse_subscribe_denied_returns_403(app_db):
    from flask import Flask
    import joblite
    from joblite_flask import JobliteFlask

    app = Flask(__name__)
    db = joblite.open(app_db)
    JobliteFlask(app, db, authorize=lambda user, channel: channel == "ok")

    with app.test_client() as client:
        r = client.get("/joblite/subscribe/forbidden")
        assert r.status_code == 403
        r2 = client.get("/joblite/subscribe/ok")
        assert r2.status_code == 200
        r2.close()


def test_sse_stream_respects_last_event_id(app_db):
    """Last-Event-ID header causes the stream view to skip earlier events."""
    from flask import Flask
    import joblite
    from joblite_flask import JobliteFlask

    db = joblite.open(app_db)
    s = db.stream("news")
    s.publish({"v": 1})  # offset 1
    s.publish({"v": 2})  # offset 2
    s.publish({"v": 3})  # offset 3

    app = Flask(__name__)
    JobliteFlask(app, db)

    with app.test_client() as client:
        r = client.get(
            "/joblite/stream/news",
            headers={"Last-Event-ID": "1"},
        )
        assert r.status_code == 200
        # Read some bytes — the Flask test client buffers streaming
        # responses, so we just verify the first event we'd see is
        # offset 2 (skipping offset 1).
        #
        # Flask's test client doesn't give us chunked iteration easily;
        # it collects .data once the response closes. For a streaming
        # SSE endpoint that never closes, we'd hang. Consume a
        # bounded number of bytes with close_after.
        chunks = []
        iter_ = r.iter_encoded()
        for _ in range(3):
            try:
                chunks.append(next(iter_))
            except StopIteration:
                break
        body = b"".join(chunks).decode()
        r.close()

    # First event in the body should be offset 2, not offset 1.
    assert "id: 2" in body
    assert "id: 1" not in body or body.index("id: 2") < body.index("id: 1")


def test_authorize_async_and_raising(app_db):
    """authorize may be async; if it raises, Flask returns 500. Test
    is intentionally sync — Flask is sync-first, and the plugin drives
    async authorize callables via a temporary event loop per request.
    Running the test itself under pytest-asyncio would conflict with
    that loop creation.
    """
    from flask import Flask
    import joblite
    from joblite_flask import JobliteFlask

    db = joblite.open(app_db)

    # Async returning False
    app = Flask(__name__)

    async def deny(user, target):
        return False

    JobliteFlask(app, db, authorize=deny)
    with app.test_client() as client:
        assert client.get("/joblite/subscribe/any").status_code == 403

    # Raising
    app2 = Flask(__name__)

    def boom(user, target):
        raise RuntimeError("auth exploded")

    JobliteFlask(app2, db, authorize=boom)
    with app2.test_client() as client:
        r = client.get("/joblite/subscribe/any")
        assert r.status_code == 500


def test_sse_generator_close_tears_down_bridge(app_db):
    """WSGI's only disconnect signal is `GeneratorExit` raised when the
    server calls `.close()` on the response iterator. The plugin's
    `_async_to_sync_gen` must catch that, signal the bridge thread to
    stop, and join it — otherwise a dropped SSE client leaks a thread
    per disconnect.

    Test strategy: drive `_async_to_sync_gen` directly as a unit. Real
    HTTP plumbing (Werkzeug test client, asyncio loops, real sockets)
    would obscure whether teardown happened on the generator's
    `close()` — we want to pin that exact behavior.
    """
    import time
    from joblite_flask import _async_to_sync_gen
    import weakref

    active: weakref.WeakSet = weakref.WeakSet()

    import asyncio
    drive_started = threading.Event()

    def make_agen():
        async def gen():
            drive_started.set()
            try:
                # Yield forever (keepalive every 50 ms).
                while True:
                    await asyncio.sleep(0.05)
                    yield b": keepalive\n\n"
            except asyncio.CancelledError:
                return

        return gen()

    outer = _async_to_sync_gen(make_agen, active)
    # Drive the outer generator to the point the bridge is created.
    it = iter(outer)
    chunk = next(it)
    assert chunk.startswith(b":") or chunk.startswith(b"data:")
    assert drive_started.is_set()
    assert len(active) == 1

    # Simulate WSGI's disconnect: close the outer generator. The
    # finally block must tear down the bridge thread.
    outer.close()

    deadline = time.time() + 2.0
    while time.time() < deadline:
        if len(active) == 0:
            break
        time.sleep(0.02)
    assert len(active) == 0, "bridge not torn down after generator close()"


def test_sse_many_disconnects_do_not_leak(app_db):
    """100 connect+disconnect cycles must not grow the active-stream
    set past baseline. Every dropped client was leaving two zombie
    threads (bridge + inner listener) before the disconnect fix.
    """
    import asyncio
    import time
    import weakref

    from joblite_flask import _async_to_sync_gen

    active: weakref.WeakSet = weakref.WeakSet()

    def make_agen_factory():
        async def gen():
            try:
                while True:
                    await asyncio.sleep(0.01)
                    yield b": keepalive\n\n"
            except asyncio.CancelledError:
                return
        return gen

    for _ in range(100):
        outer = _async_to_sync_gen(make_agen_factory(), active)
        it = iter(outer)
        next(it)
        outer.close()

    deadline = time.time() + 3.0
    while time.time() < deadline:
        if len(active) == 0:
            break
        time.sleep(0.05)
    assert len(active) == 0, (
        f"leaked {len(active)} bridges after 100 disconnect cycles"
    )


def test_cli_worker_drains_jobs(app_db):
    """Invoke the `flask joblite_worker` CLI command in-process and
    verify it processes a seeded job before being cancelled."""
    from flask import Flask
    import joblite
    from joblite_flask import JobliteFlask

    db = joblite.open(app_db)
    app = Flask(__name__)
    jl = JobliteFlask(app, db)

    delivered: list = []

    @jl.task("mgmt-test")
    async def handler(payload):
        delivered.append(payload)

    db.queue("mgmt-test").enqueue({"n": 1})

    # Run the worker loop for a short window, then cancel.
    async def run():
        task = asyncio.create_task(jl._worker_loop(["mgmt-test"]))
        # Poll until the handler fires or 2 s elapse.
        for _ in range(100):
            if delivered:
                break
            await asyncio.sleep(0.02)
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    asyncio.run(run())
    assert delivered == [{"n": 1}]
