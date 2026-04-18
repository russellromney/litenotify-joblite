"""Tests for the joblite-fastapi plugin.

Covers: worker boot/shutdown, enqueue-in-request-tx atomicity, SSE subscribe
auth, and the Retryable flow.
"""

import asyncio
import json
import os
import tempfile

import pytest
from fastapi import FastAPI, Depends
from fastapi.testclient import TestClient

import joblite
from joblite_fastapi import JobliteApp, Retryable


@pytest.fixture
def app_db():
    d = tempfile.mkdtemp()
    path = os.path.join(d, "app.db")
    yield path


@pytest.fixture(scope="session")
def uvicorn_server(tmp_path_factory):
    """One uvicorn subprocess shared across all e2e SSE tests.

    Amortizes the ~2-5 s cold import cost (FastAPI + joblite + the Rust
    extension) over the whole e2e suite. Each test uses a unique channel or
    stream name to isolate itself.
    """
    import socket
    import subprocess
    import sys
    import textwrap
    import time

    db_path = str(tmp_path_factory.mktemp("shared") / "shared.db")
    app_code = textwrap.dedent(
        f"""
        import joblite
        from fastapi import FastAPI
        from joblite_fastapi import JobliteApp
        app = FastAPI()
        db = joblite.open({db_path!r})
        JobliteApp(app, db)

        @app.post('/honk')
        async def honk(body: dict):
            with db.transaction() as tx:
                tx.notify(body['channel'], body.get('payload', ''))
            return {{'ok': True}}

        @app.post('/publish/{{stream}}')
        async def publish(stream: str, body: dict):
            db.stream(stream).publish(body)
            return {{'ok': True}}

        @app.post('/seed/{{stream}}')
        async def seed(stream: str, body: dict):
            s = db.stream(stream)
            for i in range(body['count']):
                s.publish({{'i': i}})
            return {{'ok': True}}
        """
    )
    script = tmp_path_factory.mktemp("app") / "app.py"
    script.write_text(app_code)

    sk = socket.socket()
    sk.bind(("127.0.0.1", 0))
    port = sk.getsockname()[1]
    sk.close()

    proc = subprocess.Popen(
        [
            sys.executable, "-m", "uvicorn",
            "--app-dir", str(script.parent),
            f"{script.stem}:app",
            "--host", "127.0.0.1", "--port", str(port),
            "--log-level", "warning",
        ],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )
    deadline = time.time() + 20
    while time.time() < deadline:
        if proc.poll() is not None:
            raise RuntimeError(f"uvicorn exited early (rc={proc.returncode})")
        try:
            s = socket.create_connection(("127.0.0.1", port), timeout=0.1)
            s.close()
            break
        except OSError:
            time.sleep(0.05)
    else:
        proc.terminate()
        raise RuntimeError("uvicorn did not start within 20s")

    yield port

    proc.terminate()
    try:
        proc.wait(timeout=3)
    except subprocess.TimeoutExpired:
        proc.kill()


def _wait_for(predicate, timeout=3.0):
    import time
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(0.02)
    return False


def test_worker_boot_and_shutdown_processes_jobs(app_db):
    app = FastAPI()
    db = joblite.open(app_db)
    jl = JobliteApp(app, db)
    processed = []

    @jl.task("emails")
    async def send_email(payload):
        processed.append(payload)

    @app.post("/send")
    async def send(req: dict):
        db.queue("emails").enqueue(req)
        return {"ok": True}

    with TestClient(app) as client:
        assert client.post("/send", json={"to": "a@b.com"}).status_code == 200
        assert _wait_for(lambda: len(processed) == 1)
    assert processed == [{"to": "a@b.com"}]


def test_enqueue_in_request_tx_atomic_with_business_write(app_db):
    app = FastAPI()
    db = joblite.open(app_db)
    jl = JobliteApp(app, db)
    processed = []

    @jl.task("emails")
    async def send_email(payload):
        processed.append(payload)

    with db.transaction() as tx:
        tx.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, total REAL)")

    @app.post("/orders")
    async def create_order(order: dict):
        if order.get("fail"):
            try:
                with db.transaction() as tx:
                    tx.execute(
                        "INSERT INTO orders (total) VALUES (?)", [order["total"]]
                    )
                    db.queue("emails").enqueue(
                        {"to": order["email"]}, tx=tx
                    )
                    raise RuntimeError("business error")
            except RuntimeError:
                return {"ok": False}
        with db.transaction() as tx:
            tx.execute("INSERT INTO orders (total) VALUES (?)", [order["total"]])
            db.queue("emails").enqueue({"to": order["email"]}, tx=tx)
        return {"ok": True}

    with TestClient(app) as client:
        r = client.post(
            "/orders", json={"total": 10.0, "email": "a@b.com"}
        )
        assert r.json() == {"ok": True}
        r = client.post(
            "/orders",
            json={"total": 20.0, "email": "b@b.com", "fail": True},
        )
        assert r.json() == {"ok": False}
        assert _wait_for(lambda: len(processed) == 1, timeout=3.0)

    # Only the first order committed; only its email was enqueued + sent.
    orders = db.query("SELECT total FROM orders")
    assert [r["total"] for r in orders] == [10.0]
    assert processed == [{"to": "a@b.com"}]


def test_retryable_delays_and_eventually_dies(app_db):
    app = FastAPI()
    db = joblite.open(app_db)
    jl = JobliteApp(app, db)
    attempts = []

    @jl.task("flaky", max_attempts=2)
    async def handler(payload):
        attempts.append(payload)
        raise Retryable("nope", delay_s=0)

    with TestClient(app) as client:
        db.queue("flaky", max_attempts=2).enqueue({"n": 1})
        _wait_for(
            lambda: db.query("SELECT state FROM _joblite_jobs WHERE payload=?", ['{"n": 1}'])[0]["state"] == "dead",
            timeout=5.0,
        )

    rows = db.query("SELECT state, attempts, last_error FROM _joblite_jobs")
    assert rows[0]["state"] == "dead"
    assert rows[0]["attempts"] == 2
    assert "nope" in (rows[0]["last_error"] or "")


def test_sse_subscribe_rejects_unauthorized(app_db):
    app = FastAPI()
    db = joblite.open(app_db)

    def authorize(user, channel):
        return channel == "allowed"

    JobliteApp(app, db, authorize=authorize)

    with TestClient(app) as client:
        r = client.get("/joblite/subscribe/forbidden")
        assert r.status_code == 403


async def test_sse_delivers_honk_as_event(uvicorn_server):
    """SSE honk delivery via a real HTTP server (TestClient and
    httpx.ASGITransport both buffer the full body before exposing chunks).
    """
    import httpx
    port = uvicorn_server
    channel = "orders-honk-delivery"

    received = []
    async with httpx.AsyncClient(
        base_url=f"http://127.0.0.1:{port}", timeout=5.0
    ) as ac:
        async with ac.stream("GET", f"/joblite/subscribe/{channel}") as r:
            assert r.status_code == 200

            async def fire():
                await asyncio.sleep(0.2)
                async with httpx.AsyncClient(
                    base_url=f"http://127.0.0.1:{port}"
                ) as ac2:
                    await ac2.post(
                        "/honk",
                        json={"channel": channel, "payload": '{"id": 42}'},
                    )

            fire_task = asyncio.create_task(fire())
            try:
                async for line in r.aiter_lines():
                    if line.startswith("data:"):
                        received.append(json.loads(line[5:].strip()))
                        break
            finally:
                await fire_task

    assert received[0]["channel"] == channel
    assert json.loads(received[0]["payload"]) == {"id": 42}


async def test_stream_endpoint_replays_from_last_event_id(uvicorn_server):
    """GET /joblite/stream/{name} replays events with offset > Last-Event-ID
    then transitions to live. Each yielded event carries `id: {offset}` so
    browsers echo it back on reconnect.
    """
    import httpx
    port = uvicorn_server
    stream = "replay-events"

    async with httpx.AsyncClient(base_url=f"http://127.0.0.1:{port}") as ac:
        await ac.post(f"/seed/{stream}", json={"count": 3})

    collected = []
    last_id = None
    async with httpx.AsyncClient(
        base_url=f"http://127.0.0.1:{port}", timeout=5.0
    ) as ac:
        async with ac.stream("GET", f"/joblite/stream/{stream}") as r:
            assert r.status_code == 200
            cur_id = None
            async for line in r.aiter_lines():
                if line.startswith("id:"):
                    cur_id = int(line[3:].strip())
                elif line.startswith("data:"):
                    collected.append(json.loads(line[5:].strip()))
                    last_id = cur_id
                    if len(collected) == 3:
                        break
    assert collected == [{"i": 0}, {"i": 1}, {"i": 2}]
    assert last_id is not None and last_id >= 3

    async with httpx.AsyncClient(base_url=f"http://127.0.0.1:{port}") as ac:
        await ac.post(f"/publish/{stream}", json={"i": 777})

    resumed = []
    async with httpx.AsyncClient(
        base_url=f"http://127.0.0.1:{port}", timeout=5.0
    ) as ac:
        async with ac.stream(
            "GET",
            f"/joblite/stream/{stream}",
            headers={"Last-Event-ID": str(last_id)},
        ) as r:
            assert r.status_code == 200
            async for line in r.aiter_lines():
                if line.startswith("data:"):
                    resumed.append(json.loads(line[5:].strip()))
                    break
    assert resumed == [{"i": 777}]


def test_stream_endpoint_respects_authorize(app_db):
    """Authorize callable gates the stream endpoint the same as the listen one."""
    from fastapi.testclient import TestClient
    from fastapi import FastAPI

    app = FastAPI()
    db = joblite.open(app_db)
    JobliteApp(app, db, authorize=lambda u, n: n == "ok")

    with TestClient(app) as client:
        assert client.get("/joblite/stream/nope").status_code == 403


async def test_sse_reconnect_midstream_resumes_from_last_seen(uvicorn_server):
    """Simulate a real browser: connect, consume K events, drop the
    connection, reconnect with the actual Last-Event-ID the server sent,
    continue reading. Asserts no duplicates and no gaps across the seam."""
    import httpx
    port = uvicorn_server
    stream = "reconnect-stream"

    # Seed 10 events in one batch.
    async with httpx.AsyncClient(
        base_url=f"http://127.0.0.1:{port}"
    ) as ac:
        await ac.post(f"/seed/{stream}", json={"count": 10})

    # Connection 1: read 4 events, capture last id, disconnect.
    first: list = []
    first_last_id: int | None = None
    async with httpx.AsyncClient(
        base_url=f"http://127.0.0.1:{port}", timeout=5.0
    ) as ac:
        async with ac.stream("GET", f"/joblite/stream/{stream}") as r:
            assert r.status_code == 200
            cur_id = None
            async for line in r.aiter_lines():
                if line.startswith("id:"):
                    cur_id = int(line[3:].strip())
                elif line.startswith("data:"):
                    first.append(json.loads(line[5:].strip()))
                    first_last_id = cur_id
                    if len(first) == 4:
                        break
    # Disconnection happens when the async with ac.stream block exits.
    assert first == [{"i": 0}, {"i": 1}, {"i": 2}, {"i": 3}]
    assert first_last_id is not None

    # Publish two more events while nobody is listening.
    async with httpx.AsyncClient(
        base_url=f"http://127.0.0.1:{port}"
    ) as ac:
        await ac.post(f"/publish/{stream}", json={"i": 10})
        await ac.post(f"/publish/{stream}", json={"i": 11})

    # Connection 2: reconnect using the real Last-Event-ID we saw. Read the
    # remaining seeded events + the two we just published.
    resumed: list = []
    resumed_ids: list = []
    async with httpx.AsyncClient(
        base_url=f"http://127.0.0.1:{port}", timeout=5.0
    ) as ac:
        async with ac.stream(
            "GET",
            f"/joblite/stream/{stream}",
            headers={"Last-Event-ID": str(first_last_id)},
        ) as r:
            assert r.status_code == 200
            cur_id = None
            async for line in r.aiter_lines():
                if line.startswith("id:"):
                    cur_id = int(line[3:].strip())
                elif line.startswith("data:"):
                    resumed.append(json.loads(line[5:].strip()))
                    resumed_ids.append(cur_id)
                    if len(resumed) == 8:
                        break

    # No duplicates across the seam: first's ids should be all < resumed ids.
    resumed_payloads = [r["i"] for r in resumed]
    # We don't know the exact offset→payload mapping (other tests may have
    # consumed offsets from this shared stream), but we know the payloads
    # should be the 6 unseen seeded events (i=4..9) plus the 2 live ones
    # (i=10, 11), in that order, with no duplicates and no gaps.
    assert resumed_payloads == [4, 5, 6, 7, 8, 9, 10, 11]
    # Ids strictly increasing across the seam.
    assert all(rid > first_last_id for rid in resumed_ids)


async def test_sse_reconnect_without_last_event_id_replays_from_start(
    uvicorn_server,
):
    """Without a Last-Event-ID header, a reconnecting client gets the full
    history. This is the "initial connect" path for a client that hasn't
    previously seen any events."""
    import httpx
    port = uvicorn_server
    stream = "from-start-stream"

    async with httpx.AsyncClient(
        base_url=f"http://127.0.0.1:{port}"
    ) as ac:
        await ac.post(f"/seed/{stream}", json={"count": 5})

    got: list = []
    async with httpx.AsyncClient(
        base_url=f"http://127.0.0.1:{port}", timeout=5.0
    ) as ac:
        async with ac.stream("GET", f"/joblite/stream/{stream}") as r:
            assert r.status_code == 200
            async for line in r.aiter_lines():
                if line.startswith("data:"):
                    got.append(json.loads(line[5:].strip()))
                    if len(got) == 5:
                        break
    assert got == [{"i": i} for i in range(5)]


async def test_last_event_id_beyond_end_does_not_error(uvicorn_server):
    """Documented invariant: Last-Event-ID is authoritative. A too-high ID
    still yields 200 — the stream just waits for events with an even higher
    offset, which won't arrive. Verify the server handles this gracefully
    (no 4xx/5xx, no crash)."""
    import httpx
    port = uvicorn_server
    stream = "beyond-end"

    async with httpx.AsyncClient(
        base_url=f"http://127.0.0.1:{port}", timeout=2.0
    ) as ac:
        async with ac.stream(
            "GET",
            f"/joblite/stream/{stream}",
            headers={"Last-Event-ID": "999999"},
        ) as r:
            assert r.status_code == 200


def test_malformed_last_event_id_treated_as_zero(app_db):
    """A bad Last-Event-ID header must fall back to from_offset=0, not 500."""
    from fastapi import Request
    from joblite_fastapi.joblite_fastapi import _parse_last_event_id

    class FakeReq:
        def __init__(self, v):
            self.headers = {"last-event-id": v} if v is not None else {}

    assert _parse_last_event_id(FakeReq(None)) == 0
    assert _parse_last_event_id(FakeReq("")) == 0
    assert _parse_last_event_id(FakeReq("abc")) == 0
    assert _parse_last_event_id(FakeReq("-5")) == 0
    assert _parse_last_event_id(FakeReq("42")) == 42


def test_user_dependency_passed_to_authorize(app_db):
    app = FastAPI()
    db = joblite.open(app_db)

    def current_user():
        return {"id": 1, "can": ["alpha"]}

    def authorize(user, channel):
        return channel in user["can"]

    JobliteApp(app, db, authorize=authorize, user_dependency=current_user)

    with TestClient(app) as client:
        assert client.get("/joblite/subscribe/beta").status_code == 403


# ---------------------------------------------------------------------------
# authorize callable policy:
#   * sync or async callables supported — async detected by coroutine return
#   * if authorize raises, the exception propagates and the SSE stream is
#     never opened; FastAPI returns HTTP 500
#
# The "allowed" path (200 streaming) would hang in TestClient because
# TestClient buffers the whole body before returning — so we test it via
# the unit-level `_run_authorize` helper. The denial paths (403) and the
# exception paths (500) return immediately and are tested through
# TestClient.
# ---------------------------------------------------------------------------


async def test_run_authorize_supports_sync_truthy_and_falsy():
    from joblite_fastapi.joblite_fastapi import _run_authorize

    assert await _run_authorize(lambda u, c: True, "u", "c") is True
    assert await _run_authorize(lambda u, c: False, "u", "c") is False
    # Non-bool truthy / falsy values are coerced.
    assert await _run_authorize(lambda u, c: 1, "u", "c") is True
    assert await _run_authorize(lambda u, c: 0, "u", "c") is False


async def test_run_authorize_supports_async_truthy_and_falsy():
    from joblite_fastapi.joblite_fastapi import _run_authorize

    async def allow(user, target):
        return True

    async def deny(user, target):
        return False

    assert await _run_authorize(allow, "u", "c") is True
    assert await _run_authorize(deny, "u", "c") is False


async def test_run_authorize_propagates_sync_and_async_exceptions():
    from joblite_fastapi.joblite_fastapi import _run_authorize

    def sync_bad(user, target):
        raise RuntimeError("sync boom")

    async def async_bad(user, target):
        raise RuntimeError("async boom")

    with pytest.raises(RuntimeError, match="sync boom"):
        await _run_authorize(sync_bad, "u", "c")
    with pytest.raises(RuntimeError, match="async boom"):
        await _run_authorize(async_bad, "u", "c")


async def test_run_authorize_none_returns_true():
    from joblite_fastapi.joblite_fastapi import _run_authorize

    # No authorize configured -> pass-through.
    assert await _run_authorize(None, "u", "c") is True


def test_async_authorize_denies_subscribe_returns_403(app_db):
    """Async authorize returning False must 403 — not be treated as truthy
    because the return value is a coroutine (which would be the bug this
    test locks out)."""
    app = FastAPI()
    db = joblite.open(app_db)

    async def authorize(user, channel):
        return False

    JobliteApp(app, db, authorize=authorize)
    with TestClient(app) as client:
        assert client.get("/joblite/subscribe/anything").status_code == 403


def test_async_authorize_denies_stream_returns_403(app_db):
    app = FastAPI()
    db = joblite.open(app_db)
    db.stream("s").publish({"i": 1})

    async def authorize(user, name):
        return False

    JobliteApp(app, db, authorize=authorize)
    with TestClient(app) as client:
        assert client.get("/joblite/stream/s").status_code == 403


def test_sync_authorize_raises_returns_500_subscribe(app_db):
    """Sync raise in authorize -> 500, SSE stream never opens, no hang."""
    app = FastAPI()
    db = joblite.open(app_db)

    def authorize(user, channel):
        raise RuntimeError("auth backend exploded")

    JobliteApp(app, db, authorize=authorize)
    with TestClient(app, raise_server_exceptions=False) as client:
        r = client.get("/joblite/subscribe/anything")
        assert r.status_code == 500


def test_async_authorize_raises_returns_500_subscribe(app_db):
    """Async raise in authorize -> 500."""
    app = FastAPI()
    db = joblite.open(app_db)

    async def authorize(user, channel):
        raise RuntimeError("async auth exploded")

    JobliteApp(app, db, authorize=authorize)
    with TestClient(app, raise_server_exceptions=False) as client:
        r = client.get("/joblite/subscribe/anything")
        assert r.status_code == 500


def test_authorize_raises_returns_500_stream(app_db):
    """Stream endpoint must also 500 on raised authorize, not hang."""
    app = FastAPI()
    db = joblite.open(app_db)
    db.stream("s").publish({"i": 1})

    async def authorize(user, name):
        raise ValueError("nope")

    JobliteApp(app, db, authorize=authorize)
    with TestClient(app, raise_server_exceptions=False) as client:
        r = client.get("/joblite/stream/s")
        assert r.status_code == 500
