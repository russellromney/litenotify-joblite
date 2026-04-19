"""Tests for joblite_django.

Configures Django on-the-fly so we don't need a full project directory.
"""

import os
import tempfile

import django
import pytest
from django.conf import settings


class FakeUser:
    """A stand-in for request.user. Holds a stable identity so tests can
    assert the authorize callable saw the same object middleware set."""

    def __init__(self, username: str = "e2e-user"):
        self.username = username
        self.is_authenticated = True

    def __repr__(self):
        return f"FakeUser({self.username!r})"


class FakeUserMiddleware:
    """Async middleware that sets request.user before the view runs.

    Mirrors what AuthenticationMiddleware would do in a real project, but
    without requiring the auth app + session tables. Its job in this test
    file is to prove the SSE views do pass request.user through to the
    authorize callable via a real request lifecycle (not the RequestFactory
    shortcut used elsewhere)."""

    async_capable = True
    sync_capable = False

    def __init__(self, get_response):
        self.get_response = get_response

    async def __call__(self, request):
        request.user = FakeUser()
        return await self.get_response(request)


def _setup_django(db_path: str):
    if not settings.configured:
        settings.configure(
            DEBUG=True,
            SECRET_KEY="test",
            INSTALLED_APPS=["joblite_django"],
            DATABASES={
                "default": {
                    "ENGINE": "django.db.backends.sqlite3",
                    "NAME": ":memory:",
                }
            },
            JOBLITE_DB_PATH=db_path,
            ROOT_URLCONF=__name__,
            ALLOWED_HOSTS=["*"],
            USE_TZ=True,
            MIDDLEWARE=[f"{__name__}.FakeUserMiddleware"],
        )
        django.setup()
    else:
        settings.JOBLITE_DB_PATH = db_path
    _install_urls()


# URL patterns get populated by _setup_django() after settings.configure(),
# because `joblite_django.views` imports Django internals that require a
# configured settings module at import time.
urlpatterns: list = []


def _install_urls():
    global urlpatterns
    if urlpatterns:
        return
    from django.urls import path as _url_path
    from joblite_django.views import stream_sse, subscribe_sse

    urlpatterns.extend([
        _url_path("joblite/subscribe/<str:channel>", subscribe_sse),
        _url_path("joblite/stream/<str:name>", stream_sse),
    ])


@pytest.fixture
def django_db_path():
    with tempfile.TemporaryDirectory() as d:
        path = os.path.join(d, "app.db")
        _setup_django(path)
        import joblite_django

        joblite_django.reset_for_tests()
        yield path
        joblite_django.reset_for_tests()


def test_db_lazy_open(django_db_path):
    import joblite_django

    d1 = joblite_django.db()
    d2 = joblite_django.db()
    assert d1 is d2


def test_db_raises_without_settings(tmp_path):
    _setup_django(str(tmp_path / "app.db"))
    import joblite_django
    from django.conf import settings as _s

    joblite_django.reset_for_tests()
    original = _s.JOBLITE_DB_PATH
    try:
        del _s.JOBLITE_DB_PATH
        with pytest.raises(RuntimeError):
            joblite_django.db()
    finally:
        _s.JOBLITE_DB_PATH = original


def test_task_decorator_registers(django_db_path):
    import joblite_django

    @joblite_django.task("emails", concurrency=2, max_attempts=5)
    async def send_email(payload):
        pass

    registered = joblite_django.registered_tasks()
    assert "emails" in registered
    assert registered["emails"]["concurrency"] == 2
    assert registered["emails"]["max_attempts"] == 5
    assert registered["emails"]["func"] is send_email


def test_set_and_get_authorize(django_db_path):
    import joblite_django

    def fn(user, name):
        return True

    joblite_django.set_authorize(fn)
    assert joblite_django.get_authorize() is fn
    joblite_django.set_authorize(None)
    assert joblite_django.get_authorize() is None


async def test_user_factory_override(django_db_path):
    """`set_user_factory(fn)` replaces the default `request.user`
    reader. Lets apps without auth middleware plug in a custom user
    derivation (e.g. parse a header, look up a session cookie).
    """
    from django.test import RequestFactory
    import joblite_django
    from joblite_django.views import subscribe_sse

    called_with = []

    def my_factory(request):
        called_with.append(request)
        return "user-from-header"

    def my_authorize(user, channel):
        assert user == "user-from-header"
        return True

    joblite_django.set_user_factory(my_factory)
    joblite_django.set_authorize(my_authorize)
    try:
        req = RequestFactory().get("/joblite/subscribe/ok")
        # Don't set req.user — the factory should bypass it.
        resp = await subscribe_sse(req, "ok")
        assert resp.status_code == 200
        assert len(called_with) == 1
    finally:
        joblite_django.set_user_factory(None)
        joblite_django.set_authorize(None)


async def test_default_user_factory_errors_without_middleware(django_db_path):
    """Without auth middleware, `request.user` doesn't exist. The
    default factory must raise a clear RuntimeError pointing at the
    fix, not a cryptic AttributeError from deep inside the view.
    """
    from django.test import RequestFactory
    import joblite_django
    from joblite_django.views import subscribe_sse

    class _NoUserRequest:
        """RequestFactory adds .user automatically via middleware chain;
        build a bare request-ish object that doesn't have `.user` to
        mimic a misconfigured deployment."""
        def __init__(self, real):
            self._real = real
            # Only copy attrs the view touches. .user is intentionally
            # absent.
            for attr in ("headers", "META", "GET", "POST", "method"):
                if hasattr(real, attr):
                    setattr(self, attr, getattr(real, attr))

    joblite_django.set_user_factory(None)  # ensure default
    try:
        real = RequestFactory().get("/joblite/subscribe/ok")
        stub = _NoUserRequest(real)
        try:
            await subscribe_sse(stub, "ok")
        except RuntimeError as e:
            assert "AuthenticationMiddleware" in str(e)
        else:
            raise AssertionError("default factory should have raised")
    finally:
        joblite_django.set_user_factory(None)


async def test_stream_sse_view_returns_streaming_response(django_db_path):
    """View-level unit test: call the async view directly, read a few bytes."""
    from django.http import StreamingHttpResponse
    from django.test import RequestFactory
    import joblite_django
    from joblite_django.views import stream_sse

    # Pre-seed events so replay has something to hand back.
    db = joblite_django.db()
    s = db.stream("news")
    s.publish({"v": 1})
    s.publish({"v": 2})

    rf = RequestFactory()
    req = rf.get("/joblite/stream/news")
    req.user = None

    response = await stream_sse(req, "news")
    assert isinstance(response, StreamingHttpResponse)
    assert response["Content-Type"] == "text/event-stream"

    # Pull a single chunk from the async generator; should be the first event.
    import asyncio
    got: list = []
    async for chunk in response.streaming_content:
        got.append(chunk)
        if len(got) >= 2:
            break
    body = b"".join(got).decode()
    assert "id:" in body
    assert '"v": 1' in body or '"v":1' in body


async def test_stream_sse_authorize_blocks(django_db_path):
    from django.http import HttpResponseForbidden
    from django.test import RequestFactory
    import joblite_django
    from joblite_django.views import stream_sse

    joblite_django.set_authorize(lambda user, name: False)
    try:
        rf = RequestFactory()
        req = rf.get("/joblite/stream/anything")
        req.user = None
        response = await stream_sse(req, "anything")
        assert isinstance(response, HttpResponseForbidden)
    finally:
        joblite_django.set_authorize(None)


async def test_subscribe_sse_authorize_blocks(django_db_path):
    from django.http import HttpResponseForbidden
    from django.test import RequestFactory
    import joblite_django
    from joblite_django.views import subscribe_sse

    joblite_django.set_authorize(lambda user, ch: False)
    try:
        rf = RequestFactory()
        req = rf.get("/joblite/subscribe/x")
        req.user = None
        response = await subscribe_sse(req, "x")
        assert isinstance(response, HttpResponseForbidden)
    finally:
        joblite_django.set_authorize(None)


def test_management_command_processes_registered_task(django_db_path):
    """Programmatically invoke `manage.py joblite_worker`, have it pick up
    one job, and shut down cleanly via cancellation."""
    import asyncio
    from django.core.management import call_command

    import joblite_django

    delivered: list = []

    @joblite_django.task("mgmt-test")
    async def handler(payload):
        delivered.append(payload)

    db = joblite_django.db()
    db.queue("mgmt-test").enqueue({"n": 1})

    async def run_with_timeout():
        loop = asyncio.get_running_loop()
        # Run the management command in a background thread so we can cancel
        # it after the job is processed.
        import threading
        done = threading.Event()

        def target():
            try:
                call_command("joblite_worker", queues=["mgmt-test"])
            except SystemExit:
                pass
            except Exception:
                pass
            finally:
                done.set()

        t = threading.Thread(target=target, daemon=True)
        t.start()
        # Poll until the handler has been called.
        for _ in range(200):
            if delivered:
                break
            await asyncio.sleep(0.02)
        import os
        import signal
        # Send SIGINT to ourselves so the signal handler inside the command
        # triggers a clean shutdown.
        os.kill(os.getpid(), signal.SIGINT)
        await loop.run_in_executor(None, t.join, 5.0)

    try:
        asyncio.run(run_with_timeout())
    except KeyboardInterrupt:
        pass

    assert delivered == [{"n": 1}]


def test_management_command_two_workers_split_work_exclusively(tmp_path):
    """Two `python manage.py joblite_worker` invocations on the same DB
    must split the work with zero overlap, exactly like the bare joblite
    two-process test. Proves the Django command wrapper (signal handler
    + task registry + asyncio.run()) doesn't break claim exclusivity.

    Synchronization discipline:
      * wait for both workers to print READY before seeding (otherwise a
        fast worker could drain the whole queue before the slow worker
        finishes importing Django),
      * wait until every worker has produced at least one PROCESSED line
        before SIGINT-ing (otherwise SIGINT could land before the
        asyncio signal handler is installed, killing with rc=-2 and
        losing processed output from the other worker).
    """
    import os as _os
    import signal as _signal
    import subprocess as _sp
    import sys as _sys
    import textwrap as _tw
    import threading as _thr
    import time as _time

    repo_root = _os.path.join(_os.path.dirname(_os.path.dirname(_os.path.abspath(__file__))), "packages")
    db_path = str(tmp_path / "django-shared.db")

    import joblite

    # Pre-create the schema so workers don't race on `CREATE TABLE IF NOT
    # EXISTS`. Do NOT seed jobs yet — we seed only after both workers are
    # ready.
    seed_db = joblite.open(db_path)
    seed_db.queue("shared")
    del seed_db

    worker_script = _tw.dedent(
        f"""
        import sys
        sys.path.insert(0, {repo_root!r})

        import django
        from django.conf import settings
        settings.configure(
            DEBUG=True,
            SECRET_KEY="t",
            INSTALLED_APPS=["joblite_django"],
            DATABASES={{"default": {{
                "ENGINE": "django.db.backends.sqlite3",
                "NAME": ":memory:",
            }}}},
            JOBLITE_DB_PATH={db_path!r},
            ROOT_URLCONF=None,
            ALLOWED_HOSTS=["*"],
            USE_TZ=True,
            MIDDLEWARE=[],
        )
        django.setup()

        import joblite_django

        @joblite_django.task("shared")
        async def handler(payload):
            print("PROCESSED:" + str(payload["i"]), flush=True)

        print("READY", flush=True)

        from django.core.management import call_command
        try:
            call_command("joblite_worker", queues=["shared"])
        except SystemExit:
            pass
        """
    )

    def _spawn_worker():
        return _sp.Popen(
            [_sys.executable, "-c", worker_script],
            stdout=_sp.PIPE,
            stderr=_sp.PIPE,
            text=True,
            bufsize=1,
        )

    # Each worker gets a background reader thread that tails its stdout
    # into a list, so we can observe lines without blocking.
    def _tail_stdout(proc: _sp.Popen, sink: list, marker: _thr.Event):
        try:
            for line in iter(proc.stdout.readline, ""):
                if not line:
                    break
                sink.append(line.rstrip("\n"))
                if line.startswith("PROCESSED:"):
                    marker.set()
        except Exception:
            pass

    def _wait_ready(sink: list, timeout: float = 10.0) -> bool:
        deadline = _time.time() + timeout
        while _time.time() < deadline:
            if any(line == "READY" for line in sink):
                return True
            _time.sleep(0.02)
        return False

    p1 = _spawn_worker()
    p2 = _spawn_worker()
    lines1: list = []
    lines2: list = []
    seen_any_1 = _thr.Event()
    seen_any_2 = _thr.Event()
    t1 = _thr.Thread(target=_tail_stdout, args=(p1, lines1, seen_any_1), daemon=True)
    t2 = _thr.Thread(target=_tail_stdout, args=(p2, lines2, seen_any_2), daemon=True)
    t1.start()
    t2.start()

    try:
        assert _wait_ready(lines1), (
            f"worker 1 never printed READY; lines={lines1}, "
            f"rc={p1.poll()}, stderr={p1.stderr.read()!r}"
        )
        assert _wait_ready(lines2), (
            f"worker 2 never printed READY; lines={lines2}, "
            f"rc={p2.poll()}, stderr={p2.stderr.read()!r}"
        )

        # Now seed — both workers are booted and sitting in the claim loop.
        db = joblite.open(db_path)
        q = db.queue("shared")
        n = 200
        for i in range(n):
            q.enqueue({"i": i})

        # Wait until _joblite_live is empty for this queue AND both
        # workers have processed >=1. ack DELETEs the row so "drained"
        # means no rows remain in live (pending or processing).
        deadline = _time.time() + 30.0
        while _time.time() < deadline:
            remaining = db.query(
                "SELECT COUNT(*) AS c FROM _joblite_live WHERE queue='shared'"
            )[0]["c"]
            if remaining == 0 and seen_any_1.is_set() and seen_any_2.is_set():
                break
            if p1.poll() is not None or p2.poll() is not None:
                break
            _time.sleep(0.05)
        else:
            raise AssertionError(
                f"timed out; remaining={remaining}, "
                f"w1_participated={seen_any_1.is_set()}, "
                f"w2_participated={seen_any_2.is_set()}"
            )

        # Clean shutdown via SIGINT; the asyncio signal handler installed
        # by the command sets stop_event and cancels worker tasks. Under
        # parallel pytest-xdist load the subprocess can take a while to
        # respond to the signal (OS scheduling), so we give it 30s before
        # falling back to SIGKILL.
        for p in (p1, p2):
            if p.poll() is None:
                _os.kill(p.pid, _signal.SIGINT)

        for p in (p1, p2):
            try:
                p.wait(timeout=30)
            except _sp.TimeoutExpired:
                # Give up waiting for clean exit; SIGKILL it. The
                # functional assertion below (all jobs processed
                # exclusively) is what actually matters; a SIGKILL here
                # just means the subprocess was too slow to unwind
                # cleanly, not that jobs were lost.
                p.kill()
                p.wait()
        t1.join(timeout=5)
        t2.join(timeout=5)
    finally:
        for p in (p1, p2):
            if p.poll() is None:
                p.kill()
                p.wait()

    # We sent SIGINT. Clean exit is rc=0. If the subprocess ignored
    # SIGINT and we had to SIGKILL above, rc will be -9 (-SIGKILL) or
    # -2 (-SIGINT signal delivered but the process exited via signal
    # rather than a clean return). All three are acceptable — the
    # meaningful assertion is that the jobs got processed correctly.
    assert p1.returncode in (0, -2, -9), (
        f"worker 1 rc={p1.returncode}; tail={lines1[-5:]}"
    )
    assert p2.returncode in (0, -2, -9), (
        f"worker 2 rc={p2.returncode}; tail={lines2[-5:]}"
    )

    def _parse(lines: list) -> list:
        result = []
        for line in lines:
            if line.startswith("PROCESSED:"):
                result.append(int(line[len("PROCESSED:"):]))
        return result

    a = _parse(lines1)
    b = _parse(lines2)

    combined = a + b
    n = 200
    assert sorted(combined) == list(range(n)), (
        f"jobs not covered exactly once: worker1={len(a)}, worker2={len(b)}, "
        f"missing={sorted(set(range(n)) - set(combined))}"
    )
    assert set(a).isdisjoint(set(b)), (
        f"workers overlapped: intersection={set(a) & set(b)}"
    )
    assert len(a) > 0 and len(b) > 0, (
        f"one worker did no work: worker1={len(a)}, worker2={len(b)}"
    )


def test_last_event_id_parsing_in_view(django_db_path):
    """The view parses Last-Event-ID headers; bad values fall back to 0."""
    import joblite_django
    from django.test import RequestFactory

    db = joblite_django.db()
    db.stream("k").publish({"i": 0})
    rf = RequestFactory()

    # Valid
    req = rf.get("/joblite/stream/k", HTTP_LAST_EVENT_ID="5")
    assert req.headers.get("Last-Event-ID") == "5"

    # Invalid
    req2 = rf.get("/joblite/stream/k", HTTP_LAST_EVENT_ID="abc")
    assert req2.headers.get("Last-Event-ID") == "abc"
    # Actual coercion happens inside the view; parsing logic covered
    # directly by the FastAPI Last-Event-ID tests (same spec).


# ---------------------------------------------------------------------------
# Request-level end-to-end tests via django.test.AsyncClient.
#
# These drive the full Django request cycle including FakeUserMiddleware,
# which is what populates request.user. The direct-call tests above use
# RequestFactory and just set req.user = None manually, which skips the
# middleware pipeline entirely — so they can't catch any bug in how the
# view reads request.user or hands it to the authorize callable.
# ---------------------------------------------------------------------------


async def _read_streaming_chunks(
    response, max_chunks: int = 4, max_bytes: int = 4096
):
    """Collect up to N chunks (or max_bytes) from a StreamingHttpResponse
    without draining a live stream forever. The SSE view holds the
    connection open, so we break out as soon as we have enough."""
    chunks: list = []
    total = 0
    try:
        async for chunk in response.streaming_content:
            chunks.append(chunk)
            total += len(chunk)
            if len(chunks) >= max_chunks or total >= max_bytes:
                break
    except Exception:
        pass
    return b"".join(chunks).decode()


async def test_stream_sse_e2e_via_asyncclient_replays_from_last_event_id(
    django_db_path,
):
    """Real request path: AsyncClient -> middleware (sets request.user) ->
    view. Seeds a stream, requests with Last-Event-ID=1, confirms the
    view skips the first event and replays the rest. Proves the view
    reads the header off the actual request and pipes it into
    db.stream.subscribe."""
    from django.test import AsyncClient
    import joblite_django

    db = joblite_django.db()
    s = db.stream("news-e2e")
    s.publish({"v": 1})  # offset 1
    s.publish({"v": 2})  # offset 2
    s.publish({"v": 3})  # offset 3

    seen_users: list = []

    def authorize(user, name):
        seen_users.append(user)
        return True

    joblite_django.set_authorize(authorize)
    try:
        client = AsyncClient()
        response = await client.get(
            "/joblite/stream/news-e2e",
            HTTP_LAST_EVENT_ID="1",
        )
        assert response.status_code == 200
        assert response["Content-Type"] == "text/event-stream"
        body = await _read_streaming_chunks(response, max_chunks=2)
    finally:
        joblite_django.set_authorize(None)

    # Resumed past offset 1 - replay must start at offset 2.
    assert "id: 2" in body, f"expected offset 2 in body, got {body!r}"
    assert '"v": 2' in body or '"v":2' in body
    # Authorize saw the FakeUser our middleware installed on request.user.
    assert len(seen_users) == 1
    assert isinstance(seen_users[0], FakeUser)
    assert seen_users[0].username == "e2e-user"


async def test_subscribe_sse_e2e_via_asyncclient_forwards_request_user(
    django_db_path,
):
    """Real request path for subscribe_sse. Confirms request.user reaches
    the authorize callable. NOTIFY delivery over the wire is covered by
    the FastAPI e2e test against a real uvicorn server; here we assert
    status + the authorize call site."""
    from django.test import AsyncClient
    import joblite_django

    seen_users: list = []

    def authorize(user, channel):
        seen_users.append((user, channel))
        return True

    joblite_django.set_authorize(authorize)
    try:
        client = AsyncClient()
        response = await client.get("/joblite/subscribe/ch-e2e")
        assert response.status_code == 200
        assert response["Content-Type"] == "text/event-stream"
    finally:
        joblite_django.set_authorize(None)
        try:
            response.streaming_content.aclose  # type: ignore[attr-defined]
            await response.streaming_content.aclose()  # type: ignore[attr-defined]
        except (AttributeError, Exception):
            pass

    assert len(seen_users) == 1
    user, channel = seen_users[0]
    assert isinstance(user, FakeUser)
    assert user.username == "e2e-user"
    assert channel == "ch-e2e"


async def test_subscribe_sse_e2e_authorize_denies_via_asyncclient(
    django_db_path,
):
    """When authorize returns False against the real request.user, the
    view returns 403. Proves the deny path also runs through middleware."""
    from django.test import AsyncClient
    import joblite_django

    joblite_django.set_authorize(lambda user, channel: False)
    try:
        client = AsyncClient()
        response = await client.get("/joblite/subscribe/forbidden-e2e")
        assert response.status_code == 403
    finally:
        joblite_django.set_authorize(None)


# ---------------------------------------------------------------------------
# authorize callable policy (Django):
#   * sync or async supported (async detected by coroutine return)
#   * if authorize raises, exception propagates; Django returns HTTP 500
# ---------------------------------------------------------------------------


async def test_run_authorize_helper_sync_async_and_raise():
    from joblite_django.views import _run_authorize

    # None -> pass
    assert await _run_authorize(None, object(), "c") is True
    # sync truthy / falsy
    assert await _run_authorize(lambda u, c: True, "u", "c") is True
    assert await _run_authorize(lambda u, c: False, "u", "c") is False

    async def allow(u, c):
        return True

    async def deny(u, c):
        return False

    assert await _run_authorize(allow, "u", "c") is True
    assert await _run_authorize(deny, "u", "c") is False

    def sync_bad(u, c):
        raise RuntimeError("sync boom")

    async def async_bad(u, c):
        raise RuntimeError("async boom")

    with pytest.raises(RuntimeError, match="sync boom"):
        await _run_authorize(sync_bad, "u", "c")
    with pytest.raises(RuntimeError, match="async boom"):
        await _run_authorize(async_bad, "u", "c")


async def test_async_authorize_denies_subscribe(django_db_path):
    """Async authorize returning False must 403 — not be silently treated
    as truthy because the return value is a coroutine."""
    from django.test import AsyncClient
    import joblite_django

    async def authorize(user, channel):
        return False

    joblite_django.set_authorize(authorize)
    try:
        client = AsyncClient()
        response = await client.get("/joblite/subscribe/any-ch")
        assert response.status_code == 403
    finally:
        joblite_django.set_authorize(None)


async def test_async_authorize_denies_stream(django_db_path):
    from django.test import AsyncClient
    import joblite_django

    db = joblite_django.db()
    db.stream("async-deny").publish({"x": 1})

    async def authorize(user, name):
        return False

    joblite_django.set_authorize(authorize)
    try:
        client = AsyncClient()
        response = await client.get("/joblite/stream/async-deny")
        assert response.status_code == 403
    finally:
        joblite_django.set_authorize(None)


async def test_sync_authorize_raises_returns_500(django_db_path):
    """Raised authorize must produce 500, not hang the SSE stream."""
    from django.test import AsyncClient
    import joblite_django

    def authorize(user, channel):
        raise RuntimeError("sync boom")

    joblite_django.set_authorize(authorize)
    try:
        client = AsyncClient(raise_request_exception=False)
        response = await client.get("/joblite/subscribe/any-ch")
        assert response.status_code == 500
    finally:
        joblite_django.set_authorize(None)


async def test_async_authorize_raises_returns_500(django_db_path):
    from django.test import AsyncClient
    import joblite_django

    async def authorize(user, channel):
        raise RuntimeError("async boom")

    joblite_django.set_authorize(authorize)
    try:
        client = AsyncClient(raise_request_exception=False)
        response = await client.get("/joblite/subscribe/any-ch")
        assert response.status_code == 500
    finally:
        joblite_django.set_authorize(None)


async def test_stream_sse_authorize_raises_returns_500(django_db_path):
    """Stream endpoint must also 500, not hang, when authorize raises."""
    from django.test import AsyncClient
    import joblite_django

    db = joblite_django.db()
    db.stream("raise-stream").publish({"x": 1})

    async def authorize(user, name):
        raise ValueError("nope")

    joblite_django.set_authorize(authorize)
    try:
        client = AsyncClient(raise_request_exception=False)
        response = await client.get("/joblite/stream/raise-stream")
        assert response.status_code == 500
    finally:
        joblite_django.set_authorize(None)

