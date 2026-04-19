"""joblite integration for Django 4.1+.

Minimal surface:

    # settings.py
    INSTALLED_APPS = [..., "joblite_django"]
    JOBLITE_DB_PATH = BASE_DIR / "app.db"

    # somewhere at import time (e.g. tasks.py)
    import joblite_django
    @joblite_django.task("emails")
    async def send_email(payload):
        ...

    # urls.py
    from joblite_django.views import stream_sse, subscribe_sse
    urlpatterns = [
        path("joblite/stream/<str:name>", stream_sse),
        path("joblite/subscribe/<str:channel>", subscribe_sse),
    ]

    # run workers in a dedicated process
    python manage.py joblite_worker
"""

from typing import Any, Callable, Dict, Optional

import joblite

default_app_config = "joblite_django.apps.JobliteConfig"

_db: Optional[joblite.Database] = None
_tasks: Dict[str, dict] = {}
_authorize: Optional[Callable] = None
_user_factory: Optional[Callable] = None


def _default_user_factory(request) -> Any:
    """Default: pull `request.user` if the auth middleware has set it.
    Raises a clear error when it hasn't, with a pointer at the usual
    Django setup — previously the view crashed with a confusing
    AttributeError on `request.user` when the middleware was missing.
    """
    try:
        return request.user
    except AttributeError:
        raise RuntimeError(
            "joblite_django: request.user is not available. Install "
            "`django.contrib.auth.middleware.AuthenticationMiddleware` "
            "in MIDDLEWARE, or call `set_user_factory(...)` with a "
            "callable that derives the user from the request."
        )


def db() -> joblite.Database:
    """Return the lazily-opened joblite Database. Uses settings.JOBLITE_DB_PATH."""
    global _db
    if _db is None:
        from django.conf import settings

        path = getattr(settings, "JOBLITE_DB_PATH", None)
        if path is None:
            raise RuntimeError(
                "settings.JOBLITE_DB_PATH is not set; point it at a writable .db file"
            )
        _db = joblite.open(str(path))
    return _db


def reset_for_tests() -> None:
    """Test hook: drop the memoized db so a new one is opened next call."""
    global _db, _user_factory, _authorize
    _db = None
    _user_factory = None
    _authorize = None
    _tasks.clear()


def task(
    queue_name: str,
    concurrency: int = 1,
    visibility_timeout_s: int = 300,
    max_attempts: int = 3,
):
    """Register a handler for a named queue. The worker command discovers
    registered tasks and runs them."""

    def decorator(func: Callable) -> Callable:
        _tasks[queue_name] = {
            "func": func,
            "concurrency": concurrency,
            "visibility_timeout_s": visibility_timeout_s,
            "max_attempts": max_attempts,
        }
        return func

    return decorator


def registered_tasks() -> Dict[str, dict]:
    """Read-only view of tasks registered via @task."""
    return dict(_tasks)


def set_authorize(fn: Optional[Callable]) -> None:
    """Install an authorize(user, name) callable used by the SSE views.

    `fn` may be sync or async. If it raises, the exception propagates and
    Django returns HTTP 500; the SSE stream is never opened.
    """
    global _authorize
    _authorize = fn


def get_authorize() -> Optional[Callable]:
    return _authorize


def set_user_factory(fn: Optional[Callable]) -> None:
    """Install a `user_factory(request) -> user` callable that the
    SSE views pass to `authorize(...)`. If unset, we fall back to
    `request.user` (which requires Django's auth middleware).
    """
    global _user_factory
    _user_factory = fn


def get_user_factory() -> Callable:
    """Return the installed factory, or the default
    `request.user`-reading one. Callers should invoke it to resolve
    the user for the current request.
    """
    return _user_factory or _default_user_factory


__all__ = [
    "db",
    "task",
    "registered_tasks",
    "set_authorize",
    "get_authorize",
    "set_user_factory",
    "get_user_factory",
    "reset_for_tests",
]
