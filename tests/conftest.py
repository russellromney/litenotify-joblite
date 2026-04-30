import gc
import os
import shutil
import sys
import tempfile
import time

import pytest

# Put packages/ on sys.path so the `honker` package is importable in
# tests without needing a `pip install -e`.
_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_PACKAGES_ROOT = os.path.join(_REPO_ROOT, "packages")
if _PACKAGES_ROOT not in sys.path:
    sys.path.insert(0, _PACKAGES_ROOT)

import honker


def _cleanup_tempdir(path: str) -> None:
    last_err = None
    attempts = 60 if sys.platform == "win32" else 1
    for i in range(attempts):
        try:
            shutil.rmtree(path)
            return
        except FileNotFoundError:
            return
        except PermissionError as err:
            last_err = err
        except OSError as err:
            if sys.platform != "win32":
                raise
            last_err = err
        gc.collect()
        time.sleep(0.05 * (i + 1))
    if last_err is not None:
        raise last_err


def _track_closeable(resource, closables):
    if resource is not None and hasattr(resource, "close"):
        closables.append(resource)
    return resource


def _instrument_database(db, closables):
    _track_closeable(db, closables)

    real_listen = db.listen

    def listen(*args, **kwargs):
        return _track_closeable(real_listen(*args, **kwargs), closables)

    db.listen = listen

    real_update_events = db.update_events

    def update_events(*args, **kwargs):
        return _track_closeable(
            real_update_events(*args, **kwargs), closables
        )

    db.update_events = update_events

    real_stream = db.stream

    def stream(*args, **kwargs):
        stream_obj = real_stream(*args, **kwargs)
        real_subscribe = stream_obj.subscribe

        def subscribe(*sub_args, **sub_kwargs):
            return _track_closeable(
                real_subscribe(*sub_args, **sub_kwargs), closables
            )

        stream_obj.subscribe = subscribe
        return stream_obj

    db.stream = stream

    real_queue = db.queue

    def queue(*args, **kwargs):
        queue_obj = real_queue(*args, **kwargs)
        real_claim = queue_obj.claim

        def claim(*claim_args, **claim_kwargs):
            return _track_closeable(
                real_claim(*claim_args, **claim_kwargs), closables
            )

        queue_obj.claim = claim
        return queue_obj

    db.queue = queue
    return db


@pytest.fixture
def db_path():
    d = tempfile.mkdtemp()
    try:
        yield os.path.join(d, "t.db")
    finally:
        _cleanup_tempdir(d)


@pytest.fixture(autouse=True)
def close_opened_honker_dbs(monkeypatch):
    closables = []
    real_open = honker.open

    def tracked_open(*args, **kwargs):
        db = real_open(*args, **kwargs)
        return _instrument_database(db, closables)

    monkeypatch.setattr(honker, "open", tracked_open)
    yield
    closed = set()
    while closables:
        db = closables.pop()
        ident = id(db)
        if ident in closed:
            continue
        closed.add(ident)
        try:
            db.close()
        except Exception:
            pass
    gc.collect()
    if sys.platform == "win32":
        time.sleep(0.5)
