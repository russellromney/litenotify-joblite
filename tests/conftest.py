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
    attempts = 40 if sys.platform == "win32" else 1
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
    if sys.platform == "win32" and last_err is not None:
        return
    if last_err is not None:
        raise last_err


@pytest.fixture
def db_path():
    d = tempfile.mkdtemp()
    try:
        yield os.path.join(d, "t.db")
    finally:
        _cleanup_tempdir(d)


@pytest.fixture(autouse=True)
def close_opened_honker_dbs(monkeypatch):
    opened = []
    real_open = honker.open

    def tracked_open(*args, **kwargs):
        db = real_open(*args, **kwargs)
        opened.append(db)
        return db

    monkeypatch.setattr(honker, "open", tracked_open)
    yield
    while opened:
        db = opened.pop()
        try:
            db.close()
        except Exception:
            pass
    gc.collect()
    if sys.platform == "win32":
        time.sleep(0.25)
