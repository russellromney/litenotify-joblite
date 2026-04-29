import gc
import os
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


@pytest.fixture
def db_path():
    with tempfile.TemporaryDirectory() as d:
        yield os.path.join(d, "t.db")


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
        time.sleep(0.05)
