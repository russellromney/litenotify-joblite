import gc
import os
import sys
import tempfile

import pytest

# Put packages/ on sys.path so the `honker` package is importable in
# tests without needing a `pip install -e`.
_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_PACKAGES_ROOT = os.path.join(_REPO_ROOT, "packages")
_HONKER_PYTHON_ROOT = os.path.join(_PACKAGES_ROOT, "honker", "python")
for path in (_HONKER_PYTHON_ROOT, _PACKAGES_ROOT):
    if os.path.isdir(path) and path not in sys.path:
        sys.path.insert(0, path)


@pytest.fixture
def db_path():
    with tempfile.TemporaryDirectory() as d:
        yield os.path.join(d, "t.db")
        # Pytest captures test-function locals for failure reporting,
        # so the test's `db = honker.open(path)` reference can outlive
        # the test body and delay Database's Drop until after the
        # `with` exits. On Linux/macOS unlink-while-open hides this;
        # on Windows tempfile cleanup hits WinError 32.
        # Force a collection cycle here so Drop runs and releases the
        # SQLite handles before TemporaryDirectory.__exit__ unlinks.
        gc.collect()
