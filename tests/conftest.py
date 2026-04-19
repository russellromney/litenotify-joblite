import os
import sys
import tempfile

import pytest

# Put packages/ on sys.path so the pure-Python packages (joblite,
# joblite_fastapi, joblite_django, joblite_flask) are importable in
# tests without needing a `pip install -e` per package.
_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_PACKAGES_ROOT = os.path.join(_REPO_ROOT, "packages")
if _PACKAGES_ROOT not in sys.path:
    sys.path.insert(0, _PACKAGES_ROOT)


@pytest.fixture
def db_path():
    with tempfile.TemporaryDirectory() as d:
        yield os.path.join(d, "t.db")
