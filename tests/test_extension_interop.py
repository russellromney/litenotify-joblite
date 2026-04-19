"""Cross-binding interop: the SQLite loadable extension and the Python
binding must agree on schema and ack semantics. Root-caused an earlier
bug where the extension had a 6-column ``_joblite_dead`` while Python
expected 10, and where ``jl_ack_batch`` UPDATEd rows to state='done'
while Python DELETEd them. Both now share
``litenotify-core::bootstrap_joblite_schema`` and both DELETE on ack.
"""

import json
import os
import sqlite3
import sys

import pytest

import joblite


REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Extension filename differs by platform — look for whichever exists.
_CANDIDATES = [
    os.path.join(REPO_ROOT, "target", "release", "liblitenotify_ext.dylib"),
    os.path.join(REPO_ROOT, "target", "release", "liblitenotify_ext.so"),
]
_EXT_PATH = next((p for p in _CANDIDATES if os.path.exists(p)), None)

_SKIP_REASON = (
    "litenotify-extension .dylib/.so not found under target/release — "
    "run `cargo build -p litenotify-extension --release` first"
)


@pytest.fixture
def ext_db_path(tmp_path):
    return str(tmp_path / "interop.db")


@pytest.mark.skipif(_EXT_PATH is None, reason=_SKIP_REASON)
def test_extension_and_python_share_schema(ext_db_path):
    """``jl_bootstrap`` + Python's ``_init_schema`` must produce a
    schema Python can operate on without errors. The earlier extension
    had a 6-column ``_joblite_dead`` and Python's ``fail()`` tripped
    on 'no column named priority'.
    """
    # Bootstrap via the extension, then open from Python joblite.
    conn = sqlite3.connect(ext_db_path)
    conn.enable_load_extension(True)
    conn.load_extension(_EXT_PATH)
    conn.execute("SELECT jl_bootstrap()")
    conn.close()

    # Python side should be able to run through the full failure path
    # (which writes into _joblite_dead) without schema errors.
    db = joblite.open(ext_db_path)
    q = db.queue("interop", max_attempts=1)
    q.enqueue({"kind": "interop"})

    worker = "py-worker"
    job = q.claim_one(worker)
    assert job is not None

    # Explicit fail → INSERT into _joblite_dead with 10 cols. Pre-fix
    # this raised "no column named priority".
    assert q.fail(job.id, worker, "forced failure for schema check")

    dead = db.query(
        "SELECT id, queue, payload, priority, run_at, max_attempts, "
        "attempts, last_error, created_at, died_at "
        "FROM _joblite_dead"
    )
    assert len(dead) == 1
    assert dead[0]["queue"] == "interop"
    assert dead[0]["last_error"] == "forced failure for schema check"


@pytest.mark.skipif(_EXT_PATH is None, reason=_SKIP_REASON)
def test_extension_jl_ack_deletes_row(ext_db_path):
    """``jl_ack_batch`` must DELETE the row (match Python ``Queue.ack``)
    rather than UPDATE ``state='done'`` and leave it in ``_joblite_live``
    forever.
    """
    # Enqueue via Python joblite.
    db = joblite.open(ext_db_path)
    q = db.queue("ext-ack")
    q.enqueue({"i": 1})
    q.enqueue({"i": 2})
    q.enqueue({"i": 3})

    # Claim + ack via the extension.
    conn = sqlite3.connect(ext_db_path)
    conn.enable_load_extension(True)
    conn.load_extension(_EXT_PATH)

    rows_json = conn.execute(
        "SELECT jl_claim_batch('ext-ack', 'ext-worker', 10, 300)"
    ).fetchone()[0]
    claimed = json.loads(rows_json)
    assert len(claimed) == 3
    claimed_ids = [r["id"] for r in claimed]

    # Ack all three via the extension — must DELETE, not UPDATE.
    acked = conn.execute(
        "SELECT jl_ack_batch(?, 'ext-worker')",
        [json.dumps(claimed_ids)],
    ).fetchone()[0]
    assert acked == 3
    conn.close()

    # _joblite_live must be empty — no state='done' residue.
    remaining = db.query(
        "SELECT COUNT(*) AS c FROM _joblite_live WHERE queue = 'ext-ack'"
    )[0]["c"]
    assert remaining == 0, (
        f"extension-acked rows left behind in _joblite_live (count={remaining}); "
        f"jl_ack_batch must DELETE, not UPDATE"
    )


@pytest.mark.skipif(_EXT_PATH is None, reason=_SKIP_REASON)
def test_extension_registers_notify_function(ext_db_path):
    """Loading the extension must also register ``notify()`` + the
    ``_litenotify_notifications`` table. The extension's docstring has
    always advertised this; earlier builds didn't actually install it.
    """
    conn = sqlite3.connect(ext_db_path)
    conn.enable_load_extension(True)
    conn.load_extension(_EXT_PATH)

    conn.execute("BEGIN IMMEDIATE")
    row = conn.execute("SELECT notify('orders', 'hello')").fetchone()
    assert row[0] >= 1  # returned inserted id
    conn.execute("COMMIT")

    count = conn.execute(
        "SELECT COUNT(*) FROM _litenotify_notifications WHERE channel='orders'"
    ).fetchone()[0]
    assert count == 1

    conn.close()
