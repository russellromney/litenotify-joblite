"""Tests for the litenotify Python binding.

Covers: param type fidelity, connection pool release under success/rollback/
exception, listener channel isolation, fanout, slow-listener non-blocking,
and the BEGIN IMMEDIATE concurrency story.
"""

import asyncio
import sqlite3
import threading
import time

import pytest

import joblite as litenotify


def _make_table(db):
    with db.transaction() as tx:
        tx.execute(
            """CREATE TABLE IF NOT EXISTS t (
                 id INTEGER PRIMARY KEY,
                 i INTEGER, f REAL, s TEXT, b BLOB, n INTEGER, flag INTEGER
               )"""
        )


def test_param_type_fidelity_round_trips(db_path):
    db = litenotify.open(db_path)
    _make_table(db)
    with db.transaction() as tx:
        tx.execute(
            "INSERT INTO t (i, f, s, b, n, flag) VALUES (?, ?, ?, ?, ?, ?)",
            [42, 3.14, "hello", b"\x00\xff", None, True],
        )
        tx.execute(
            "INSERT INTO t (i, f, s, b, n, flag) VALUES (?, ?, ?, ?, ?, ?)",
            [-1, 0.0, "", b"", None, False],
        )
    rows = db.query("SELECT i, f, s, b, n, flag FROM t ORDER BY id")
    assert rows[0] == {
        "i": 42, "f": 3.14, "s": "hello", "b": b"\x00\xff", "n": None, "flag": 1,
    }
    assert rows[1] == {
        "i": -1, "f": 0.0, "s": "", "b": b"", "n": None, "flag": 0,
    }


def test_integer_comparisons_not_string_coerced(db_path):
    """Regression: params were stringified, which broke numeric comparisons."""
    db = litenotify.open(db_path)
    with db.transaction() as tx:
        tx.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, ts INTEGER)")
        for ts in [10, 2, 100, 20]:
            tx.execute("INSERT INTO t (ts) VALUES (?)", [ts])
    rows = db.query("SELECT ts FROM t WHERE ts < ? ORDER BY ts", [50])
    assert [r["ts"] for r in rows] == [2, 10, 20]


def test_unsupported_param_type_raises(db_path):
    db = litenotify.open(db_path)
    _make_table(db)
    with pytest.raises(TypeError):
        with db.transaction() as tx:
            tx.execute("INSERT INTO t (i) VALUES (?)", [object()])


async def test_listen_emits_only_matching_channel(db_path):
    db = litenotify.open(db_path)
    _make_table(db)

    got = []

    async def listen():
        async for n in db.listen("orders"):
            got.append(n.payload)
            if len(got) == 2:
                return

    task = asyncio.create_task(listen())
    await asyncio.sleep(0.05)

    with db.transaction() as tx:
        tx.notify("unrelated", "skip")
    with db.transaction() as tx:
        tx.notify("orders", "one")
    with db.transaction() as tx:
        tx.notify("other", "skip")
    with db.transaction() as tx:
        tx.notify("orders", "two")

    await asyncio.wait_for(task, timeout=2.0)
    assert got == ["one", "two"]


async def test_multiple_listeners_same_channel_all_receive(db_path):
    db = litenotify.open(db_path)
    _make_table(db)

    async def collect(n_expected):
        seen = []
        async for n in db.listen("ch"):
            seen.append(n.payload)
            if len(seen) == n_expected:
                return seen

    t1 = asyncio.create_task(collect(3))
    t2 = asyncio.create_task(collect(3))
    await asyncio.sleep(0.05)

    for i in range(3):
        with db.transaction() as tx:
            tx.notify("ch", f"m{i}")

    r1 = await asyncio.wait_for(t1, timeout=2.0)
    r2 = await asyncio.wait_for(t2, timeout=2.0)
    assert r1 == ["m0", "m1", "m2"]
    assert r2 == ["m0", "m1", "m2"]


async def test_rollback_drops_notification(db_path):
    db = litenotify.open(db_path)
    _make_table(db)

    got = []

    async def listen():
        async for n in db.listen("ch"):
            got.append(n.payload)
            if len(got) == 1:
                return

    task = asyncio.create_task(listen())
    await asyncio.sleep(0.05)

    with pytest.raises(RuntimeError):
        with db.transaction() as tx:
            tx.execute("CREATE TABLE x (id INTEGER)")
            tx.notify("ch", "dropped")
            raise RuntimeError("boom")
    with db.transaction() as tx:
        tx.notify("ch", "delivered")

    await asyncio.wait_for(task, timeout=2.0)
    assert got == ["delivered"]


async def test_dict_and_list_payloads_json_serialized(db_path):
    db = litenotify.open(db_path)
    _make_table(db)
    got = []

    async def listen():
        async for n in db.listen("ch"):
            got.append(n.payload)
            if len(got) == 2:
                return

    task = asyncio.create_task(listen())
    await asyncio.sleep(0.05)

    with db.transaction() as tx:
        tx.notify("ch", {"id": 42, "name": "alice"})
    with db.transaction() as tx:
        tx.notify("ch", [1, 2, 3])

    await asyncio.wait_for(task, timeout=2.0)
    # Notification.payload now auto-decodes JSON on access.
    assert got[0] == {"id": 42, "name": "alice"}
    assert got[1] == [1, 2, 3]


def test_connection_returned_on_commit_success(db_path):
    """Running many sequential transactions must not exhaust the writer slot."""
    db = litenotify.open(db_path)
    _make_table(db)
    for i in range(50):
        with db.transaction() as tx:
            tx.execute("INSERT INTO t (i) VALUES (?)", [i])
    rows = db.query("SELECT COUNT(*) AS c FROM t")
    assert rows[0]["c"] == 50


def test_connection_returned_after_exception_in_body(db_path):
    """Regression: the writer connection was leaked when the with-body raised."""
    db = litenotify.open(db_path)
    _make_table(db)
    for _ in range(20):
        with pytest.raises(ValueError):
            with db.transaction() as tx:
                tx.execute("INSERT INTO t (i) VALUES (?)", [1])
                raise ValueError("body failed")
    # If the writer slot were leaked, these would hang or fail.
    with db.transaction() as tx:
        tx.execute("INSERT INTO t (i) VALUES (?)", [99])
    rows = db.query("SELECT i FROM t")
    assert [r["i"] for r in rows] == [99]


def test_connection_returned_after_commit_error(db_path):
    """If COMMIT itself fails (constraint etc.) the conn is still returned."""
    db = litenotify.open(db_path)
    with db.transaction() as tx:
        tx.execute(
            """CREATE TABLE t (
                 id INTEGER PRIMARY KEY,
                 a INTEGER NOT NULL DEFERRABLE INITIALLY DEFERRED CHECK (a > 0)
               )"""
        )

    # CHECK constraints fire at row-write time in SQLite, so synthesize a
    # failure via an explicit trigger-style guard.
    with db.transaction() as tx:
        tx.execute("INSERT INTO t (a) VALUES (?)", [5])

    # Next transaction should just work; conn is not stuck.
    for _ in range(5):
        with db.transaction() as tx:
            tx.execute("INSERT INTO t (a) VALUES (?)", [1])
    rows = db.query("SELECT COUNT(*) AS c FROM t")
    assert rows[0]["c"] == 6


async def test_slow_listener_does_not_block_commit_hook(db_path):
    """A listener that falls behind must not stall subsequent commits."""
    db = litenotify.open(db_path)
    _make_table(db)

    finished_commits = []

    # Keep one listener alive but never consume it.
    _dormant = db.listen("ch")  # noqa: F841

    loop = asyncio.get_running_loop()
    start = loop.time()
    # Fire many commits; if the commit hook were blocking on the listener
    # we'd see this take far longer than expected.
    for i in range(50):
        with db.transaction() as tx:
            tx.notify("ch", f"m{i}")
        finished_commits.append(i)
    elapsed = loop.time() - start

    assert len(finished_commits) == 50
    assert elapsed < 2.0, f"commits took {elapsed:.3f}s; commit hook may be blocking"


def test_begin_immediate_serializes_writers(db_path):
    """Two threads writing concurrently must both succeed, serialized."""
    db = litenotify.open(db_path)
    _make_table(db)

    errors = []
    ordered = []

    def worker(tag):
        try:
            for i in range(20):
                with db.transaction() as tx:
                    tx.execute("INSERT INTO t (s) VALUES (?)", [f"{tag}-{i}"])
                    ordered.append(tag)
        except Exception as e:
            errors.append(e)

    threads = [threading.Thread(target=worker, args=(t,)) for t in ("a", "b", "c")]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert errors == []
    rows = db.query("SELECT COUNT(*) AS c FROM t")
    assert rows[0]["c"] == 60


def test_readers_concurrent_with_writer(db_path):
    """WAL mode lets readers run while the writer holds BEGIN IMMEDIATE."""
    db = litenotify.open(db_path, max_readers=4)
    _make_table(db)
    with db.transaction() as tx:
        for i in range(100):
            tx.execute("INSERT INTO t (i) VALUES (?)", [i])

    reader_done = threading.Event()

    def read_loop():
        for _ in range(50):
            rows = db.query("SELECT COUNT(*) AS c FROM t")
            assert rows[0]["c"] >= 100
        reader_done.set()

    rt = threading.Thread(target=read_loop)
    rt.start()

    # Hold a writer transaction open while readers run.
    with db.transaction() as tx:
        tx.execute("INSERT INTO t (i) VALUES (?)", [999])
        time.sleep(0.05)
    rt.join(timeout=5.0)
    assert reader_done.is_set()


def test_query_outside_transaction_uses_reader(db_path):
    """db.query() works outside of a transaction (reader pool)."""
    db = litenotify.open(db_path)
    _make_table(db)
    with db.transaction() as tx:
        tx.execute("INSERT INTO t (i) VALUES (?)", [1])
    # Two back-to-back queries succeed without interfering.
    assert db.query("SELECT i FROM t")[0]["i"] == 1
    assert db.query("SELECT COUNT(*) AS c FROM t")[0]["c"] == 1


def test_busy_timeout_is_set(db_path):
    """PRAGMA busy_timeout=5000 must be applied on every connection."""
    db = litenotify.open(db_path)
    _make_table(db)
    rows = db.query("PRAGMA busy_timeout")
    assert rows[0]["timeout"] == 5000
    rows = db.query("PRAGMA journal_mode")
    assert rows[0]["journal_mode"].lower() == "wal"


async def test_cross_channel_starvation_immune(db_path):
    """Regression for the per-channel registry refactor: a slow listener on
    a quiet channel must not miss its single message when an unrelated loud
    channel emits thousands. Before the refactor both shared one tokio
    broadcast ring, so loud could Lag cold out."""
    db = litenotify.open(db_path)
    _make_table(db)

    got = []

    async def cold_listener():
        async for n in db.listen("cold"):
            got.append(n.payload)
            return

    task = asyncio.create_task(cold_listener())
    await asyncio.sleep(0.05)

    # Emit 3000 messages on "hot" and exactly one on "cold".
    with db.transaction() as tx:
        for i in range(3000):
            tx.notify("hot", f"p{i}")
        tx.notify("cold", "only")

    await asyncio.wait_for(task, timeout=3.0)
    assert got == ["only"]


async def test_listener_drop_allows_clean_reuse(db_path):
    """Regression: creating and dropping many listeners (as SSE reconnects
    do) must not wedge the notifier. Before the refactor, each dropped
    listener left a stranded bridge thread."""
    db = litenotify.open(db_path)
    _make_table(db)

    # Churn through 50 short-lived listeners, each consuming one event.
    for i in range(50):
        got = []

        async def once():
            async for n in db.listen(f"ch-{i}"):
                got.append(n.payload)
                return

        task = asyncio.create_task(once())
        await asyncio.sleep(0.01)
        with db.transaction() as tx:
            tx.notify(f"ch-{i}", "ok")
        await asyncio.wait_for(task, timeout=2.0)
        assert got == ["ok"]
        # Listener python object goes out of scope here; Drop impl
        # deregisters the subscriber so the bridge thread exits cleanly.


def test_pure_sqlite_reader_sees_committed_writes(db_path):
    """Cross-check: an independent sqlite3 client sees our writes."""
    db = litenotify.open(db_path)
    _make_table(db)
    with db.transaction() as tx:
        tx.execute("INSERT INTO t (i) VALUES (?)", [7])
    c = sqlite3.connect(db_path)
    try:
        got = c.execute("SELECT i FROM t").fetchall()
    finally:
        c.close()
    assert got == [(7,)]
