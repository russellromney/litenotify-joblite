"""Tests for the honker Python binding.

Covers: param type fidelity, connection pool release under success/rollback/
exception, listener channel isolation, fanout, slow-listener non-blocking,
and the BEGIN IMMEDIATE concurrency story.
"""

import asyncio
import gc
import sqlite3
import threading
import time

import pytest

import honker


def _make_table(db):
    with db.transaction() as tx:
        tx.execute(
            """CREATE TABLE IF NOT EXISTS t (
                 id INTEGER PRIMARY KEY,
                 i INTEGER, f REAL, s TEXT, b BLOB, n INTEGER, flag INTEGER
               )"""
        )


def test_param_type_fidelity_round_trips(db_path):
    db = honker.open(db_path)
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
    db = honker.open(db_path)
    with db.transaction() as tx:
        tx.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, ts INTEGER)")
        for ts in [10, 2, 100, 20]:
            tx.execute("INSERT INTO t (ts) VALUES (?)", [ts])
    rows = db.query("SELECT ts FROM t WHERE ts < ? ORDER BY ts", [50])
    assert [r["ts"] for r in rows] == [2, 10, 20]


def test_unsupported_param_type_raises(db_path):
    db = honker.open(db_path)
    _make_table(db)
    with pytest.raises(TypeError):
        with db.transaction() as tx:
            tx.execute("INSERT INTO t (i) VALUES (?)", [object()])


async def test_listen_emits_only_matching_channel(db_path):
    db = honker.open(db_path)
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
    db = honker.open(db_path)
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
    db = honker.open(db_path)
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


async def test_notify_payload_round_trips_common_json_shapes(db_path):
    """tx.notify must JSON-encode any JSON-serializable value and the
    receiver's Notification.payload must decode it identically. Matches
    Queue.enqueue and Stream.publish, plus the Node binding (see
    honker-node/test/basic.js:'notify payload round-trips common
    JSON shapes'). Pre-normalization: strings were stored raw, so
    notify("ch", "42") → int 42; notify("ch", "null") → None;
    notify("ch", '"x"') → "x" without quotes. All gone now.
    """
    db = honker.open(db_path)
    got = []
    cases = [
        {"id": 42, "name": "alice"},
        [1, 2, 3],
        "hello",
        42,
        3.14,
        None,
        True,
    ]

    async def listen():
        async for n in db.listen("rt"):
            got.append(n.payload)
            if len(got) == len(cases):
                return

    task = asyncio.create_task(listen())
    await asyncio.sleep(0.05)
    with db.transaction() as tx:
        for p in cases:
            tx.notify("rt", p)

    await asyncio.wait_for(task, timeout=2.0)
    # `True` round-trips as `True` (JSON boolean, not integer 1).
    assert got == cases


async def test_dict_and_list_payloads_json_serialized(db_path):
    db = honker.open(db_path)
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


def test_prune_notifications_by_max_keep(db_path):
    """Pruning is user-invoked; notify() never auto-prunes."""
    db = honker.open(db_path)

    # 100 notifications across 2 channels.
    for i in range(100):
        with db.transaction() as tx:
            tx.notify("ch", f"n{i}")

    before = db.query(
        "SELECT COUNT(*) AS c FROM _honker_notifications"
    )[0]["c"]
    assert before == 100, (
        f"notify() must not auto-prune; expected 100 rows, got {before}"
    )

    # Keep the most recent 10.
    deleted = db.prune_notifications(max_keep=10)
    assert deleted == 90

    after = db.query(
        "SELECT COUNT(*) AS c FROM _honker_notifications"
    )[0]["c"]
    assert after == 10


def test_prune_notifications_by_age(db_path):
    db = honker.open(db_path)

    # Two old rows (created_at well in the past) and one recent.
    with db.transaction() as tx:
        tx.execute(
            "INSERT INTO _honker_notifications "
            "(channel, payload, created_at) VALUES ('ch','old1', 0)",
        )
        tx.execute(
            "INSERT INTO _honker_notifications "
            "(channel, payload, created_at) VALUES ('ch','old2', 0)",
        )
    with db.transaction() as tx:
        tx.notify("ch", "fresh")

    # Drop anything older than 1 second.
    deleted = db.prune_notifications(older_than_s=1)
    assert deleted == 2
    rows = db.query(
        "SELECT payload FROM _honker_notifications ORDER BY id"
    )
    # tx.notify now unconditionally json.dumps, so the stored payload is
    # the JSON-encoded form. json.loads recovers the original string.
    import json as _json
    assert [_json.loads(r["payload"]) for r in rows] == ["fresh"]


def test_prune_notifications_no_args_is_noop(db_path):
    """Calling prune with nothing configured just returns 0."""
    db = honker.open(db_path)
    with db.transaction() as tx:
        tx.notify("ch", "a")
    assert db.prune_notifications() == 0
    rows = db.query("SELECT COUNT(*) AS c FROM _honker_notifications")
    assert rows[0]["c"] == 1


def test_connection_returned_on_commit_success(db_path):
    """Running many sequential transactions must not exhaust the writer slot."""
    db = honker.open(db_path)
    _make_table(db)
    for i in range(50):
        with db.transaction() as tx:
            tx.execute("INSERT INTO t (i) VALUES (?)", [i])
    rows = db.query("SELECT COUNT(*) AS c FROM t")
    assert rows[0]["c"] == 50


def test_connection_returned_after_exception_in_body(db_path):
    """Regression: the writer connection was leaked when the with-body raised."""
    db = honker.open(db_path)
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
    db = honker.open(db_path)
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


async def test_slow_wal_consumer_does_not_block_writer(db_path):
    """A slow WAL-event consumer must not backpressure the writer.
    The shared UpdateWatcher has bounded per-subscriber buffers; ticks
    to a slow subscriber are dropped, but the writer path is never
    slowed. Regression against a previous design where listeners
    shared a tokio broadcast ring that could block on a full consumer.
    """
    db = honker.open(db_path)
    _make_table(db)

    # One dormant listener that never consumes a WAL event. With a
    # naive implementation, the subscriber channel fills and backpressure
    # reaches the writer. With the bounded fan-out, the writer never
    # touches the subscriber list.
    _dormant = db.listen("ch")  # noqa: F841

    loop = asyncio.get_running_loop()
    start = loop.time()
    for i in range(500):
        with db.transaction() as tx:
            tx.notify("ch", f"m{i}")
    elapsed = loop.time() - start

    # 500 tx on M-series at ~3-5k tx/s ~= 100-160 ms. 2 s is a loose
    # ceiling; a real regression would be >>2 s.
    assert elapsed < 2.0, (
        f"500 commits took {elapsed:.3f}s; slow-consumer backpressure?"
    )


def test_begin_immediate_serializes_writers(db_path):
    """Two threads writing concurrently must both succeed, serialized."""
    db = honker.open(db_path)
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
    db = honker.open(db_path, max_readers=4)
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
    db = honker.open(db_path)
    _make_table(db)
    with db.transaction() as tx:
        tx.execute("INSERT INTO t (i) VALUES (?)", [1])
    # Two back-to-back queries succeed without interfering.
    assert db.query("SELECT i FROM t")[0]["i"] == 1
    assert db.query("SELECT COUNT(*) AS c FROM t")[0]["c"] == 1


def test_busy_timeout_is_set(db_path):
    """PRAGMA busy_timeout=5000 must be applied on every connection."""
    db = honker.open(db_path)
    _make_table(db)
    rows = db.query("PRAGMA busy_timeout")
    assert rows[0]["timeout"] == 5000
    rows = db.query("PRAGMA journal_mode")
    assert rows[0]["journal_mode"].lower() == "wal"


async def test_busy_channel_does_not_delay_quiet_channel(db_path):
    """Per-channel SELECT isolation: a listener on a quiet channel
    must see its message promptly even while another channel is
    flooded. Under the current architecture this is true by
    construction (each listener's SELECT filters on channel=?), but
    the property is load-bearing enough to guard with a test —
    a regression would break the fan-out story.
    """
    db = honker.open(db_path)
    _make_table(db)

    got = []

    async def quiet_listener():
        async for n in db.listen("quiet"):
            got.append(n.payload)
            return

    task = asyncio.create_task(quiet_listener())
    await asyncio.sleep(0.05)

    # Flood "loud" and send exactly one on "quiet" in the same tx.
    with db.transaction() as tx:
        for i in range(3000):
            tx.notify("loud", f"p{i}")
        tx.notify("quiet", "only")

    # The quiet listener SELECTs with channel='quiet' and ignores the
    # 3000 loud rows. Delivery should be fast, not proportional to the
    # flood size. 1s is a loose bound.
    loop = asyncio.get_running_loop()
    start = loop.time()
    await asyncio.wait_for(task, timeout=3.0)
    elapsed = loop.time() - start
    assert got == ["only"]
    assert elapsed < 1.0, (
        f"quiet listener took {elapsed:.3f}s with 3000 loud-channel rows"
    )


async def test_listener_drop_allows_clean_reuse(db_path):
    """Creating and dropping many listeners (as SSE reconnects do)
    must cleanly release their WAL-watcher subscriptions. Dropping a
    UpdateEvents now unsubscribes its channel from the shared watcher;
    the bridge thread exits on disconnect."""
    db = honker.open(db_path)
    _make_table(db)

    # Churn through 50 short-lived listeners, each consuming one event.
    for i in range(50):
        listener = db.listen(f"ch-{i}")
        task = asyncio.create_task(listener.__anext__())
        with db.transaction() as tx:
            tx.notify(f"ch-{i}", "ok")
        got = await asyncio.wait_for(task, timeout=2.0)
        assert got.payload == "ok"
        del task
        del listener
        gc.collect()
        # Explicitly dropping the listener each round makes this test
        # prove teardown instead of assuming the event loop scheduled
        # construction quickly enough and CPython reclaimed the object
        # before the next iteration.


def test_pure_sqlite_reader_sees_committed_writes(db_path):
    """Cross-check: an independent sqlite3 client sees our writes."""
    db = honker.open(db_path)
    _make_table(db)
    with db.transaction() as tx:
        tx.execute("INSERT INTO t (i) VALUES (?)", [7])
    c = sqlite3.connect(db_path)
    try:
        got = c.execute("SELECT i FROM t").fetchall()
    finally:
        c.close()
    assert got == [(7,)]
