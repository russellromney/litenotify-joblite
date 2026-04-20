"""Pure-Python native layer for honker.

Replaces the previous PyO3 crate. Every hot code path now lives in the
`libhonker_ext` SQLite loadable extension (shared with Node, Go, Ruby,
extension-only callers) — this module is the ~200-line Python glue that
opens a SQLite connection, loads the extension, and exposes the same
`Database`/`Transaction`/`WalEvents`/`open`/`cron_next_after` surface the
rest of the package (`_honker.py`, `_scheduler.py`, `_worker.py`) expects.

Contract with the rest of the package:

* `open(path, max_readers=8) -> Database`
* `cron_next_after(expr, from_unix) -> int`
* `Database.transaction()` → `Transaction` context manager
* `Database.wal_events()` → async iterator of wake pings
* `Database.query(sql, params=None) -> list[dict]`
* `Transaction.execute/query/notify/bootstrap_honker_schema`
* `WalEvents.path` property

The extension is located relative to this file (bundled in the wheel)
or under `<repo>/target/release/` for editable dev installs. Override
via `HONKER_EXTENSION_PATH`. `sqlite3.enable_load_extension` must be
available — Apple's system Python ships it disabled; use python.org /
homebrew / pyenv / conda instead.
"""

from __future__ import annotations

import asyncio
import json
import os
import pathlib
import sqlite3
import sys
import threading
import time
from typing import Any, Optional


# ---------------------------------------------------------------------
# Extension discovery + connection setup
# ---------------------------------------------------------------------

_DEFAULT_PRAGMAS = """
    PRAGMA journal_mode = WAL;
    PRAGMA synchronous = NORMAL;
    PRAGMA busy_timeout = 5000;
    PRAGMA foreign_keys = ON;
    PRAGMA cache_size = -32000;
    PRAGMA temp_store = MEMORY;
    PRAGMA wal_autocheckpoint = 10000;
"""

_EXT_FILENAMES = ("libhonker_ext.dylib", "libhonker_ext.so", "libhonker_ext.dll")

_ext_path_cache: Optional[str] = None


def _find_extension() -> str:
    global _ext_path_cache
    if _ext_path_cache is not None:
        return _ext_path_cache
    env = os.environ.get("HONKER_EXTENSION_PATH")
    if env:
        _ext_path_cache = env
        return env
    here = pathlib.Path(__file__).resolve().parent
    # Wheel install: extension bundled next to this file.
    for name in _EXT_FILENAMES:
        p = here / name
        if p.exists():
            _ext_path_cache = str(p)
            return _ext_path_cache
    # Dev tree: walk up to find `target/release/`.
    for parent in here.parents:
        rel = parent / "target" / "release"
        if rel.is_dir():
            for name in _EXT_FILENAMES:
                p = rel / name
                if p.exists():
                    _ext_path_cache = str(p)
                    return _ext_path_cache
    raise RuntimeError(
        "honker: libhonker_ext not found. In a dev checkout, run "
        "`cargo build --release -p honker-extension`. In a wheel install, "
        "the extension should be bundled alongside this module — this is "
        "a packaging bug. Override with HONKER_EXTENSION_PATH=<path>."
    )


def _open_conn(path: str, load_extension: bool = True) -> sqlite3.Connection:
    """Open a SQLite connection with honker's PRAGMA defaults. When
    `load_extension=True` (writer + the cron worker conn) we also
    `conn.load_extension(libhonker_ext)` so `honker_*` SQL functions and
    the `_honker_notifications` table exist. Readers don't need it — they
    only run plain SELECTs — so we skip the load to shave ~2 ms off every
    pool open."""
    if load_extension and not hasattr(
        sqlite3.Connection, "enable_load_extension"
    ):
        raise RuntimeError(
            "honker: this Python's sqlite3 was built without extension "
            "loading support. Apple's system Python ships with it disabled. "
            "Use python.org / Homebrew / pyenv / conda Python instead."
        )
    try:
        conn = sqlite3.connect(
            path,
            isolation_level=None,  # manual BEGIN/COMMIT/ROLLBACK — no magic.
            check_same_thread=False,  # we gate access with our own locks.
        )
        if load_extension:
            conn.enable_load_extension(True)
            conn.load_extension(_find_extension())
            conn.enable_load_extension(False)
        conn.executescript(_DEFAULT_PRAGMAS)
    except sqlite3.Error as e:
        raise RuntimeError(str(e)) from e
    return conn


def _rows_to_dicts(cursor: sqlite3.Cursor) -> list[dict]:
    if cursor.description is None:
        return []
    cols = [c[0] for c in cursor.description]
    return [dict(zip(cols, row)) for row in cursor.fetchall()]


def _run_execute(conn: sqlite3.Connection, sql: str, params) -> sqlite3.Cursor:
    """Execute a single SQL statement, normalizing sqlite3 errors into
    the exception types the rest of the package (and the old PyO3 layer)
    promised: `TypeError` for unsupported parameter types, `RuntimeError`
    for everything else."""
    try:
        return conn.execute(sql, tuple(params) if params else ())
    except sqlite3.ProgrammingError as e:
        msg = str(e)
        if "not supported" in msg or "binding parameter" in msg:
            raise TypeError(msg) from e
        raise RuntimeError(msg) from e
    except sqlite3.Error as e:
        raise RuntimeError(str(e)) from e


# ---------------------------------------------------------------------
# Connection pools
# ---------------------------------------------------------------------


class _Writer:
    """Single writer connection behind a mutex. SQLite WAL mode permits
    only one writer at a time anyway; serializing in user space avoids
    `SQLITE_BUSY` retries under our own process's contention."""

    def __init__(self, conn: sqlite3.Connection):
        self._conn = conn
        self._lock = threading.Lock()
        self._cond = threading.Condition(self._lock)
        self._held = False
        self._closed = False

    def try_acquire(self) -> Optional[sqlite3.Connection]:
        with self._lock:
            if self._held or self._closed:
                return None
            self._held = True
            return self._conn

    def acquire(self) -> sqlite3.Connection:
        with self._cond:
            while self._held and not self._closed:
                self._cond.wait()
            if self._closed:
                raise RuntimeError("Database is closed")
            self._held = True
            return self._conn

    def release(self, conn: sqlite3.Connection) -> None:
        with self._cond:
            assert conn is self._conn
            self._held = False
            self._cond.notify()

    def close(self) -> None:
        with self._cond:
            self._closed = True
            self._cond.notify_all()
            try:
                self._conn.close()
            except sqlite3.Error:
                pass


class _Readers:
    """Bounded pool of read-only SQLite connections, opened lazily up to
    `max_readers`. Blocks when the pool is exhausted."""

    def __init__(self, path: str, max_readers: int):
        self._path = path
        self._max = max(1, int(max_readers))
        self._lock = threading.Lock()
        self._cond = threading.Condition(self._lock)
        self._idle: list[sqlite3.Connection] = []
        self._count = 0
        self._closed = False

    def acquire(self) -> sqlite3.Connection:
        with self._cond:
            while True:
                if self._closed:
                    raise RuntimeError("Database is closed")
                if self._idle:
                    return self._idle.pop()
                if self._count < self._max:
                    self._count += 1
                    break
                self._cond.wait()
        # Open outside the lock so slow opens don't block the pool.
        try:
            return _open_conn(self._path, load_extension=False)
        except Exception:
            with self._cond:
                self._count -= 1
                self._cond.notify()
            raise

    def release(self, conn: sqlite3.Connection) -> None:
        with self._cond:
            if self._closed:
                try:
                    conn.close()
                except sqlite3.Error:
                    pass
                return
            self._idle.append(conn)
            self._cond.notify()

    def close(self) -> None:
        with self._cond:
            self._closed = True
            idle = self._idle
            self._idle = []
            self._cond.notify_all()
        for c in idle:
            try:
                c.close()
            except sqlite3.Error:
                pass


# ---------------------------------------------------------------------
# WAL watcher: one stat-poll thread per Database, fan-out to subscribers
# ---------------------------------------------------------------------


class _SharedWalWatcher:
    """One stat-polling thread per Database. Subscribers register an
    asyncio.Queue + event loop; on every WAL file change, the watcher
    posts a wake ping to each subscriber's loop via `call_soon_threadsafe`.

    Coalescing: subscribers use `maxsize=1` queues and drop wakes if one
    is already pending. Every wake means "re-SELECT since last id" — two
    wakes coalesce to one SELECT.
    """

    _POLL_INTERVAL_S = 0.001  # 1 ms

    def __init__(self, wal_path: str):
        self._wal_path = wal_path
        self._lock = threading.Lock()
        self._subs: dict[int, tuple[asyncio.AbstractEventLoop, asyncio.Queue]] = {}
        self._next_id = 0
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._last_stat = self._stat()

    def _stat(self) -> Optional[tuple[int, int]]:
        try:
            s = os.stat(self._wal_path)
        except FileNotFoundError:
            return None
        return (s.st_size, s.st_mtime_ns)

    def subscribe(
        self, loop: asyncio.AbstractEventLoop, queue: asyncio.Queue
    ) -> int:
        with self._lock:
            sid = self._next_id
            self._next_id += 1
            self._subs[sid] = (loop, queue)
            if self._thread is None:
                self._thread = threading.Thread(
                    target=self._run,
                    name="honker-wal-watch",
                    daemon=True,
                )
                self._thread.start()
            return sid

    def unsubscribe(self, sid: int) -> None:
        with self._lock:
            self._subs.pop(sid, None)

    def _run(self) -> None:
        while not self._stop.is_set():
            cur = self._stat()
            if cur is not None and cur != self._last_stat:
                self._last_stat = cur
                with self._lock:
                    targets = list(self._subs.values())
                for loop, queue in targets:
                    try:
                        loop.call_soon_threadsafe(_enqueue_wake, queue)
                    except RuntimeError:
                        # Loop closed; subscriber already gone — skip.
                        pass
            time.sleep(self._POLL_INTERVAL_S)

    def stop(self) -> None:
        self._stop.set()
        t = self._thread
        if t is not None:
            t.join(timeout=1.0)


def _enqueue_wake(queue: asyncio.Queue) -> None:
    try:
        queue.put_nowait(None)
    except asyncio.QueueFull:
        pass  # coalesce: one pending wake is enough.


# ---------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------


class Database:
    """Connection owner. Holds one writer slot + a bounded reader pool +
    a lazy WAL watcher. Users drive it via `transaction()` (writes) or
    `query()` (reads)."""

    def __init__(self, path: str, max_readers: int = 8):
        self._path = path
        writer_conn = _open_conn(path)
        self._writer = _Writer(writer_conn)
        self._readers = _Readers(path, max_readers)
        self._wal_path = f"{path}-wal"
        self._watcher_lock = threading.Lock()
        self._watcher: Optional[_SharedWalWatcher] = None
        self._closed = False

    def transaction(self) -> "Transaction":
        return Transaction(self._writer)

    def wal_events(self) -> "WalEvents":
        with self._watcher_lock:
            if self._watcher is None:
                self._watcher = _SharedWalWatcher(self._wal_path)
            watcher = self._watcher
        return WalEvents(self._wal_path, watcher)

    def query(self, sql: str, params: Optional[list] = None) -> list[dict]:
        conn = self._readers.acquire()
        try:
            cur = _run_execute(conn, sql, params)
            return _rows_to_dicts(cur)
        finally:
            self._readers.release(conn)

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        with self._watcher_lock:
            if self._watcher is not None:
                self._watcher.stop()
                self._watcher = None
        self._readers.close()
        self._writer.close()

    def __del__(self) -> None:
        try:
            self.close()
        except Exception:
            pass


# ---------------------------------------------------------------------
# Transaction
# ---------------------------------------------------------------------


class Transaction:
    """Writer-slot-holding context manager. `BEGIN IMMEDIATE` on enter,
    `COMMIT` on clean exit, `ROLLBACK` on exception. The slot is released
    on exit so other writers can proceed."""

    __slots__ = ("_writer", "_conn", "_started", "_released")

    def __init__(self, writer: _Writer):
        self._writer = writer
        self._conn: Optional[sqlite3.Connection] = None
        self._started = False
        self._released = True

    def __enter__(self) -> "Transaction":
        # Fast path: uncontended acquire.
        conn = self._writer.try_acquire() or self._writer.acquire()
        try:
            conn.execute("BEGIN IMMEDIATE")
        except sqlite3.Error as e:
            self._writer.release(conn)
            raise RuntimeError(str(e)) from e
        self._conn = conn
        self._started = True
        self._released = False
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        if self._released or self._conn is None:
            return False
        conn = self._conn
        self._conn = None
        try:
            if self._started:
                if exc_type is not None:
                    try:
                        conn.execute("ROLLBACK")
                    except sqlite3.Error:
                        pass
                else:
                    try:
                        conn.execute("COMMIT")
                    except sqlite3.Error as e:
                        try:
                            conn.execute("ROLLBACK")
                        except sqlite3.Error:
                            pass
                        self._released = True
                        self._started = False
                        self._writer.release(conn)
                        raise RuntimeError(str(e)) from e
            self._started = False
        finally:
            if not self._released:
                self._released = True
                self._writer.release(conn)
        return False

    def __del__(self) -> None:
        # Mirrors PyO3 Drop: if the tx was never exited cleanly, rollback
        # and return the slot so we don't leak the writer lock.
        if self._released or self._conn is None:
            return
        conn = self._conn
        self._conn = None
        try:
            if self._started:
                try:
                    conn.execute("ROLLBACK")
                except sqlite3.Error:
                    pass
        finally:
            self._released = True
            try:
                self._writer.release(conn)
            except Exception:
                pass

    def _require_conn(self) -> sqlite3.Connection:
        if self._conn is None:
            raise RuntimeError("Transaction not started")
        return self._conn

    def execute(self, sql: str, params: Optional[list] = None) -> None:
        _run_execute(self._require_conn(), sql, params)

    def query(self, sql: str, params: Optional[list] = None) -> list[dict]:
        cur = _run_execute(self._require_conn(), sql, params)
        return _rows_to_dicts(cur)

    def notify(self, channel: str, payload: Any) -> int:
        """Insert a cross-process notification under the open tx. Payload
        is always `json.dumps`-encoded to match Queue.enqueue / Stream.publish
        and the other language bindings — receivers can rely on a single
        wire format."""
        payload_str = json.dumps(payload)
        row = _run_execute(
            self._require_conn(), "SELECT notify(?, ?)", [channel, payload_str]
        ).fetchone()
        return int(row[0])

    def bootstrap_honker_schema(self) -> None:
        """Idempotent install of the canonical queue/lock/stream tables
        via the extension's `honker_bootstrap()` function. DDL lives in
        `honker-core` so bindings can't drift on column counts."""
        _run_execute(self._require_conn(), "SELECT honker_bootstrap()", None)


# ---------------------------------------------------------------------
# WalEvents — async iterator of wake pings
# ---------------------------------------------------------------------


class WalEvents:
    """Async iterator that yields None every time this database's WAL
    file changes — i.e. every time any process committed. Subscribers
    share one stat-poll thread via the owning `Database`'s watcher."""

    def __init__(self, wal_path: str, watcher: _SharedWalWatcher):
        self._wal_path = wal_path
        self._watcher = watcher
        self._sid: Optional[int] = None
        self._queue: Optional[asyncio.Queue] = None

    @property
    def path(self) -> str:
        return self._wal_path

    def _ensure_subscribed(self) -> asyncio.Queue:
        if self._queue is not None:
            return self._queue
        loop = asyncio.get_running_loop()
        queue: asyncio.Queue = asyncio.Queue(maxsize=1)
        sid = self._watcher.subscribe(loop, queue)
        self._queue = queue
        self._sid = sid
        return queue

    def __aiter__(self) -> "WalEvents":
        self._ensure_subscribed()
        return self

    async def __anext__(self) -> None:
        queue = self._ensure_subscribed()
        return await queue.get()

    def close(self) -> None:
        if self._sid is not None:
            self._watcher.unsubscribe(self._sid)
            self._sid = None

    def __del__(self) -> None:
        try:
            self.close()
        except Exception:
            pass


# ---------------------------------------------------------------------
# Module-level functions
# ---------------------------------------------------------------------


def open(path: str, max_readers: int = 8) -> Database:
    return Database(path, max_readers=max_readers)


_cron_conn: Optional[sqlite3.Connection] = None
_cron_conn_lock = threading.Lock()


def cron_next_after(expr: str, from_unix: int) -> int:
    """Compute the next unix timestamp strictly after `from_unix` that
    matches `expr`. Delegates to `honker_cron_next_after` in the
    extension so every language binding shares one parser."""
    global _cron_conn
    with _cron_conn_lock:
        if _cron_conn is None:
            c = sqlite3.connect(":memory:", check_same_thread=False)
            c.enable_load_extension(True)
            c.load_extension(_find_extension())
            c.enable_load_extension(False)
            _cron_conn = c
        try:
            row = _cron_conn.execute(
                "SELECT honker_cron_next_after(?, ?)", (expr, int(from_unix))
            ).fetchone()
        except sqlite3.Error as e:
            # Extension raises "user function error" which sqlite3 surfaces
            # as OperationalError. Remap to ValueError so the scheduler's
            # validation path sees the right exception type.
            raise ValueError(str(e)) from e
        return int(row[0])
