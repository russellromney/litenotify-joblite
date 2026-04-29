#!/usr/bin/env python3
"""
proof_fcntl_vs_pragma.py — KEPT AS A RECORD OF AN INCORRECT INITIAL CONCLUSION.

This script was written to prove that `SQLITE_FCNTL_DATA_VERSION` is
per-pager and that `PRAGMA data_version` is global. The print
statements still say so.

That is wrong, in two different ways:

  1. The "expected outcome" comment below says FCNTL won't see the
     other connection's writes. Run the script and you will see FCNTL
     IS detected — exactly the same delta as PRAGMA. The conclusion
     branch in `main()` then prints "Unexpected result", which is the
     real signal.

  2. The actual reason FCNTL appears to detect here is that the
     interleaved PRAGMA calls in this very script implicitly start
     read transactions on connection A, which refresh the cached
     `pPager->iDataVersion` value that FCNTL reads. Strip the PRAGMA
     calls and FCNTL stops detecting. (See bench/wal_index_methods/
     for a Rust reproduction that controls for this.)

What is actually true (verified in SQLite 3.51.3 source):

  - FCNTL_DATA_VERSION returns `pPager->iDataVersion` — a cached
    integer on the pager (sqlite3.c:60702, :189063).
  - That field is incremented in exactly two places: the writer's own
    commit path (sqlite3.c:65628), and `pager_reset` called from
    `pagerBeginReadTransaction` *after* `walTryBeginRead` notices the
    wal-index header has moved (sqlite3.c:60694).
  - For an idle watcher that does NO other SQL between FCNTL polls,
    the cached value never refreshes. The docs are correct about what
    *changes* the value; they don't say when this connection sees it.

What replaced it: directly mmap the `<db>-shm` file and read the
wal-index `iChange` counter from shared memory — same number SQLite
reads internally, ~0.6 ns per poll, no read transaction needed. See
bench/wal_index_methods/ for the full comparison.

This script is left untouched (apart from this header) so the
mistaken reasoning is auditable. Do not "fix" the print statements —
they are the artifact.

Run: python3 scripts/proof_fcntl_vs_pragma.py
"""

import ctypes
import ctypes.util
import os
import tempfile

sqlite3_path = ctypes.util.find_library("sqlite3")
if not sqlite3_path:
    raise RuntimeError("libsqlite3 not found on this system")
lib = ctypes.CDLL(sqlite3_path)

# --- sqlite3 C API bindings ---
lib.sqlite3_open.argtypes = [ctypes.c_char_p, ctypes.POINTER(ctypes.c_void_p)]
lib.sqlite3_open.restype = ctypes.c_int

lib.sqlite3_close.argtypes = [ctypes.c_void_p]
lib.sqlite3_close.restype = ctypes.c_int

lib.sqlite3_exec.argtypes = [
    ctypes.c_void_p, ctypes.c_char_p, ctypes.c_void_p,
    ctypes.c_void_p, ctypes.POINTER(ctypes.c_char_p),
]
lib.sqlite3_exec.restype = ctypes.c_int

lib.sqlite3_prepare_v2.argtypes = [
    ctypes.c_void_p, ctypes.c_char_p, ctypes.c_int,
    ctypes.POINTER(ctypes.c_void_p), ctypes.POINTER(ctypes.c_char_p),
]
lib.sqlite3_prepare_v2.restype = ctypes.c_int

lib.sqlite3_step.argtypes = [ctypes.c_void_p]
lib.sqlite3_step.restype = ctypes.c_int

lib.sqlite3_column_int.argtypes = [ctypes.c_void_p, ctypes.c_int]
lib.sqlite3_column_int.restype = ctypes.c_int

lib.sqlite3_finalize.argtypes = [ctypes.c_void_p]
lib.sqlite3_finalize.restype = ctypes.c_int

lib.sqlite3_file_control.argtypes = [
    ctypes.c_void_p, ctypes.c_char_p, ctypes.c_int, ctypes.c_void_p,
]
lib.sqlite3_file_control.restype = ctypes.c_int

SQLITE_OK = 0
SQLITE_ROW = 100
SQLITE_FCNTL_DATA_VERSION = 35


def open_db(path: str) -> ctypes.c_void_p:
    handle = ctypes.c_void_p()
    rc = lib.sqlite3_open(path.encode(), ctypes.byref(handle))
    assert rc == SQLITE_OK, f"sqlite3_open failed: {rc}"
    return handle


def exec_sql(db: ctypes.c_void_p, sql: str) -> None:
    rc = lib.sqlite3_exec(db, sql.encode(), None, None, None)
    assert rc == SQLITE_OK, f"sqlite3_exec({sql!r}) failed: {rc}"


def pragma_data_version(db: ctypes.c_void_p) -> int:
    stmt = ctypes.c_void_p()
    rc = lib.sqlite3_prepare_v2(db, b"PRAGMA data_version", -1, ctypes.byref(stmt), None)
    assert rc == SQLITE_OK, f"prepare failed: {rc}"
    rc = lib.sqlite3_step(stmt)
    assert rc == SQLITE_ROW, f"step failed: {rc}"
    version = lib.sqlite3_column_int(stmt, 0)
    lib.sqlite3_finalize(stmt)
    return version


def fcntl_data_version(db: ctypes.c_void_p) -> int:
    version = ctypes.c_uint32(0)
    rc = lib.sqlite3_file_control(db, b"main", SQLITE_FCNTL_DATA_VERSION, ctypes.byref(version))
    assert rc == SQLITE_OK, f"file_control failed: {rc}"
    return version.value


def main() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        db = os.path.join(tmp, "test.db")

        conn_a = open_db(db)
        conn_b = open_db(db)
        exec_sql(conn_a, "PRAGMA journal_mode = WAL")

        print("=" * 62)
        print("Proof: FCNTL is per-pager; PRAGMA data_version is global")
        print("=" * 62)
        print(f"SQLite library: {sqlite3_path}")
        print(f"Database:       {db}")
        print()

        fcntl_a_before = fcntl_data_version(conn_a)
        pragma_a_before = pragma_data_version(conn_a)
        print("Connection A — before B writes:")
        print(f"  FCNTL data_version:  {fcntl_a_before}")
        print(f"  PRAGMA data_version: {pragma_a_before}")
        print()

        exec_sql(conn_b, "CREATE TABLE t(x INTEGER)")
        exec_sql(conn_b, "INSERT INTO t VALUES (42)")
        print("Connection B committed: CREATE TABLE + INSERT")
        print()

        fcntl_a_after = fcntl_data_version(conn_a)
        pragma_a_after = pragma_data_version(conn_a)
        print("Connection A — after B writes:")
        print(f"  FCNTL data_version:  {fcntl_a_after}")
        print(f"  PRAGMA data_version: {pragma_a_after}")
        print()

        fcntl_delta = int(fcntl_a_after) - int(fcntl_a_before)
        pragma_delta = int(pragma_a_after) - int(pragma_a_before)

        print("-" * 62)
        print("RESULTS (from Connection A's perspective):")
        print(f"  FCNTL delta:  {fcntl_delta}  ({'DETECTED ✓' if fcntl_delta > 0 else 'MISSED ✗'})")
        print(f"  PRAGMA delta: {pragma_delta}  ({'DETECTED ✓' if pragma_delta > 0 else 'MISSED ✗'})")
        print()
        print("CONCLUSION:")
        if fcntl_delta == 0 and pragma_delta > 0:
            print("  FCNTL data_version  → per-pager (same connection only)")
            print("  PRAGMA data_version → global  (any connection)")
            print("  → Honker correctly uses PRAGMA for cross-process wake")
        else:
            print("  Unexpected result — see deltas above")

        lib.sqlite3_close(conn_a)
        lib.sqlite3_close(conn_b)


if __name__ == "__main__":
    main()
