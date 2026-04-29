#!/usr/bin/env python3
"""
Investigates when SQLITE_FCNTL_DATA_VERSION misses cross-connection commits.
Tests multiple SQLite paths: system lib, Homebrew, bundled with Python, etc.
"""

import ctypes
import ctypes.util
import os
import subprocess
import sys
import tempfile


def find_sqlite_libs():
    """Find all sqlite3 libraries available on this system."""
    candidates = []

    # System library
    system = ctypes.util.find_library("sqlite3")
    if system:
        candidates.append(("system", system))

    # Common macOS locations
    for path in [
        "/opt/homebrew/opt/sqlite/lib/libsqlite3.dylib",
        "/usr/local/opt/sqlite/lib/libsqlite3.dylib",
        "/usr/lib/libsqlite3.dylib",
    ]:
        if os.path.exists(path):
            label = path.split("/")[4] if "homebrew" in path else "system-alt"
            if not any(c[1] == path for c in candidates):
                candidates.append((label, path))

    # Python's bundled sqlite3 (via _sqlite3 module path)
    try:
        import sqlite3
        import _sqlite3
        mod_path = _sqlite3.__file__
        # The _sqlite3 module links against some sqlite3. We can check which one
        # by using `otool -L` on macOS or `ldd` on Linux
        if sys.platform == "darwin":
            try:
                out = subprocess.check_output(["otool", "-L", mod_path], text=True)
                for line in out.splitlines():
                    if "sqlite3" in line.lower():
                        lib = line.split()[0]
                        if os.path.exists(lib) and not any(c[1] == lib for c in candidates):
                            candidates.append(("python-bundled", lib))
                        break
            except FileNotFoundError:
                pass
    except Exception:
        pass

    return candidates


def test_lib(label: str, lib_path: str) -> dict:
    """Test one sqlite3 library. Returns results dict."""
    lib = ctypes.CDLL(lib_path)

    # Bindings
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
    lib.sqlite3_libversion.argtypes = []
    lib.sqlite3_libversion.restype = ctypes.c_char_p

    SQLITE_OK = 0
    SQLITE_ROW = 100
    SQLITE_FCNTL_DATA_VERSION = 35

    def open_db(path: str):
        handle = ctypes.c_void_p()
        rc = lib.sqlite3_open(path.encode(), ctypes.byref(handle))
        assert rc == SQLITE_OK, f"open failed: {rc}"
        return handle

    def exec_sql(db, sql: str):
        rc = lib.sqlite3_exec(db, sql.encode(), None, None, None)
        assert rc == SQLITE_OK, f"exec({sql!r}) failed: {rc}"

    def pragma_data_version(db):
        stmt = ctypes.c_void_p()
        rc = lib.sqlite3_prepare_v2(db, b"PRAGMA data_version", -1, ctypes.byref(stmt), None)
        assert rc == SQLITE_OK
        rc = lib.sqlite3_step(stmt)
        assert rc == SQLITE_ROW
        version = lib.sqlite3_column_int(stmt, 0)
        lib.sqlite3_finalize(stmt)
        return version

    def fcntl_data_version(db):
        version = ctypes.c_uint32(0)
        rc = lib.sqlite3_file_control(db, b"main", SQLITE_FCNTL_DATA_VERSION, ctypes.byref(version))
        assert rc == SQLITE_OK, f"file_control failed: {rc}"
        return version.value

    with tempfile.TemporaryDirectory() as tmp:
        db = os.path.join(tmp, "test.db")
        conn_a = open_db(db)
        conn_b = open_db(db)
        exec_sql(conn_a, "PRAGMA journal_mode = WAL")

        version_str = lib.sqlite3_libversion().decode()

        # Baseline
        fcntl_before = fcntl_data_version(conn_a)
        pragma_before = pragma_data_version(conn_a)

        # B writes
        exec_sql(conn_b, "CREATE TABLE t(x INTEGER)")
        exec_sql(conn_b, "INSERT INTO t VALUES (42)")

        # Read from A again
        fcntl_after = fcntl_data_version(conn_a)
        pragma_after = pragma_data_version(conn_a)

        lib.sqlite3_close(conn_a)
        lib.sqlite3_close(conn_b)

        return {
            "label": label,
            "path": lib_path,
            "version": version_str,
            "fcntl_before": fcntl_before,
            "pragma_before": pragma_before,
            "fcntl_after": fcntl_after,
            "pragma_after": pragma_after,
            "fcntl_delta": fcntl_after - fcntl_before,
            "pragma_delta": pragma_after - pragma_before,
            "fcntl_detected": (fcntl_after - fcntl_before) > 0,
            "pragma_detected": (pragma_after - pragma_before) > 0,
        }


def main():
    libs = find_sqlite_libs()
    if not libs:
        print("No sqlite3 libraries found on this system.")
        sys.exit(1)

    print(f"Testing {len(libs)} SQLite library(s)...\n")

    results = []
    for label, path in libs:
        try:
            result = test_lib(label, path)
            results.append(result)
        except Exception as e:
            print(f"  {label:25s} {path}")
            print(f"    ERROR: {e}\n")

    print("=" * 70)
    print(f"{'Source':<25s} {'Version':<10s} {'FCNTL':<10s} {'PRAGMA':<10s}")
    print("=" * 70)

    for r in results:
        fcntl_status = "DETECTED" if r["fcntl_detected"] else "MISSED"
        pragma_status = "DETECTED" if r["pragma_detected"] else "MISSED"
        print(f"{r['label']:<25s} {r['version']:<10s} {fcntl_status:<10s} {pragma_status:<10s}")
        print(f"  Path: {r['path']}")
        print(f"  FCNTL:  {r['fcntl_before']} → {r['fcntl_after']} (Δ{r['fcntl_delta']})")
        print(f"  PRAGMA: {r['pragma_before']} → {r['pragma_after']} (Δ{r['pragma_delta']})")
        print()


if __name__ == "__main__":
    main()
