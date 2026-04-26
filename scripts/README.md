# scripts

Diagnostic and proof scripts for honker's WAL change-detection design.
The full story lives in issue #5: https://github.com/russellromney/honker/issues/5

## `proof_data_version.py`

Shows that `PRAGMA data_version` detects commits from any connection
(the current honker mechanism). Two ctypes-free Python `sqlite3`
connections, A and B; A polls PRAGMA, B writes; PRAGMA on A
increments. Runs against Python's bundled `sqlite3` module.

```bash
python3 scripts/proof_data_version.py
```

## `proof_fcntl_vs_pragma.py`

**Kept as a record of an incorrect initial conclusion** — see header
comment in the file. This script tried to prove `SQLITE_FCNTL_DATA_VERSION`
is per-pager and that `PRAGMA data_version` is global. The numerical
output disagrees with that conclusion, and the print code branches
into "Unexpected result". The actual reason is that interleaved
PRAGMA calls in the script silently start read transactions that
refresh FCNTL's cached value, making FCNTL appear to work. Strip the
PRAGMA calls and FCNTL stops detecting. The corrected reasoning lives
in `bench/wal_index_methods/README.md`.

```bash
python3 scripts/proof_fcntl_vs_pragma.py
```

## `test_sqlite_versions.py`

Diagnostic that runs the FCNTL-vs-PRAGMA probe against every
`libsqlite3` it can find on the machine (system, Homebrew, Python
bundled). Useful when chasing version-specific behavior.

```bash
python3 scripts/test_sqlite_versions.py
```

## See also

- `bench/wal_index_methods/` — the Rust bench that actually controls
  for the FCNTL-vs-PRAGMA confusion and adds the mmap-of-`-shm` path.
- Issue #5 comments — full archaeology of the wrong-then-right path
  through this question.
