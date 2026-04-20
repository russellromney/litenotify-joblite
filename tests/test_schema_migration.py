"""Schema migration tests.

Users upgrading from a pre-refactor `joblite` release have a `.db`
file with old schemas on disk. Opening it with current code must
not crash, silently corrupt data, or leave the user stuck. These
tests build legacy schemas directly via sqlite3, then open the
file with joblite and assert the upgrade path.

What we're defending against:
  * old indexes / tables lingering and confusing the query planner,
  * a fresh `enqueue → claim → ack` failing on an upgraded DB,
  * a new-schema column reference hitting an old-schema table.
"""

import sqlite3

import joblite


def test_legacy_pending_processing_tables_dropped_on_open(db_path):
    """Pre-v0.1 layout had separate `_joblite_pending` and
    `_joblite_processing` tables plus their claim/reclaim indexes.
    Current code consolidates into `_joblite_live` and
    `Queue._init_schema` DROPs the old objects. Verify the drop
    runs cleanly on an existing legacy DB and that the full
    enqueue/claim/ack path works afterwards.
    """
    # Build a legacy DB by hand.
    raw = sqlite3.connect(db_path)
    raw.executescript(
        """
        PRAGMA journal_mode=WAL;
        CREATE TABLE _joblite_pending (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          queue TEXT NOT NULL,
          payload TEXT NOT NULL,
          priority INTEGER NOT NULL DEFAULT 0,
          run_at INTEGER NOT NULL DEFAULT (unixepoch()),
          max_attempts INTEGER NOT NULL DEFAULT 3,
          attempts INTEGER NOT NULL DEFAULT 0,
          created_at INTEGER NOT NULL DEFAULT (unixepoch())
        );
        CREATE INDEX _joblite_pending_claim
          ON _joblite_pending(queue, priority DESC, run_at, id);
        CREATE TABLE _joblite_processing (
          id INTEGER PRIMARY KEY,
          queue TEXT NOT NULL,
          payload TEXT NOT NULL,
          worker_id TEXT NOT NULL,
          claim_expires_at INTEGER NOT NULL,
          attempts INTEGER NOT NULL DEFAULT 0,
          max_attempts INTEGER NOT NULL DEFAULT 3,
          priority INTEGER NOT NULL DEFAULT 0,
          run_at INTEGER NOT NULL DEFAULT 0,
          created_at INTEGER NOT NULL DEFAULT 0
        );
        CREATE INDEX _joblite_processing_reclaim
          ON _joblite_processing(claim_expires_at);
        -- Seed one row into the old pending table; the migration
        -- drops the table, so its contents are lost — users running
        -- the migration are expected to drain the queue first. This
        -- test just verifies the drop happens, not that rows move.
        INSERT INTO _joblite_pending (queue, payload)
          VALUES ('old-queue', '{"stale": true}');
        """
    )
    raw.commit()
    raw.close()

    # Open with current joblite — triggers Queue._init_schema.
    db = joblite.open(db_path)
    db.queue("new-queue")

    # Legacy objects dropped.
    check = sqlite3.connect(db_path)
    leftover = check.execute(
        "SELECT name FROM sqlite_master "
        "WHERE type IN ('table', 'index') "
        "  AND name IN ('_joblite_pending', '_joblite_processing', "
        "              '_joblite_pending_claim', "
        "              '_joblite_processing_reclaim')"
    ).fetchall()
    assert leftover == [], f"legacy objects still present: {leftover}"

    # Current schema present.
    live_cols = [
        r[1]
        for r in check.execute("PRAGMA table_info(_joblite_live)").fetchall()
    ]
    assert "queue" in live_cols
    assert "state" in live_cols
    assert "expires_at" in live_cols
    check.close()

    # Full round-trip on the upgraded DB.
    q = db.queue("new-queue")
    q.enqueue({"ok": True})
    job = q.claim_one("w1")
    assert job is not None
    assert job.payload == {"ok": True}
    assert job.ack() is True


def test_legacy_scheduler_state_table_leftover_is_harmless(db_path):
    """Commit 5/6 replaced `_joblite_scheduler_state(name, last_fire_at)`
    with `_joblite_scheduler_tasks(name, queue, cron_expr, payload,
    priority, expires_s, next_fire_at)`. The old table isn't
    automatically dropped — it's harmless dead weight. Verify that
    its presence doesn't break the new scheduler path.
    """
    raw = sqlite3.connect(db_path)
    raw.executescript(
        """
        PRAGMA journal_mode=WAL;
        CREATE TABLE _joblite_scheduler_state (
          name TEXT PRIMARY KEY,
          last_fire_at INTEGER NOT NULL
        );
        INSERT INTO _joblite_scheduler_state VALUES ('nightly', 1700000000);
        """
    )
    raw.commit()
    raw.close()

    # Opening + registering a scheduler task must succeed.
    db = joblite.open(db_path)
    from joblite import Scheduler, crontab

    sched = Scheduler(db)
    sched.add(name="nightly", queue="backups", schedule=crontab("0 3 * * *"))

    rows = db.query(
        "SELECT queue FROM _joblite_scheduler_tasks WHERE name='nightly'"
    )
    assert rows[0]["queue"] == "backups"

    # Old table still there but untouched — that's fine.
    check = sqlite3.connect(db_path)
    old = check.execute(
        "SELECT COUNT(*) FROM _joblite_scheduler_state"
    ).fetchone()
    assert old[0] == 1  # our seeded row is preserved
    check.close()


def test_fresh_db_has_all_current_tables(db_path):
    """Sanity: a fresh (non-legacy) DB boots with every table the
    current schema expects. Catches regressions where a table is
    added to BOOTSTRAP_JOBLITE_SQL but not to the bootstrap call
    path."""
    db = joblite.open(db_path)
    db.queue("q")  # triggers bootstrap if not already done

    check = sqlite3.connect(db_path)
    names = {
        r[0]
        for r in check.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()
    }
    check.close()

    expected = {
        "_litenotify_notifications",
        "_joblite_live",
        "_joblite_dead",
        "_joblite_locks",
        "_joblite_rate_limits",
        "_joblite_scheduler_tasks",
        "_joblite_results",
        "_joblite_stream",
        "_joblite_stream_consumers",
    }
    missing = expected - names
    assert not missing, f"missing tables on fresh DB: {missing}"
