"""Tests for db.lock() / joblite.LockHeld.

Named advisory locks backed by the `_joblite_locks` table. Main use
case: cron-like tasks that shouldn't overlap with themselves.
"""

import time

import pytest

import joblite


def test_lock_acquire_and_release(db_path):
    db = joblite.open(db_path)

    with db.lock("backup", ttl=60) as lk:
        assert lk.acquired
        rows = db.query("SELECT name, owner FROM _joblite_locks")
        assert len(rows) == 1
        assert rows[0]["name"] == "backup"

    # Released on exit.
    rows = db.query("SELECT COUNT(*) AS c FROM _joblite_locks")
    assert rows[0]["c"] == 0


def test_lock_already_held_raises(db_path):
    db = joblite.open(db_path)

    with db.lock("singleton", ttl=60):
        with pytest.raises(joblite.LockHeld):
            with db.lock("singleton", ttl=60):
                pytest.fail("should not reach here")


def test_lock_different_names_independent(db_path):
    db = joblite.open(db_path)

    with db.lock("a", ttl=60):
        with db.lock("b", ttl=60):
            rows = db.query("SELECT COUNT(*) AS c FROM _joblite_locks")
            assert rows[0]["c"] == 2


def test_lock_released_after_exception(db_path):
    db = joblite.open(db_path)

    with pytest.raises(RuntimeError):
        with db.lock("crashy", ttl=60):
            raise RuntimeError("kaboom")

    # Lock row should be gone even though the with-body raised.
    rows = db.query("SELECT COUNT(*) AS c FROM _joblite_locks")
    assert rows[0]["c"] == 0


def test_lock_expired_is_reclaimable(db_path):
    """A lock whose TTL elapsed can be reacquired without explicit
    release — the prune-on-acquire path cleans it up. Needed so a
    crashed holder doesn't block others forever."""
    db = joblite.open(db_path)

    # Manually insert an expired lock row (simulating a holder that
    # crashed an hour ago without releasing).
    with db.transaction() as tx:
        tx.execute(
            "INSERT INTO _joblite_locks (name, owner, expires_at) "
            "VALUES ('stale', 'crashed-owner', unixepoch() - 3600)",
        )

    with db.lock("stale", ttl=60) as lk:
        assert lk.acquired
        rows = db.query("SELECT owner FROM _joblite_locks WHERE name='stale'")
        assert rows[0]["owner"] != "crashed-owner"


def test_lock_released_under_ttl_does_not_block_others(db_path):
    """After clean release (via __exit__), a new acquirer gets the
    lock immediately even though the original TTL hasn't elapsed.
    """
    db = joblite.open(db_path)

    with db.lock("quick", ttl=60):
        pass

    with db.lock("quick", ttl=60) as lk:
        assert lk.acquired


def test_lock_uuid_owners_are_distinct(db_path):
    """Default owner is a fresh uuid per call, so the same process
    grabbing the lock twice via nested context managers doesn't
    accidentally match owners."""
    db = joblite.open(db_path)

    with db.lock("same-name", ttl=60) as a:
        with pytest.raises(joblite.LockHeld):
            with db.lock("same-name", ttl=60) as b:
                # Would only reach here if owners matched.
                pytest.fail()
        # Explicit owners differ even if you look at them.
        # (Can't actually check b's owner because __init__ fails
        # to enter; a's owner is whatever uuid4 produced.)
        assert a.owner
