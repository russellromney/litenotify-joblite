"""Tests for db.try_rate_limit() / db.sweep_rate_limits().

Fixed-window rate limiter. Good enough for "don't hammer this endpoint
past X per Y seconds"; at window boundaries the counter resets.
"""

import time

import pytest

import joblite


def test_rate_limit_allows_under_limit(db_path):
    db = joblite.open(db_path)

    # 5 calls in the same 60s window should all pass.
    for _ in range(5):
        assert db.try_rate_limit("api", limit=5, per=60) is True


def test_rate_limit_rejects_over_limit(db_path):
    db = joblite.open(db_path)

    # Fill the window.
    for _ in range(3):
        assert db.try_rate_limit("api", limit=3, per=60) is True
    # Next call in same window is rejected.
    assert db.try_rate_limit("api", limit=3, per=60) is False
    assert db.try_rate_limit("api", limit=3, per=60) is False


def test_rate_limit_rejected_call_not_counted(db_path):
    """When over limit, we return False without incrementing the
    counter. Otherwise a hot loop of rejected calls would inflate the
    count past the limit.
    """
    db = joblite.open(db_path)
    for _ in range(3):
        db.try_rate_limit("api", limit=3, per=60)
    # Hammer it with rejections.
    for _ in range(100):
        assert db.try_rate_limit("api", limit=3, per=60) is False

    rows = db.query("SELECT count FROM _joblite_rate_limits WHERE name='api'")
    assert rows[0]["count"] == 3


def test_rate_limit_different_names_independent(db_path):
    db = joblite.open(db_path)

    # Fill 'api' but not 'email'.
    for _ in range(2):
        db.try_rate_limit("api", limit=2, per=60)
    assert db.try_rate_limit("api", limit=2, per=60) is False

    # 'email' is untouched.
    assert db.try_rate_limit("email", limit=2, per=60) is True


def test_rate_limit_invalid_args_raises(db_path):
    db = joblite.open(db_path)

    with pytest.raises(ValueError):
        db.try_rate_limit("x", limit=0, per=60)
    with pytest.raises(ValueError):
        db.try_rate_limit("x", limit=5, per=0)
    with pytest.raises(ValueError):
        db.try_rate_limit("x", limit=-1, per=60)


def test_rate_limit_window_advances(db_path):
    """Using per=1 (1-second windows), we fill one window, wait until
    the next one, and verify we can fire again.

    Test is slow-ish (waits out a window) but doesn't depend on
    mocking time.
    """
    db = joblite.open(db_path)
    assert db.try_rate_limit("slow", limit=1, per=1) is True
    assert db.try_rate_limit("slow", limit=1, per=1) is False

    # Wait until the next 1-second window starts. Use a small extra
    # buffer since window_start is `floor(t/per)*per`.
    now = time.time()
    sleep_for = 1.0 - (now % 1.0) + 0.1
    time.sleep(sleep_for)

    assert db.try_rate_limit("slow", limit=1, per=1) is True


def test_sweep_rate_limits_removes_old_windows(db_path):
    db = joblite.open(db_path)

    # Manually insert a stale row far in the past.
    with db.transaction() as tx:
        tx.execute(
            "INSERT INTO _joblite_rate_limits (name, window_start, count) "
            "VALUES ('old', unixepoch() - 100000, 5)",
        )
    # And a fresh one from try_rate_limit.
    db.try_rate_limit("fresh", limit=10, per=60)

    # Sweep anything older than 1 hour.
    deleted = db.sweep_rate_limits(older_than_s=3600)
    assert deleted == 1

    remaining = db.query(
        "SELECT name FROM _joblite_rate_limits ORDER BY name"
    )
    assert [r["name"] for r in remaining] == ["fresh"]
