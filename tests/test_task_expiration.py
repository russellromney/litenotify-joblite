"""Tests for task expiration.

Jobs enqueued with `expires=N` become unclaimable N seconds after
enqueue. The claim path filters expired rows; `queue.sweep_expired()`
moves them into `_joblite_dead`.
"""

import time

import joblite


def test_expired_job_not_claimable(db_path):
    db = joblite.open(db_path)
    q = db.queue("exp-claim")

    # Expire in the past (negative delta — expires_at already elapsed).
    q.enqueue({"x": 1}, expires=-1)

    job = q.claim_one("worker-1")
    assert job is None, "expired job should not be claimable"


def test_non_expired_job_claimable(db_path):
    db = joblite.open(db_path)
    q = db.queue("exp-ok")

    q.enqueue({"x": 1}, expires=60)

    job = q.claim_one("worker-1")
    assert job is not None
    assert job.payload == {"x": 1}


def test_no_expires_means_never_expires(db_path):
    db = joblite.open(db_path)
    q = db.queue("exp-none")

    q.enqueue({"x": 1})  # no expires
    rows = db.query(
        "SELECT expires_at FROM _joblite_live WHERE queue='exp-none'"
    )
    assert rows[0]["expires_at"] is None

    job = q.claim_one("worker-1")
    assert job is not None


def test_sweep_expired_moves_to_dead(db_path):
    db = joblite.open(db_path)
    q = db.queue("exp-sweep")

    # Mix: 2 expired, 1 live, 1 already-processing (not touched by sweep).
    q.enqueue({"i": 1}, expires=-1)
    q.enqueue({"i": 2}, expires=-1)
    q.enqueue({"i": 3}, expires=3600)
    q.enqueue({"i": 4})  # no expires

    moved = q.sweep_expired()
    assert moved == 2

    live = db.query(
        "SELECT COUNT(*) AS c FROM _joblite_live WHERE queue='exp-sweep'"
    )[0]["c"]
    assert live == 2  # {i:3} and {i:4} remain

    dead = db.query(
        "SELECT COUNT(*) AS c, last_error "
        "FROM _joblite_dead WHERE queue='exp-sweep' GROUP BY last_error"
    )
    assert dead[0]["c"] == 2
    assert dead[0]["last_error"] == "expired"


def test_sweep_expired_is_idempotent(db_path):
    db = joblite.open(db_path)
    q = db.queue("exp-idem")

    q.enqueue({"i": 1}, expires=-1)
    assert q.sweep_expired() == 1
    # Second sweep: nothing new to move.
    assert q.sweep_expired() == 0


def test_enqueue_with_tx_sets_expires(db_path):
    db = joblite.open(db_path)
    q = db.queue("exp-tx")

    before = int(time.time())
    with db.transaction() as tx:
        q.enqueue({"i": 1}, tx=tx, expires=120)
    rows = db.query("SELECT expires_at FROM _joblite_live WHERE queue='exp-tx'")
    exp = rows[0]["expires_at"]
    assert before + 118 <= exp <= before + 122


def test_expired_job_ignores_claim_but_sweep_cleans_up(db_path):
    """Even without sweep, the claim path doesn't return expired jobs,
    so they don't get worked on. Sweep just reclaims table space.
    """
    db = joblite.open(db_path)
    q = db.queue("exp-workflow")

    q.enqueue({"i": 1}, expires=-1)

    # Row exists in _joblite_live but claim finds nothing.
    assert q.claim_one("w1") is None
    rows = db.query(
        "SELECT COUNT(*) AS c FROM _joblite_live WHERE queue='exp-workflow'"
    )
    assert rows[0]["c"] == 1

    # Sweep moves it.
    assert q.sweep_expired() == 1
    rows = db.query(
        "SELECT COUNT(*) AS c FROM _joblite_live WHERE queue='exp-workflow'"
    )
    assert rows[0]["c"] == 0
