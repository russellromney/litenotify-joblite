"""Regression tests for the "subscribe-after-first-read" race.

Consumers that (a) snapshot some state and (b) wait on the update watcher
must subscribe to update_events BEFORE the snapshot. Otherwise a commit
landing in the window between snapshot and subscribe fires its update tick
to no subscriber, and the consumer parks on its paranoia timeout
(15s / idle_poll_s) before re-polling.

These tests cover the three iterator/consumer types:
  * Listener        (ephemeral pub/sub)
  * _WorkerQueueIter (queue.claim() iterator)
  * Queue.wait_result

Each test has a structural check (the subscription exists at
construction / before the first read) and a behavioral check (a commit
that races the initial snapshot is observed within the update-watcher
cadence, never the paranoia timeout).
"""

import asyncio
import threading
import time

import pytest

import honker


# ---------------------------------------------------------------------
# Structural invariants: subscribe happens before the first snapshot
# ---------------------------------------------------------------------


def test_listener_subscribes_at_construction(db_path):
    db = honker.open(db_path)
    lst = db.listen("orders")
    # Eager subscription: the UpdateEvents handle exists before any
    # __anext__ call. If someone ever makes this lazy, this test fires.
    assert lst._updates is not None
    assert hasattr(lst._updates, "path")


def test_worker_iter_subscribes_at_construction(db_path):
    db = honker.open(db_path)
    q = db.queue("work")
    it = q.claim("w1")
    # Eager subscription: _updates is already a UpdateEvents, not None. Regression
    # guard for the old "subscribe lazily inside __anext__" bug where a
    # commit landing between claim_batch() and update_events() was lost.
    assert it._updates is not None
    assert hasattr(it._updates, "path")


# ---------------------------------------------------------------------
# Behavioral: a commit racing the initial snapshot wakes the consumer
# within the update-watcher cadence, not the paranoia fallback
# ---------------------------------------------------------------------


async def test_listener_observes_publish_racing_construction(db_path):
    """A publish landing after Listener.__init__ but before the first
    __anext__ must be delivered within the update-watcher cadence, not wait
    for the 15s paranoia timeout.

    We construct the listener, then publish from a worker thread with
    a tiny delay (simulating the "between MAX(id) snapshot and first
    __anext__" race). The publish must be visible in <1s.
    """
    db = honker.open(db_path)
    lst = db.listen("orders")

    # Publish from a thread after a small jitter — exercises the window
    # between Listener() returning and the first __anext__ being awaited.
    def publisher():
        time.sleep(0.05)
        with db.transaction() as tx:
            tx.notify("orders", {"id": 42})

    t = threading.Thread(target=publisher)
    t.start()

    # Bound by 1s — if the subscribe ordering is wrong AND __anext__'s
    # SELECT-then-wait also somehow misses it, we'd park for 15s and
    # time out.
    n = await asyncio.wait_for(lst.__anext__(), timeout=1.0)
    t.join()
    assert n.channel == "orders"
    assert n.payload == {"id": 42}


async def test_worker_iter_observes_enqueue_racing_construction(db_path):
    """An enqueue landing right after q.claim() returns (but before
    __anext__ is awaited) must be delivered within the update cadence.

    Previously `_WorkerQueueIter` subscribed lazily on first
    claim_batch()==[], so an enqueue in this window fired a update tick
    to no subscriber; the worker would park for idle_poll_s (5s
    default) before re-polling. We set idle_poll_s=30 to make the
    regression visible — a failing test times out at 2s rather than
    hiding behind a 5s fallback.
    """
    db = honker.open(db_path)
    q = db.queue("work")
    it = q.claim("w1", idle_poll_s=30.0)

    def enqueuer():
        time.sleep(0.05)
        q.enqueue({"n": 1})

    t = threading.Thread(target=enqueuer)
    t.start()

    start = time.perf_counter()
    job = await asyncio.wait_for(it.__anext__(), timeout=2.0)
    elapsed = time.perf_counter() - start
    t.join()

    assert job.payload == {"n": 1}
    # Wake must come via the update watcher, not idle_poll_s (30s). If the
    # subscription raced, we'd have timed out at 2s already; the elapsed
    # check is a stronger assertion that we actually wake fast.
    assert elapsed < 1.0, (
        f"worker woke in {elapsed:.2f}s — looks like a fallback poll, "
        "not a update tick"
    )
    job.ack()


async def test_wait_result_observes_save_racing_the_call(db_path):
    """A save_result landing between `wait_result`'s initial check and
    its update_events subscribe must not be lost to the 15s paranoia
    timeout.

    We kick off a background thread that saves the result ~50ms after
    wait_result is entered — close to the window where the old code
    (check-then-subscribe) could lose the wake. The call must return
    within ~1s.
    """
    db = honker.open(db_path)
    q = db.queue("work")
    job_id = q.enqueue({"k": 1})

    def saver():
        time.sleep(0.05)
        q.save_result(job_id, {"ok": True})

    t = threading.Thread(target=saver)
    t.start()

    start = time.perf_counter()
    value = await asyncio.wait_for(q.wait_result(job_id), timeout=2.0)
    elapsed = time.perf_counter() - start
    t.join()

    assert value == {"ok": True}
    # If we'd raced the subscribe, the save's update tick would have
    # fired to no subscriber and we'd be sitting on the 15s paranoia
    # poll. <1s means the WAL wake got through.
    assert elapsed < 1.0, (
        f"wait_result resolved in {elapsed:.2f}s — smells like a "
        "paranoia-poll fallback"
    )


# ---------------------------------------------------------------------
# Stress variant: many constructions + immediate enqueues. If the race
# window existed, even one miss out of N would surface as a 30s tail.
# ---------------------------------------------------------------------


async def test_worker_iter_many_rapid_enqueues_no_fallback(db_path):
    """100 enqueues racing 100 iterator constructions. Drain must
    complete in well under idle_poll_s — if even one enqueue lost its
    wake to an unsubscribed listener, we'd see a 30s tail in the
    total wall time.
    """
    db = honker.open(db_path)
    q = db.queue("work")
    N = 100

    def enqueuer():
        for i in range(N):
            q.enqueue({"i": i})
            # Tiny jitter so enqueues spread across iterator awaits
            # instead of being buffered into one batch.
            time.sleep(0.001)

    it = q.claim("w1", idle_poll_s=30.0)
    t = threading.Thread(target=enqueuer)
    t.start()

    start = time.perf_counter()
    got = 0
    while got < N:
        job = await asyncio.wait_for(it.__anext__(), timeout=5.0)
        job.ack()
        got += 1
    elapsed = time.perf_counter() - start
    t.join()

    # Loose: N enqueues with 1ms jitter bounds the ideal at ~100ms.
    # If any wake is lost, the 30s idle_poll_s dominates. Threshold
    # well below 30s but well above the happy path.
    assert elapsed < 5.0, (
        f"drain took {elapsed:.2f}s for {N} jobs — a wake was likely "
        "lost and filled in by idle_poll_s"
    )
