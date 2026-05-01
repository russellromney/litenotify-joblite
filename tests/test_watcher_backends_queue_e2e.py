"""Cross-process queue worker e2e proof for the experimental watcher backends.

Where `test_watcher_backends_e2e.py` exercises the notify/listen surface
across processes, this file does the same for the **queue worker**
surface — `queue.enqueue()` from one process, `queue.claim()` consumed
from another. Both go through `db.update_events()` under the hood, but
the queue path adds claim leases, ack semantics, and visibility
timeouts on top, so it deserves its own direct proof.

Three topologies × three backends × Python binding:

  * **1 × 1** — one writer process enqueues N, one worker process claims
    and acks all N.
  * **1 × N** — one writer enqueues N, M worker processes compete; every
    job processed exactly once with no double-claims.
  * **N × 1** — M writers each enqueue K, one worker drains all M*K.

The worker subprocesses use the wake-driven `async for job in
queue.claim(worker_id)` iterator — that's the path that exercises the
watcher backend. `idle_poll_s` defaults to 5 s; the workers exit after
**2 s** of `await anext(...)` timeout, so a backend that fails to
deliver wakes shows up as "worker processed 0 jobs", not as "test
hangs for 5 seconds and falls back to polling."

A polling-backend variant runs as the control: every backend must
match the polling baseline on this surface.
"""

import asyncio
import json
import os
import subprocess
import sys
import textwrap
from typing import Optional

import pytest

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PACKAGES_ROOT = os.path.join(REPO_ROOT, "packages")

BACKENDS = [None, "kernel", "shm"]


def _backend_arg(backend: Optional[str]) -> str:
    return "" if backend is None else backend


# ---------------------------------------------------------------------
# Worker subprocess: wake-driven claim loop. Exits after `idle_exit_s`
# seconds without a job (caught via asyncio.wait_for on the iterator).
# Exiting on idle (rather than after exactly N jobs) is required for
# the 1×N topology where each worker doesn't know its own share.
# ---------------------------------------------------------------------

_WORKER_SCRIPT = textwrap.dedent(
    r"""
    import asyncio, json, sys
    sys.path.insert(0, {packages!r})
    import honker

    async def main():
        db_path = {db_path!r}
        worker_id = {worker_id!r}
        backend = {backend!r} or None
        idle_exit_s = {idle_exit_s}

        db = honker.open(db_path, watcher_backend=backend)
        q = db.queue("shared")
        print("READY", flush=True)

        processed = []
        # Wake-driven iterator — uses update_events() under the hood,
        # which is the surface the watcher backends drive. idle_poll_s
        # is the paranoia fallback (5 s default); we exit on a tighter
        # 2 s asyncio.wait_for so a broken backend surfaces as
        # "processed 0 jobs," not as "fell back to idle_poll_s and
        # passed anyway."
        iterator = q.claim(worker_id).__aiter__()
        while True:
            try:
                job = await asyncio.wait_for(iterator.__anext__(), timeout=idle_exit_s)
            except asyncio.TimeoutError:
                break
            except StopAsyncIteration:
                break
            processed.append(job.payload["i"])
            job.ack()

        print("RESULT", json.dumps(processed), flush=True)

    asyncio.run(main())
    """
)


def _spawn_worker(db_path: str, worker_id: str, backend: Optional[str], idle_exit_s: float = 2.0):
    script = _WORKER_SCRIPT.format(
        packages=PACKAGES_ROOT,
        db_path=db_path,
        worker_id=worker_id,
        backend=_backend_arg(backend),
        idle_exit_s=idle_exit_s,
    )
    proc = subprocess.Popen(
        [sys.executable, "-c", script],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        bufsize=1,
    )
    # Wait for READY so the worker's claim() iterator has subscribed
    # before the writer publishes anything (avoids the same subscribe-
    # race the listener tests guard against).
    ready = proc.stdout.readline()
    if ready.strip() != "READY":
        err = proc.stderr.read() if proc.stderr else ""
        proc.kill()
        raise RuntimeError(f"worker {worker_id} did not READY: ready={ready!r} stderr={err!r}")
    return proc


def _wait_worker(proc, worker_id: str, timeout_s: float = 20.0) -> list:
    """Block until the worker prints RESULT or times out. Returns the
    list of processed `i` values."""
    try:
        proc.wait(timeout=timeout_s)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait()
        raise AssertionError(f"worker {worker_id} did not exit within {timeout_s}s")

    # Read the whole stdout; the RESULT line is always last.
    rest = proc.stdout.read() if proc.stdout else ""
    err = proc.stderr.read() if proc.stderr else ""
    result_line = ""
    for line in rest.splitlines():
        if line.startswith("RESULT "):
            result_line = line
    if not result_line:
        raise AssertionError(
            f"worker {worker_id} produced no RESULT line. stdout={rest!r} stderr={err!r}"
        )
    return json.loads(result_line[len("RESULT "):])


# ---------------------------------------------------------------------
# Writer subprocess: enqueues `n` jobs into the "shared" queue with
# payload {"i": offset+0..n}. Synchronous; exits when done.
# Writer doesn't need any watcher backend — it only enqueues.
# ---------------------------------------------------------------------

_WRITER_SCRIPT = textwrap.dedent(
    r"""
    import sys
    sys.path.insert(0, {packages!r})
    import honker
    db = honker.open({db_path!r})
    q = db.queue("shared")
    for i in range({offset}, {offset} + {n}):
        q.enqueue({{"i": i}})
    """
)


def _run_writer(db_path: str, n: int, offset: int = 0, timeout_s: float = 15.0):
    script = _WRITER_SCRIPT.format(
        packages=PACKAGES_ROOT, db_path=db_path, n=n, offset=offset
    )
    res = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True, text=True, timeout=timeout_s,
    )
    assert res.returncode == 0, (
        f"writer (offset={offset} n={n}) failed: stdout={res.stdout!r} stderr={res.stderr!r}"
    )


# ---------------------------------------------------------------------
# Topology 1×1
# ---------------------------------------------------------------------


@pytest.mark.parametrize("backend", BACKENDS)
def test_queue_1writer_1worker(tmp_path, backend):
    db_path = str(tmp_path / "q.db")

    # Bootstrap the db file (creates honker schema, WAL, the works) so
    # the worker's first claim() doesn't have to race the schema
    # bootstrap.
    import honker
    honker.open(db_path).queue("shared")  # forces bootstrap_honker_schema

    n = 25

    # Worker subscribes first; writer publishes after.
    worker = _spawn_worker(db_path, "w1", backend)
    try:
        _run_writer(db_path, n)
        processed = _wait_worker(worker, "w1")
    finally:
        if worker.poll() is None:
            worker.kill()
            worker.wait()

    assert sorted(processed) == list(range(n)), (
        f"backend={backend!r} 1x1: worker processed {sorted(processed)}, "
        f"expected {list(range(n))}"
    )


# ---------------------------------------------------------------------
# Topology 1×N: one writer, M workers, claim exclusivity must hold.
# ---------------------------------------------------------------------


@pytest.mark.parametrize("backend", BACKENDS)
def test_queue_1writer_many_workers_no_double_claim(tmp_path, backend):
    db_path = str(tmp_path / "q.db")

    import honker
    honker.open(db_path).queue("shared")

    n = 60
    num_workers = 3

    workers = [
        _spawn_worker(db_path, f"w{i}", backend) for i in range(num_workers)
    ]
    try:
        _run_writer(db_path, n)
        results = [_wait_worker(w, f"w{i}") for i, w in enumerate(workers)]
    finally:
        for w in workers:
            if w.poll() is None:
                w.kill()
                w.wait()

    combined = sorted(sum(results, []))
    assert combined == list(range(n)), (
        f"backend={backend!r} 1xN: combined {combined} != expected {list(range(n))} "
        f"(missing or duplicated jobs)"
    )

    # Pairwise disjoint = no two workers claimed the same job.
    for i in range(num_workers):
        for j in range(i + 1, num_workers):
            overlap = set(results[i]) & set(results[j])
            assert not overlap, (
                f"backend={backend!r} 1xN: workers w{i} and w{j} double-claimed "
                f"jobs {overlap}"
            )

    # Sanity: each worker did SOMETHING (otherwise we proved exclusivity
    # by accident — all jobs to one worker). Single-worker drain isn't
    # a test failure per se, but it would mean the wake distribution is
    # broken so we'd want to know.
    assert all(len(r) > 0 for r in results), (
        f"backend={backend!r} 1xN: per-worker counts {[len(r) for r in results]} — "
        "one or more workers got nothing; wake distribution may be broken"
    )


# ---------------------------------------------------------------------
# Topology N×1: M writers, one worker, drain must complete.
# ---------------------------------------------------------------------


@pytest.mark.parametrize("backend", BACKENDS)
def test_queue_many_writers_1worker_drains(tmp_path, backend):
    db_path = str(tmp_path / "q.db")

    import honker
    honker.open(db_path).queue("shared")

    num_writers = 3
    per_writer = 15
    total = num_writers * per_writer

    worker = _spawn_worker(db_path, "w1", backend, idle_exit_s=3.0)
    try:
        # Writers run in parallel via threads (each runs a subprocess).
        import concurrent.futures

        with concurrent.futures.ThreadPoolExecutor(max_workers=num_writers) as ex:
            futures = [
                ex.submit(_run_writer, db_path, per_writer, i * per_writer)
                for i in range(num_writers)
            ]
            for f in futures:
                f.result()

        processed = _wait_worker(worker, "w1", timeout_s=25.0)
    finally:
        if worker.poll() is None:
            worker.kill()
            worker.wait()

    assert sorted(processed) == list(range(total)), (
        f"backend={backend!r} Nx1: worker processed {sorted(processed)}, "
        f"expected {list(range(total))}"
    )
