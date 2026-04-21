"""`python -m honker` CLI.

Usage:

    python -m honker worker <module:db_var> [options]

Example:

    python -m honker worker myapp.tasks:db --queue=emails --concurrency=4

This is a thin front for `honker.run_workers(...)`. See the Python
quickstart for details.
"""

from __future__ import annotations

import argparse
import asyncio
import os
import signal
import sys

from honker._tasks import load_app, registry, run_workers


def _make_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="python -m honker")
    sub = p.add_subparsers(dest="cmd", required=True)

    w = sub.add_parser(
        "worker",
        help="Run task workers against a Database.",
    )
    w.add_argument(
        "target",
        help=(
            "Import path to a Database instance, e.g. "
            "'myapp.tasks:db'. Importing the module fires the "
            "@task() decorators and populates the task registry."
        ),
    )
    w.add_argument(
        "--queue",
        action="append",
        default=None,
        help=(
            "Queue to drain. Can repeat. Default: every queue with "
            "at least one registered task."
        ),
    )
    w.add_argument(
        "--concurrency",
        type=int,
        default=None,
        help="Workers per queue. Default: os.cpu_count().",
    )
    w.add_argument(
        "--list",
        action="store_true",
        help="Print registered tasks and exit.",
    )

    return p


async def _run_worker_cmd(args) -> int:
    db = load_app(args.target)
    reg = registry()

    if args.list:
        print(f"Registered tasks on {args.target}:")
        for name in reg.names():
            spec = reg.get(name)
            print(f"  {name}  queue={spec.queue_name}")
        return 0

    if not reg.names():
        print(
            f"No @task-decorated functions found in {args.target}. "
            f"Make sure the module you import actually registers tasks.",
            file=sys.stderr,
        )
        return 1

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()

    def _shutdown(*_):
        stop.set()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _shutdown)
        except NotImplementedError:
            # Windows doesn't support add_signal_handler; skip.
            pass

    queues = args.queue or list(reg.queues())
    concurrency = args.concurrency or os.cpu_count() or 1

    print(
        f"honker worker: pid={os.getpid()} "
        f"queues={queues} concurrency={concurrency} "
        f"tasks={reg.names()}",
        flush=True,
    )

    # If multiple queues were requested, run a worker pool per queue
    # in parallel. run_workers with queue=None already handles "every
    # queue with registered tasks" but respects an explicit list too.
    tasks = []
    for q in queues:
        tasks.append(asyncio.create_task(
            run_workers(db, queue=q, concurrency=concurrency, stop_event=stop),
        ))

    await stop.wait()
    for t in tasks:
        t.cancel()
    for t in tasks:
        try:
            await t
        except (asyncio.CancelledError, Exception):
            pass
    print("honker worker: stopped cleanly", flush=True)
    return 0


def main(argv=None) -> int:
    parser = _make_parser()
    args = parser.parse_args(argv)
    if args.cmd == "worker":
        return asyncio.run(_run_worker_cmd(args))
    parser.error(f"unknown command: {args.cmd}")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
