# Python examples

Each example is runnable from the repo root with the Python package on `PYTHONPATH`:

```bash
PYTHONPATH=packages .venv/bin/python packages/honker/examples/atomic.py
```

| File | What it shows |
|---|---|
| [`tasks.py`](tasks.py) | **Huey-style `@queue.task()` decorators.** `add(2, 3)` returns a `TaskResult`; `.get()` waits for the worker. Includes a timeout-to-dead-letter path. |
| [`atomic.py`](atomic.py) | `INSERT INTO orders` + `queue.enqueue(...)` committed in one transaction. Rollback drops both. |
| [`worker.py`](worker.py) | Low-level async worker loop with retry → dead-letter (no decorators). |
| [`notify_listen.py`](notify_listen.py) | Ephemeral `pg_notify`-style pub/sub. |
| [`stream.py`](stream.py) | Durable pub/sub with per-consumer offset tracking + resume-after-crash. |
| [`scheduler.py`](scheduler.py) | Time-trigger scheduling with leader election. Shows cron-style schedules; the library also supports `every_s(...)` and 6-field cron. |

The task example also demonstrates the CLI worker flow:

```bash
# In one terminal — enqueues 2 add() calls + 2 fire-and-forget log_events,
# runs workers inline for a few seconds, shuts down.
PYTHONPATH=packages .venv/bin/python packages/honker/examples/tasks.py

# Alternative: split the enqueue + worker into two processes
PYTHONPATH=packages .venv/bin/python -m honker worker \
    packages.honker.examples.tasks:db --list
```

All examples use a temporary SQLite file, so running them leaves nothing behind.
