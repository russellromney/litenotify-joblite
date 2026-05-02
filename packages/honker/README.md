# honker (Python)

Python binding for [Honker](https://github.com/russellromney/honker): durable queues, streams, pub/sub, and time-trigger scheduling on SQLite.

Full docs live in the main repo and docs site:

- [Main repo](https://github.com/russellromney/honker)
- [Docs](https://honker.dev)

## Install

```bash
pip install honker
```

You also need the Honker SQLite extension. Use a release build from the main repo, or build it yourself:

```bash
git clone https://github.com/russellromney/honker
cd honker
cargo build --release -p honker-extension
```

## Quick start

```python
import honker

db = honker.open("app.db")
q = db.queue("emails")

q.enqueue({"to": "alice@example.com"})

async for job in q.claim("worker-1"):
    send_email(job.payload)
    job.ack()
```

Delayed jobs use `run_at`:

```python
import time

q.enqueue({"to": "later@example.com"}, run_at=int(time.time()) + 10)
```

Recurring schedules use `schedule` expressions:

```python
sched = db.scheduler()
sched.add("fast", queue="emails", schedule="@every 1s", payload={"kind": "tick"})
```

Supported schedule forms:

- 5-field cron: `0 3 * * *`
- 6-field cron: `*/2 * * * * *`
- interval: `@every 1s`

## Notes

- `claim()` wakes on database updates and on due deadlines like `run_at`.
- `schedule` is the canonical recurring-schedule name.
- `cron` still works as a compatibility alias in older call sites.

For streams, notify/listen, tasks, SQL extension usage, and full scheduler docs, see the main repo and docs site.
