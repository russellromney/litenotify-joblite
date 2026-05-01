# honker (Ruby)

Ruby binding for [Honker](https://github.com/russellromney/honker): durable queues, streams, pub/sub, and time-trigger scheduling on SQLite.

Full docs:

- [Main repo](https://github.com/russellromney/honker)
- [Docs](https://honker.dev)

## Install

```ruby
gem "honker"
```

You also need the Honker SQLite extension from the main repo.

## Quick start

```ruby
require "honker"

db = Honker::Database.new("app.db", extension_path: "./libhonker_ext.dylib")
q = db.queue("emails")

q.enqueue({to: "alice@example.com"})

if (job = q.claim_one("worker-1"))
  send_email(job.payload)
  job.ack
end
```

Delayed jobs use `run_at:`:

```ruby
q.enqueue({to: "later@example.com"}, run_at: Time.now.to_i + 10)
```

Recurring schedules use `schedule:`:

```ruby
sched = db.scheduler
sched.add(name: "fast", queue: "emails", schedule: "@every 1s", payload: {kind: "tick"})
```

Supported schedule forms:

- `0 3 * * *`
- `*/2 * * * * *`
- `@every 1s`

`schedule:` is the canonical recurring name. `cron:` still works as a compatibility alias.
