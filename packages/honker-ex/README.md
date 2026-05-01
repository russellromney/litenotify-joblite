# honker (Elixir)

Elixir binding for [Honker](https://github.com/russellromney/honker): durable queues, streams, pub/sub, and time-trigger scheduling on SQLite.

Full docs:

- [Main repo](https://github.com/russellromney/honker)
- [Docs](https://honker.dev)

## Install

```elixir
def deps do
  [
    {:honker, "~> 0.1"}
  ]
end
```

You also need the Honker SQLite extension from the main repo.

## Quick start

```elixir
{:ok, db} = Honker.open("app.db", extension_path: "./libhonker_ext.dylib")

{:ok, _id} = Honker.Queue.enqueue(db, "emails", %{to: "alice@example.com"})

case Honker.Queue.claim_one(db, "emails", "worker-1") do
  {:ok, nil} -> :empty
  {:ok, job} ->
    send_email(job.payload)
    Honker.Job.ack(db, job)
end
```

Delayed jobs use `run_at:`:

```elixir
{:ok, _id} = Honker.Queue.enqueue(db, "emails", %{to: "later@example.com"}, run_at: System.os_time(:second) + 10)
```

Recurring schedules use `schedule:`:

```elixir
:ok = Honker.Scheduler.add(db, name: "fast", queue: "emails", schedule: "@every 1s", payload: %{kind: "tick"})
```

Supported schedule forms:

- `0 3 * * *`
- `*/2 * * * * *`
- `@every 1s`

`schedule:` is the canonical recurring name. `cron:` is compatibility-only.
