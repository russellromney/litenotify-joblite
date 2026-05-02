# honker-go

Go binding for [Honker](https://github.com/russellromney/honker): durable queues, streams, pub/sub, and time-trigger scheduling on SQLite.

Full docs:

- [Main repo](https://github.com/russellromney/honker)
- [Docs](https://honker.dev)

## Install

```bash
go get github.com/russellromney/honker-go
```

You also need the Honker SQLite extension from the main repo.

## Quick start

```go
db, err := honker.Open("app.db", honker.WithExtensionPath("./libhonker_ext.dylib"))
if err != nil {
    panic(err)
}

q := db.Queue("emails")

if _, err := q.Enqueue(map[string]any{"to": "alice@example.com"}); err != nil {
    panic(err)
}

job, err := q.ClaimOne("worker-1")
if err != nil {
    panic(err)
}
if job != nil {
    sendEmail(job.Payload)
    _ = job.Ack()
}
```

Delayed jobs use `RunAt`:

```go
_, _ = q.Enqueue(map[string]any{"to": "later@example.com"}, honker.RunAt(time.Now().Unix()+10))
```

Recurring schedules use `Schedule`:

```go
s := db.Scheduler()
_ = s.Add("fast", "emails", "@every 1s", map[string]any{"kind": "tick"})
```

Supported schedule forms:

- `0 3 * * *`
- `*/2 * * * * *`
- `@every 1s`

`Schedule` is the canonical recurring-schedule name. Older `Cron` naming is compatibility-only.
