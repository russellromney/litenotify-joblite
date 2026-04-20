# honker-go

Go binding for [Honker](https://honker.dev) — durable queues, streams, pub/sub, and scheduler on SQLite.

## Install

```bash
go get github.com/russellromney/honker-go
```

You'll also need the Honker SQLite extension (`libhonker.dylib` on macOS, `libhonker.so` on Linux). Prebuilds live at [GitHub releases](https://github.com/russellromney/honker/releases/latest), or build from source:

```bash
git clone https://github.com/russellromney/honker
cd honker
cargo build --release -p honker-extension
# → target/release/libhonker_extension.{dylib,so}
```

## Build requirement

This binding uses [mattn/go-sqlite3](https://github.com/mattn/go-sqlite3), which requires cgo and the `sqlite_load_extension` build tag to enable `LoadExtension`:

```bash
go build -tags sqlite_load_extension ./...
go test  -tags sqlite_load_extension ./...
```

## Quick start

```go
package main

import (
    "encoding/json"
    "fmt"
    "log"

    "github.com/russellromney/honker-go"
)

func main() {
    db, err := honker.Open("app.db", "./libhonker_extension.dylib")
    if err != nil { log.Fatal(err) }
    defer db.Close()

    q := db.Queue("emails", honker.QueueOptions{})

    // Enqueue
    _, err = q.Enqueue(map[string]any{"to": "alice@example.com"}, honker.EnqueueOptions{})
    if err != nil { log.Fatal(err) }

    // Claim + process + ack
    job, err := q.ClaimOne("worker-1")
    if err != nil { log.Fatal(err) }

    var body map[string]any
    json.Unmarshal(job.Payload, &body)
    fmt.Printf("sending to %s\n", body["to"])

    if _, err := job.Ack(); err != nil { log.Fatal(err) }
}
```

## API

### `Open(path, extensionPath string) (*Database, error)`

Opens (or creates) a SQLite database, loads the Honker extension on every pooled connection, applies default PRAGMAs, and bootstraps the schema.

### `(*Database).Queue(name, QueueOptions) *Queue`

Handle to a named queue. `QueueOptions{VisibilityTimeoutS, MaxAttempts}` — zero values resolve to 300s / 3 attempts.

### `(*Queue).Enqueue(payload any, EnqueueOptions) (int64, error)`

Inserts a job. `payload` is marshaled via `json.Marshal`. `EnqueueOptions{Delay, RunAt, Priority, Expires}` — all optional, `Delay` / `RunAt` / `Expires` are `*int64` so you can distinguish unset from zero.

### `(*Queue).ClaimBatch(workerID string, n int) ([]*Job, error)`

Atomically claims up to N jobs. Returns empty slice if queue is empty.

### `(*Queue).ClaimOne(workerID string) (*Job, error)`

Claims a single job or returns `(nil, nil)` on empty.

### `(*Job).Ack() (bool, error)`

Deletes the claim row. Returns `(true, nil)` iff the caller's claim hadn't expired.

### `(*Job).Retry(delaySec int64, errMsg string) (bool, error)`

Puts the job back with a delay. After `MaxAttempts`, moves to `_honker_dead`.

### `(*Job).Fail(errMsg string) (bool, error)`

Unconditionally moves to `_honker_dead`.

### `(*Job).Heartbeat(extendSec int64) (bool, error)`

Extends the claim's visibility timeout for long-running jobs.

### `(*Database).Notify(channel string, payload any) (int64, error)`

Fires a `pg_notify`-style pub/sub signal. Other processes listening on the same channel wake within the stat-poll cadence.

## What's not here yet

- `Listen` / `.db-wal` watcher async API (use `SELECT id, channel, payload FROM _honker_notifications WHERE id > ? ORDER BY id` polling for now)
- Streams (durable pub/sub with per-consumer offsets)
- Scheduler (cron-style periodic tasks)

All of those are available via raw SQL on the same database — `SELECT honker_stream_publish(...)` etc. work from Go via `db.Raw().Exec(...)`. They'll be wrapped in idiomatic Go APIs in a future release.

## Testing

```bash
# Build the extension first
cd /path/to/honker && cargo build --release -p honker-extension

# Then run Go tests
cd packages/honker-go
go test -tags sqlite_load_extension -v ./...
```
