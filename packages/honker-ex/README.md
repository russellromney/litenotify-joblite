# honker (Elixir)

Elixir binding for [Honker](https://honker.dev) — durable queues, streams, pub/sub, and scheduler on SQLite.

## Install

```elixir
def deps do
  [
    {:honker, "~> 0.1"}
  ]
end
```

You'll also need the Honker SQLite extension. Prebuilds at [GitHub releases](https://github.com/russellromney/honker/releases/latest), or build from source:

```bash
git clone https://github.com/russellromney/honker
cd honker
cargo build --release -p honker-extension
# → target/release/libhonker_ext.{dylib,so}
```

## Why this matters for Elixir

SQLite-backed task queues compete directly with [Oban](https://github.com/sorentwo/oban) (Postgres-based). For single-node Phoenix / Nerves / LiveView apps that don't already run Postgres, Honker removes the whole "provision and operate a second database" step.

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

## API

### `Honker.open(path, extension_path: ...)` → `{:ok, %Honker.Database{}}`

Opens or creates a SQLite DB, loads the Honker loadable extension, applies default PRAGMAs, and bootstraps the schema.

### `Honker.configure_queue(db, queue_name, opts)` → `%Honker.Database{}`

Register per-queue options: `visibility_timeout_s` (default 300), `max_attempts` (default 3).

### `Honker.Queue.enqueue(db, queue, payload, opts)` → `{:ok, id}`

Insert a job. `payload` is any term — encoded via `Jason`. `opts` supports `:delay`, `:run_at`, `:priority`, `:expires`.

### `Honker.Queue.claim_batch(db, queue, worker_id, n)` / `Honker.Queue.claim_one(db, queue, worker_id)`

Atomic claim. Returns `{:ok, [%Job{}]}` or `{:ok, nil}`.

### `Honker.Job.ack(db, job)` / `retry(db, job, delay_s, error)` / `fail(db, job, error)` / `heartbeat(db, job, extend_s)`

Claim lifecycle. Each returns `{:ok, bool}` — true iff the claim was still valid.

### `Honker.notify(db, channel, payload)` → `{:ok, id}`

Fire a `pg_notify`-style pub/sub signal.

## What's not here yet

- `GenServer`-backed worker / Supervisor tree (you can build this in ~50 lines with the claim + ack primitives)
- Typed Streams / Scheduler wrappers (use raw SQL on `db.conn` for now)
- `listen` / WAL-wake async API

The design intent: keep the binding *tiny*, let the Elixir / Phoenix side build whatever Supervisor/GenServer shape fits the app.

## Testing

```bash
# Build the extension first
cd /path/to/honker && cargo build --release -p honker-extension

cd packages/honker-ex
mix deps.get
mix test
```
