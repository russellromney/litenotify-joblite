# honker-bun

Bun binding for [Honker](https://honker.dev) — durable queues, streams, pub/sub, and scheduler on SQLite.

## Install

```bash
bun add honker-bun
```

You'll also need the Honker SQLite loadable extension (`libhonker_extension.dylib` on macOS, `.so` on Linux). Prebuilds at [GitHub releases](https://github.com/russellromney/honker/releases/latest), or build from source:

```bash
git clone https://github.com/russellromney/honker
cd honker
cargo build --release -p honker-extension
# → target/release/libhonker_extension.{dylib,so}
```

## Why Bun

Bun ships with `bun:sqlite` as a built-in — no npm deps, no native addon to rebuild per platform. This binding is pure TypeScript over Bun's sqlite bindings, so `bun add honker-bun` is all the install you need beyond the `.dylib`/`.so`.

## Quick start

```ts
import { open } from "honker-bun";

const db = open("app.db", "./libhonker_extension.dylib");
const q = db.queue("emails");

q.enqueue({ to: "alice@example.com" });

const job = q.claimOne("worker-1");
if (job) {
  console.log(job.payload);
  job.ack();
}
```

## API

### `open(path, extensionPath) → Database`

Opens/creates SQLite, loads the extension, applies default PRAGMAs, bootstraps schema.

### `Database#queue(name, { visibilityTimeoutS = 300, maxAttempts = 3 }) → Queue`

Named queue handle.

### `Queue#enqueue(payload, { delay, runAt, priority, expires }) → id`

Insert a job. Payload is JSON-serialized.

### `Queue#claimBatch(workerId, n) → Job[]` / `Queue#claimOne(workerId) → Job | null`

Atomically claim. Batch form for throughput.

### `Job#ack() / retry(delaySec, errorMsg) / fail(errorMsg) / heartbeat(extendSec) → boolean`

Claim lifecycle. Returns `true` iff the caller's claim was still valid.

### `Database#notify(channel, payload) → id`

Fire a `pg_notify`-style pub/sub signal.

### `Database#raw`

Escape hatch to the underlying `bun:sqlite` Database for advanced queries.

## What's not here yet

- `listen` / wake watcher (needs a Bun-compatible WAL watcher; planning)
- Typed Streams / Scheduler wrappers (raw SQL works via `db.raw.query(...)`)

## Testing

```bash
# Build the extension first
cd /path/to/honker && cargo build --release -p honker-extension

cd packages/honker-bun
bun test
```
