# honker-bun

Bun binding for [Honker](https://github.com/russellromney/honker): durable queues, streams, pub/sub, and time-trigger scheduling on SQLite.

Full docs:

- [Main repo](https://github.com/russellromney/honker)
- [Docs](https://honker.dev)

## Install

```bash
bun add @russellthehippo/honker-bun
```

You also need the Honker SQLite extension from the main repo.

## Quick start

```ts
import { open } from "@russellthehippo/honker-bun";

const db = open("app.db");
const q = db.queue("emails");

q.enqueue({ to: "alice@example.com" });

for await (const job of q.claim("worker-1")) {
  sendEmail(job.payload);
  job.ack();
}
```

Delayed jobs use `runAt`:

```ts
q.enqueue({ to: "later@example.com" }, { runAt: Math.floor(Date.now() / 1000) + 10 });
```

Recurring schedules use `schedule`:

```ts
const sched = db.scheduler();
sched.add("fast", { queue: "emails", schedule: "@every 1s", payload: { kind: "tick" } });
```

Supported schedule forms:

- `0 3 * * *`
- `*/2 * * * * *`
- `@every 1s`

For full API docs and SQL details, see the main repo and docs site.
