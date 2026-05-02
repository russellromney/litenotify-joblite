# @russellthehippo/honker-node

Node.js binding for [Honker](https://github.com/russellromney/honker): durable queues, streams, pub/sub, and time-trigger scheduling on SQLite.

Full docs live here:

- [Main repo](https://github.com/russellromney/honker)
- [Docs](https://honker.dev)

## Install

```bash
npm install @russellthehippo/honker-node
```

You also need the Honker SQLite extension. Build it from the main repo or use a release artifact.

## Quick start

```js
const honker = require("@russellthehippo/honker-node");

const db = honker.open("app.db");
const q = db.queue("emails");

q.enqueue({ to: "alice@example.com" });

for await (const job of q.claim("worker-1")) {
  sendEmail(job.payload);
  job.ack();
}
```

Delayed jobs use `runAt`:

```js
q.enqueue({ to: "later@example.com" }, { runAt: Math.floor(Date.now() / 1000) + 10 });
```

Recurring schedules use `schedule`:

```js
const sched = db.scheduler();
sched.add("fast", { queue: "emails", schedule: "@every 1s", payload: { kind: "tick" } });
```

Supported schedule forms:

- `0 3 * * *`
- `*/2 * * * * *`
- `@every 1s`

## Notes

- `claim()` wakes on database updates and on due deadlines.
- `schedule` is the canonical recurring-schedule option.
- `cron` still works as a compatibility alias.

For streams, notify/listen, SQL functions, and full scheduler docs, see the main repo and docs site.
