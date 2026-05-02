# Node examples

```bash
npm install
npx napi build --platform --release    # if you haven't already
node examples/<name>.js
```

| File | What it shows |
|---|---|
| [`basic.js`](basic.js) | enqueue → claim → ack via the typed JS queue wrapper |
| [`atomic.js`](atomic.js) | `INSERT INTO orders` + enqueue committed in one transaction. Rollback drops both. |
| [`notify_listen.js`](notify_listen.js) | `updateEvents()` + `tx.notify()` pub/sub |

The Node binding now exposes typed JS wrappers for queues, streams, locks, pub/sub, and the scheduler. The raw SQL surface is still available via `db.query(...)` / `tx.query(...)` when you want it.
