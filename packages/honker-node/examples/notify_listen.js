// Ephemeral pub/sub — pg_notify semantics on SQLite.
//
// A subscriber sleeps on updateEvents().next() and wakes on every DB
// commit (same process or across processes on the same file), then
// reads new rows from _honker_notifications by id.
//
//   node examples/notify_listen.js

const lit = require('..');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');

const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'honker-'));
const db = lit.open(path.join(dir, 'app.db'));

async function run() {
  const received = [];
  const ev = db.updateEvents();

  // Snapshot the current high-water mark so we only see NEW
  // notifications.
  let lastSeen = db.query(
    "SELECT COALESCE(MAX(id), 0) AS m FROM _honker_notifications",
  )[0].m;

  const listener = (async () => {
    while (received.length < 3) {
      await ev.next();
      const rows = db.query(
        "SELECT id, channel, payload FROM _honker_notifications "
        + "WHERE channel='orders' AND id > ? ORDER BY id",
        [lastSeen],
      );
      for (const r of rows) {
        received.push(JSON.parse(r.payload));
        lastSeen = r.id;
      }
    }
  })();

  // Give the listener a tick to park on updateEvents.
  await new Promise((r) => setTimeout(r, 50));

  for (const id of [1, 2, 3]) {
    const tx = db.transaction();
    tx.notify('orders', { id, event: 'placed' });
    tx.commit();
  }

  await listener;
  ev.close();

  console.log(`received ${received.length} notifications:`);
  for (const p of received) console.log(`  ${JSON.stringify(p)}`);
  if (received.length !== 3)
    throw new Error(`expected 3, got ${received.length}`);
}

run()
  .catch((e) => { console.error(e); process.exitCode = 1; })
  .finally(() => fs.rmSync(dir, { recursive: true, force: true }));
