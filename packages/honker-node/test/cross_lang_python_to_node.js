'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');

const honker = require('..');
const { createTempDb } = require('./helpers');
const {
  PACKAGES,
  spawnPython,
  stopChild,
  waitForExit,
} = require('./cross_lang_shared');

async function waitForGone(dir, timeoutMs = 5000) {
  const deadline = Date.now() + timeoutMs;
  while (fs.existsSync(dir)) {
    if (Date.now() >= deadline) return false;
    await new Promise((resolve) => setTimeout(resolve, 50));
  }
  return true;
}

test('direct proof: python writes notifications; node updateEvents() + SELECT observes them', async () => {
  const { path: dbPath, open, cleanup } = createTempDb(
    'xlang-py-to-node-',
    honker.open.bind(honker),
  );
  let db;
  let ev;
  let proc;
  try {
    db = open(dbPath);
    ev = db.updateEvents();
    let lastSeen = 0;
    const initial = db.query(
      'SELECT COALESCE(MAX(id), 0) AS m FROM _honker_notifications',
    );
    lastSeen = initial[0].m;

    const pyScript = `
import sys, time
sys.path.insert(0, ${JSON.stringify(PACKAGES)})
import honker
time.sleep(0.3)
db = honker.open(${JSON.stringify(dbPath)})
with db.transaction() as tx:
    tx.notify("orders", {"id": 1})
    tx.notify("orders", {"id": 2})
with db.transaction() as tx:
    tx.notify("orders", {"id": 3})
`;
    proc = spawnPython(pyScript, ['ignore', 'inherit', 'inherit']);

    const received = [];
    while (received.length < 3) {
      await Promise.race([
        ev.next(),
        new Promise((resolve) => setTimeout(resolve, 3000)),
      ]);
      const rows = db.query(
        "SELECT id, payload FROM _honker_notifications " +
          "WHERE channel='orders' AND id > ? ORDER BY id",
        [lastSeen],
      );
      for (const row of rows) {
        received.push(JSON.parse(row.payload).id);
        lastSeen = row.id;
      }
    }

    await waitForExit(proc);
    assert.deepEqual(received, [1, 2, 3]);
  } finally {
    ev?.close();
    db?.close();
    await stopChild(proc);
    cleanup();
  }
});

test(
  'direct proof: windows node listener path closes cleanly enough to delete the tempdir',
  { skip: process.platform !== 'win32' },
  async () => {
    const { path: dbPath, dir, open, cleanup } = createTempDb(
      'xlang-win-clean-',
      honker.open.bind(honker),
    );
    let db;
    let ev;
    let proc;
    let cleaned = false;
    try {
      db = open(dbPath);
      ev = db.updateEvents();
      let lastSeen = 0;
      const initial = db.query(
        'SELECT COALESCE(MAX(id), 0) AS m FROM _honker_notifications',
      );
      lastSeen = initial[0].m;

      const pyScript = `
import sys, time
sys.path.insert(0, ${JSON.stringify(PACKAGES)})
import honker
time.sleep(0.2)
db = honker.open(${JSON.stringify(dbPath)})
with db.transaction() as tx:
    tx.notify("cleanup", {"ok": True})
`;
      proc = spawnPython(pyScript, ['ignore', 'inherit', 'inherit']);
      await Promise.race([
        ev.next(),
        new Promise((resolve) => setTimeout(resolve, 5000)),
      ]);
      const rows = db.query(
        "SELECT id, payload FROM _honker_notifications " +
          "WHERE channel='cleanup' AND id > ? ORDER BY id",
        [lastSeen],
      );
      assert.equal(rows.length, 1, 'updateEvents path did not surface the cleanup notification');
      assert.deepEqual(JSON.parse(rows[0].payload), { ok: true });

      ev.close();
      ev = null;
      db.close();
      db = null;
      await waitForExit(proc);
      const removedNow = cleanup();
      cleaned = true;
      const gone = removedNow || (await waitForGone(dir));
      assert.equal(gone, true, `tempdir still exists after cleanup: ${dir}`);
    } finally {
      if (ev) ev.close();
      if (db) db.close();
      await stopChild(proc);
      if (!cleaned) cleanup();
    }
  },
);
