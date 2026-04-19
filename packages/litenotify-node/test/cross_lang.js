// Cross-language e2e: Python subprocess writes notifications,
// Node subscriber reads them via walEvents + SELECT.
const test = require('node:test');
const assert = require('node:assert/strict');
const { spawn } = require('node:child_process');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');

const lit = require('..');

test('python writes notifications; node reads via WAL wake + SELECT', async () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'xlang-'));
  const dbPath = path.join(dir, 't.db');
  try {
    // Open from Node first so the schema + WAL file exist.
    const db = lit.open(dbPath);
    const ev = db.walEvents();

    // Record last_seen before the writer fires.
    let lastSeen = 0;
    const initial = db.query(
      'SELECT COALESCE(MAX(id), 0) AS m FROM _litenotify_notifications',
    );
    lastSeen = initial[0].m;

    // Python writer: emits 3 notifications on channel 'orders'.
    // __dirname = packages/litenotify-node/test
    // ../../..  = repo root;  ../..  = packages/
    const REPO = path.resolve(__dirname, '..', '..', '..');
    const PACKAGES = path.resolve(__dirname, '..', '..');
    const pyScript = `
import sys, time
sys.path.insert(0, ${JSON.stringify(PACKAGES)})
import joblite
time.sleep(0.3)
db = joblite.open(${JSON.stringify(dbPath)})
with db.transaction() as tx:
    tx.notify("orders", {"id": 1})
    tx.notify("orders", {"id": 2})
with db.transaction() as tx:
    tx.notify("orders", {"id": 3})
`;
    const proc = spawn(
      path.join(REPO, '.venv/bin/python'),
      ['-c', pyScript],
      { stdio: ['ignore', 'inherit', 'inherit'] },
    );

    const received = [];
    const collectUntil = async (n, timeoutMs) => {
      const deadline = Date.now() + timeoutMs;
      while (received.length < n && Date.now() < deadline) {
        await Promise.race([
          ev.next(),
          new Promise((resolve) =>
            setTimeout(resolve, Math.max(0, deadline - Date.now())),
          ),
        ]);
        const rows = db.query(
          "SELECT id, channel, payload FROM _litenotify_notifications " +
            "WHERE channel='orders' AND id > ? ORDER BY id",
          [lastSeen],
        );
        for (const r of rows) {
          received.push({ id: r.id, payload: JSON.parse(r.payload) });
          lastSeen = r.id;
        }
      }
    };

    await collectUntil(3, 3000);
    ev.close();
    await new Promise((resolve) => proc.on('exit', resolve));

    assert.equal(received.length, 3, `got ${received.length} events`);
    assert.deepEqual(
      received.map((r) => r.payload.id),
      [1, 2, 3],
    );
  } finally {
    fs.rmSync(dir, { recursive: true, force: true });
  }
});
