// Cross-language e2e: Python ↔ Node share the same .db file.
//
// Three scenarios:
//   1. Python writes → Node reads (walEvents + SELECT)
//   2. Node writes → Python reads (listen() iterator)
//   3. Schema compat: Python bootstraps joblite tables, Node sees
//      them via raw SQL (Node binding doesn't expose jl_* functions —
//      it's the `litenotify` binding — but the shared _litenotify_
//      _notifications + _joblite_* tables are interoperable.)
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

// Reverse direction: Node is the writer, Python subscribes via
// joblite's Listener. Python's Listener starts from MAX(id) at
// construction — if Node wrote before Python subscribed, those
// events would be skipped. So Python signals READY on stdout
// before Node commits.
test('node writes notifications; python reads via listen()', async () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'xlang-rev-'));
  const dbPath = path.join(dir, 't.db');
  try {
    // Open from Node first to create schema + WAL file.
    const db = lit.open(dbPath);

    const REPO = path.resolve(__dirname, '..', '..', '..');
    const PACKAGES = path.resolve(__dirname, '..', '..');
    const pyScript = `
import asyncio, json, sys
sys.path.insert(0, ${JSON.stringify(PACKAGES)})
import joblite

async def main():
    db = joblite.open(${JSON.stringify(dbPath)})
    lst = db.listen("reverse")
    print("READY", flush=True)
    got = []
    async def consume():
        async for n in lst:
            got.append(n.payload)
            if len(got) == 3:
                return
    await asyncio.wait_for(consume(), timeout=5.0)
    print("RESULT", json.dumps(got), flush=True)

asyncio.run(main())
`;
    const proc = spawn(
      path.join(REPO, '.venv/bin/python'),
      ['-c', pyScript],
      { stdio: ['ignore', 'pipe', 'inherit'] },
    );

    // Line-buffered reader — keeps a single listener on stdout
    // across both READY and RESULT waits, so we don't detach/
    // reattach and drop bytes. A BrokenPipe in the Python child
    // typically means the Node side closed stdout mid-write;
    // keeping the pipe open until the child exits avoids that.
    const lines = [];
    const waiters = [];
    let buf = '';
    proc.stdout.on('data', (chunk) => {
      buf += chunk.toString('utf8');
      let nl;
      while ((nl = buf.indexOf('\n')) >= 0) {
        const line = buf.slice(0, nl);
        buf = buf.slice(nl + 1);
        lines.push(line);
        const w = waiters.shift();
        if (w) w(line);
      }
    });

    const nextLineMatching = (predicate, timeoutMs) =>
      new Promise((resolve, reject) => {
        const timer = setTimeout(
          () => reject(new Error('timeout waiting for line')),
          timeoutMs,
        );
        const check = () => {
          for (let i = 0; i < lines.length; i++) {
            if (predicate(lines[i])) {
              const line = lines.splice(i, 1)[0];
              clearTimeout(timer);
              return resolve(line);
            }
          }
          waiters.push((line) => {
            if (predicate(line)) {
              clearTimeout(timer);
              resolve(line);
            } else {
              check();
            }
          });
        };
        check();
      });

    // Wait for READY so Python's listener is attached before we
    // commit. Without this, Python skips events that happened
    // before subscription (Listener starts at MAX(id)).
    await nextLineMatching((l) => l === 'READY', 5000);

    // Now publish from Node.
    const tx = db.transaction();
    tx.notify('reverse', { tag: 'a', i: 1 });
    tx.notify('reverse', { tag: 'b', i: 2 });
    tx.commit();
    const tx2 = db.transaction();
    tx2.notify('reverse', { tag: 'c', i: 3 });
    tx2.commit();

    // Read Python's RESULT line.
    const resultLine = await nextLineMatching(
      (l) => l.startsWith('RESULT '),
      5000,
    );
    const result = JSON.parse(resultLine.slice('RESULT '.length));
    await new Promise((resolve) => proc.on('exit', resolve));

    assert.equal(result.length, 3);
    assert.deepEqual(
      result.map((r) => r.i),
      [1, 2, 3],
    );
    assert.deepEqual(
      result.map((r) => r.tag),
      ['a', 'b', 'c'],
    );
  } finally {
    fs.rmSync(dir, { recursive: true, force: true });
  }
});

// Schema compat: a .db bootstrapped by joblite.open() in Python
// has every _joblite_* + _litenotify_* table. Node (which only
// knows about _litenotify_notifications natively) can still read
// them via raw SQL. Proves the tables aren't PyO3-gated — the
// on-disk format is language-neutral.
test(
  'python bootstraps joblite schema; node reads tables via raw SQL',
  async () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'xlang-schema-'));
    const dbPath = path.join(dir, 't.db');
    try {
      const REPO = path.resolve(__dirname, '..', '..', '..');
      const PACKAGES = path.resolve(__dirname, '..', '..');
      const pyScript = `
import sys
sys.path.insert(0, ${JSON.stringify(PACKAGES)})
import joblite
db = joblite.open(${JSON.stringify(dbPath)})
q = db.queue("shared")
q.enqueue({"from": "python", "i": 1})
q.enqueue({"from": "python", "i": 2})
print("DONE", flush=True)
`;
      await new Promise((resolve, reject) => {
        const proc = spawn(
          path.join(REPO, '.venv/bin/python'),
          ['-c', pyScript],
          { stdio: ['ignore', 'inherit', 'inherit'] },
        );
        proc.on('exit', (code) =>
          code === 0
            ? resolve()
            : reject(new Error(`python exited ${code}`)),
        );
      });

      // Now open from Node. Schema already exists; Node should
      // see both enqueued jobs via raw SELECT on the shared table.
      const db = lit.open(dbPath);
      const rows = db.query(
        "SELECT id, queue, payload FROM _joblite_live " +
          "WHERE queue='shared' ORDER BY id",
      );
      assert.equal(rows.length, 2);
      assert.equal(rows[0].queue, 'shared');
      const payloads = rows.map((r) => JSON.parse(r.payload));
      assert.deepEqual(
        payloads.map((p) => p.i),
        [1, 2],
      );
      assert.equal(payloads[0].from, 'python');

      // Verify every BOOTSTRAP_JOBLITE_SQL table is present from
      // Node's perspective too — catches a binding-gated bootstrap.
      const tableNames = db
        .query(
          "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name",
        )
        .map((r) => r.name);
      for (const expected of [
        '_litenotify_notifications',
        '_joblite_live',
        '_joblite_dead',
        '_joblite_locks',
        '_joblite_rate_limits',
        '_joblite_scheduler_tasks',
        '_joblite_results',
        '_joblite_stream',
        '_joblite_stream_consumers',
      ]) {
        assert.ok(
          tableNames.includes(expected),
          `Node cannot see ${expected} on a Python-bootstrapped DB`,
        );
      }
    } finally {
      fs.rmSync(dir, { recursive: true, force: true });
    }
  },
);
