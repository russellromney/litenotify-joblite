// Cross-process end-to-end proof for the experimental watcher backends,
// Node side. Mirrors tests/test_watcher_backends_e2e.py — same three
// scenarios (basic delivery, burst load, writer-restart resilience),
// each parameterized over polling / kernel / shm.
//
// Node's wake API is `db.updateEvents()` (fires on every commit) plus a
// raw SELECT against `_honker_notifications` for the payloads, so the
// consumer here is a little more low-level than Python's `db.listen()`,
// which bundles both.
//
// Wheels built without the experimental Cargo features silently fall
// back to polling for `"kernel"` and `"shm"` — the suite still runs
// them so a regression in the fallback path also surfaces here.

'use strict';

const { spawn } = require('node:child_process');
const path = require('node:path');
const test = require('node:test');
const assert = require('node:assert/strict');

const honker = require('..');
const { createTempDb } = require('./helpers');

// Spawn a Node child running an inline script. We use process.execPath
// (the running interpreter) so the child uses the same Node + the same
// honker binding we just built.
function spawnNode(script) {
  return spawn(process.execPath, ['-e', script], {
    stdio: ['pipe', 'pipe', 'inherit'],
  });
}

function waitForExit(proc) {
  if (!proc || proc.exitCode !== null || proc.signalCode !== null) {
    return Promise.resolve(proc?.exitCode);
  }
  return new Promise((resolve) => proc.once('exit', resolve));
}

async function stopChild(proc) {
  if (!proc || proc.exitCode !== null || proc.signalCode !== null) return;
  try {
    if (proc.stdin && !proc.stdin.destroyed) {
      proc.stdin.end('exit\n');
    }
  } catch {}
  // Give the child up to 1s to drain stdin and exit cleanly; otherwise
  // signal it.
  const drained = await Promise.race([
    waitForExit(proc),
    new Promise((r) => setTimeout(() => r(undefined), 1000)),
  ]);
  if (drained === undefined) {
    proc.kill();
    await waitForExit(proc);
  }
}

// Small line reader so we can wait for "READY" / "DONE" markers from
// the writer subprocess without buffering the entire stream.
function createLineReader(stream) {
  const lines = [];
  const waiters = [];
  let buf = '';
  stream.on('data', (chunk) => {
    buf += chunk.toString('utf8');
    let nl;
    while ((nl = buf.indexOf('\n')) >= 0) {
      let line = buf.slice(0, nl);
      buf = buf.slice(nl + 1);
      if (line.endsWith('\r')) line = line.slice(0, -1);
      lines.push(line);
      const w = waiters.shift();
      if (w) w(line);
    }
  });
  return function nextLine(predicate, timeoutMs) {
    return new Promise((resolve, reject) => {
      const timer = setTimeout(
        () => reject(new Error(`timeout waiting for line after ${timeoutMs}ms`)),
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
  };
}

const REQUIRE_HONKER = path.resolve(__dirname, '..');

// Inline script for the writer subprocess. Reads commands from stdin
// to gate when commits start, so the parent can subscribe to updateEvents()
// before the writer publishes anything (avoids the same subscribe-after-
// first-write race the Python e2e calls out).
function writerScript({ dbPath, channel, n, spacingMs }) {
  return `
'use strict';
const honker = require(${JSON.stringify(REQUIRE_HONKER)});
const db = honker.open(${JSON.stringify(dbPath)});

function sleep(ms) { return new Promise((r) => setTimeout(r, ms)); }

(async () => {
  process.stdout.write('READY\\n');
  // Block until parent releases us.
  await new Promise((resolve) => {
    process.stdin.once('data', resolve);
  });
  for (let i = 0; i < ${n}; i++) {
    const tx = db.transaction();
    tx.notify(${JSON.stringify(channel)}, String(i));
    tx.commit();
    if (${spacingMs} > 0) await sleep(${spacingMs});
  }
  process.stdout.write('DONE\\n');
  // Keep stdin open until parent says exit so we don't close
  // the db handle out from under the parent's listener.
  await new Promise((resolve) => process.stdin.once('data', resolve));
  db.close();
})();
`;
}

// Drain notifications for `channel` from the parent's updateEvents()
// stream. Each `await ev.next()` resolves on every commit; we then
// SELECT new rows above the last seen id. Returns the payloads in
// commit order.
async function drainNotifications({ db, ev, channel, sinceId, n, timeoutMs }) {
  const seen = [];
  let lastId = sinceId;
  const deadline = Date.now() + timeoutMs;
  while (seen.length < n) {
    const remaining = deadline - Date.now();
    if (remaining <= 0) break;
    const wakeOrTimeout = await Promise.race([
      ev.next().then(() => 'wake'),
      new Promise((r) => setTimeout(() => r('timeout'), remaining)),
    ]);
    if (wakeOrTimeout === 'timeout') break;
    const rows = db.query(
      'SELECT id, payload FROM _honker_notifications ' +
        'WHERE channel = ? AND id > ? ORDER BY id',
      [channel, lastId],
    );
    for (const row of rows) {
      // Payloads round-trip as JSON (notify() in the binding does
      // JSON.stringify on the way in). Parse so callers get the
      // original value, not the JSON-encoded text.
      seen.push(JSON.parse(row.payload));
      if (row.id > lastId) lastId = row.id;
      if (seen.length >= n) break;
    }
  }
  return { seen, lastId };
}

function tmpdb() {
  return createTempDb('honker-node-e2e-', honker.open.bind(honker));
}

// ----------------------------------------------------------------------
// Scenario 1: cross-process correctness — listener (this process, under
// `backend`) opens first; writer subprocess fires N committed
// notifications. Listener must observe every one through updateEvents().
// ----------------------------------------------------------------------

for (const backend of [null, 'kernel', 'shm']) {
  const label = backend === null ? 'polling' : backend;

  test(`watcherBackend=${label} cross-process: listener detects every commit`, async () => {
    const { path: dbPath, open, cleanup } = tmpdb();
    let proc;
    try {
      const db = open(dbPath, undefined, backend);
      // Pre-warm the WAL so the kernel-watcher can attach a per-file
      // -wal watch at startup.
      const tx = db.transaction();
      tx.execute('CREATE TABLE _warm (i INTEGER)');
      tx.commit();
      await new Promise((r) => setTimeout(r, 50));

      const channel = 'orders';
      const n = 10;
      const spacingMs = 15;

      proc = spawnNode(writerScript({ dbPath, channel, n, spacingMs }));
      const nextLine = createLineReader(proc.stdout);
      await nextLine((l) => l === 'READY', 5000);

      // Subscribe BEFORE releasing the writer.
      const ev = db.updateEvents();
      // One tick so the napi-rs blocking task is actually scheduled.
      await new Promise((r) => setTimeout(r, 20));

      proc.stdin.write('go\n');

      const timeoutMs = n * spacingMs + 2000;
      const { seen } = await drainNotifications({
        db, ev, channel, sinceId: 0, n, timeoutMs,
      });
      ev.close();

      const payloads = seen.map((p) => parseInt(p, 10)).sort((a, b) => a - b);
      const expected = Array.from({ length: n }, (_, i) => i);
      assert.deepEqual(
        payloads, expected,
        `backend=${label}: cross-process listener missed notifications`,
      );
    } finally {
      await stopChild(proc);
      cleanup();
    }
  });

  test(`watcherBackend=${label} cross-process: burst load — no missed notifications`, async () => {
    const { path: dbPath, open, cleanup } = tmpdb();
    let proc;
    try {
      const db = open(dbPath, undefined, backend);
      const tx = db.transaction();
      tx.execute('CREATE TABLE _warm (i INTEGER)');
      tx.commit();
      await new Promise((r) => setTimeout(r, 50));

      const channel = 'burst';
      const n = 50;

      proc = spawnNode(writerScript({ dbPath, channel, n, spacingMs: 0 }));
      const nextLine = createLineReader(proc.stdout);
      await nextLine((l) => l === 'READY', 5000);

      const ev = db.updateEvents();
      await new Promise((r) => setTimeout(r, 20));

      proc.stdin.write('go\n');
      // Wait for writer to finish committing. Persisted-row check
      // below is the real assertion; this just bounds the wait.
      await nextLine((l) => l === 'DONE', 5000);

      const { seen } = await drainNotifications({
        db, ev, channel, sinceId: 0, n, timeoutMs: 3000,
      });
      ev.close();

      const persisted = db.query(
        "SELECT payload FROM _honker_notifications WHERE channel = ? ORDER BY id",
        [channel],
      );
      assert.equal(
        persisted.length, n,
        `backend=${label}: persisted ${persisted.length} of ${n} (writer didn't commit them all)`,
      );
      assert.equal(
        seen.length, n,
        `backend=${label}: listener observed ${seen.length} of ${n} notifications`,
      );
    } finally {
      await stopChild(proc);
      cleanup();
    }
  });

  test(`watcherBackend=${label} cross-process: listener survives writer SIGKILL`, async () => {
    const { path: dbPath, open, cleanup } = tmpdb();
    let proc1; let proc2;
    try {
      const db = open(dbPath, undefined, backend);
      const tx = db.transaction();
      tx.execute('CREATE TABLE _warm (i INTEGER)');
      tx.commit();
      await new Promise((r) => setTimeout(r, 50));

      const channel = 'resilience';

      // Single, long-lived listener that must survive the writer epoch
      // boundary. Subscribing now (before any commit) snapshots id=0.
      const ev = db.updateEvents();
      await new Promise((r) => setTimeout(r, 20));

      // Writer #1 emits 5 notifications.
      proc1 = spawnNode(writerScript({ dbPath, channel, n: 5, spacingMs: 20 }));
      const nextLine1 = createLineReader(proc1.stdout);
      await nextLine1((l) => l === 'READY', 5000);
      proc1.stdin.write('go\n');

      // Drain batch 1 BEFORE killing — proves writer #1's notifications
      // were delivered and gives us the last-seen id for batch 2's
      // SELECT cursor.
      const drained1 = await drainNotifications({
        db, ev, channel, sinceId: 0, n: 5, timeoutMs: 2000,
      });
      assert.equal(
        drained1.seen.length, 5,
        `backend=${label}: writer #1 delivery saw ${drained1.seen.length} of 5`,
      );

      // SIGKILL — harshest restart. No stdin drain, no clean exit.
      proc1.kill('SIGKILL');
      await waitForExit(proc1);

      // Writer #2: same db, fresh process. Listener with the watcher
      // still attached should see this batch too.
      proc2 = spawnNode(writerScript({ dbPath, channel, n: 5, spacingMs: 20 }));
      const nextLine2 = createLineReader(proc2.stdout);
      await nextLine2((l) => l === 'READY', 5000);
      proc2.stdin.write('go\n');

      const drained2 = await drainNotifications({
        db, ev, channel, sinceId: drained1.lastId, n: 5, timeoutMs: 2500,
      });
      ev.close();

      assert.equal(
        drained2.seen.length, 5,
        `backend=${label}: writer #2 delivery (after writer #1 SIGKILL) ` +
          `saw ${drained2.seen.length} of 5 — backend may have latched ` +
          `onto a stale file handle`,
      );

      const persisted = db.query(
        "SELECT payload FROM _honker_notifications WHERE channel = ? ORDER BY id",
        [channel],
      );
      assert.equal(
        persisted.length, 10,
        `backend=${label}: persisted ${persisted.length} of 10 across two writer lifetimes`,
      );
    } finally {
      await stopChild(proc1);
      await stopChild(proc2);
      cleanup();
    }
  });
}
