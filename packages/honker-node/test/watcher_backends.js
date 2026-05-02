// Cross-language proof for the experimental watcher backends, Node side.
//
// Mirrors tests/test_watcher_backends.py: opens a Database with each
// backend, drives a workload, verifies updateEvents() fires through the
// public Node API. Wheels built without the corresponding Cargo features
// silently fall back to polling, so a missing feature shows up as
// "looks like polling," not a crash.
//
// A control assertion runs the same workload against the default polling
// backend so a regression in the experimental path stands out from
// test-environment flakiness.

const test = require('node:test');
const assert = require('node:assert/strict');

const lit = require('..');
const { createTempDb } = require('./helpers');

function tmpdb() {
  return createTempDb('honker-node-watchers-', lit.open.bind(lit));
}

async function driveCommitsAndCountWakes(db, n, spacingMs) {
  // Force WAL to exist before subscribing so the kernel-watcher can
  // attach a per-file watch on the -wal at startup.
  {
    const tx = db.transaction();
    tx.execute('CREATE TABLE IF NOT EXISTS t (x INTEGER)');
    tx.commit();
  }
  await new Promise((r) => setTimeout(r, 50));

  const ev = db.updateEvents();
  let counted = 0;
  let stop = false;
  // Track when the consumer has actually entered its first `await ev.next()`
  // so we don't race the first commit against the napi runtime startup.
  let consumerReady;
  const ready = new Promise((r) => { consumerReady = r; });
  const consumer = (async () => {
    let firstIter = true;
    while (!stop) {
      try {
        if (firstIter) {
          // Signal readiness on the same microtask we begin awaiting.
          consumerReady();
          firstIter = false;
        }
        await ev.next();
        counted += 1;
      } catch {
        return;
      }
    }
  })();
  await ready;
  // One more tick so the napi blocking task is actually scheduled.
  await new Promise((r) => setTimeout(r, 20));

  for (let i = 0; i < n; i++) {
    const tx = db.transaction();
    tx.execute('INSERT INTO t (x) VALUES (?)', [i]);
    tx.commit();
    await new Promise((r) => setTimeout(r, spacingMs));
  }

  // Wait long enough for the slowest backend's safety net (500 ms) plus
  // event delivery and the napi-rs Promise round-trip latency.
  await new Promise((r) => setTimeout(r, 800));
  stop = true;
  ev.close();
  try { await consumer; } catch {}
  return counted;
}

for (const backend of [null, 'kernel', 'shm']) {
  const label = backend === null ? 'polling (default)' : backend;
  test(`watcherBackend=${label} detects commits`, async () => {
    const { path: dbPath, open, cleanup } = tmpdb();
    let db;
    try {
      db = open(dbPath, undefined, backend);
      const n = 4;
      const counted = await driveCommitsAndCountWakes(db, n, 30);
      assert.ok(
        counted >= n,
        `watcherBackend=${label}: only ${counted} wakes for ${n} commits`,
      );
      assert.ok(
        counted <= n + 2,
        `watcherBackend=${label}: ${counted} wakes for ${n} commits exceeds bound`,
      );
    } finally {
      cleanup();
    }
  });
}

test('unknown watcherBackend throws', () => {
  const { path: dbPath, cleanup } = tmpdb();
  try {
    assert.throws(
      () => lit.open(dbPath, undefined, 'bogus'),
      /unknown watcherBackend/,
    );
  } finally {
    cleanup();
  }
});
