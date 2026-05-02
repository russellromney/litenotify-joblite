// Smoke test for the Node binding. Run with `node --test test/`.
const test = require('node:test');
const assert = require('node:assert/strict');

const lit = require('..');
const { createTempDb } = require('./helpers');

// `createTempDb` wraps every Database/UpdateEvents/etc. it hands out
// so `cleanup()` can close them before unlinking the tempdir. Required
// on Windows: SQLite holds the file handle, and `rmSync` hits EBUSY
// until every Connection has been dropped. The wrapper makes the
// "always close before unlink" rule structural rather than something
// each test has to remember.
function tmpdb() {
  return createTempDb('honker-node-', lit.open.bind(lit));
}

test('open / transaction / commit', () => {
  const { path: dbPath, open, cleanup } = tmpdb();
  let db;
  try {
    db = open(dbPath);
    const tx = db.transaction();
    tx.execute('CREATE TABLE t (id INTEGER PRIMARY KEY, payload TEXT)');
    tx.execute('INSERT INTO t (payload) VALUES (?)', ['hello']);
    tx.commit();
    const rows = db.query('SELECT id, payload FROM t ORDER BY id');
    assert.equal(rows.length, 1);
    assert.equal(rows[0].payload, 'hello');
  } finally {
    db?.close();
    cleanup();
  }
});

test('rollback drops writes', () => {
  const { path: dbPath, open, cleanup } = tmpdb();
  let db;
  try {
    db = open(dbPath);
    {
      const tx = db.transaction();
      tx.execute('CREATE TABLE t (v INTEGER)');
      tx.commit();
    }
    {
      const tx = db.transaction();
      tx.execute('INSERT INTO t (v) VALUES (1)');
      tx.rollback();
    }
    const rows = db.query('SELECT COUNT(*) AS c FROM t');
    assert.equal(rows[0].c, 0);
  } finally {
    db?.close();
    cleanup();
  }
});

test('notify inserts into _honker_notifications and rollback drops it', () => {
  const { path: dbPath, open, cleanup } = tmpdb();
  let db;
  try {
    db = open(dbPath);

    // Committed notify. payload is any JSON-serializable value —
    // stringified inside the binding, matches Python's json.dumps.
    {
      const tx = db.transaction();
      const id = tx.notify('orders', { id: 42 });
      assert.ok(id > 0);
      tx.commit();
    }
    // Rolled-back notify
    {
      const tx = db.transaction();
      tx.notify('orders', { id: 99 });
      tx.rollback();
    }
    const rows = db.query(
      "SELECT channel, payload FROM _honker_notifications WHERE channel='orders' ORDER BY id"
    );
    assert.equal(rows.length, 1);
    assert.equal(rows[0].channel, 'orders');
    assert.deepEqual(JSON.parse(rows[0].payload), { id: 42 });
  } finally {
    db?.close();
    cleanup();
  }
});

test('notify payload round-trips common JSON shapes', () => {
  const { path: dbPath, open, cleanup } = tmpdb();
  let db;
  try {
    db = open(dbPath);
    const cases = [
      { id: 42, name: 'alice' },  // object
      [1, 2, 3],                   // array
      'hello',                     // string
      42,                          // integer
      3.14,                        // float
      null,                        // null
      true,                        // bool
    ];
    {
      const tx = db.transaction();
      for (const p of cases) tx.notify('rt', p);
      tx.commit();
    }
    const rows = db.query(
      "SELECT payload FROM _honker_notifications WHERE channel='rt' ORDER BY id"
    );
    const decoded = rows.map((r) => JSON.parse(r.payload));
    assert.deepEqual(decoded, cases);
  } finally {
    db?.close();
    cleanup();
  }
});

test('updateEvents fires on commit', async () => {
  const { path: dbPath, open, cleanup } = tmpdb();
  let db;
  try {
    db = open(dbPath);
    // Force WAL file to exist by doing a first commit.
    {
      const tx = db.transaction();
      tx.execute('CREATE TABLE t (n INTEGER)');
      tx.commit();
    }
    const ev = db.updateEvents();
    // Fire a commit after a short delay; updateEvents.next() should resolve.
    setTimeout(() => {
      const tx = db.transaction();
      tx.execute('INSERT INTO t (n) VALUES (1)');
      tx.commit();
    }, 50);
    const t0 = performance.now();
    await ev.next();
    const dt = performance.now() - t0;
    assert.ok(dt < 500, `WAL wake took ${dt.toFixed(1)}ms, expected < 500`);
    ev.close();
  } finally {
    db?.close();
    cleanup();
  }
});

test('updateEvents dropped without close() still releases the watcher thread', async () => {
  // Create+drop many UpdateEvents instances without calling .close() on
  // any of them. Dropping must cascade to the core UpdateWatcher's Drop,
  // which signals stop to the update watcher thread. Without Drop semantics,
  // 100 abandoned UpdateEvents = 100 stuck threads.
  const { path: dbPath, open, cleanup } = tmpdb();
  let db;
  try {
    db = open(dbPath);
    // Force the WAL to exist.
    {
      const tx = db.transaction();
      tx.execute('CREATE TABLE t (n INTEGER)');
      tx.commit();
    }

    for (let i = 0; i < 100; i++) {
      const ev = db.updateEvents();
      // Let the thread spawn; don't call close(). Abandon and move on.
      void ev;
    }

    // Give GC a chance and the stop flags a tick to propagate.
    if (global.gc) global.gc();
    await new Promise((r) => setTimeout(r, 100));

    // Sanity: a fresh updateEvents still wakes on commit. If prior watchers
    // somehow deadlocked the poll infrastructure this would time out.
    const ev = db.updateEvents();
    setTimeout(() => {
      const tx = db.transaction();
      tx.execute('INSERT INTO t (n) VALUES (1)');
      tx.commit();
    }, 50);
    const t0 = performance.now();
    await ev.next();
    const dt = performance.now() - t0;
    assert.ok(dt < 1000, `post-churn WAL wake took ${dt.toFixed(1)}ms`);
    ev.close();
  } finally {
    db?.close();
    cleanup();
  }
});

test('pruneNotifications by max_keep', () => {
  const { path: dbPath, open, cleanup } = tmpdb();
  let db;
  try {
    db = open(dbPath);
    for (let i = 0; i < 50; i++) {
      const tx = db.transaction();
      tx.notify('ch', `n${i}`);
      tx.commit();
    }
    const before = db.query('SELECT COUNT(*) AS c FROM _honker_notifications');
    assert.equal(before[0].c, 50);
    const deleted = db.pruneNotifications(null, 5);
    assert.equal(deleted, 45);
    const after = db.query('SELECT COUNT(*) AS c FROM _honker_notifications');
    assert.equal(after[0].c, 5);
  } finally {
    db?.close();
    cleanup();
  }
});
