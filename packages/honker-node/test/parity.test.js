// Parity tests for the Node wrapper — mirrors the surface of the
// Rust / Python / Ruby / Elixir bindings. Covers queues, transactions,
// streams, listen, scheduler, locks, rate limits, results, and the
// claim-waker. Run with `node --test test/parity.test.js`.

'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');

const honker = require('..');
const realOpen = honker.open.bind(honker);

const deferredCleanupDirs = new Set();
let deferredCleanupInstalled = false;
const trackedDbsByPath = new Map();

function installDeferredCleanup() {
  if (deferredCleanupInstalled) return;
  deferredCleanupInstalled = true;
  process.on('exit', () => {
    for (const dir of deferredCleanupDirs) {
      try {
        fs.rmSync(dir, { recursive: true, force: true });
      } catch {}
    }
    deferredCleanupDirs.clear();
  });
}

honker.open = function trackedOpen(dbPath, ...args) {
  const db = realOpen(dbPath, ...args);
  const tracked = trackedDbsByPath.get(dbPath);
  if (tracked) tracked.push(db);
  return db;
};

function tmpdb() {
  installDeferredCleanup();
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'honker-parity-'));
  const dbPath = path.join(dir, 't.db');
  trackedDbsByPath.set(dbPath, []);
  return {
    path: dbPath,
    cleanup: () => {
      const tracked = trackedDbsByPath.get(dbPath) || [];
      trackedDbsByPath.delete(dbPath);
      while (tracked.length) {
        const db = tracked.pop();
        try {
          db.close?.();
        } catch {}
      }
      deferredCleanupDirs.add(dir);
    },
  };
}

// ---------------------------------------------------------------------
// Queue — enqueue / claim / ack / retry / fail / heartbeat
// ---------------------------------------------------------------------

test('queue: enqueue + claimOne + ack', () => {
  const { path: p, cleanup } = tmpdb();
  try {
    const db = honker.open(p);
    const q = db.queue('emails');
    const id = q.enqueue({ to: 'alice@example.com' });
    assert.ok(id > 0);
    const job = q.claimOne('w1');
    assert.ok(job);
    assert.equal(job.queue, 'emails');
    assert.deepEqual(job.payload, { to: 'alice@example.com' });
    assert.equal(job.workerId, 'w1');
    assert.equal(job.attempts, 1);
    assert.equal(job.ack(), true);
    assert.equal(q.claimOne('w1'), null);
  } finally {
    cleanup();
  }
});

test('queue: claimBatch with multiple jobs', () => {
  const { path: p, cleanup } = tmpdb();
  try {
    const db = honker.open(p);
    const q = db.queue('batch');
    for (let i = 0; i < 5; i++) q.enqueue({ i });
    const jobs = q.claimBatch('w1', 3);
    assert.equal(jobs.length, 3);
    const ids = jobs.map((j) => j.id);
    const acked = q.ackBatch(ids, 'w1');
    assert.equal(acked, 3);
    const leftover = q.claimBatch('w1', 10);
    assert.equal(leftover.length, 2);
  } finally {
    cleanup();
  }
});

test('queue: retry + fail semantics', () => {
  const { path: p, cleanup } = tmpdb();
  try {
    const db = honker.open(p);
    const q = db.queue('retries', { maxAttempts: 2 });
    q.enqueue({ x: 1 });
    const j1 = q.claimOne('w1');
    assert.ok(j1);
    // Retry with a small delay so it's immediately re-claimable once
    // the delay elapses. Here we set 0 for test simplicity.
    assert.equal(j1.retry(0, 'transient'), true);
    const j2 = q.claimOne('w2');
    assert.ok(j2);
    assert.equal(j2.attempts, 2);
    // fail() moves it to _honker_dead
    assert.equal(j2.fail('permanent'), true);
    const dead = db.query('SELECT COUNT(*) AS c FROM _honker_dead');
    assert.equal(dead[0].c, 1);
  } finally {
    cleanup();
  }
});

test('queue: heartbeat extends visibility', () => {
  const { path: p, cleanup } = tmpdb();
  try {
    const db = honker.open(p);
    const q = db.queue('hb', { visibilityTimeoutS: 1 });
    q.enqueue({});
    const j = q.claimOne('w1');
    assert.ok(j);
    assert.equal(j.heartbeat(60), true);
  } finally {
    cleanup();
  }
});

test('queue: enqueueTx atomic with business write (commit)', () => {
  const { path: p, cleanup } = tmpdb();
  try {
    const db = honker.open(p);
    const q = db.queue('atomic');
    {
      const tx = db.transaction();
      tx.execute('CREATE TABLE orders (id INTEGER PRIMARY KEY)');
      tx.commit();
    }
    const tx = db.transaction();
    tx.execute('INSERT INTO orders (id) VALUES (?)', [42]);
    q.enqueueTx(tx, { order_id: 42 });
    tx.commit();
    const orders = db.query('SELECT id FROM orders');
    assert.equal(orders.length, 1);
    const jobs = db.query("SELECT id FROM _honker_live WHERE queue='atomic'");
    assert.equal(jobs.length, 1);
  } finally {
    cleanup();
  }
});

test('queue: enqueueTx with rollback drops both writes', () => {
  const { path: p, cleanup } = tmpdb();
  try {
    const db = honker.open(p);
    const q = db.queue('rollback');
    {
      const tx = db.transaction();
      tx.execute('CREATE TABLE orders (id INTEGER PRIMARY KEY)');
      tx.commit();
    }
    const tx = db.transaction();
    tx.execute('INSERT INTO orders (id) VALUES (?)', [42]);
    q.enqueueTx(tx, { order_id: 42 });
    tx.rollback();
    const orders = db.query('SELECT id FROM orders');
    assert.equal(orders.length, 0);
    const jobs = db.query("SELECT id FROM _honker_live WHERE queue='rollback'");
    assert.equal(jobs.length, 0);
  } finally {
    cleanup();
  }
});

test('queue: enqueue with opts.tx routes through transaction', () => {
  const { path: p, cleanup } = tmpdb();
  try {
    const db = honker.open(p);
    const q = db.queue('tx-opt');
    const tx = db.transaction();
    q.enqueue({ v: 1 }, { tx });
    tx.rollback();
    assert.equal(
      db.query("SELECT COUNT(*) AS c FROM _honker_live WHERE queue='tx-opt'")[0].c,
      0,
    );
  } finally {
    cleanup();
  }
});

test('queue: sweepExpired returns a count', () => {
  const { path: p, cleanup } = tmpdb();
  try {
    const db = honker.open(p);
    const q = db.queue('sweep');
    q.enqueue({});
    const n = q.sweepExpired();
    assert.ok(n >= 0);
  } finally {
    cleanup();
  }
});

// ---------------------------------------------------------------------
// Stream — publish / readSince / consumer offsets
// ---------------------------------------------------------------------

test('stream: publish + readSince', () => {
  const { path: p, cleanup } = tmpdb();
  try {
    const db = honker.open(p);
    const s = db.stream('events');
    const o1 = s.publish({ e: 1 });
    const o2 = s.publish({ e: 2 });
    assert.ok(o2 > o1);
    const evs = s.readSince(0, 10);
    assert.equal(evs.length, 2);
    assert.deepEqual(evs[0].payload, { e: 1 });
    assert.equal(evs[0].topic, 'events');
    assert.equal(evs[0].key, null);
  } finally {
    cleanup();
  }
});

test('stream: publishWithKey', () => {
  const { path: p, cleanup } = tmpdb();
  try {
    const db = honker.open(p);
    const s = db.stream('events');
    s.publishWithKey('alice', { msg: 'hi' });
    const evs = s.readSince(0, 10);
    assert.equal(evs[0].key, 'alice');
  } finally {
    cleanup();
  }
});

test('stream: consumer offsets + readFromConsumer', () => {
  const { path: p, cleanup } = tmpdb();
  try {
    const db = honker.open(p);
    const s = db.stream('events');
    for (let i = 0; i < 5; i++) s.publish({ i });
    assert.equal(s.getOffset('c1'), 0);
    const evs1 = s.readFromConsumer('c1', 3);
    assert.equal(evs1.length, 3);
    s.saveOffset('c1', evs1[evs1.length - 1].offset);
    const evs2 = s.readFromConsumer('c1', 10);
    assert.equal(evs2.length, 2);
  } finally {
    cleanup();
  }
});

test('stream: saveOffsetTx respects rollback', () => {
  const { path: p, cleanup } = tmpdb();
  try {
    const db = honker.open(p);
    const s = db.stream('events');
    s.publish({ i: 1 });
    s.publish({ i: 2 });
    const tx = db.transaction();
    s.saveOffsetTx(tx, 'c1', 2);
    tx.rollback();
    assert.equal(s.getOffset('c1'), 0);
  } finally {
    cleanup();
  }
});

test('stream: publishTx honors transaction atomicity', () => {
  const { path: p, cleanup } = tmpdb();
  try {
    const db = honker.open(p);
    const s = db.stream('tx-events');
    const tx = db.transaction();
    s.publishTx(tx, { v: 1 });
    tx.rollback();
    assert.equal(s.readSince(0, 10).length, 0);
  } finally {
    cleanup();
  }
});

// ---------------------------------------------------------------------
// Listen — pub/sub cross-"thread" wakeup
// ---------------------------------------------------------------------

test('listen: fires asynchronously; filters by channel', async () => {
  const { path: p, cleanup } = tmpdb();
  try {
    const db = honker.open(p);
    const sub = db.listen('orders');
    // Emit a notification on the event loop — simulates another
    // "thread" writing. Node is single-threaded; the setTimeout is
    // the closest analog to a concurrent producer.
    setTimeout(() => {
      db.notify('other', { ignored: true });
      db.notify('orders', { id: 42 });
    }, 50);
    const deadline = Date.now() + 2000;
    const iter = sub[Symbol.asyncIterator]();
    const winner = await Promise.race([
      iter.next(),
      new Promise((resolve) =>
        setTimeout(() => resolve({ done: true, value: null }), 2000),
      ),
    ]);
    assert.ok(Date.now() < deadline);
    assert.equal(winner.done, false);
    assert.equal(winner.value.channel, 'orders');
    assert.deepEqual(winner.value.payload, { id: 42 });
    sub.close();
  } finally {
    cleanup();
  }
});

test('database.notify commits outside an open transaction', () => {
  const { path: p, cleanup } = tmpdb();
  try {
    const db = honker.open(p);
    const id = db.notify('ch', { hello: 'world' });
    assert.ok(id > 0);
    const rows = db.query(
      "SELECT payload FROM _honker_notifications WHERE channel='ch'",
    );
    assert.equal(rows.length, 1);
    assert.deepEqual(JSON.parse(rows[0].payload), { hello: 'world' });
  } finally {
    cleanup();
  }
});

// ---------------------------------------------------------------------
// Scheduler
// ---------------------------------------------------------------------

test('scheduler: add / soonest / tick', () => {
  const { path: p, cleanup } = tmpdb();
  try {
    const db = honker.open(p);
    const sched = db.scheduler();
    sched.add({
      name: 'every-minute',
      queue: 'q',
      schedule: '* * * * *',
      payload: { hello: 'world' },
    });
    const soonest = sched.soonest();
    assert.ok(soonest > 0, `soonest was ${soonest}`);
    // tick() should be non-error even if no boundary has elapsed
    const fires = sched.tick();
    assert.ok(Array.isArray(fires));
  } finally {
    cleanup();
  }
});

test('scheduler: remove returns deletion count', () => {
  const { path: p, cleanup } = tmpdb();
  try {
    const db = honker.open(p);
    const sched = db.scheduler();
    sched.add({
      name: 'doomed',
      queue: 'q',
      schedule: '* * * * *',
      payload: null,
    });
    assert.equal(sched.remove('doomed'), 1);
    assert.equal(sched.remove('doomed'), 0);
  } finally {
    cleanup();
  }
});

test('scheduler: run + AbortSignal stops the loop', async () => {
  const { path: p, cleanup } = tmpdb();
  try {
    const db = honker.open(p);
    const sched = db.scheduler();
    sched.add({
      name: 'spin',
      queue: 'q',
      schedule: '* * * * *',
      payload: {},
    });
    const ctrl = new AbortController();
    const started = Date.now();
    const runPromise = sched.run('owner-1', ctrl.signal);
    // Let the leader loop actually acquire the lock and tick once.
    await new Promise((r) => setTimeout(r, 200));
    ctrl.abort();
    await runPromise;
    const elapsed = Date.now() - started;
    assert.ok(elapsed < 2500, `expected fast shutdown; took ${elapsed}ms`);
  } finally {
    cleanup();
  }
});

test('scheduler: accepts legacy cron alias', () => {
  const { path: p, cleanup } = tmpdb();
  try {
    const db = honker.open(p);
    const sched = db.scheduler();
    sched.add({
      name: 'legacy',
      queue: 'q',
      cron: '@every 1s',
      payload: { legacy: true },
    });
    assert.ok(sched.soonest() > 0);
  } finally {
    cleanup();
  }
});

test('scheduler: accepts every-second schedule', () => {
  const { path: p, cleanup } = tmpdb();
  try {
    const db = honker.open(p);
    const sched = db.scheduler();
    sched.add({
      name: 'fast',
      queue: 'q',
      schedule: '@every 1s',
      payload: { ok: true },
    });
    const soonest = sched.soonest();
    assert.ok(soonest > 0);
    const fires = sched.tick(soonest);
    assert.equal(fires.length, 1);
    assert.equal(fires[0].name, 'fast');
  } finally {
    cleanup();
  }
});

// ---------------------------------------------------------------------
// Locks
// ---------------------------------------------------------------------

test('lock: mutual exclusion between owners', () => {
  const { path: p, cleanup } = tmpdb();
  try {
    const db = honker.open(p);
    const l = db.tryLock('resource', 'a', 60);
    assert.ok(l);
    const collider = db.tryLock('resource', 'b', 60);
    assert.equal(collider, null);
    assert.equal(l.release(), true);
    const second = db.tryLock('resource', 'b', 60);
    assert.ok(second);
    second.release();
  } finally {
    cleanup();
  }
});

test('lock: heartbeat keeps ownership', () => {
  const { path: p, cleanup } = tmpdb();
  try {
    const db = honker.open(p);
    const l = db.tryLock('beat', 'a', 60);
    assert.ok(l);
    assert.equal(l.heartbeat(120), true);
    l.release();
  } finally {
    cleanup();
  }
});

test('lock: release is idempotent', () => {
  const { path: p, cleanup } = tmpdb();
  try {
    const db = honker.open(p);
    const l = db.tryLock('idem', 'a', 60);
    assert.ok(l);
    assert.equal(l.release(), true);
    assert.equal(l.release(), false);
  } finally {
    cleanup();
  }
});

// ---------------------------------------------------------------------
// Rate limit
// ---------------------------------------------------------------------

test('rate limit: allows up to limit then blocks', () => {
  const { path: p, cleanup } = tmpdb();
  try {
    const db = honker.open(p);
    for (let i = 0; i < 3; i++) {
      assert.equal(db.tryRateLimit('rl', 3, 60), true);
    }
    assert.equal(db.tryRateLimit('rl', 3, 60), false);
  } finally {
    cleanup();
  }
});

// ---------------------------------------------------------------------
// Results
// ---------------------------------------------------------------------

test('results: save / get / missing returns null', () => {
  const { path: p, cleanup } = tmpdb();
  try {
    const db = honker.open(p);
    db.saveResult(1, JSON.stringify({ status: 'ok' }), 60);
    const v = db.getResult(1);
    assert.deepEqual(JSON.parse(v), { status: 'ok' });
    assert.equal(db.getResult(999), null);
    const swept = db.sweepResults();
    assert.ok(swept >= 0);
  } finally {
    cleanup();
  }
});

// ---------------------------------------------------------------------
// claimWaker — wakes on enqueue from another "thread" (setTimeout)
// ---------------------------------------------------------------------

test('claimWaker: wakes on enqueue from setImmediate', async () => {
  const { path: p, cleanup } = tmpdb();
  try {
    const db = honker.open(p);
    const q = db.queue('waker');
    const waker = q.claimWaker();
    setImmediate(() => {
      q.enqueue({ hello: 'waker' });
    });
    const t0 = Date.now();
    const job = await Promise.race([
      waker.next('w1'),
      new Promise((resolve) => setTimeout(() => resolve(null), 3000)),
    ]);
    const dt = Date.now() - t0;
    assert.ok(job, 'claimWaker timed out');
    assert.deepEqual(job.payload, { hello: 'waker' });
    assert.ok(dt < 2500, `claimWaker wake took ${dt}ms`);
    job.ack();
    waker.close();
  } finally {
    cleanup();
  }
});

test('claimWaker: returns immediately when a job is already pending', async () => {
  const { path: p, cleanup } = tmpdb();
  try {
    const db = honker.open(p);
    const q = db.queue('prewarm');
    q.enqueue({ x: 1 });
    const waker = q.claimWaker();
    const job = await waker.next('w1');
    assert.ok(job);
    assert.deepEqual(job.payload, { x: 1 });
    job.ack();
    waker.close();
  } finally {
    cleanup();
  }
});

test('claimWaker: wakes when runAt deadline arrives', async () => {
  const { path: p, cleanup } = tmpdb();
  try {
    const db = honker.open(p);
    const q = db.queue('deadline');
    const runAt = Math.floor(Date.now() / 1000) + 2;
    const msUntilDue = runAt * 1000 - Date.now();
    q.enqueue({ hello: 'future' }, { runAt });
    const waker = q.claimWaker({ idlePollS: 30 });
    const t0 = Date.now();
    const job = await Promise.race([
      waker.next('w1'),
      new Promise((resolve) => setTimeout(() => resolve(null), 5000)),
    ]);
    const dt = Date.now() - t0;
    assert.ok(job, 'claimWaker timed out waiting for runAt job');
    assert.deepEqual(job.payload, { hello: 'future' });
    assert.ok(
      dt >= Math.max(0, msUntilDue - 250),
      `runAt wake came too early: ${dt}ms (expected about ${msUntilDue}ms)`,
    );
    assert.ok(
      dt <= msUntilDue + 2500,
      `runAt wake came too late: ${dt}ms (expected about ${msUntilDue}ms)`,
    );
    job.ack();
    waker.close();
  } finally {
    cleanup();
  }
});

// ---------------------------------------------------------------------
// Stream subscription — async iterator, WAL wake, auto-save
// ---------------------------------------------------------------------

test('stream subscribe: async iterates and saves offset on close', async () => {
  const { path: p, cleanup } = tmpdb();
  try {
    const db = honker.open(p);
    const s = db.stream('sub');
    s.publish({ n: 1 });
    s.publish({ n: 2 });
    const sub = s.subscribe('consumer-a');
    const got = [];
    const iter = sub[Symbol.asyncIterator]();
    // Drain 2 events then close.
    const v1 = await Promise.race([
      iter.next(),
      new Promise((r) => setTimeout(() => r({ done: true }), 2000)),
    ]);
    assert.equal(v1.done, false);
    got.push(v1.value);
    const v2 = await Promise.race([
      iter.next(),
      new Promise((r) => setTimeout(() => r({ done: true }), 2000)),
    ]);
    assert.equal(v2.done, false);
    got.push(v2.value);
    assert.equal(got.length, 2);
    sub.close();
    // After close the consumer's offset should be persisted.
    assert.equal(s.getOffset('consumer-a'), got[got.length - 1].offset);
  } finally {
    cleanup();
  }
});

// ---------------------------------------------------------------------
// Transaction helpers — commit / rollback idempotency
// ---------------------------------------------------------------------

test('transaction: commit then rollback is a no-op', () => {
  const { path: p, cleanup } = tmpdb();
  try {
    const db = honker.open(p);
    const tx = db.transaction();
    tx.execute('CREATE TABLE t (v INTEGER)');
    tx.commit();
    // Second commit/rollback should be no-ops (not throw).
    tx.commit();
    tx.rollback();
  } finally {
    cleanup();
  }
});

test('notifyTx convenience matches tx.notify', () => {
  const { path: p, cleanup } = tmpdb();
  try {
    const db = honker.open(p);
    const tx = db.transaction();
    const id = db.notifyTx(tx, 'ch', { hi: true });
    assert.ok(id > 0);
    tx.commit();
    const rows = db.query(
      "SELECT payload FROM _honker_notifications WHERE channel='ch'",
    );
    assert.equal(rows.length, 1);
    assert.deepEqual(JSON.parse(rows[0].payload), { hi: true });
  } finally {
    cleanup();
  }
});
