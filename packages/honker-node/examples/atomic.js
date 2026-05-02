// Atomic business-write + enqueue in one transaction.
//
// This is honker's killer feature vs Redis/BullMQ: the job enqueue
// commits in the same SQLite transaction as your business INSERT.
// Rollback drops both. No dual-write, no outbox table.
//
//   node examples/atomic.js

const lit = require('..');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const assert = require('node:assert/strict');

const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'honker-'));
const db = lit.open(path.join(dir, 'app.db'));

try {
  // One-time: bootstrap honker + create a plain business table.
  {
    const tx = db.transaction();
    tx.query("SELECT honker_bootstrap()");
    tx.execute(
      "CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER, total INTEGER)",
    );
    tx.commit();
  }

  // Success path: order INSERT and job enqueue commit together.
  {
    const tx = db.transaction();
    tx.execute(
      "INSERT INTO orders (user_id, total) VALUES (?, ?)",
      [42, 9900],
    );
    tx.query(
      "SELECT honker_enqueue(?, ?, ?, ?, ?, ?, ?)",
      ['emails', JSON.stringify({ to: 'alice@example.com', order_id: 42 }),
       null, null, 0, 3, null],
    );
    tx.commit();
  }

  {
    const orders = db.query("SELECT id FROM orders");
    const queued = db.query("SELECT payload FROM _honker_live WHERE queue='emails'");
    console.log(`committed: ${orders.length} order(s), ${queued.length} job(s)`);
    assert.equal(orders.length, 1);
    assert.equal(queued.length, 1);
  }

  // Rollback path: business logic fails, both writes disappear.
  try {
    const tx = db.transaction();
    tx.execute(
      "INSERT INTO orders (user_id, total) VALUES (?, ?)",
      [43, 5000],
    );
    tx.query(
      "SELECT honker_enqueue(?, ?, ?, ?, ?, ?, ?)",
      ['emails', JSON.stringify({ to: 'bob@example.com', order_id: 43 }),
       null, null, 0, 3, null],
    );
    throw new Error("boom — simulated payment-processing failure");
    // tx.commit() would have been here, but the throw jumps to catch
  } catch (e) {
    console.log(`rolled back: ${e.message}`);
    // Transaction auto-rolls back when the object goes out of scope
    // without .commit(). We don't need to do anything here.
  }

  {
    const orders = db.query("SELECT id FROM orders");
    const queued = db.query("SELECT payload FROM _honker_live WHERE queue='emails'");
    console.log(`after rollback: ${orders.length} order(s), ${queued.length} job(s)`);
    assert.equal(orders.length, 1);
    assert.equal(queued.length, 1);
  }

  console.log("atomic enqueue + rollback both work as expected");
} finally {
  fs.rmSync(dir, { recursive: true, force: true });
}
