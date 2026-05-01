// Basic: enqueue a job, claim it, ack, via the honker_* SQL functions.
//
//   node examples/basic.js
//
// All honker_* functions must run against the writer connection, so
// we always wrap their calls in a transaction.

const lit = require('..');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');

const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'honker-'));
const db = lit.open(path.join(dir, 'app.db'));

try {
  // Install the honker schema. The Python binding does this
  // automatically on Database creation; the Node binding is a
  // lower-level primitive layer and expects the caller to bootstrap.
  {
    const tx = db.transaction();
    tx.query("SELECT honker_bootstrap()");
    tx.commit();
  }

  // Enqueue.
  {
    const tx = db.transaction();
    const id = tx.query(
      "SELECT honker_enqueue(?, ?, ?, ?, ?, ?, ?) AS id",
      ['emails', JSON.stringify({ to: 'alice@example.com' }),
       null, null, 0, 3, null],
    )[0].id;
    tx.commit();
    console.log(`enqueued job id=${id}`);
  }

  // Claim.
  let job;
  {
    const tx = db.transaction();
    const rowsJson = tx.query(
      "SELECT honker_claim_batch(?, ?, ?, ?) AS j",
      ['emails', 'worker-1', 1, 300],
    )[0].j;
    tx.commit();
    job = JSON.parse(rowsJson)[0];
    console.log(`claimed job id=${job.id}, payload=`, JSON.parse(job.payload));
  }

  // Ack.
  {
    const tx = db.transaction();
    const n = tx.query(
      "SELECT honker_ack_batch(?, ?) AS n",
      [JSON.stringify([job.id]), job.worker_id],
    )[0].n;
    tx.commit();
    console.log(`acked ${n} job(s)`);
  }
} finally {
  fs.rmSync(dir, { recursive: true, force: true });
}
