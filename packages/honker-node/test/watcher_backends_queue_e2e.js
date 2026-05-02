// Cross-process queue worker e2e proof for the experimental watcher
// backends, Node side. Mirrors tests/test_watcher_backends_queue_e2e.py
// — three topologies × three backends — but uses the Node binding's
// lower-level queue API (`honker_enqueue` / `honker_claim_batch` /
// `honker_ack_batch` SQL calls) on top of `db.updateEvents()` for wake.
//
// Topologies:
//
//   * 1×1 — one writer subprocess enqueues N, one worker subprocess
//     claims and acks all N.
//   * 1×N — one writer enqueues N, M worker subprocesses compete.
//     Every job processed exactly once with no double-claims.
//   * N×1 — M writer subprocesses each enqueue K; one worker drains
//     all M*K.
//
// Workers exit on a 2 s `Promise.race` timeout against
// `updateEvents().next()` — well below honker's 5 s `idle_poll_s`
// fallback, so a backend that fails to deliver wakes shows up as
// "worker processed 0 jobs" rather than "fell back to polling and
// passed anyway."

'use strict';

const { spawn } = require('node:child_process');
const path = require('node:path');
const test = require('node:test');
const assert = require('node:assert/strict');

const honker = require('..');
const { createTempDb } = require('./helpers');

const REQUIRE_HONKER = path.resolve(__dirname, '..');

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
        () => reject(new Error(`timeout after ${timeoutMs}ms`)),
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

// ---- Worker subprocess: wake-driven claim/ack loop ---------------------
//
// Uses updateEvents() for wakes (under the chosen backend), then
// honker_claim_batch in a transaction. Drains the queue between wakes
// (multiple jobs may arrive per wake under burst). Exits on
// 2 s of no jobs.

function workerScript({ dbPath, workerId, backend, idleExitMs }) {
  return `
'use strict';
const honker = require(${JSON.stringify(REQUIRE_HONKER)});
const db = honker.open(${JSON.stringify(dbPath)}, undefined, ${JSON.stringify(backend)});

function claimOne() {
  const tx = db.transaction();
  let json;
  try {
    json = tx.query(
      "SELECT honker_claim_batch(?, ?, ?, ?) AS j",
      ["shared", ${JSON.stringify(workerId)}, 1, 300],
    )[0].j;
    tx.commit();
  } catch (e) {
    try { tx.rollback(); } catch {}
    throw e;
  }
  const arr = JSON.parse(json);
  return arr.length === 0 ? null : arr[0];
}

function ack(jobId) {
  const tx = db.transaction();
  try {
    tx.query("SELECT honker_ack_batch(?, ?) AS n",
      [JSON.stringify([jobId]), ${JSON.stringify(workerId)}]);
    tx.commit();
  } catch (e) {
    try { tx.rollback(); } catch {}
    throw e;
  }
}

(async () => {
  const ev = db.updateEvents();
  // Microtask hop so the napi blocking task is scheduled.
  await new Promise((r) => setTimeout(r, 20));
  process.stdout.write('READY\\n');

  const processed = [];
  while (true) {
    // Drain everything currently visible before blocking on the next wake.
    let job;
    while ((job = claimOne()) !== null) {
      const payload = JSON.parse(job.payload);
      processed.push(payload.i);
      ack(job.id);
    }
    // Block on the next wake or exit on idle timeout.
    const result = await Promise.race([
      ev.next().then(() => 'wake'),
      new Promise((r) => setTimeout(() => r('idle'), ${idleExitMs})),
    ]);
    if (result === 'idle') break;
  }
  ev.close();
  process.stdout.write('RESULT ' + JSON.stringify(processed) + '\\n');
  db.close();
})().catch((err) => {
  process.stderr.write('worker error: ' + err.stack + '\\n');
  process.exit(1);
});
`;
}

// ---- Writer subprocess: fire-and-forget enqueue + exit ---------------

function writerScript({ dbPath, n, offset }) {
  return `
'use strict';
const honker = require(${JSON.stringify(REQUIRE_HONKER)});
const db = honker.open(${JSON.stringify(dbPath)});
for (let i = ${offset}; i < ${offset} + ${n}; i++) {
  const tx = db.transaction();
  try {
    tx.query("SELECT honker_enqueue(?, ?, ?, ?, ?, ?, ?)",
      ["shared", JSON.stringify({ i }), null, null, 0, 3, null]);
    tx.commit();
  } catch (e) {
    try { tx.rollback(); } catch {}
    throw e;
  }
}
db.close();
`;
}

async function spawnWorker(dbPath, workerId, backend, idleExitMs = 2000) {
  const proc = spawnNode(workerScript({ dbPath, workerId, backend, idleExitMs }));
  proc._lineReader = createLineReader(proc.stdout);
  await proc._lineReader((l) => l === 'READY', 5000);
  return proc;
}

async function waitWorkerResult(proc, workerId, timeoutMs = 25000) {
  const line = await proc._lineReader((l) => l.startsWith('RESULT '), timeoutMs);
  await waitForExit(proc);
  return JSON.parse(line.slice('RESULT '.length));
}

async function runWriter(dbPath, n, offset = 0, timeoutMs = 15000) {
  const proc = spawnNode(writerScript({ dbPath, n, offset }));
  // Drain stdout so the buffer doesn't fill (we don't expect any).
  proc.stdout.on('data', () => {});
  const result = await Promise.race([
    waitForExit(proc),
    new Promise((_, rej) => setTimeout(() => rej(new Error('writer timeout')), timeoutMs)),
  ]).catch(async (e) => {
    proc.kill();
    throw e;
  });
  if (result !== 0) {
    throw new Error(`writer (offset=${offset} n=${n}) exited with code ${result}`);
  }
}

function tmpdb() {
  return createTempDb('honker-node-q-e2e-', honker.open.bind(honker));
}

// Bootstrap the honker schema on a fresh db file. The Python binding
// does this on Database construction; the Node binding leaves it to
// the caller (see packages/honker-node/examples/basic.js).
function bootstrap(dbPath) {
  const db = honker.open(dbPath);
  const tx = db.transaction();
  try {
    tx.query("SELECT honker_bootstrap()");
    tx.commit();
  } catch (e) {
    try { tx.rollback(); } catch {}
    throw e;
  }
  db.close();
}

// ----------------------------------------------------------------------
// Topology 1×1
// ----------------------------------------------------------------------

for (const backend of [null, 'kernel', 'shm']) {
  const label = backend === null ? 'polling' : backend;

  test(`watcherBackend=${label} queue 1×1: writer enqueues, worker drains`, async () => {
    const { path: dbPath, cleanup } = tmpdb();
    let worker;
    try {
      bootstrap(dbPath);
      const n = 25;

      // Worker subscribes first, writer publishes after.
      worker = await spawnWorker(dbPath, 'w1', backend);
      await runWriter(dbPath, n);
      const processed = await waitWorkerResult(worker, 'w1');

      assert.deepEqual(
        processed.slice().sort((a, b) => a - b),
        Array.from({ length: n }, (_, i) => i),
        `backend=${label} 1x1: worker processed wrong set`,
      );
    } finally {
      if (worker && worker.exitCode === null) worker.kill();
      cleanup();
    }
  });

  test(`watcherBackend=${label} queue 1×N: workers split work, no double-claim`, async () => {
    const { path: dbPath, cleanup } = tmpdb();
    const workers = [];
    try {
      bootstrap(dbPath);
      const n = 60;
      const numWorkers = 3;

      for (let i = 0; i < numWorkers; i++) {
        workers.push(await spawnWorker(dbPath, `w${i}`, backend));
      }
      await runWriter(dbPath, n);
      const results = [];
      for (let i = 0; i < numWorkers; i++) {
        results.push(await waitWorkerResult(workers[i], `w${i}`));
      }

      const combined = results.flat().sort((a, b) => a - b);
      const expected = Array.from({ length: n }, (_, i) => i);
      assert.deepEqual(
        combined, expected,
        `backend=${label} 1xN: combined ${combined} != expected ${expected}`,
      );

      // Pairwise disjoint — claim exclusivity.
      for (let i = 0; i < numWorkers; i++) {
        for (let j = i + 1; j < numWorkers; j++) {
          const a = new Set(results[i]);
          const b = new Set(results[j]);
          const overlap = [...a].filter((x) => b.has(x));
          assert.equal(
            overlap.length, 0,
            `backend=${label} 1xN: workers w${i} and w${j} double-claimed: ${overlap}`,
          );
        }
      }
      // Sanity: each worker took at least one — proves wake distribution
      // actually fanned out, not "all jobs to worker 0 by accident."
      for (let i = 0; i < numWorkers; i++) {
        assert.ok(
          results[i].length > 0,
          `backend=${label} 1xN: w${i} got 0 jobs (per-worker counts: ` +
            `${results.map((r) => r.length).join(',')})`,
        );
      }
    } finally {
      for (const w of workers) {
        if (w.exitCode === null) w.kill();
      }
      cleanup();
    }
  });

  test(`watcherBackend=${label} queue N×1: many writers, one worker drains all`, async () => {
    const { path: dbPath, cleanup } = tmpdb();
    let worker;
    try {
      bootstrap(dbPath);
      const numWriters = 3;
      const perWriter = 15;
      const total = numWriters * perWriter;

      worker = await spawnWorker(dbPath, 'w1', backend, 3000);
      // Writers in parallel.
      await Promise.all(
        Array.from({ length: numWriters }, (_, i) =>
          runWriter(dbPath, perWriter, i * perWriter)
        ),
      );
      const processed = await waitWorkerResult(worker, 'w1');

      assert.deepEqual(
        processed.slice().sort((a, b) => a - b),
        Array.from({ length: total }, (_, i) => i),
        `backend=${label} Nx1: worker processed wrong set`,
      );
    } finally {
      if (worker && worker.exitCode === null) worker.kill();
      cleanup();
    }
  });
}
