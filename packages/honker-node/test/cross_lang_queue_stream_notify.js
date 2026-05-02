'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');

const honker = require('..');
const { createTempDb } = require('./helpers');
const { PACKAGES, spawnPython } = require('./cross_lang_shared');

test('direct proof: node and python share queue, stream, and notify rows', async () => {
  const { path: dbPath, open, cleanup } = createTempDb(
    'xlang-node-py-contract-',
    honker.open.bind(honker),
  );
  let db;
  try {
    db = open(dbPath);
    const nodeQueue = db.queue('node-to-python');
    for (let i = 0; i < 25; i += 1) {
      nodeQueue.enqueue({
        source: 'node',
        seq: i,
        key: `node-${String(i).padStart(2, '0')}`,
      });
    }
    db.notify('from-node', { source: 'node', count: 25 });
    db.stream('interop').publish({ source: 'node', kind: 'stream' });

    const pyScript = `
import json, sys
sys.path.insert(0, ${JSON.stringify(PACKAGES)})
import honker

db = honker.open(${JSON.stringify(dbPath)})
jobs = db.queue("node-to-python").claim_batch("python-worker", 50)
payloads = [job.payload for job in jobs]
acked = db.queue("node-to-python").ack_batch([job.id for job in jobs], "python-worker")
note = db.query(
    "SELECT payload FROM _honker_notifications "
    "WHERE channel='from-node' ORDER BY id DESC LIMIT 1"
)
events = db.stream("interop")._read_since(0, 10)

py_q = db.queue("python-to-node")
for i in range(25):
    py_q.enqueue({"source": "python", "seq": i, "key": f"py-{i:02d}"})
with db.transaction() as tx:
    tx.notify("from-python", {"source": "python", "count": len(jobs)})
db.stream("interop").publish({"source": "python", "kind": "stream"})

print(json.dumps({
    "acked": acked,
    "payloads": payloads,
    "note": json.loads(note[0]["payload"]),
    "event_count": len(events),
}))
`;
    const proc = spawnPython(pyScript, ['ignore', 'pipe', 'inherit']);
    let stdout = '';
    proc.stdout.on('data', (chunk) => {
      stdout += chunk.toString('utf8');
    });
    const code = await new Promise((resolve) => proc.on('exit', resolve));
    assert.equal(code, 0);
    const observed = JSON.parse(stdout);
    assert.equal(observed.acked, 25);
    assert.equal(observed.payloads.length, 25);
    assert.deepEqual(
      observed.payloads.map((p) => p.seq).sort((a, b) => a - b),
      Array.from({ length: 25 }, (_, i) => i),
    );
    assert.deepEqual(observed.note, { source: 'node', count: 25 });
    assert.equal(observed.event_count, 1);

    const pyQueue = db.queue('python-to-node');
    const pyJobs = pyQueue.claimBatch('node-worker', 50);
    assert.equal(pyJobs.length, 25);
    assert.deepEqual(
      pyJobs.map((job) => job.payload.seq).sort((a, b) => a - b),
      Array.from({ length: 25 }, (_, i) => i),
    );
    assert.equal(pyQueue.ackBatch(pyJobs.map((job) => job.id), 'node-worker'), 25);

    const note = db.query(
      "SELECT payload FROM _honker_notifications WHERE channel='from-python' ORDER BY id DESC LIMIT 1",
    );
    assert.deepEqual(JSON.parse(note[0].payload), { source: 'python', count: 25 });
    assert.equal(db.stream('interop').readSince(0, 10).length, 2);
  } finally {
    db?.close();
    cleanup();
  }
});
