'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');

const honker = require('..');
const { createTempDb } = require('./helpers');
const { PACKAGES, spawnPython } = require('./cross_lang_shared');

test(
  'supporting proof: on Windows, node writes notifications and python can read the shared rows via raw SQL',
  { skip: process.platform !== 'win32' },
  async () => {
    const { path: dbPath, open, cleanup } = createTempDb(
      'xlang-rev-sql-',
      honker.open.bind(honker),
    );
    let db;
    try {
      db = open(dbPath);
      const tx = db.transaction();
      tx.notify('reverse-sql', { tag: 'a', i: 1 });
      tx.notify('reverse-sql', { tag: 'b', i: 2 });
      tx.commit();
      const tx2 = db.transaction();
      tx2.notify('reverse-sql', { tag: 'c', i: 3 });
      tx2.commit();
      db.close();
      db = null;

      const pyScript = `
import json, sys
sys.path.insert(0, ${JSON.stringify(PACKAGES)})
import honker

db = honker.open(${JSON.stringify(dbPath)})
rows = db.query(
    "SELECT payload FROM _honker_notifications "
    "WHERE channel='reverse-sql' ORDER BY id"
)
print("RESULT", json.dumps([json.loads(r["payload"]) for r in rows]), flush=True)
`;
      const result = await new Promise((resolve, reject) => {
        const proc = spawnPython(pyScript);
        let out = '';
        proc.stdout.on('data', (chunk) => {
          out += chunk.toString('utf8');
        });
        proc.on('exit', (code) => {
          if (code !== 0) return reject(new Error(`python exited ${code}`));
          const line = out
            .split(/\r?\n/)
            .find((candidate) => candidate.startsWith('RESULT '));
          if (!line) return reject(new Error(`missing RESULT line: ${out}`));
          resolve(JSON.parse(line.slice('RESULT '.length)));
        });
      });

      assert.deepEqual(
        result.map((row) => row.i),
        [1, 2, 3],
      );
      assert.deepEqual(
        result.map((row) => row.tag),
        ['a', 'b', 'c'],
      );
    } finally {
      db?.close();
      cleanup();
    }
  },
);

test('supporting proof: python bootstraps honker schema and node can read the shared tables via raw SQL', async () => {
  const { path: dbPath, open, cleanup } = createTempDb(
    'xlang-schema-',
    honker.open.bind(honker),
  );
  let db;
  try {
    const pyScript = `
import sys
sys.path.insert(0, ${JSON.stringify(PACKAGES)})
import honker
db = honker.open(${JSON.stringify(dbPath)})
q = db.queue("shared")
q.enqueue({"from": "python", "i": 1})
q.enqueue({"from": "python", "i": 2})
print("DONE", flush=True)
`;
    await new Promise((resolve, reject) => {
      const proc = spawnPython(pyScript, ['ignore', 'inherit', 'inherit']);
      proc.on('exit', (code) =>
        code === 0
          ? resolve()
          : reject(new Error(`python exited ${code}`)),
      );
    });

    db = open(dbPath);
    const rows = db.query(
      "SELECT id, queue, payload FROM _honker_live " +
        "WHERE queue='shared' ORDER BY id",
    );
    assert.equal(rows.length, 2);
    assert.equal(rows[0].queue, 'shared');
    const payloads = rows.map((row) => JSON.parse(row.payload));
    assert.deepEqual(
      payloads.map((payload) => payload.i),
      [1, 2],
    );
    assert.equal(payloads[0].from, 'python');

    const tableNames = db
      .query("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
      .map((row) => row.name);
    for (const expected of [
      '_honker_notifications',
      '_honker_live',
      '_honker_dead',
      '_honker_locks',
      '_honker_rate_limits',
      '_honker_scheduler_tasks',
      '_honker_results',
      '_honker_stream',
      '_honker_stream_consumers',
    ]) {
      assert.ok(
        tableNames.includes(expected),
        `Node cannot see ${expected} on a Python-bootstrapped DB`,
      );
    }
  } finally {
    db?.close();
    cleanup();
  }
});
