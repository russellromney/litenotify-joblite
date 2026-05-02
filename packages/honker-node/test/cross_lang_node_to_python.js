'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');

const honker = require('..');
const { createTempDb } = require('./helpers');
const {
  PACKAGES,
  createLineReader,
  spawnPython,
  stopChild,
} = require('./cross_lang_shared');

test(
  'direct proof: node writes notifications; python listen() observes them',
  async () => {
    const { path: dbPath, open, cleanup } = createTempDb(
      'xlang-node-to-py-',
      honker.open.bind(honker),
    );
    let db;
    let proc;
    try {
      db = open(dbPath);

      const pyScript = `
import asyncio, json, sys
sys.path.insert(0, ${JSON.stringify(PACKAGES)})
import honker

async def main():
    db = honker.open(${JSON.stringify(dbPath)})
    lst = db.listen("reverse")
    print("READY", flush=True)
    got = []
    async def consume():
        async for n in lst:
            got.append(n.payload)
            if len(got) == 3:
                return
    await asyncio.wait_for(consume(), timeout=20.0)
    print("RESULT", json.dumps(got), flush=True)

asyncio.run(main())
`;
      proc = spawnPython(pyScript);
      const nextLineMatching = createLineReader(proc.stdout);

      await nextLineMatching((line) => line === 'READY', 25000);

      const tx = db.transaction();
      tx.notify('reverse', { tag: 'a', i: 1 });
      tx.notify('reverse', { tag: 'b', i: 2 });
      tx.commit();
      const tx2 = db.transaction();
      tx2.notify('reverse', { tag: 'c', i: 3 });
      tx2.commit();

      const resultLine = await nextLineMatching(
        (line) => line.startsWith('RESULT '),
        25000,
      );
      const result = JSON.parse(resultLine.slice('RESULT '.length));

      await new Promise((resolve) => proc.on('exit', resolve));
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
      await stopChild(proc);
      cleanup();
    }
  },
);
