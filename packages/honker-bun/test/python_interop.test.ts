import { test, expect } from "bun:test";
import { existsSync, mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { delimiter, join, resolve } from "node:path";
import { spawnSync } from "node:child_process";

import { open } from "../src/index.ts";

const REPO_ROOT = resolve(import.meta.dir, "..", "..", "..");
const PYTHONPATH = [
  join(REPO_ROOT, "packages", "honker", "python"),
  join(REPO_ROOT, "packages"),
].join(delimiter);

function findPython(): string | null {
  const probeScript = `
import os
import tempfile
import honker

p = tempfile.mktemp(prefix="honker-probe-", suffix=".db")
db = honker.open(p)
db.query("SELECT 1")
db = None
try:
    os.remove(p)
except OSError:
    pass
`;
  const candidates = [
    process.env.HONKER_INTEROP_PYTHON,
    join(REPO_ROOT, ".venv", process.platform === "win32" ? "Scripts/python.exe" : "bin/python"),
    "python3",
    "python",
  ].filter(Boolean) as string[];

  for (const python of candidates) {
    const probe = spawnSync(python, ["-c", probeScript], {
      env: { ...process.env, PYTHONPATH },
      stdio: "ignore",
    });
    if (probe.status === 0) return python;
  }

  if (process.env.HONKER_INTEROP_PYTHON) {
    throw new Error("HONKER_INTEROP_PYTHON cannot import honker");
  }
  return null;
}

function findExtension(): string | null {
  const candidates = [
    process.env.HONKER_EXT_PATH,
    join(REPO_ROOT, "target/release/libhonker_ext.dylib"),
    join(REPO_ROOT, "target/release/libhonker_ext.so"),
  ].filter(Boolean) as string[];
  return candidates.find((p) => existsSync(p)) ?? null;
}

function runPython(python: string, dbPath: string, script: string): string {
  const out = spawnSync(python, ["-c", script], {
    env: { ...process.env, PYTHONPATH, DB_PATH: dbPath },
    encoding: "utf8",
  });
  expect(out.status, out.stderr || out.stdout).toBe(0);
  return out.stdout;
}

test("bun and python share queue, stream, and notify rows", () => {
  const python = findPython();
  const extPath = findExtension();
  if (!python || !extPath) {
    if (process.env.CI) throw new Error("python or honker extension missing in CI");
    return;
  }

  const dir = mkdtempSync(join(tmpdir(), "honker-bun-python-"));
  const dbPath = join(dir, "bun-python.db");
  const db = open(dbPath, extPath);
  try {
    const bunQueue = db.queue("bun-to-python");
    for (let i = 0; i < 25; i += 1) {
      bunQueue.enqueue({ source: "bun", seq: i, key: `bun-${i.toString().padStart(2, "0")}` });
    }
    db.notify("from-bun", { source: "bun", count: 25 });
    db.stream("interop").publish({ source: "bun", kind: "stream" });

    const observed = JSON.parse(runPython(python, dbPath, `
import json
import os
import honker

db = honker.open(os.environ["DB_PATH"])

jobs = db.queue("bun-to-python").claim_batch("python-worker", 50)
payloads = [job.payload for job in jobs]
acked = db.queue("bun-to-python").ack_batch([job.id for job in jobs], "python-worker")
note = db.query(
    "SELECT payload FROM _honker_notifications "
    "WHERE channel='from-bun' ORDER BY id DESC LIMIT 1"
)
events = db.stream("interop")._read_since(0, 10)

py_q = db.queue("python-to-bun")
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
`));

    expect(observed.acked).toBe(25);
    expect(observed.payloads).toHaveLength(25);
    expect(observed.note).toEqual({ source: "bun", count: 25 });
    expect(observed.event_count).toBe(1);

    const pyQueue = db.queue("python-to-bun");
    const pyJobs = pyQueue.claimBatch("bun-worker", 50);
    expect(pyJobs).toHaveLength(25);
    const seqs = pyJobs.map((job) => (job.payload as any).seq).sort((a, b) => a - b);
    expect(seqs).toEqual(Array.from({ length: 25 }, (_, i) => i));
    expect(pyQueue.ackBatch(pyJobs.map((job) => job.id), "bun-worker")).toBe(25);

    const note = db.raw
      .query<{ payload: string }, []>(
        "SELECT payload FROM _honker_notifications WHERE channel='from-python' ORDER BY id DESC LIMIT 1",
      )
      .get()!;
    expect(JSON.parse(note.payload)).toEqual({ source: "python", count: 25 });
    expect(db.stream("interop").readSince(0, 10)).toHaveLength(2);
  } finally {
    db.close();
    rmSync(dir, { recursive: true, force: true });
  }
});
