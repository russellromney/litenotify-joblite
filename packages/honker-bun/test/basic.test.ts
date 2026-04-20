import { test, expect, describe } from "bun:test";
import { existsSync } from "node:fs";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";

import { open } from "../src/index.ts";

const REPO_ROOT = resolve(import.meta.dir, "..", "..", "..");
const EXT_CANDIDATES = [
  "target/release/libhonker_ext.dylib",
  "target/release/libhonker_ext.so",
  "target/release/libhonker_extension.dylib",
  "target/release/libhonker_extension.so",
];

function findExtension(): string | null {
  for (const rel of EXT_CANDIDATES) {
    const p = join(REPO_ROOT, rel);
    if (existsSync(p)) return p;
  }
  return null;
}

const extPath = findExtension();
const maybe = extPath ? describe : describe.skip;

maybe("honker-bun basic", () => {
  let dir: string;
  let dbPath: string;

  function setup() {
    dir = mkdtempSync(join(tmpdir(), "honker-bun-"));
    dbPath = join(dir, "t.db");
  }
  function teardown() {
    rmSync(dir, { recursive: true, force: true });
  }

  test("enqueue / claim / ack round-trips", () => {
    setup();
    const db = open(dbPath, extPath!);
    try {
      const q = db.queue("emails");
      const id = q.enqueue({ to: "alice@example.com" });
      expect(id).toBeGreaterThan(0);

      const job = q.claimOne("worker-1");
      expect(job).not.toBeNull();
      expect(job!.id).toBe(id);
      expect(job!.attempts).toBe(1);
      expect((job!.payload as any).to).toBe("alice@example.com");

      expect(job!.ack()).toBe(true);
      expect(q.claimOne("worker-1")).toBeNull();
    } finally {
      db.close();
      teardown();
    }
  });

  test("retry exhausts into dead after maxAttempts", () => {
    setup();
    const db = open(dbPath, extPath!);
    try {
      const q = db.queue("retries", { maxAttempts: 2 });
      q.enqueue({ i: 1 });

      const j1 = q.claimOne("w")!;
      expect(j1.retry(0, "first")).toBe(true);

      const j2 = q.claimOne("w")!;
      expect(j2.attempts).toBe(2);
      expect(j2.retry(0, "second")).toBe(true);

      expect(q.claimOne("w")).toBeNull();

      const deadCount = db.raw
        .query<{ c: number }, []>(
          "SELECT COUNT(*) AS c FROM _honker_dead WHERE queue='retries'",
        )
        .get()!.c;
      expect(deadCount).toBe(1);
    } finally {
      db.close();
      teardown();
    }
  });

  test("notify inserts into _honker_notifications", () => {
    setup();
    const db = open(dbPath, extPath!);
    try {
      const id = db.notify("orders", { id: 42 });
      expect(id).toBeGreaterThan(0);

      const row = db.raw
        .query<{ channel: string; payload: string }, [number]>(
          "SELECT channel, payload FROM _honker_notifications WHERE id = ?",
        )
        .get(id)!;
      expect(row.channel).toBe("orders");
      expect(JSON.parse(row.payload)).toEqual({ id: 42 });
    } finally {
      db.close();
      teardown();
    }
  });
});
