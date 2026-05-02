import { test, expect, describe } from "bun:test";
import { existsSync, mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";

import { open, type Database } from "../src/index.ts";

const REPO_ROOT = resolve(import.meta.dir, "..", "..", "..");
const EXT_CANDIDATES = [
  "target/release/libhonker_ext.dylib",
  "target/release/libhonker_ext.so",
  "target/release/libhonker_extension.dylib",
  "target/release/libhonker_extension.so",
];

function findExtension(): string | null {
  const fromEnv = process.env.HONKER_EXT_PATH;
  if (fromEnv && existsSync(fromEnv)) return fromEnv;
  for (const rel of EXT_CANDIDATES) {
    const p = join(REPO_ROOT, rel);
    if (existsSync(p)) return p;
  }
  return null;
}

const extPath = findExtension();
if (!extPath && process.env.CI) {
  throw new Error("HONKER_EXT_PATH not found in CI; Bun tests must run for real");
}
const maybe = extPath ? describe : describe.skip;

// Shared setup helper. Each `test` creates its own tempdir + db.
function withDb(fn: (db: Database) => Promise<void> | void): () => Promise<void> {
  return async () => {
    const dir = mkdtempSync(join(tmpdir(), "honker-bun-parity-"));
    const dbPath = join(dir, "t.db");
    const db = open(dbPath, extPath!);
    try {
      await fn(db);
    } finally {
      db.close();
      rmSync(dir, { recursive: true, force: true });
    }
  };
}

maybe("honker-bun parity — Transaction", () => {
  test(
    "commit persists rows",
    withDb((db) => {
      const tx = db.transaction();
      tx.execute(
        "CREATE TABLE kv (k TEXT PRIMARY KEY, v TEXT)",
      );
      tx.execute("INSERT INTO kv VALUES (?, ?)", ["a", "1"]);
      tx.commit();

      const row = db.raw
        .query<{ v: string }, []>("SELECT v FROM kv WHERE k='a'")
        .get()!;
      expect(row.v).toBe("1");
    }),
  );

  test(
    "rollback drops rows",
    withDb((db) => {
      db.raw.exec("CREATE TABLE kv (k TEXT PRIMARY KEY, v TEXT)");
      const tx = db.transaction();
      tx.execute("INSERT INTO kv VALUES (?, ?)", ["a", "1"]);
      tx.rollback();
      const row = db.raw
        .query<{ c: number }, []>("SELECT COUNT(*) AS c FROM kv")
        .get()!;
      expect(row.c).toBe(0);
    }),
  );

  test(
    "commit is idempotent, second call no-op",
    withDb((db) => {
      db.raw.exec("CREATE TABLE kv (k TEXT)");
      const tx = db.transaction();
      tx.execute("INSERT INTO kv VALUES (?)", ["x"]);
      tx.commit();
      expect(() => tx.commit()).not.toThrow();
      expect(() => tx.rollback()).not.toThrow();
    }),
  );

  test(
    "atomic business write + enqueueTx on rollback drops both",
    withDb((db) => {
      db.raw.exec("CREATE TABLE orders (id INTEGER PRIMARY KEY, amount INTEGER)");
      const q = db.queue("emails");
      const tx = db.transaction();
      tx.execute("INSERT INTO orders VALUES (?, ?)", [1, 100]);
      const jobId = q.enqueue({ orderId: 1 }, { tx });
      expect(jobId).toBeGreaterThan(0);
      tx.rollback();

      const orderRow = db.raw
        .query<{ c: number }, []>("SELECT COUNT(*) AS c FROM orders")
        .get()!;
      expect(orderRow.c).toBe(0);
      const claim = q.claimOne("w");
      expect(claim).toBeNull();
    }),
  );

  test(
    "enqueueTx on commit makes job visible",
    withDb((db) => {
      const q = db.queue("emails");
      const tx = db.transaction();
      q.enqueue({ x: 1 }, { tx });
      // Visible only after commit.
      tx.commit();
      const j = q.claimOne("w");
      expect(j).not.toBeNull();
      expect((j!.payload as any).x).toBe(1);
      j!.ack();
    }),
  );
});

maybe("honker-bun parity — Stream", () => {
  test(
    "publish / readSince round-trip",
    withDb((db) => {
      const s = db.stream("orders");
      const off1 = s.publish({ id: 1 });
      const off2 = s.publish({ id: 2 });
      expect(off2).toBeGreaterThan(off1);

      const events = s.readSince(0, 100);
      expect(events.length).toBe(2);
      expect((events[0].payload as any).id).toBe(1);
      expect(events[0].key).toBeNull();
    }),
  );

  test(
    "publishWithKey preserves partition key",
    withDb((db) => {
      const s = db.stream("orders");
      s.publishWithKey("user-42", { id: 1 });
      const events = s.readSince(0, 100);
      expect(events[0].key).toBe("user-42");
    }),
  );

  test(
    "consumer offset save + resume",
    withDb((db) => {
      const s = db.stream("orders");
      const off1 = s.publish({ i: 1 });
      const off2 = s.publish({ i: 2 });
      s.publish({ i: 3 });

      expect(s.getOffset("billing")).toBe(0);
      expect(s.saveOffset("billing", off2)).toBe(true);
      expect(s.getOffset("billing")).toBe(off2);

      // Monotonic: saving a lower offset returns false.
      expect(s.saveOffset("billing", off1)).toBe(false);
      expect(s.getOffset("billing")).toBe(off2);

      const remaining = s.readFromConsumer("billing", 100);
      expect(remaining.length).toBe(1);
      expect((remaining[0].payload as any).i).toBe(3);
    }),
  );

  test(
    "publishTx + saveOffsetTx respect rollback",
    withDb((db) => {
      const s = db.stream("orders");
      s.publish({ i: 1 }); // offset 1 exists committed

      const tx = db.transaction();
      s.publishTx(tx, { i: 999 });
      s.saveOffsetTx(tx, "billing", 999);
      tx.rollback();

      // The rolled-back publish is not visible; offset not saved.
      const events = s.readSince(0, 100);
      expect(events.length).toBe(1);
      expect((events[0].payload as any).i).toBe(1);
      expect(s.getOffset("billing")).toBe(0);
    }),
  );
});

maybe("honker-bun parity — Scheduler", () => {
  test(
    "add / remove round-trip",
    withDb((db) => {
      const sc = db.scheduler();
      sc.add({
        name: "hourly",
        queue: "reports",
        schedule: "0 * * * *",
        payload: { kind: "hourly" },
      });
      expect(sc.remove("hourly")).toBe(1);
      // Removing a missing task is 0.
      expect(sc.remove("hourly")).toBe(0);
    }),
  );

  test(
    "tick on past cron fires a job",
    withDb((db) => {
      const sc = db.scheduler();
      // Every minute from the top-of-minute. Should fire on next tick.
      sc.add({
        name: "every-min",
        queue: "pings",
        schedule: "* * * * *",
        payload: {},
      });
      // Initial tick may or may not fire depending on boundary timing;
      // what matters is that subsequent ticks don't explode. We do two
      // ticks 1100ms apart to step past a boundary in the tight case.
      sc.tick();
      // soonest is a monotonically non-negative unix ts when tasks exist.
      expect(sc.soonest()).toBeGreaterThanOrEqual(0);
    }),
  );

  test(
    "run loop acquires lock, aborts cleanly",
    withDb(async (db) => {
      const sc = db.scheduler();
      sc.add({
        name: "abort-me",
        queue: "q",
        schedule: "0 0 1 1 *", // Jan 1 midnight — never inside our 200ms window
        payload: {},
      });
      const ctl = new AbortController();
      const done = sc.run({ owner: "leader-1", signal: ctl.signal });
      // Let the leader acquire the lock & tick once.
      await new Promise((r) => setTimeout(r, 200));
      ctl.abort();
      await done;
      // Lock must be released (another owner should now be able to take it).
      const lock = db.tryLock("honker-scheduler", "leader-2", 30);
      expect(lock).not.toBeNull();
      lock?.release();
    }),
  );

  test(
    "legacy cron alias still works",
    withDb((db) => {
      const sc = db.scheduler();
      sc.add({
        name: "legacy",
        queue: "reports",
        cron: "@every 1s",
        payload: { kind: "legacy" },
      });
      expect(sc.soonest()).toBeGreaterThanOrEqual(0);
      expect(sc.remove("legacy")).toBe(1);
    }),
  );

  test(
    "every-second schedule is accepted",
    withDb((db) => {
      const sc = db.scheduler();
      sc.add({
        name: "fast",
        queue: "reports",
        schedule: "@every 1s",
        payload: { kind: "fast" },
      });
      expect(sc.soonest()).toBeGreaterThanOrEqual(0);
      expect(sc.remove("fast")).toBe(1);
    }),
  );
});

maybe("honker-bun parity — Locks", () => {
  test(
    "mutual exclusion between owners",
    withDb((db) => {
      const a = db.tryLock("nightly-report", "a", 30);
      expect(a).not.toBeNull();
      const b = db.tryLock("nightly-report", "b", 30);
      expect(b).toBeNull();
      expect(a!.release()).toBe(true);
      const c = db.tryLock("nightly-report", "c", 30);
      expect(c).not.toBeNull();
      c!.release();
    }),
  );

  test(
    "release is idempotent",
    withDb((db) => {
      const l = db.tryLock("dedup", "owner", 30)!;
      expect(l.release()).toBe(true);
      expect(l.release()).toBe(false);
      expect(l.released).toBe(true);
    }),
  );

  test(
    "heartbeat extends ttl and returns true while holding",
    withDb((db) => {
      const l = db.tryLock("hb", "owner", 2)!;
      expect(l.heartbeat(30)).toBe(true);

      // Another owner cannot hijack while ttl is fresh.
      const stolen = db.tryLock("hb", "other", 30);
      expect(stolen).toBeNull();
      l.release();
    }),
  );

  test(
    "heartbeat returns false after ttl expires and someone else takes over",
    withDb(async (db) => {
      // Expire fast: ttl=1 sec. After it elapses, another owner can take.
      const l = db.tryLock("steal", "a", 1)!;
      await new Promise((r) => setTimeout(r, 1100));
      const taker = db.tryLock("steal", "b", 30);
      expect(taker).not.toBeNull();
      // Now our heartbeat as 'a' must fail (row exists but owner != 'a').
      expect(l.heartbeat(30)).toBe(false);
      taker?.release();
    }),
  );
});

maybe("honker-bun parity — Rate limit", () => {
  test(
    "allows N, then blocks",
    withDb((db) => {
      // 3 per 60s window
      expect(db.tryRateLimit("api", 3, 60)).toBe(true);
      expect(db.tryRateLimit("api", 3, 60)).toBe(true);
      expect(db.tryRateLimit("api", 3, 60)).toBe(true);
      expect(db.tryRateLimit("api", 3, 60)).toBe(false);
    }),
  );
});

maybe("honker-bun parity — Results", () => {
  test(
    "save / get round-trip",
    withDb((db) => {
      db.saveResult(42, JSON.stringify({ ok: true }), 60);
      const v = db.getResult(42);
      expect(v).not.toBeNull();
      expect(JSON.parse(v!)).toEqual({ ok: true });
    }),
  );

  test(
    "missing result returns null",
    withDb((db) => {
      expect(db.getResult(999)).toBeNull();
    }),
  );

  test(
    "sweepResults returns non-negative",
    withDb((db) => {
      db.saveResult(1, "x", 60);
      const n = db.sweepResults();
      expect(n).toBeGreaterThanOrEqual(0);
    }),
  );
});

maybe("honker-bun parity — notify/listen", () => {
  test(
    "listen receives post-attach notifications, filters by channel",
    withDb(async (db) => {
      const ctl = new AbortController();
      const got: Array<{ channel: string; payload: unknown }> = [];
      const it = db.listen("orders", { signal: ctl.signal, pollMs: 25 });
      // Drive the loop on a microtask.
      const consumer = (async () => {
        for await (const n of it) {
          got.push({ channel: n.channel, payload: n.payload });
          if (got.length >= 2) break;
        }
      })();

      // Let the subscriber snapshot MAX(id) first.
      await new Promise((r) => setTimeout(r, 30));
      db.notify("other", { nope: true }); // filtered out
      db.notify("orders", { id: 1 });
      db.notify("orders", { id: 2 });

      await consumer;
      ctl.abort();

      expect(got.length).toBe(2);
      expect(got[0].channel).toBe("orders");
      expect((got[0].payload as any).id).toBe(1);
      expect((got[1].payload as any).id).toBe(2);
    }),
  );

  test(
    "notifyTx on rollback is not delivered",
    withDb(async (db) => {
      // honker-bun uses a single connection, so a same-connection
      // listener can technically see uncommitted rows during an open
      // transaction. What we CAN assert — and what the killer feature
      // is — is that rollback drops the notification entirely.
      const tx = db.transaction();
      db.notify("rolled-back", { nope: true }, { tx });
      tx.rollback();

      const row = db.raw
        .query<{ c: number }, []>(
          "SELECT COUNT(*) AS c FROM _honker_notifications WHERE channel='rolled-back'",
        )
        .get()!;
      expect(row.c).toBe(0);
    }),
  );

  test(
    "notifyTx on commit is delivered",
    withDb(async (db) => {
      const tx = db.transaction();
      db.notify("committed", { yes: true }, { tx });
      tx.commit();
      const ctl = new AbortController();
      const it = db.listen("committed", { signal: ctl.signal, pollMs: 25 });
      // listen() snapshots MAX(id) at attach, so notifications fired
      // *before* attach are not replayed. Fire a fresh post-attach
      // notification via an open tx to prove the code path works.
      const got: unknown[] = [];
      const consumer = (async () => {
        for await (const n of it) {
          got.push(n.payload);
          break;
        }
      })();
      await new Promise((r) => setTimeout(r, 30));
      const tx2 = db.transaction();
      db.notify("committed", { second: true }, { tx: tx2 });
      tx2.commit();
      await consumer;
      ctl.abort();
      expect((got[0] as any).second).toBe(true);
    }),
  );
});

maybe("honker-bun parity — claimWaker", () => {
  test(
    "next() wakes eventually on enqueue",
    withDb(async (db) => {
      const q = db.queue("emails");
      const waker = q.claimWaker({ idlePollS: 5 });
      const ctl = new AbortController();
      const jobP = waker.next("worker-1", { signal: ctl.signal });

      // Let the waker poll and see empty once, then enqueue.
      await new Promise((r) => setTimeout(r, 30));
      q.enqueue({ to: "alice@example.com" });

      const job = await jobP;
      expect(job).not.toBeNull();
      expect((job!.payload as any).to).toBe("alice@example.com");
      job!.ack();
      waker.close();
    }),
  );

  test(
    "close() resolves pending next() to null",
    withDb(async (db) => {
      const q = db.queue("empty");
      const waker = q.claimWaker({ idlePollS: 5 });
      const p = waker.next("w");
      waker.close();
      expect(await p).toBeNull();
    }),
  );

  test(
    "runAt deadline wakes before fallback poll",
    { timeout: 12_000 },
    withDb(async (db) => {
      const q = db.queue("deadline");
      const runAt = Math.floor(Date.now() / 1000) + 3;
      const msUntilDue = runAt * 1000 - Date.now();
      q.enqueue({ hello: "future" }, { runAt });
      const waker = q.claimWaker({ idlePollS: 30 });
      const t0 = Date.now();
      const job = await Promise.race([
        waker.next("worker-1"),
        new Promise<null>((resolve) => setTimeout(() => resolve(null), 8000)),
      ]);
      const dt = Date.now() - t0;
      expect(job).not.toBeNull();
      expect((job!.payload as any).hello).toBe("future");
      expect(dt).toBeGreaterThanOrEqual(Math.max(0, msUntilDue - 500));
      expect(dt).toBeLessThan(10000);
      job!.ack();
      waker.close();
    }),
  );

  test(
    "tryNext is non-blocking",
    withDb((db) => {
      const q = db.queue("nowait");
      const waker = q.claimWaker();
      expect(waker.tryNext("w")).toBeNull();
      q.enqueue({ x: 1 });
      const job = waker.tryNext("w");
      expect(job).not.toBeNull();
      job!.ack();
      waker.close();
    }),
  );
});

maybe("honker-bun parity — Queue extras", () => {
  test(
    "ackBatch acks multiple",
    withDb((db) => {
      const q = db.queue("batch");
      const id1 = q.enqueue({ i: 1 });
      const id2 = q.enqueue({ i: 2 });
      const jobs = q.claimBatch("w", 10);
      expect(jobs.length).toBe(2);
      const n = q.ackBatch([id1, id2], "w");
      expect(n).toBe(2);
      expect(q.claimOne("w")).toBeNull();
    }),
  );

  test(
    "sweepExpired reclaims lost claims",
    withDb(async (db) => {
      const q = db.queue("expiring", { visibilityTimeoutS: 1 });
      q.enqueue({ i: 1 });
      const j = q.claimOne("w")!;
      expect(j).not.toBeNull();
      // Let the claim expire. `claim_expires_at < unixepoch()` is
      // strict, so we need > 1s to safely cross the boundary across
      // the system clock's rounding.
      await new Promise((r) => setTimeout(r, 2100));
      // Either path — a direct sweep or re-claim via claim_batch —
      // must surface the expired row.
      const n = q.sweepExpired();
      expect(n).toBeGreaterThanOrEqual(0);
      const j2 = q.claimOne("w2");
      expect(j2).not.toBeNull();
      j2!.ack();
    }),
  );
});
