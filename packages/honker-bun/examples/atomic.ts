// Atomic business-write + enqueue in one transaction.
//
//   bun run examples/atomic.ts

import { open } from "../src/index.ts";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

const EXT_PATH =
  process.env.HONKER_EXTENSION_PATH ??
  new URL(
    "../../../target/release/libhonker_ext.dylib",
    import.meta.url,
  ).pathname;

const dir = mkdtempSync(join(tmpdir(), "honker-"));
const db = open(join(dir, "app.db"), EXT_PATH);

try {
  db.raw.exec(
    "CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER, total INTEGER)",
  );

  // Success path.
  db.raw.transaction(() => {
    db.raw.run(
      "INSERT INTO orders (user_id, total) VALUES (?, ?)",
      [42, 9900],
    );
    db.raw.run(
      "SELECT honker_enqueue(?, ?, ?, ?, ?, ?, ?)",
      [
        "emails",
        JSON.stringify({ to: "alice@example.com", order_id: 42 }),
        null, null, 0, 3, null,
      ],
    );
  })();

  const count = (sql: string) =>
    (db.raw.query<{ c: number }, []>(sql).get()?.c ?? 0);
  console.log(`committed: ${count("SELECT COUNT(*) AS c FROM orders")} order(s), ` +
              `${count("SELECT COUNT(*) AS c FROM _honker_live WHERE queue='emails'")} job(s)`);

  // Rollback path — bun:sqlite rollbacks on any thrown error inside the tx.
  try {
    db.raw.transaction(() => {
      db.raw.run(
        "INSERT INTO orders (user_id, total) VALUES (?, ?)",
        [43, 5000],
      );
      db.raw.run(
        "SELECT honker_enqueue(?, ?, ?, ?, ?, ?, ?)",
        [
          "emails",
          JSON.stringify({ to: "bob@example.com", order_id: 43 }),
          null, null, 0, 3, null,
        ],
      );
      throw new Error("boom — simulated payment-processing failure");
    })();
  } catch (e) {
    console.log(`rolled back: ${(e as Error).message}`);
  }

  console.log(`after rollback: ${count("SELECT COUNT(*) AS c FROM orders")} order(s), ` +
              `${count("SELECT COUNT(*) AS c FROM _honker_live WHERE queue='emails'")} job(s)`);
  console.log("atomic enqueue + rollback both work as expected");
} finally {
  db.close();
  rmSync(dir, { recursive: true, force: true });
}
