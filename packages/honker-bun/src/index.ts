/**
 * Bun binding for Honker — a SQLite-native task runtime.
 *
 * Uses `bun:sqlite` (built into Bun, no native deps) + the Honker
 * loadable extension (libhonker_extension.dylib / .so). Each method
 * is one SQL call; no wrapper state.
 *
 * @example
 * ```ts
 * import { open, QueueOptions } from "honker-bun";
 *
 * const db = open("app.db", "/path/to/libhonker_extension.dylib");
 * const q = db.queue("emails");
 *
 * q.enqueue({ to: "alice@example.com" });
 *
 * const job = q.claimOne("worker-1");
 * if (job) {
 *   await sendEmail(job.payload);
 *   job.ack();
 * }
 * ```
 */

import { Database as BunDB } from "bun:sqlite";

/**
 * Bun's bundled SQLite is compiled without `SQLITE_ENABLE_LOAD_EXTENSION`.
 * To use loadable extensions (required for Honker), Bun needs a
 * system SQLite pointed to via `Database.setCustomSQLite(path)`.
 *
 * Pass `sqliteLibPath` to `open()` to override. If unset, we try a
 * handful of common locations (Homebrew on macOS, apt on Linux). If
 * none are present, `open()` throws a clear error with install tips
 * instead of the cryptic "does not support dynamic extension loading".
 */
const COMMON_SQLITE_PATHS = [
  // macOS / Homebrew (arm64 + x86_64)
  "/opt/homebrew/opt/sqlite/lib/libsqlite3.dylib",
  "/usr/local/opt/sqlite/lib/libsqlite3.dylib",
  // Linux (apt, dnf)
  "/usr/lib/x86_64-linux-gnu/libsqlite3.so.0",
  "/usr/lib/aarch64-linux-gnu/libsqlite3.so.0",
  "/usr/lib64/libsqlite3.so.0",
];

function locateSystemSqlite(): string | null {
  // Node.js fs may not be imported here; use Bun's file API.
  for (const p of COMMON_SQLITE_PATHS) {
    if (Bun.file(p).size > 0) return p;
  }
  return null;
}

let customSqliteConfigured = false;
function ensureCustomSqlite(override?: string): void {
  if (customSqliteConfigured) return;
  const path = override ?? locateSystemSqlite();
  if (!path) {
    throw new Error(
      "honker-bun: no extension-enabled SQLite found. Bun's bundled " +
        "SQLite is compiled without SQLITE_ENABLE_LOAD_EXTENSION. " +
        "Install SQLite (e.g. `brew install sqlite` on macOS, " +
        "`apt install libsqlite3-dev` on Linux) and pass its path as " +
        "`open(path, extPath, { sqliteLibPath: '/path/to/libsqlite3.dylib' })`.",
    );
  }
  BunDB.setCustomSQLite(path);
  customSqliteConfigured = true;
}

const DEFAULT_PRAGMAS = `
  PRAGMA journal_mode = WAL;
  PRAGMA synchronous = NORMAL;
  PRAGMA busy_timeout = 5000;
  PRAGMA foreign_keys = ON;
  PRAGMA cache_size = -32000;
  PRAGMA temp_store = MEMORY;
  PRAGMA wal_autocheckpoint = 10000;
`;

export interface QueueOptions {
  visibilityTimeoutS?: number;
  maxAttempts?: number;
}

export interface EnqueueOptions {
  delay?: number;
  runAt?: number;
  priority?: number;
  expires?: number;
}

interface RawJob {
  id: number;
  queue: string;
  payload: string;
  worker_id: string;
  attempts: number;
  claim_expires_at: number;
}

/**
 * Honker database handle. Wraps a single `bun:sqlite` Database with
 * the Honker loadable extension loaded and the schema bootstrapped.
 */
export class Database {
  constructor(public readonly raw: BunDB) {}

  /** Get a handle to a named queue. */
  queue(name: string, opts: QueueOptions = {}): Queue {
    return new Queue(this, name, {
      visibilityTimeoutS: opts.visibilityTimeoutS ?? 300,
      maxAttempts: opts.maxAttempts ?? 3,
    });
  }

  /**
   * Fire a pg_notify-style pub/sub signal. Payload is any
   * JSON-serializable value. Returns the notification id.
   */
  notify(channel: string, payload: unknown): number {
    const json = JSON.stringify(payload);
    const row = this.raw
      .query<{ v: number }, [string, string]>("SELECT notify(?, ?) AS v")
      .get(channel, json)!;
    return row.v;
  }

  /** Close the underlying database. */
  close(): void {
    this.raw.close();
  }
}

export interface OpenOptions {
  /**
   * Path to a system SQLite library compiled with
   * `SQLITE_ENABLE_LOAD_EXTENSION`. If unset, common locations
   * (Homebrew on macOS, apt on Linux) are probed. Required once
   * per process; subsequent `open()` calls reuse the configured
   * library.
   */
  sqliteLibPath?: string;
}

/** Open (or create) a SQLite DB, load the Honker extension, bootstrap. */
export function open(
  path: string,
  extensionPath: string,
  opts: OpenOptions = {},
): Database {
  ensureCustomSqlite(opts.sqliteLibPath);
  const raw = new BunDB(path, { create: true, readwrite: true });
  raw.loadExtension(extensionPath);
  raw.exec(DEFAULT_PRAGMAS);
  raw.exec("SELECT honker_bootstrap()");
  return new Database(raw);
}

export class Queue {
  constructor(
    private readonly db: Database,
    public readonly name: string,
    private readonly opts: Required<QueueOptions>,
  ) {}

  /** Enqueue a job. Payload is JSON-stringified. Returns the row id. */
  enqueue(payload: unknown, opts: EnqueueOptions = {}): number {
    const json = JSON.stringify(payload);
    const row = this.db.raw
      .query<
        { v: number },
        [
          string,
          string,
          number | null,
          number | null,
          number,
          number,
          number | null,
        ]
      >("SELECT honker_enqueue(?, ?, ?, ?, ?, ?, ?) AS v")
      .get(
        this.name,
        json,
        opts.runAt ?? null,
        opts.delay ?? null,
        opts.priority ?? 0,
        this.opts.maxAttempts,
        opts.expires ?? null,
      )!;
    return row.v;
  }

  /** Atomically claim up to n jobs. */
  claimBatch(workerId: string, n: number): Job[] {
    const row = this.db.raw
      .query<{ v: string }, [string, string, number, number]>(
        "SELECT honker_claim_batch(?, ?, ?, ?) AS v",
      )
      .get(this.name, workerId, n, this.opts.visibilityTimeoutS)!;
    const raw: RawJob[] = JSON.parse(row.v);
    return raw.map((r) => new Job(this.db, r));
  }

  /** Claim a single job or return null. */
  claimOne(workerId: string): Job | null {
    return this.claimBatch(workerId, 1)[0] ?? null;
  }
}

/** A claimed unit of work. */
export class Job {
  readonly id: number;
  readonly queue: string;
  readonly payload: unknown;
  readonly workerId: string;
  readonly attempts: number;

  constructor(
    private readonly db: Database,
    row: RawJob,
  ) {
    this.id = row.id;
    this.queue = row.queue;
    this.payload = JSON.parse(row.payload);
    this.workerId = row.worker_id;
    this.attempts = row.attempts;
  }

  /** DELETE the row if the claim is still valid. */
  ack(): boolean {
    return this.db.raw
      .query<{ v: number }, [number, string]>("SELECT honker_ack(?, ?) AS v")
      .get(this.id, this.workerId)!.v > 0;
  }

  /** Put the job back with a delay, or move to dead after maxAttempts. */
  retry(delaySec: number, errorMsg: string): boolean {
    return this.db.raw
      .query<{ v: number }, [number, string, number, string]>(
        "SELECT honker_retry(?, ?, ?, ?) AS v",
      )
      .get(this.id, this.workerId, delaySec, errorMsg)!.v > 0;
  }

  /** Unconditionally move to dead. */
  fail(errorMsg: string): boolean {
    return this.db.raw
      .query<{ v: number }, [number, string, string]>(
        "SELECT honker_fail(?, ?, ?) AS v",
      )
      .get(this.id, this.workerId, errorMsg)!.v > 0;
  }

  /** Extend the visibility timeout. */
  heartbeat(extendSec: number): boolean {
    return this.db.raw
      .query<{ v: number }, [number, string, number]>(
        "SELECT honker_heartbeat(?, ?, ?) AS v",
      )
      .get(this.id, this.workerId, extendSec)!.v > 0;
  }
}
