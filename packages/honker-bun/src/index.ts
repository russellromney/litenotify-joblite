/**
 * Bun binding for Honker — a SQLite-native task runtime.
 *
 * Uses `bun:sqlite` (built into Bun, no native deps) + the Honker
 * loadable extension (libhonker_ext.dylib / .so). Each method is
 * essentially one SQL call; no wrapper state beyond the DB handle.
 *
 * This binding uses a lightweight data_version watcher instead of the
 * Rust-native watcher threads used in some other bindings. So the API
 * shape matches the rest of the family even though Bun gets there with
 * a small in-process poll loop.
 */

import { Database as BunDB } from "bun:sqlite";

// ---------------------------------------------------------------------
// SQLite lib shim (Bun's bundled SQLite lacks loadable-extension support)
// ---------------------------------------------------------------------

const COMMON_SQLITE_PATHS = [
  "/opt/homebrew/opt/sqlite/lib/libsqlite3.dylib",
  "/usr/local/opt/sqlite/lib/libsqlite3.dylib",
  "/usr/lib/x86_64-linux-gnu/libsqlite3.so.0",
  "/usr/lib/aarch64-linux-gnu/libsqlite3.so.0",
  "/usr/lib64/libsqlite3.so.0",
];

function locateSystemSqlite(): string | null {
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

// ---------------------------------------------------------------------
// Options / types
// ---------------------------------------------------------------------

export interface QueueOptions {
  visibilityTimeoutS?: number;
  maxAttempts?: number;
}

export interface EnqueueOptions {
  delay?: number;
  runAt?: number;
  priority?: number;
  expires?: number;
  /**
   * Enqueue inside an open transaction. The row only becomes visible
   * after `tx.commit()`; a rollback drops it. Use for atomic
   * business-write + enqueue.
   */
  tx?: Transaction;
}

export interface NotifyOptions {
  tx?: Transaction;
}

export interface OpenOptions {
  sqliteLibPath?: string;
}

export interface ScheduledTask {
  name: string;
  queue: string;
  /** Canonical recurring schedule expression. */
  schedule?: string;
  /** Backward-compatible alias for `schedule`. */
  cron?: string;
  payload: unknown;
  priority?: number;
  expiresS?: number | null;
}

export interface ScheduledFire {
  name: string;
  queue: string;
  fire_at: number;
  job_id: number;
}

export interface StreamEvent {
  offset: number;
  topic: string;
  key: string | null;
  payload: unknown;
  created_at: number;
}

export interface Notification {
  id: number;
  channel: string;
  payload: unknown;
}

interface RawJob {
  id: number;
  queue: string;
  payload: string;
  worker_id: string;
  attempts: number;
  claim_expires_at: number;
}

interface RawStreamEvent {
  offset: number;
  topic: string;
  key: string | null;
  payload: string;
  created_at: number;
}

function firstValue(row: Record<string, unknown> | null | undefined): unknown {
  if (!row) return null;
  const keys = Object.keys(row);
  return keys.length === 0 ? null : row[keys[0]];
}

function readDataVersion(raw: BunDB): number {
  const row = raw.query<Record<string, unknown>, []>("PRAGMA data_version").get();
  return Number(firstValue(row) ?? 0);
}

// ---------------------------------------------------------------------
// Database
// ---------------------------------------------------------------------

/**
 * Honker database handle. Wraps a single `bun:sqlite` Database with
 * the Honker loadable extension loaded and the schema bootstrapped.
 */
export class Database {
  private _eventSeq = 0;

  constructor(public readonly raw: BunDB) {}

  /** Get a handle to a named queue. */
  queue(name: string, opts: QueueOptions = {}): Queue {
    return new Queue(this, name, {
      visibilityTimeoutS: opts.visibilityTimeoutS ?? 300,
      maxAttempts: opts.maxAttempts ?? 3,
    });
  }

  /** Get a handle to a named stream. */
  stream(name: string): Stream {
    return new Stream(this, name);
  }

  /** Get a scheduler facade. Cheap; no persistent state. */
  scheduler(): Scheduler {
    return new Scheduler(this);
  }

  /** Update watcher facade used by higher-level waits in this binding. */
  updateEvents(): UpdateEvents {
    return new UpdateEvents(this);
  }

  /**
   * Fire a pg_notify-style pub/sub signal. Payload is any
   * JSON-serializable value. Returns the notification id.
   *
   * Pass `{tx}` to emit inside an open transaction; listeners see it
   * only after commit.
   */
  notify(
    channel: string,
    payload: unknown,
    opts: NotifyOptions = {},
  ): number {
    const json = JSON.stringify(payload);
    const row = (opts.tx ? opts.tx.raw : this.raw)
      .query<{ v: number }, [string, string]>("SELECT notify(?, ?) AS v")
      .get(channel, json)!;
    if (!opts.tx) this._markUpdated();
    return row.v;
  }

  /**
   * Async iterator over notifications on `channel`. Poll-based in
   * Pass 1 (100ms interval). Only rows with id greater than the
   * max-at-attach-time are yielded; no replay.
   *
   * Cancel by breaking the `for await` or calling `.return()` on the
   * iterator. `signal` (AbortSignal) is honored.
   */
  listen(
    channel: string,
    opts: { signal?: AbortSignal; pollMs?: number } = {},
  ): AsyncIterableIterator<Notification> {
    return listenImpl(this, channel, opts.signal, opts.pollMs ?? 100);
  }

  /**
   * Begin a transaction (`BEGIN IMMEDIATE`). Call `commit()` or
   * `rollback()`. If GC'd without either, a best-effort rollback is
   * issued via FinalizationRegistry — but don't rely on it: commit or
   * rollback explicitly.
   */
  transaction(): Transaction {
    this.raw.exec("BEGIN IMMEDIATE");
    return new Transaction(this, this.raw);
  }

  /** Try to acquire an advisory lock. Returns a Lock or null. */
  tryLock(name: string, owner: string, ttlS: number): Lock | null {
    const row = this.raw
      .query<{ v: number }, [string, string, number]>(
        "SELECT honker_lock_acquire(?, ?, ?) AS v",
      )
      .get(name, owner, ttlS)!;
    if (row.v !== 1) return null;
    return new Lock(this, name, owner);
  }

  /** Fixed-window rate limit. True if the request fits. */
  tryRateLimit(name: string, limit: number, per: number): boolean {
    const row = this.raw
      .query<{ v: number }, [string, number, number]>(
        "SELECT honker_rate_limit_try(?, ?, ?) AS v",
      )
      .get(name, limit, per)!;
    return row.v === 1;
  }

  /** Persist a job result for later retrieval via `getResult`. */
  saveResult(jobId: number, value: string, ttlS: number): void {
    this.raw
      .query<{ v: unknown }, [number, string, number]>(
        "SELECT honker_result_save(?, ?, ?) AS v",
      )
      .get(jobId, value, ttlS);
  }

  /** Fetch a stored result, or null if missing/expired. */
  getResult(jobId: number): string | null {
    const row = this.raw
      .query<{ v: string | null }, [number]>(
        "SELECT honker_result_get(?) AS v",
      )
      .get(jobId);
    return row?.v ?? null;
  }

  /** Drop expired results. Returns rows deleted. */
  sweepResults(): number {
    const row = this.raw
      .query<{ v: number }, []>("SELECT honker_result_sweep() AS v")
      .get()!;
    return row.v;
  }

  /** Close the underlying database. */
  close(): void {
    this.raw.close();
  }

  _markUpdated(): void {
    this._eventSeq += 1;
  }

  _eventSnapshot(): number {
    return this._eventSeq;
  }
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

// ---------------------------------------------------------------------
// Transactions
// ---------------------------------------------------------------------

// Best-effort rollback on GC. Holds only the raw BunDB reference so
// the Transaction itself can be collected.
const txFinalizer = new FinalizationRegistry<{
  raw: BunDB;
  doneRef: { done: boolean };
}>((held) => {
  if (held.doneRef.done) return;
  try {
    held.raw.exec("ROLLBACK");
  } catch {
    // Finalizer may fire during teardown; swallow.
  }
});

/**
 * An open transaction. Call `commit()` or `rollback()` exactly once.
 * `tx.raw` is the underlying `bun:sqlite` Database — `*_tx` helpers
 * route through it.
 */
export class Transaction {
  private doneRef = { done: false };

  constructor(
    private readonly db: Database,
    public readonly raw: BunDB,
  ) {
    txFinalizer.register(this, { raw, doneRef: this.doneRef }, this);
  }

  /** Run a statement with optional params. Returns the Statement run result. */
  execute(sql: string, params: unknown[] = []): void {
    const stmt = this.raw.query(sql);
    stmt.run(...(params as Parameters<typeof stmt.run>));
  }

  /** Run a query and return the first row, or null. */
  query<T = Record<string, unknown>>(
    sql: string,
    params: unknown[] = [],
  ): T | null {
    const stmt = this.raw.query<T, unknown[]>(sql);
    return (stmt.get(...params) as T | null) ?? null;
  }

  /** Commit. Idempotent — subsequent calls are no-ops. */
  commit(): void {
    if (this.doneRef.done) return;
    this.raw.exec("COMMIT");
    this.doneRef.done = true;
    txFinalizer.unregister(this);
    this.db._markUpdated();
  }

  /** Roll back. Idempotent — subsequent calls are no-ops. */
  rollback(): void {
    if (this.doneRef.done) return;
    this.raw.exec("ROLLBACK");
    this.doneRef.done = true;
    txFinalizer.unregister(this);
  }

  /** Has this transaction been committed or rolled back? */
  get done(): boolean {
    return this.doneRef.done;
  }
}

// ---------------------------------------------------------------------
// Queues
// ---------------------------------------------------------------------

export class UpdateEvents {
  private closed = false;
  private lastDataVersion: number;
  private lastEventSeq: number;

  constructor(
    private readonly db: Database,
    private readonly pollMs: number = 50,
  ) {
    this.lastDataVersion = readDataVersion(db.raw);
    this.lastEventSeq = db._eventSnapshot();
  }

  async next(signal?: AbortSignal): Promise<void> {
    while (!this.closed && !signal?.aborted) {
      const dataVersion = readDataVersion(this.db.raw);
      const eventSeq = this.db._eventSnapshot();
      if (dataVersion !== this.lastDataVersion || eventSeq !== this.lastEventSeq) {
        this.lastDataVersion = dataVersion;
        this.lastEventSeq = eventSeq;
        return;
      }
      await sleep(this.pollMs, signal);
    }
  }

  close(): void {
    this.closed = true;
  }
}

export class Queue {
  constructor(
    private readonly db: Database,
    public readonly name: string,
    private readonly opts: Required<Omit<QueueOptions, "maxAttempts">> & {
      maxAttempts: number;
    },
  ) {}

  /** Enqueue a job. Payload is JSON-stringified. Returns the row id. */
  enqueue(payload: unknown, opts: EnqueueOptions = {}): number {
    const json = JSON.stringify(payload);
    const conn = opts.tx ? opts.tx.raw : this.db.raw;
    const row = conn
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
    if (!opts.tx) this.db._markUpdated();
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

  /** Ack multiple job ids in one transaction. Returns count acked. */
  ackBatch(ids: number[], workerId: string): number {
    const json = JSON.stringify(ids);
    const row = this.db.raw
      .query<{ v: number }, [string, string]>(
        "SELECT honker_ack_batch(?, ?) AS v",
      )
      .get(json, workerId)!;
    return row.v;
  }

  /** Sweep expired claim rows back to pending. Returns rows touched. */
  sweepExpired(): number {
    const row = this.db.raw
      .query<{ v: number }, [string]>("SELECT honker_sweep_expired(?) AS v")
      .get(this.name)!;
    this.db._markUpdated();
    return row.v;
  }

  nextClaimAt(): number | null {
    const row = this.db.raw
      .query<{ v: number | null }, [string]>(
        "SELECT honker_queue_next_claim_at(?) AS v",
      )
      .get(this.name)!;
    return row.v ?? null;
  }

  dbHandle(): Database {
    return this.db;
  }

  /**
   * Returns a ClaimWaker that waits on DB updates or the next claim
   * deadline, with a fallback poll.
   */
  claimWaker(opts: { idlePollS?: number; pollMs?: number } = {}): ClaimWaker {
    const idlePollMs =
      opts.idlePollS != null ? Math.max(0, opts.idlePollS * 1000) : opts.pollMs ?? 5000;
    return new ClaimWaker(this, idlePollMs);
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
    const ok = (
      this.db.raw
        .query<{ v: number }, [number, string]>("SELECT honker_ack(?, ?) AS v")
        .get(this.id, this.workerId)!.v > 0
    );
    if (ok) this.db._markUpdated();
    return ok;
  }

  /** Put the job back with a delay, or move to dead after maxAttempts. */
  retry(delaySec: number, errorMsg: string): boolean {
    const ok = (
      this.db.raw
        .query<{ v: number }, [number, string, number, string]>(
          "SELECT honker_retry(?, ?, ?, ?) AS v",
        )
        .get(this.id, this.workerId, delaySec, errorMsg)!.v > 0
    );
    if (ok) this.db._markUpdated();
    return ok;
  }

  /** Unconditionally move to dead. */
  fail(errorMsg: string): boolean {
    const ok = (
      this.db.raw
        .query<{ v: number }, [number, string, string]>(
          "SELECT honker_fail(?, ?, ?) AS v",
        )
        .get(this.id, this.workerId, errorMsg)!.v > 0
    );
    if (ok) this.db._markUpdated();
    return ok;
  }

  /** Extend the visibility timeout. */
  heartbeat(extendSec: number): boolean {
    const ok = (
      this.db.raw
        .query<{ v: number }, [number, string, number]>(
          "SELECT honker_heartbeat(?, ?, ?) AS v",
        )
        .get(this.id, this.workerId, extendSec)!.v > 0
    );
    if (ok) this.db._markUpdated();
    return ok;
  }
}

/**
 * Claim waker. Call `next(workerId)` to block on an incoming job;
 * poll-based (Pass 1).
 */
export class ClaimWaker {
  private stopped = false;
  private readonly updates: UpdateEvents;

  constructor(
    private readonly queue: Queue,
    private readonly idlePollMs: number,
  ) {
    this.updates = queue.dbHandle().updateEvents();
  }

  /** Claim one job without blocking. Null if the queue is empty. */
  tryNext(workerId: string): Job | null {
    return this.queue.claimOne(workerId);
  }

  /**
   * Block until a job is claimable; returns the claimed job. Returns
   * null if stopped via `close()`. Honors `signal` (AbortSignal).
   */
  async next(
    workerId: string,
    opts: { signal?: AbortSignal } = {},
  ): Promise<Job | null> {
    while (!this.stopped && !opts.signal?.aborted) {
      const job = this.queue.claimOne(workerId);
      if (job) return job;
      const nextClaimAt = this.queue.nextClaimAt();
      let waitMs = this.idlePollMs;
      if (nextClaimAt && nextClaimAt > 0) {
        waitMs = Math.min(waitMs, Math.max(0, nextClaimAt * 1000 - Date.now()));
      }
      await waitForUpdateOrTimeout(this.updates, opts.signal, waitMs);
    }
    return null;
  }

  /** Stop any in-flight `next()` calls (they resolve to null). */
  close(): void {
    this.stopped = true;
    this.updates.close();
  }
}

// ---------------------------------------------------------------------
// Streams
// ---------------------------------------------------------------------

export class Stream {
  constructor(
    private readonly db: Database,
    public readonly topic: string,
  ) {}

  /** Publish an event. Returns the assigned offset. */
  publish(payload: unknown): number {
    return this.publishImpl(null, payload, this.db.raw);
  }

  /** Publish with a partition key for per-key ordering downstream. */
  publishWithKey(key: string, payload: unknown): number {
    return this.publishImpl(key, payload, this.db.raw);
  }

  /** Publish inside an open transaction. */
  publishTx(tx: Transaction, payload: unknown, key: string | null = null): number {
    return this.publishImpl(key, payload, tx.raw);
  }

  private publishImpl(
    key: string | null,
    payload: unknown,
    conn: BunDB,
  ): number {
    const json = JSON.stringify(payload);
    const row = conn
      .query<{ v: number }, [string, string | null, string]>(
        "SELECT honker_stream_publish(?, ?, ?) AS v",
      )
      .get(this.topic, key, json)!;
    if (conn === this.db.raw) this.db._markUpdated();
    return row.v;
  }

  /** Read up to `limit` events with offset strictly greater than `offset`. */
  readSince(offset: number, limit: number): StreamEvent[] {
    const row = this.db.raw
      .query<{ v: string }, [string, number, number]>(
        "SELECT honker_stream_read_since(?, ?, ?) AS v",
      )
      .get(this.topic, offset, limit)!;
    const raw: RawStreamEvent[] = JSON.parse(row.v);
    return raw.map(toStreamEvent);
  }

  /** Read from the saved offset of `consumer`. Does not advance the offset. */
  readFromConsumer(consumer: string, limit: number): StreamEvent[] {
    return this.readSince(this.getOffset(consumer), limit);
  }

  /** Monotonic: saving a lower offset is a no-op. Returns true if saved. */
  saveOffset(consumer: string, offset: number): boolean {
    const ok = this.saveOffsetImpl(consumer, offset, this.db.raw);
    if (ok) this.db._markUpdated();
    return ok;
  }

  /** Save offset inside an open transaction. */
  saveOffsetTx(tx: Transaction, consumer: string, offset: number): boolean {
    return this.saveOffsetImpl(consumer, offset, tx.raw);
  }

  private saveOffsetImpl(
    consumer: string,
    offset: number,
    conn: BunDB,
  ): boolean {
    const row = conn
      .query<{ v: number }, [string, string, number]>(
        "SELECT honker_stream_save_offset(?, ?, ?) AS v",
      )
      .get(consumer, this.topic, offset)!;
    return row.v > 0;
  }

  /** Current saved offset for `consumer`, or 0 if never saved. */
  getOffset(consumer: string): number {
    const row = this.db.raw
      .query<{ v: number | null }, [string, string]>(
        "SELECT honker_stream_get_offset(?, ?) AS v",
      )
      .get(consumer, this.topic)!;
    return row.v ?? 0;
  }

  /**
   * Subscribe as a named consumer. Resumes from saved offset, polls
   * for new events, auto-saves offset every `saveEveryN` events
   * (default 1000) and on return/throw.
   *
   * Cancel with `signal` (AbortSignal) or by `break`-ing the iterator.
   */
  subscribe(
    consumer: string,
    opts: {
      saveEveryN?: number;
      saveEveryS?: number;
      idlePollS?: number;
      pollMs?: number;
      signal?: AbortSignal;
    } = {},
  ): AsyncIterableIterator<StreamEvent> {
    return subscribeImpl(
      this,
      consumer,
      opts.saveEveryN ?? 1000,
      opts.saveEveryS ?? 1,
      opts.idlePollS != null ? Math.max(0, opts.idlePollS * 1000) : opts.pollMs ?? 5000,
      opts.signal,
    );
  }

  dbHandle(): Database {
    return this.db;
  }
}

function toStreamEvent(r: RawStreamEvent): StreamEvent {
  return {
    offset: r.offset,
    topic: r.topic,
    key: r.key,
    payload: r.payload == null ? null : JSON.parse(r.payload),
    created_at: r.created_at,
  };
}

async function* subscribeImpl(
  stream: Stream,
  consumer: string,
  saveEveryN: number,
  saveEveryS: number,
  idlePollMs: number,
  signal?: AbortSignal,
): AsyncIterableIterator<StreamEvent> {
  let lastOffset = stream.getOffset(consumer);
  let lastSaved = lastOffset;
  let lastSaveAt = Date.now();
  const updates = stream.dbHandle().updateEvents();
  const flush = () => {
    if ((saveEveryN > 0 || saveEveryS > 0) && lastOffset > lastSaved) {
      stream.saveOffset(consumer, lastOffset);
      lastSaved = lastOffset;
      lastSaveAt = Date.now();
    }
  };
  try {
    while (!signal?.aborted) {
      const events = stream.readSince(lastOffset, 100);
      if (events.length === 0) {
        await waitForUpdateOrTimeout(updates, signal, idlePollMs);
        continue;
      }
      for (const ev of events) {
        yield ev;
        lastOffset = ev.offset;
        const enoughEvents = saveEveryN > 0 && lastOffset - lastSaved >= saveEveryN;
        const enoughTime = saveEveryS > 0 && Date.now() - lastSaveAt >= saveEveryS * 1000;
        if (enoughEvents || enoughTime) {
          stream.saveOffset(consumer, lastOffset);
          lastSaved = lastOffset;
          lastSaveAt = Date.now();
        }
      }
    }
  } finally {
    updates.close();
    flush();
  }
}

// ---------------------------------------------------------------------
// Pub/sub listen
// ---------------------------------------------------------------------

async function* listenImpl(
  db: Database,
  channel: string,
  signal: AbortSignal | undefined,
  idlePollMs: number,
): AsyncIterableIterator<Notification> {
  const startId = db.raw
    .query<{ v: number }, []>(
      "SELECT COALESCE(MAX(id), 0) AS v FROM _honker_notifications",
    )
    .get()!.v;
  let lastId = startId;
  const stmt = db.raw.query<
    { id: number; channel: string; payload: string },
    [number, string]
  >(
    "SELECT id, channel, payload FROM _honker_notifications " +
      "WHERE id > ? AND channel = ? ORDER BY id ASC LIMIT 1000",
  );
  const updates = db.updateEvents();
  while (!signal?.aborted) {
    const rows = stmt.all(lastId, channel);
    if (rows.length === 0) {
      await waitForUpdateOrTimeout(updates, signal, idlePollMs);
      continue;
    }
    for (const r of rows) {
      lastId = r.id;
      yield {
        id: r.id,
        channel: r.channel,
        payload: r.payload == null ? null : JSON.parse(r.payload),
      };
    }
  }
  updates.close();
}

// ---------------------------------------------------------------------
// Scheduler
// ---------------------------------------------------------------------

const SCHEDULER_LOCK = "honker-scheduler";
const SCHEDULER_LOCK_TTL_S = 60;
const SCHEDULER_HEARTBEAT_MS = 20_000;
const SCHEDULER_STANDBY_MS = 5_000;

export class Scheduler {
  constructor(private readonly db: Database) {}

  /** Register a recurring scheduled task. Idempotent by name. */
  add(task: ScheduledTask): void {
    const expr = task.schedule ?? task.cron;
    if (!expr) throw new Error("must provide schedule or cron");
    const payloadJson = JSON.stringify(task.payload);
    this.db.raw
      .query<
        { v: number },
        [string, string, string, string, number, number | null]
      >(
        "SELECT honker_scheduler_register(?, ?, ?, ?, ?, ?) AS v",
      )
      .get(
        task.name,
        task.queue,
        expr,
        payloadJson,
        task.priority ?? 0,
        task.expiresS ?? null,
      );
    this.db._markUpdated();
  }

  /** Unregister a task by name. Returns rows deleted. */
  remove(name: string): number {
    const row = this.db.raw
      .query<{ v: number }, [string]>(
        "SELECT honker_scheduler_unregister(?) AS v",
      )
      .get(name)!;
    this.db._markUpdated();
    return row.v;
  }

  /** Fire any due boundaries and return what was enqueued. */
  tick(): ScheduledFire[] {
    const now = Math.floor(Date.now() / 1000);
    const row = this.db.raw
      .query<{ v: string }, [number]>(
        "SELECT honker_scheduler_tick(?) AS v",
      )
      .get(now)!;
    const fires = JSON.parse(row.v) as ScheduledFire[];
    if (fires.length > 0) this.db._markUpdated();
    return fires;
  }

  /** Soonest `next_fire_at` across all tasks, or 0 if none. */
  soonest(): number {
    const row = this.db.raw
      .query<{ v: number | null }, []>("SELECT honker_scheduler_soonest() AS v")
      .get()!;
    return row.v ?? 0;
  }

  /**
   * Leader-elected blocking loop. Only the process holding the
   * `honker-scheduler` lock ticks. Refreshes TTL every 20s; if the
   * refresh returns false (lock stolen), exits the leader loop and
   * re-contests. On tick error, releases the lock and throws.
   *
   * Pass `signal` (AbortSignal) to shut down cleanly.
   */
  async run(opts: { owner: string; signal: AbortSignal }): Promise<void> {
    const { owner, signal } = opts;
    const updates = this.db.updateEvents();
    while (!signal.aborted) {
      const lock = this.db.tryLock(
        SCHEDULER_LOCK,
        owner,
        SCHEDULER_LOCK_TTL_S,
      );
      if (!lock) {
        await waitForUpdateOrTimeout(updates, signal, SCHEDULER_STANDBY_MS);
        continue;
      }
      try {
        await this.leaderLoop(lock, signal, updates);
      } catch (err) {
        // release then rethrow so a standby can take over immediately
        lock.release();
        updates.close();
        throw err;
      }
      // Normal exit (stopped or lost the lock) — release if still held.
      lock.release();
    }
    updates.close();
  }

  private async leaderLoop(
    lock: Lock,
    signal: AbortSignal,
    updates: UpdateEvents,
  ): Promise<void> {
    let lastHeartbeat = Date.now();
    while (!signal.aborted) {
      this.tick();
      const now = Date.now();
      if (now - lastHeartbeat >= SCHEDULER_HEARTBEAT_MS) {
        const stillOurs = lock.heartbeat(SCHEDULER_LOCK_TTL_S);
        if (!stillOurs) return; // lost — exit leader loop, re-contest
        lastHeartbeat = now;
      }
      let waitMs = Math.max(0, SCHEDULER_HEARTBEAT_MS - (Date.now() - lastHeartbeat));
      const nextFire = this.soonest();
      if (nextFire && nextFire > 0) {
        waitMs = Math.min(waitMs, Math.max(0, nextFire * 1000 - Date.now()));
      }
      await waitForUpdateOrTimeout(updates, signal, waitMs);
    }
  }
}

// ---------------------------------------------------------------------
// Locks
// ---------------------------------------------------------------------

// Best-effort release on GC. Holds closed-over primitives + raw BunDB
// so the Lock instance itself is collectable.
const lockFinalizer = new FinalizationRegistry<{
  raw: BunDB;
  name: string;
  owner: string;
  releasedRef: { released: boolean };
}>((held) => {
  if (held.releasedRef.released) return;
  try {
    held.raw
      .query("SELECT honker_lock_release(?, ?)")
      .get(held.name, held.owner);
  } catch {
    // Finalizer may fire during interpreter teardown.
  }
});

export class Lock {
  private releasedRef = { released: false };

  constructor(
    private readonly db: Database,
    public readonly name: string,
    public readonly owner: string,
  ) {
    lockFinalizer.register(
      this,
      {
        raw: db.raw,
        name,
        owner,
        releasedRef: this.releasedRef,
      },
      this,
    );
  }

  /**
   * Release the lock. Idempotent — calling twice returns false on the
   * second call. First call returns whether we actually still held it.
   */
  release(): boolean {
    if (this.releasedRef.released) return false;
    this.releasedRef.released = true;
    lockFinalizer.unregister(this);
    const row = this.db.raw
      .query<{ v: number }, [string, string]>(
        "SELECT honker_lock_release(?, ?) AS v",
      )
      .get(this.name, this.owner)!;
    return row.v > 0;
  }

  /** True once `release()` has been called on this handle. */
  get released(): boolean {
    return this.releasedRef.released;
  }

  /**
   * Extend the TTL. Returns true if we still own it, false if it was
   * stolen (TTL elapsed and another owner acquired it).
   *
   * The extension's `honker_lock_acquire` uses `INSERT OR IGNORE`, so
   * a same-owner re-acquire doesn't actually extend the row. We UPDATE
   * directly, matching the Elixir binding.
   */
  heartbeat(ttlS: number): boolean {
    const stmt = this.db.raw.query<
      unknown,
      [number, string, string]
    >(
      "UPDATE _honker_locks SET expires_at = unixepoch() + ? " +
        "WHERE name = ? AND owner = ?",
    );
    stmt.run(ttlS, this.name, this.owner);
    return this.db.raw.query<{ c: number }, []>(
      "SELECT changes() AS c",
    ).get()!.c > 0;
  }
}

// ---------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------

async function waitForUpdateOrTimeout(
  updates: UpdateEvents,
  signal: AbortSignal | undefined,
  timeoutMs: number,
): Promise<void> {
  const ms = Math.max(0, timeoutMs);
  const inner = new AbortController();
  const forwardAbort = () => inner.abort();
  signal?.addEventListener("abort", forwardAbort, { once: true });
  let timer: ReturnType<typeof setTimeout> | null = null;
  try {
    await Promise.race([
      updates.next(inner.signal),
      new Promise<void>((resolve) => {
        timer = setTimeout(() => {
          inner.abort();
          resolve();
        }, ms);
      }),
      abortPromise(signal).then(() => undefined),
    ]);
  } finally {
    if (timer) clearTimeout(timer);
    signal?.removeEventListener("abort", forwardAbort);
    inner.abort();
  }
}

function sleep(ms: number, signal?: AbortSignal): Promise<void> {
  return new Promise((resolve) => {
    if (signal?.aborted) {
      resolve();
      return;
    }
    const t = setTimeout(() => {
      signal?.removeEventListener("abort", onAbort);
      resolve();
    }, ms);
    const onAbort = () => {
      clearTimeout(t);
      resolve();
    };
    signal?.addEventListener("abort", onAbort, { once: true });
  });
}

function abortPromise(signal?: AbortSignal): Promise<void> {
  if (!signal) return new Promise(() => {});
  if (signal.aborted) return Promise.resolve();
  return new Promise((resolve) => {
    signal.addEventListener("abort", () => resolve(), { once: true });
  });
}
