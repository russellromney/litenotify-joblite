export type JsonPrimitive = string | number | boolean | null
export type JsonValue = JsonPrimitive | JsonValue[] | { [key: string]: JsonValue }

export interface Notification {
  id: number
  channel: string
  payload: JsonValue
  createdAt?: number | null
}

export interface ScheduledFire {
  name: string
  queue: string
  fire_at: number
  job_id: number
}

export interface StreamEvent {
  offset: number
  topic: string
  key: string | null
  payload: JsonValue
  createdAt: number | null
}

export interface QueueOptions {
  visibilityTimeoutS?: number
  maxAttempts?: number
}

export interface EnqueueOptions {
  tx?: Transaction | any
  runAt?: number | null
  delay?: number | null
  priority?: number
  expires?: number | null
}

export interface SchedulerAddOptions {
  name: string
  queue: string
  schedule?: string | null
  cron?: string | null
  payload: JsonValue
  priority?: number
  expiresS?: number | null
}

export class Transaction {
  raw(): any
  execute(sql: string, params?: JsonValue[] | null): number
  query(sql: string, params?: JsonValue[] | null): Array<Record<string, any>>
  notify(channel: string, payload: JsonValue): number
  commit(): void
  rollback(): void
}

export class UpdateEvents {
  raw(): any
  next(): Promise<void>
  close(): void
}

export class Lock {
  readonly name: string
  readonly owner: string
  release(): boolean
  heartbeat(ttlS: number): boolean
}

export class Job {
  readonly id: number
  readonly queue: string
  readonly payload: JsonValue
  readonly workerId: string
  readonly attempts: number
  readonly claimExpiresAt: number | null
  ack(): boolean
  retry(delayS?: number, error?: string): boolean
  fail(error?: string): boolean
  heartbeat(extendS: number): boolean
}

export class ClaimWaker {
  next(workerId: string): Promise<Job | null>
  close(): void
}

export class StreamSubscription implements AsyncIterableIterator<StreamEvent> {
  next(): Promise<IteratorResult<StreamEvent>>
  [Symbol.asyncIterator](): AsyncIterableIterator<StreamEvent>
  close(): void
}

export class Stream {
  publish(payload: JsonValue): number
  publishWithKey(key: string, payload: JsonValue): number
  publishTx(tx: Transaction | any, payload: JsonValue, key?: string | null): number
  readSince(offset: number, limit: number): StreamEvent[]
  readFromConsumer(consumer: string, limit: number): StreamEvent[]
  saveOffset(consumer: string, offset: number): boolean
  saveOffsetTx(tx: Transaction | any, consumer: string, offset: number): boolean
  getOffset(consumer: string): number
  subscribe(
    consumer: string,
    opts?: { saveEveryN?: number; saveEveryS?: number; signal?: AbortSignal }
  ): StreamSubscription
}

export class Listener implements AsyncIterableIterator<Notification> {
  next(): Promise<IteratorResult<Notification>>
  [Symbol.asyncIterator](): AsyncIterableIterator<Notification>
  close(): void
}

export class Scheduler {
  add(opts: SchedulerAddOptions): number | null
  remove(name: string): number
  tick(now?: number): ScheduledFire[]
  soonest(): number | null
  run(owner: string, signal?: AbortSignal): Promise<void>
}

export class Queue {
  readonly name: string
  readonly visibilityTimeoutS: number
  readonly maxAttempts: number
  enqueue(payload: JsonValue, opts?: EnqueueOptions): number
  enqueueTx(tx: Transaction | any, payload: JsonValue, opts?: EnqueueOptions): number
  claimBatch(workerId: string, n: number): Job[]
  claimOne(workerId: string): Job | null
  claim(workerId: string, opts?: { idlePollS?: number }): AsyncIterableIterator<Job>
  ackBatch(ids: number[], workerId: string): number
  sweepExpired(): number
  claimWaker(opts?: { idlePollS?: number }): ClaimWaker
}

export class Database {
  raw(): any
  transaction(): Transaction
  query(sql: string, params?: JsonValue[] | null): Array<Record<string, any>>
  updateEvents(): UpdateEvents
  close(): void
  pruneNotifications(olderThanS?: number | null, maxKeep?: number | null): number
  notify(channel: string, payload: JsonValue): number
  notifyTx(tx: Transaction | any, channel: string, payload: JsonValue): number
  queue(name: string, opts?: QueueOptions): Queue
  stream(name: string): Stream
  listen(channel: string): Listener
  scheduler(): Scheduler
  tryLock(name: string, owner: string, ttlS: number): Lock | null
  tryRateLimit(name: string, limit: number, per: number): boolean
  sweepRateLimits(olderThanS: number): number
  saveResult(jobId: number, value: string, ttlS: number): void
  getResult(jobId: number): string | null
  sweepResults(): number
}

export function open(path: string, maxReaders?: number | null): Database

export const native: any
export const NativeDatabase: any
export const NativeTransaction: any
export const NativeUpdateEvents: any
