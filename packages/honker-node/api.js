'use strict';

const { setTimeout: delay } = require('node:timers/promises');

function scalar(rows) {
  if (!Array.isArray(rows) || rows.length === 0) return null;
  const row = rows[0];
  if (!row || typeof row !== 'object') return null;
  const keys = Object.keys(row);
  return keys.length === 0 ? null : row[keys[0]];
}

function parseJson(text) {
  if (text == null) return null;
  try {
    return JSON.parse(text);
  } catch {
    return text;
  }
}

function jsonText(value) {
  return JSON.stringify(value);
}

function nowUnix() {
  return Math.floor(Date.now() / 1000);
}

function monotonicMs() {
  return performance.now();
}

function aborted(signal) {
  return Boolean(signal?.aborted);
}

function abortPromise(signal) {
  if (!signal) return new Promise(() => {});
  if (signal.aborted) return Promise.resolve();
  return new Promise((resolve) => {
    signal.addEventListener('abort', resolve, { once: true });
  });
}

async function waitForUpdateOrTimeout(updateEvents, signal, timeoutMs) {
  if (aborted(signal)) return;
  const ms = Math.max(0, timeoutMs);
  await Promise.race([
    updateEvents.next().catch(() => undefined),
    delay(ms),
    abortPromise(signal),
  ]);
}

function unwrapTx(tx) {
  return tx instanceof Transaction ? tx._tx : tx;
}

class Transaction {
  constructor(tx) {
    this._tx = tx;
  }

  raw() {
    return this._tx;
  }

  execute(sql, params) {
    return this._tx.execute(sql, params);
  }

  query(sql, params) {
    return this._tx.query(sql, params);
  }

  notify(channel, payload) {
    return this._tx.notify(channel, payload);
  }

  commit() {
    this._tx.commit();
  }

  rollback() {
    this._tx.rollback();
  }
}

class UpdateEvents {
  constructor(ev) {
    this._ev = ev;
    this._closed = false;
  }

  raw() {
    return this._ev;
  }

  async next() {
    if (this._closed) return;
    await this._ev.next();
  }

  close() {
    if (this._closed) return;
    this._closed = true;
    this._ev.close();
  }
}

class Lock {
  constructor(db, name, owner) {
    this._db = db;
    this.name = name;
    this.owner = owner;
  }

  release() {
    return (
      this._db._callScalar('SELECT honker_lock_release(?, ?)', [
        this.name,
        this.owner,
      ]) === 1
    );
  }

  heartbeat(ttlS) {
    return (
      this._db._callScalar('SELECT honker_lock_acquire(?, ?, ?)', [
        this.name,
        this.owner,
        ttlS,
      ]) === 1
    );
  }
}

class Job {
  constructor(queue, row) {
    this._queue = queue;
    this.id = row.id;
    this.queue = row.queue;
    this.payload = parseJson(row.payload);
    this.workerId = row.worker_id;
    this.attempts = row.attempts;
    this.claimExpiresAt = row.claim_expires_at ?? null;
  }

  ack() {
    return this._queue._ack(this.id, this.workerId);
  }

  retry(delayS = 60, error = '') {
    return this._queue._retry(this.id, this.workerId, delayS, error);
  }

  fail(error = '') {
    return this._queue._fail(this.id, this.workerId, error);
  }

  heartbeat(extendS) {
    return this._queue._heartbeat(this.id, this.workerId, extendS);
  }
}

class ClaimWaker {
  constructor(queue, { idlePollS = 5 } = {}) {
    this._queue = queue;
    this._idlePollMs = Math.max(0, idlePollS * 1000);
    this._updates = queue._db.updateEvents();
    this._closed = false;
  }

  async next(workerId) {
    if (this._closed) return null;

    let job = this._queue.claimOne(workerId);
    if (job) return job;

    while (!this._closed) {
      const nextClaimAt = this._queue._nextClaimAt();
      let waitMs = this._idlePollMs;
      if (nextClaimAt && nextClaimAt > 0) {
        waitMs = Math.min(waitMs, Math.max(0, nextClaimAt * 1000 - Date.now()));
      }
      await waitForUpdateOrTimeout(this._updates, null, waitMs);
      if (this._closed) return null;
      job = this._queue.claimOne(workerId);
      if (job) return job;
    }

    return null;
  }

  close() {
    if (this._closed) return;
    this._closed = true;
    this._updates.close();
  }
}

class Queue {
  constructor(db, name, { visibilityTimeoutS = 300, maxAttempts = 3 } = {}) {
    this._db = db;
    this.name = name;
    this.visibilityTimeoutS = visibilityTimeoutS;
    this.maxAttempts = maxAttempts;
  }

  enqueue(payload, opts = {}) {
    if (opts.tx) return this.enqueueTx(opts.tx, payload, opts);
    return this._db._callScalar('SELECT honker_enqueue(?, ?, ?, ?, ?, ?, ?) AS id', [
      this.name,
      jsonText(payload),
      opts.runAt ?? null,
      opts.delay ?? null,
      opts.priority ?? 0,
      this.maxAttempts,
      opts.expires ?? null,
    ]);
  }

  enqueueTx(tx, payload, opts = {}) {
    return scalar(
      unwrapTx(tx).query('SELECT honker_enqueue(?, ?, ?, ?, ?, ?, ?) AS id', [
        this.name,
        jsonText(payload),
        opts.runAt ?? null,
        opts.delay ?? null,
        opts.priority ?? 0,
        this.maxAttempts,
        opts.expires ?? null,
      ]),
    );
  }

  claimBatch(workerId, n) {
    const rowsJson = this._db._callScalar('SELECT honker_claim_batch(?, ?, ?, ?)', [
      this.name,
      workerId,
      n,
      this.visibilityTimeoutS,
    ]);
    return JSON.parse(rowsJson).map((row) => new Job(this, row));
  }

  claimOne(workerId) {
    return this.claimBatch(workerId, 1)[0] ?? null;
  }

  async *claim(workerId, opts = {}) {
    const waker = this.claimWaker(opts);
    try {
      while (true) {
        const job = await waker.next(workerId);
        if (!job) return;
        yield job;
      }
    } finally {
      waker.close();
    }
  }

  ackBatch(ids, workerId) {
    return this._db._callScalar('SELECT honker_ack_batch(?, ?)', [jsonText(ids), workerId]);
  }

  sweepExpired() {
    return this._db._callScalar('SELECT honker_sweep_expired(?)', [this.name]);
  }

  claimWaker(opts = {}) {
    return new ClaimWaker(this, opts);
  }

  _nextClaimAt() {
    return this._db._callScalar('SELECT honker_queue_next_claim_at(?)', [this.name]);
  }

  _ack(jobId, workerId) {
    return this._db._callScalar('SELECT honker_ack(?, ?)', [jobId, workerId]) === 1;
  }

  _retry(jobId, workerId, delayS, error) {
    return (
      this._db._callScalar('SELECT honker_retry(?, ?, ?, ?)', [
        jobId,
        workerId,
        delayS,
        error,
      ]) === 1
    );
  }

  _fail(jobId, workerId, error) {
    return (
      this._db._callScalar('SELECT honker_fail(?, ?, ?)', [jobId, workerId, error]) ===
      1
    );
  }

  _heartbeat(jobId, workerId, extendS) {
    return (
      this._db._callScalar('SELECT honker_heartbeat(?, ?, ?)', [
        jobId,
        workerId,
        extendS,
      ]) === 1
    );
  }
}

class StreamEvent {
  constructor(row) {
    this.offset = row.offset;
    this.topic = row.topic;
    this.key = row.key ?? null;
    this.payload = parseJson(row.payload);
    this.createdAt = row.created_at ?? null;
  }
}

class StreamSubscription {
  constructor(stream, consumer, { saveEveryN = 1000, saveEveryS = 1.0 } = {}) {
    this._stream = stream;
    this._consumer = consumer;
    this._saveEveryN = Math.max(0, saveEveryN);
    this._saveEveryMs = Math.max(0, saveEveryS * 1000);
    this._updates = stream._db.updateEvents();
    this._closed = false;
    this._pending = [];
    this._deliveredSinceSave = 0;
    this._lastSavedOffset = stream.getOffset(consumer);
    this._lastSeenOffset = this._lastSavedOffset;
    this._lastSaveAt = Date.now();
  }

  [Symbol.asyncIterator]() {
    return this;
  }

  _maybeSaveOffset() {
    if (this._lastSeenOffset <= this._lastSavedOffset) return;
    const hitCount =
      this._saveEveryN > 0 && this._deliveredSinceSave >= this._saveEveryN;
    const hitTime =
      this._saveEveryMs > 0 && Date.now() - this._lastSaveAt >= this._saveEveryMs;
    if (!hitCount && !hitTime) return;
    this._stream.saveOffset(this._consumer, this._lastSeenOffset);
    this._lastSavedOffset = this._lastSeenOffset;
    this._deliveredSinceSave = 0;
    this._lastSaveAt = Date.now();
  }

  _loadPending() {
    const rows = this._stream.readSince(this._lastSeenOffset, 256);
    this._pending.push(...rows);
  }

  async next() {
    while (!this._closed) {
      if (this._pending.length === 0) this._loadPending();
      if (this._pending.length > 0) {
        const event = this._pending.shift();
        this._lastSeenOffset = event.offset;
        this._deliveredSinceSave += 1;
        this._maybeSaveOffset();
        return { done: false, value: event };
      }
      await this._updates.next().catch(() => undefined);
    }
    return { done: true, value: undefined };
  }

  close() {
    if (this._closed) return;
    this._closed = true;
    if (this._lastSeenOffset > this._lastSavedOffset) {
      this._stream.saveOffset(this._consumer, this._lastSeenOffset);
      this._lastSavedOffset = this._lastSeenOffset;
    }
    this._updates.close();
  }
}

class Stream {
  constructor(db, name) {
    this._db = db;
    this.name = name;
  }

  publish(payload) {
    return this._db._callScalar('SELECT honker_stream_publish(?, NULL, ?)', [
      this.name,
      jsonText(payload),
    ]);
  }

  publishWithKey(key, payload) {
    return this._db._callScalar('SELECT honker_stream_publish(?, ?, ?)', [
      this.name,
      key,
      jsonText(payload),
    ]);
  }

  publishTx(tx, payload) {
    return scalar(
      unwrapTx(tx).query('SELECT honker_stream_publish(?, NULL, ?)', [
        this.name,
        jsonText(payload),
      ]),
    );
  }

  readSince(offset, limit) {
    const rowsJson = this._db._callScalar('SELECT honker_stream_read_since(?, ?, ?)', [
      this.name,
      offset,
      limit,
    ]);
    return JSON.parse(rowsJson).map((row) => new StreamEvent(row));
  }

  readFromConsumer(consumer, limit) {
    return this.readSince(this.getOffset(consumer), limit);
  }

  saveOffset(consumer, offset) {
    return (
      this._db._callScalar('SELECT honker_stream_save_offset(?, ?, ?)', [
        this.name,
        consumer,
        offset,
      ]) === 1
    );
  }

  saveOffsetTx(tx, consumer, offset) {
    return (
      scalar(
        unwrapTx(tx).query('SELECT honker_stream_save_offset(?, ?, ?)', [
          this.name,
          consumer,
          offset,
        ]),
      ) === 1
    );
  }

  getOffset(consumer) {
    return this._db._callScalar('SELECT honker_stream_get_offset(?, ?)', [
      this.name,
      consumer,
    ]);
  }

  subscribe(consumer, opts = {}) {
    return new StreamSubscription(this, consumer, opts);
  }
}

class Listener {
  constructor(db, channel) {
    this._db = db;
    this.channel = channel;
    this._updates = db.updateEvents();
    this._closed = false;
    this._pending = [];
    this._lastSeen = scalar(db.query('SELECT COALESCE(MAX(id), 0) FROM _honker_notifications')) ?? 0;
  }

  [Symbol.asyncIterator]() {
    return this;
  }

  _loadPending() {
    const rows = this._db.query(
      'SELECT id, channel, payload, created_at FROM _honker_notifications WHERE id > ? ORDER BY id',
      [this._lastSeen],
    );
    for (const row of rows) {
      this._lastSeen = row.id;
      if (row.channel === this.channel) {
        this._pending.push({
          id: row.id,
          channel: row.channel,
          payload: parseJson(row.payload),
          createdAt: row.created_at ?? null,
        });
      }
    }
  }

  async next() {
    while (!this._closed) {
      if (this._pending.length === 0) this._loadPending();
      if (this._pending.length > 0) {
        return { done: false, value: this._pending.shift() };
      }
      await this._updates.next().catch(() => undefined);
    }
    return { done: true, value: undefined };
  }

  close() {
    if (this._closed) return;
    this._closed = true;
    this._updates.close();
  }
}

class Scheduler {
  constructor(db) {
    this._db = db;
  }

  add({ name, queue, schedule = null, cron = null, payload, priority = 0, expiresS = null }) {
    const expr = schedule ?? cron;
    if (!expr) throw new Error('must provide schedule or cron');
    this._db._callScalar('SELECT honker_scheduler_register(?, ?, ?, ?, ?, ?)', [
      name,
      queue,
      expr,
      jsonText(payload),
      priority,
      expiresS,
    ]);
  }

  remove(name) {
    return this._db._callScalar('SELECT honker_scheduler_unregister(?)', [name]);
  }

  tick(now = nowUnix()) {
    const rowsJson = this._db._callScalar('SELECT honker_scheduler_tick(?)', [now]);
    return JSON.parse(rowsJson);
  }

  soonest() {
    return this._db._callScalar('SELECT honker_scheduler_soonest()');
  }

  async run(owner, signal) {
    const updates = this._db.updateEvents();
    try {
      while (!aborted(signal)) {
        const lock = this._db.tryLock('honker-scheduler', owner, 60);
        if (!lock) {
          await waitForUpdateOrTimeout(updates, signal, 5000);
          continue;
        }
        try {
          await this._leaderLoop(lock, signal, updates);
        } finally {
          try {
            lock.release();
          } catch {}
        }
      }
    } finally {
      updates.close();
    }
  }

  async _leaderLoop(lock, signal, updates) {
    const heartbeatMs = 20_000;
    let lastHeartbeat = monotonicMs();
    while (!aborted(signal)) {
      this.tick();
      if (monotonicMs() - lastHeartbeat >= heartbeatMs) {
        if (!lock.heartbeat(60)) return;
        lastHeartbeat = monotonicMs();
      }

      let waitMs = Math.max(0, heartbeatMs - (monotonicMs() - lastHeartbeat));
      const nextFire = this.soonest();
      if (nextFire && nextFire > 0) {
        waitMs = Math.min(waitMs, Math.max(0, nextFire * 1000 - Date.now()));
      }
      await waitForUpdateOrTimeout(updates, signal, waitMs);
    }
  }
}

class Database {
  constructor(db) {
    this._db = db;
  }

  raw() {
    return this._db;
  }

  transaction() {
    return new Transaction(this._db.transaction());
  }

  query(sql, params) {
    return this._db.query(sql, params);
  }

  _callRows(sql, params) {
    const tx = this.transaction();
    try {
      const rows = tx.query(sql, params);
      tx.commit();
      return rows;
    } catch (err) {
      try {
        tx.rollback();
      } catch {}
      throw err;
    }
  }

  _callScalar(sql, params) {
    return scalar(this._callRows(sql, params));
  }

  updateEvents() {
    return new UpdateEvents(this._db.updateEvents());
  }

  close() {
    this._db.close();
  }

  pruneNotifications(olderThanS, maxKeep) {
    return this._db.pruneNotifications(olderThanS, maxKeep);
  }

  notify(channel, payload) {
    const tx = this.transaction();
    try {
      const id = tx.notify(channel, payload);
      tx.commit();
      return id;
    } catch (err) {
      try {
        tx.rollback();
      } catch {}
      throw err;
    }
  }

  notifyTx(tx, channel, payload) {
    return unwrapTx(tx).notify(channel, payload);
  }

  queue(name, opts = {}) {
    return new Queue(this, name, opts);
  }

  stream(name) {
    return new Stream(this, name);
  }

  listen(channel) {
    return new Listener(this, channel);
  }

  scheduler() {
    return new Scheduler(this);
  }

  tryLock(name, owner, ttlS) {
    const ok = this._callScalar('SELECT honker_lock_acquire(?, ?, ?)', [
      name,
      owner,
      ttlS,
    ]);
    return ok === 1 ? new Lock(this, name, owner) : null;
  }

  tryRateLimit(name, limit, per) {
    return this._callScalar('SELECT honker_rate_limit_try(?, ?, ?)', [
      name,
      limit,
      per,
    ]) === 1;
  }

  sweepRateLimits(olderThanS) {
    return this._callScalar('SELECT honker_rate_limit_sweep(?)', [olderThanS]);
  }

  saveResult(jobId, value, ttlS) {
    this._callScalar('SELECT honker_result_save(?, ?, ?)', [jobId, value, ttlS]);
  }

  getResult(jobId) {
    return this._callScalar('SELECT honker_result_get(?)', [jobId]);
  }

  sweepResults() {
    return this._callScalar('SELECT honker_result_sweep()');
  }
}

module.exports = function buildApi(nativeBinding) {
  function open(path, maxReaders) {
    return new Database(nativeBinding.open(path, maxReaders));
  }

  return {
    open,
    Database,
    Transaction,
    UpdateEvents,
    Queue,
    Job,
    ClaimWaker,
    Stream,
    StreamEvent,
    StreamSubscription,
    Listener,
    Scheduler,
    Lock,
    native: nativeBinding,
    NativeDatabase: nativeBinding.Database,
    NativeTransaction: nativeBinding.Transaction,
    NativeUpdateEvents: nativeBinding.UpdateEvents,
  };
};
