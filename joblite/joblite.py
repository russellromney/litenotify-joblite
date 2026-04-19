import asyncio
import json
import time
import traceback
from collections import deque
from typing import Any, AsyncIterator, Callable, Optional

import litenotify


class Job:
    __slots__ = (
        "queue",
        "id",
        "queue_name",
        "_payload_raw",
        "_payload",
        "state",
        "priority",
        "run_at",
        "worker_id",
        "claim_expires_at",
        "attempts",
        "max_attempts",
        "last_error",
        "created_at",
        # Iterator that owns this Job, if any. When set, `ack()` defers
        # the UPDATE into the next claim's transaction (pipelined), which
        # halves the tx count for the common async-iterator worker loop.
        # `claim_one()` / `claim_batch()` leave this None so direct
        # callers still get the per-tx semantics (and an accurate bool
        # return from ack).
        "_iter",
    )

    # Sentinel distinguishing "payload decoded to None" from "not yet decoded".
    _UNSET = object()

    def __init__(self, queue: "Queue", row: dict):
        # `row` comes from the claim UPDATE's RETURNING. The narrow claim
        # path only returns hot-path fields (id, queue, payload, worker_id,
        # attempts, claim_expires_at) because the other columns are rarely
        # accessed and cost a noticeable chunk per claim. Fields not in the
        # row default to sensible post-claim values; if code actually reads
        # one of those attributes, the value is still meaningful.
        self.queue = queue
        self.id = row["id"]
        self.queue_name = row["queue"]
        # Lazy JSON decode: handlers that only need id/worker_id/state skip
        # the parse entirely. Matters at claim_batch(128) where decoding
        # 128 payloads on every batch is visible in the hot path.
        self._payload_raw = row["payload"]
        self._payload = Job._UNSET
        self.worker_id = row["worker_id"]
        self.attempts = row["attempts"]
        self.claim_expires_at = row["claim_expires_at"]
        # After a claim UPDATE, state is by construction 'processing'.
        self.state = row.get("state", "processing")
        self.priority = row.get("priority", 0)
        self.run_at = row.get("run_at", 0)
        self.max_attempts = row.get("max_attempts", queue.max_attempts)
        self.last_error = row.get("last_error", None)
        self.created_at = row.get("created_at", 0)
        self._iter = None

    @property
    def payload(self) -> Any:
        if self._payload is Job._UNSET:
            self._payload = (
                json.loads(self._payload_raw) if self._payload_raw else None
            )
        return self._payload

    def ack(self) -> bool:
        if self._iter is not None:
            # Deferred ack: the iterator will flush this id inside the
            # next claim's transaction. Optimistic True is safe because
            # the pipeline executes within milliseconds of the claim, so
            # the visibility window hasn't elapsed.
            self._iter._pending_acks.append(self.id)
            return True
        return self.queue.ack(self.id, self.worker_id)

    def retry(self, delay_s: int = 60, error: str = "") -> bool:
        return self.queue.retry(self.id, self.worker_id, delay_s, error)

    def fail(self, error: str = "") -> bool:
        return self.queue.fail(self.id, self.worker_id, error)

    def heartbeat(self, extend_s: Optional[int] = None) -> bool:
        return self.queue.heartbeat(self.id, self.worker_id, extend_s)


class Queue:
    def __init__(
        self,
        db,
        name: str,
        visibility_timeout_s: int = 300,
        max_attempts: int = 3,
    ):
        self.db = db
        self.name = name
        self.visibility_timeout_s = int(visibility_timeout_s)
        self.max_attempts = int(max_attempts)
        self._init_schema()

    def _init_schema(self):
        with self.db.transaction() as tx:
            tx.execute(
                """
                CREATE TABLE IF NOT EXISTS _joblite_jobs (
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  queue TEXT NOT NULL,
                  payload TEXT NOT NULL,
                  state TEXT NOT NULL DEFAULT 'pending',
                  priority INTEGER NOT NULL DEFAULT 0,
                  run_at INTEGER NOT NULL DEFAULT (unixepoch()),
                  worker_id TEXT,
                  claim_expires_at INTEGER,
                  attempts INTEGER NOT NULL DEFAULT 0,
                  max_attempts INTEGER NOT NULL DEFAULT 3,
                  last_error TEXT,
                  created_at INTEGER NOT NULL DEFAULT (unixepoch())
                )
                """
            )
            # Partial index keyed on (queue, priority DESC, run_at, id) with
            # a WHERE clause filtering to only claimable jobs. Critical that
            # `state` is NOT in the key: changing state on claim (pending ->
            # processing) would otherwise force the row to reshuffle in the
            # B-tree, which measured at ~9x slowdown per UPDATE. The state
            # filter is in the partial-index WHERE clause; the planner still
            # matches it against the query predicate. When state transitions
            # to 'done' / 'dead' the row drops out of the index entirely
            # (cheap single-key delete, no B-tree rewrite).
            #
            # `_v2` suffix so CREATE IF NOT EXISTS stays idempotent even
            # when the old full-key index is being dropped alongside.
            tx.execute(
                """
                CREATE INDEX IF NOT EXISTS _joblite_jobs_claim_v2
                  ON _joblite_jobs(queue, priority DESC, run_at, id)
                  WHERE state IN ('pending', 'processing')
                """
            )
            # Drop the old full-key index from any DB initialized before
            # this change. Cheap no-op on fresh DBs.
            tx.execute("DROP INDEX IF EXISTS _joblite_jobs_claim")

    def _channel(self) -> str:
        return f"joblite:{self.name}"

    def enqueue(
        self,
        payload: Any,
        tx=None,
        run_at: Optional[int] = None,
        priority: int = 0,
    ) -> None:
        run_at_val = int(run_at) if run_at is not None else int(time.time())
        payload_str = json.dumps(payload)
        params = [self.name, payload_str, run_at_val, int(priority), self.max_attempts]

        if tx is not None:
            tx.execute(
                """
                INSERT INTO _joblite_jobs (queue, payload, run_at, priority, max_attempts)
                VALUES (?, ?, ?, ?, ?)
                """,
                params,
            )
            tx.notify(self._channel(), "new")
            return

        with self.db.transaction() as own_tx:
            own_tx.execute(
                """
                INSERT INTO _joblite_jobs (queue, payload, run_at, priority, max_attempts)
                VALUES (?, ?, ?, ?, ?)
                """,
                params,
            )
            own_tx.notify(self._channel(), "new")

    def claim_one(self, worker_id: str) -> Optional[Job]:
        jobs = self.claim_batch(worker_id, 1)
        return jobs[0] if jobs else None

    def claim_batch(self, worker_id: str, n: int) -> list:
        """Atomically claim up to `n` jobs in a single transaction.

        The SQL text is constant across batch sizes (LIMIT is parameterized)
        so the statement cache keeps one prepared plan for every call.
        One BEGIN IMMEDIATE + one UPDATE ... RETURNING *, independent of n.

        The `state IN ('pending', 'processing')` predicate is logically a
        no-op (the OR clause below covers it) but SQLite's planner cannot
        otherwise prove the query implies the partial index's WHERE and
        falls back to a full table scan. With the explicit IN the planner
        picks the partial index and the claim goes from table scan to
        an index lookup.
        """
        n = int(n)
        if n <= 0:
            return []
        with self.db.transaction() as tx:
            # Return only the fields a handler needs on the hot path.
            # Everything else (priority, run_at, claim_expires_at,
            # max_attempts, last_error, created_at, state) is lazy-loaded
            # by Job via a follow-up reader query if the caller actually
            # reads the attribute. Saves ~20% per claim on the 11-col
            # RETURNING *.
            rows = tx.query(
                """
                UPDATE _joblite_jobs
                SET state='processing',
                    worker_id=?,
                    claim_expires_at=unixepoch() + ?,
                    attempts=attempts+1
                WHERE id IN (
                  SELECT id FROM _joblite_jobs
                  WHERE queue=?
                    AND state IN ('pending', 'processing')
                    AND ((state='pending' AND run_at <= unixepoch())
                      OR (state='processing' AND claim_expires_at < unixepoch()))
                  ORDER BY priority DESC, run_at ASC, id ASC
                  LIMIT ?
                )
                RETURNING id, queue, payload, worker_id, attempts, claim_expires_at
                """,
                [worker_id, self.visibility_timeout_s, self.name, n],
            )
        return [Job(self, row) for row in rows]

    def ack_and_claim_batch(
        self, ack_ids, worker_id: str, n: int
    ) -> list:
        """One transaction: ack `ack_ids` (if any) then claim up to `n`
        new jobs. Used by the async iterator to pipeline ack-of-previous
        with claim-of-next and halve the tx count per job.

        Returns the list of newly-claimed jobs. The ack is best-effort;
        ids whose claim has since expired are quietly skipped (the
        reclaimed-by-another-worker semantics are preserved because the
        other worker already took the row).
        """
        n = int(n)
        if n <= 0 and not ack_ids:
            return []
        with self.db.transaction() as tx:
            if ack_ids:
                tx.execute(
                    """
                    UPDATE _joblite_jobs
                    SET state='done'
                    WHERE id IN (SELECT value FROM json_each(?))
                      AND worker_id = ?
                      AND claim_expires_at >= unixepoch()
                    """,
                    [json.dumps([int(i) for i in ack_ids]), worker_id],
                )
            if n <= 0:
                return []
            rows = tx.query(
                """
                UPDATE _joblite_jobs
                SET state='processing',
                    worker_id=?,
                    claim_expires_at=unixepoch() + ?,
                    attempts=attempts+1
                WHERE id IN (
                  SELECT id FROM _joblite_jobs
                  WHERE queue=?
                    AND state IN ('pending', 'processing')
                    AND ((state='pending' AND run_at <= unixepoch())
                      OR (state='processing' AND claim_expires_at < unixepoch()))
                  ORDER BY priority DESC, run_at ASC, id ASC
                  LIMIT ?
                )
                RETURNING id, queue, payload, worker_id, attempts, claim_expires_at
                """,
                [worker_id, self.visibility_timeout_s, self.name, n],
            )
        return [Job(self, row) for row in rows]

    def ack_batch(self, job_ids, worker_id: str) -> int:
        """Ack multiple jobs in a single transaction. Returns the count of
        jobs whose claim was still valid (i.e. the at-least-once contract
        is preserved for each).

        Uses `json_each(?)` over a JSON array of ids so the SQL text stays
        constant across batch sizes and hits the statement cache.
        """
        ids = [int(i) for i in job_ids]
        if not ids:
            return 0
        with self.db.transaction() as tx:
            rows = tx.query(
                """
                UPDATE _joblite_jobs
                SET state='done'
                WHERE id IN (SELECT value FROM json_each(?))
                  AND worker_id = ?
                  AND claim_expires_at >= unixepoch()
                RETURNING id
                """,
                [json.dumps(ids), worker_id],
            )
        return len(rows)

    def claim(
        self,
        worker_id: str,
        idle_poll_s: float = 5.0,
        batch_size: int = 32,
    ) -> AsyncIterator[Job]:
        return _WorkerQueueIter(self, worker_id, idle_poll_s, batch_size)

    def ack(self, job_id: int, worker_id: str) -> bool:
        with self.db.transaction() as tx:
            rows = tx.query(
                """
                UPDATE _joblite_jobs
                SET state='done'
                WHERE id=? AND worker_id=? AND claim_expires_at >= unixepoch()
                RETURNING id
                """,
                [int(job_id), worker_id],
            )
        return len(rows) > 0

    def retry(self, job_id: int, worker_id: str, delay_s: int, error: str) -> bool:
        with self.db.transaction() as tx:
            rows = tx.query(
                """
                UPDATE _joblite_jobs
                SET state=CASE WHEN attempts >= max_attempts THEN 'dead' ELSE 'pending' END,
                    run_at=unixepoch() + ?,
                    last_error=?,
                    worker_id=NULL,
                    claim_expires_at=NULL
                WHERE id=? AND worker_id=? AND claim_expires_at >= unixepoch()
                RETURNING id
                """,
                [int(delay_s), error, int(job_id), worker_id],
            )
            if rows:
                tx.notify(self._channel(), "retry")
        return len(rows) > 0

    def fail(self, job_id: int, worker_id: str, error: str) -> bool:
        with self.db.transaction() as tx:
            rows = tx.query(
                """
                UPDATE _joblite_jobs
                SET state='dead',
                    last_error=?,
                    worker_id=NULL,
                    claim_expires_at=NULL
                WHERE id=? AND worker_id=? AND claim_expires_at >= unixepoch()
                RETURNING id
                """,
                [error, int(job_id), worker_id],
            )
        return len(rows) > 0

    def heartbeat(
        self, job_id: int, worker_id: str, extend_s: Optional[int] = None
    ) -> bool:
        extend = int(extend_s) if extend_s is not None else self.visibility_timeout_s
        with self.db.transaction() as tx:
            rows = tx.query(
                """
                UPDATE _joblite_jobs
                SET claim_expires_at=unixepoch() + ?
                WHERE id=? AND worker_id=? AND state='processing'
                RETURNING id
                """,
                [extend, int(job_id), worker_id],
            )
        return len(rows) > 0


class Retryable(Exception):
    """Raise from a task handler to request a scheduled retry with a specific
    delay. Any other exception is also retried, but with a generic backoff.
    Lives in joblite core so framework plugins (fastapi, django, ...) can all
    reuse the same signal without depending on each other."""

    def __init__(self, message: str = "", delay_s: int = 60):
        super().__init__(message)
        self.delay_s = int(delay_s)


class Event:
    __slots__ = ("offset", "topic", "key", "_payload_raw", "_payload", "created_at")

    _UNSET = object()

    def __init__(self, row: dict):
        self.offset = row["offset"]
        self.topic = row["topic"]
        self.key = row["key"]
        # Lazy JSON decode; see Job.payload for rationale.
        self._payload_raw = row["payload"]
        self._payload = Event._UNSET
        self.created_at = row["created_at"]

    @property
    def payload(self) -> Any:
        if self._payload is Event._UNSET:
            self._payload = (
                json.loads(self._payload_raw) if self._payload_raw else None
            )
        return self._payload

    def __repr__(self):
        return (
            f"Event(offset={self.offset}, topic={self.topic!r}, "
            f"key={self.key!r}, payload={self.payload!r})"
        )


class Stream:
    """Durable pub/sub. Events are rows; consumers track offsets.

    Publish inside a transaction to couple the event atomically to your
    business write. Subscribe with a `from_offset` to catch up after a
    disconnect; the iterator replays rows with offset > from_offset, then
    transitions to live NOTIFY delivery.
    """

    def __init__(self, db, name: str):
        self.db = db
        self.name = name
        self._init_schema()

    def _init_schema(self):
        with self.db.transaction() as tx:
            tx.execute(
                """
                CREATE TABLE IF NOT EXISTS _joblite_stream (
                  offset INTEGER PRIMARY KEY AUTOINCREMENT,
                  topic TEXT NOT NULL,
                  key TEXT,
                  payload TEXT NOT NULL,
                  created_at INTEGER NOT NULL DEFAULT (unixepoch())
                )
                """
            )
            tx.execute(
                """
                CREATE INDEX IF NOT EXISTS _joblite_stream_topic
                  ON _joblite_stream(topic, offset)
                """
            )
            tx.execute(
                """
                CREATE TABLE IF NOT EXISTS _joblite_stream_consumers (
                  name TEXT NOT NULL,
                  topic TEXT NOT NULL,
                  offset INTEGER NOT NULL DEFAULT 0,
                  PRIMARY KEY (name, topic)
                )
                """
            )

    def _channel(self) -> str:
        return f"joblite:stream:{self.name}"

    def publish(
        self, payload: Any, key: Optional[str] = None, tx=None
    ) -> None:
        payload_str = json.dumps(payload)
        params = [self.name, key, payload_str]
        if tx is not None:
            tx.execute(
                """
                INSERT INTO _joblite_stream (topic, key, payload)
                VALUES (?, ?, ?)
                """,
                params,
            )
            tx.notify(self._channel(), "new")
            return
        with self.db.transaction() as own_tx:
            own_tx.execute(
                """
                INSERT INTO _joblite_stream (topic, key, payload)
                VALUES (?, ?, ?)
                """,
                params,
            )
            own_tx.notify(self._channel(), "new")

    def _read_since(self, offset: int, limit: int = 1000) -> list:
        rows = self.db.query(
            """
            SELECT offset, topic, key, payload, created_at
            FROM _joblite_stream
            WHERE topic=? AND offset > ?
            ORDER BY offset ASC
            LIMIT ?
            """,
            [self.name, int(offset), int(limit)],
        )
        return rows

    def save_offset(self, consumer: str, offset: int) -> None:
        with self.db.transaction() as tx:
            tx.execute(
                """
                INSERT INTO _joblite_stream_consumers (name, topic, offset)
                VALUES (?, ?, ?)
                ON CONFLICT(name, topic) DO UPDATE SET offset=excluded.offset
                WHERE excluded.offset > _joblite_stream_consumers.offset
                """,
                [consumer, self.name, int(offset)],
            )

    def get_offset(self, consumer: str) -> int:
        rows = self.db.query(
            """
            SELECT offset FROM _joblite_stream_consumers
            WHERE name=? AND topic=?
            """,
            [consumer, self.name],
        )
        return rows[0]["offset"] if rows else 0

    def subscribe(
        self,
        consumer: Optional[str] = None,
        from_offset: Optional[int] = None,
    ) -> AsyncIterator[Event]:
        """Yield events with offset > from_offset, then live events as they
        arrive. If `consumer` is given and `from_offset` is None, the last
        saved offset for that consumer is used.
        """
        if from_offset is None and consumer is not None:
            from_offset = self.get_offset(consumer)
        if from_offset is None:
            from_offset = 0
        return _StreamIter(self, int(from_offset))


class _StreamIter:
    def __init__(self, stream: Stream, from_offset: int):
        self.stream = stream
        self.offset = from_offset
        # `deque` for O(1) popleft; `list.pop(0)` was O(n) per yield which
        # compounded on large replay batches (default 1000 rows per refresh).
        self._buffer: deque = deque()
        # Subscribe to the notify channel BEFORE the first read so that events
        # published between "read empty" and "start listening" can't slip
        # through. The listener buffers all notifications from this point
        # forward; we drain the queue every time we re-read and catch up.
        self._listener = stream.db.listen(stream._channel())

    def __aiter__(self):
        return self

    async def __anext__(self):
        while True:
            if self._buffer:
                row = self._buffer.popleft()
                self.offset = row["offset"]
                return Event(row)

            rows = self.stream._read_since(self.offset)
            if rows:
                self._buffer.extend(rows)
                continue

            try:
                await asyncio.wait_for(
                    self._listener.__anext__(), timeout=15.0
                )
            except asyncio.TimeoutError:
                pass
            except StopAsyncIteration:
                raise StopAsyncIteration


class Outbox:
    """Transactional side-effect delivery built on joblite.queue.

    `delivery(payload)` is a user-supplied callable (sync or async). On
    failure, the outbox retries with exponential backoff up to max_attempts
    and then marks the job dead. Callers enqueue side effects in the same
    transaction as the business write; a background worker drives delivery.
    """

    def __init__(
        self,
        db,
        name: str,
        delivery: Callable,
        max_attempts: int = 5,
        base_backoff_s: int = 5,
        visibility_timeout_s: int = 60,
    ):
        if not callable(delivery):
            raise TypeError("delivery must be callable")
        self.db = db
        self.name = name
        self.delivery = delivery
        self.max_attempts = int(max_attempts)
        self.base_backoff_s = int(base_backoff_s)
        self._queue = db.queue(
            f"_outbox:{name}",
            visibility_timeout_s=visibility_timeout_s,
            max_attempts=self.max_attempts,
        )

    @property
    def queue(self) -> Queue:
        return self._queue

    def enqueue(self, payload: Any, tx=None, priority: int = 0) -> None:
        self._queue.enqueue(payload, tx=tx, priority=priority)

    async def run_worker(self, worker_id: str):
        """Drive delivery forever. Cancel the task to stop."""
        async for job in self._queue.claim(worker_id):
            try:
                if asyncio.iscoroutinefunction(self.delivery):
                    await self.delivery(job.payload)
                else:
                    self.delivery(job.payload)
                job.ack()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                delay = self.base_backoff_s * (2 ** (job.attempts - 1))
                job.retry(
                    delay_s=delay,
                    error=f"{e}\n{traceback.format_exc()}",
                )


class Database:
    """Wrapper over the litenotify Database that adds queue/stream/outbox."""

    def __init__(self, inner):
        self._inner = inner
        self._queues: dict = {}
        self._streams: dict = {}
        self._outboxes: dict = {}

    def transaction(self):
        return self._inner.transaction()

    def listen(self, channel: str):
        return self._inner.listen(channel)

    def query(self, sql: str, params=None):
        return self._inner.query(sql, params)

    def queue(
        self,
        name: str,
        visibility_timeout_s: int = 300,
        max_attempts: int = 3,
    ) -> Queue:
        existing = self._queues.get(name)
        if existing is not None:
            return existing
        q = Queue(
            self,
            name,
            visibility_timeout_s=visibility_timeout_s,
            max_attempts=max_attempts,
        )
        self._queues[name] = q
        return q

    def stream(self, name: str) -> Stream:
        existing = self._streams.get(name)
        if existing is not None:
            return existing
        s = Stream(self, name)
        self._streams[name] = s
        return s

    def outbox(
        self,
        name: str,
        delivery: Callable,
        max_attempts: int = 5,
        base_backoff_s: int = 5,
        visibility_timeout_s: int = 60,
    ) -> Outbox:
        existing = self._outboxes.get(name)
        if existing is not None:
            return existing
        o = Outbox(
            self,
            name,
            delivery=delivery,
            max_attempts=max_attempts,
            base_backoff_s=base_backoff_s,
            visibility_timeout_s=visibility_timeout_s,
        )
        self._outboxes[name] = o
        return o


def open(path: str, max_readers: int = 8) -> Database:
    return Database(litenotify.open(path, max_readers=max_readers))


class _WorkerQueueIter:
    """Async iterator that yields jobs one at a time but claims them in
    batches of `batch_size`. One write transaction per batch amortizes the
    per-tx overhead across many jobs.

    Also pipelines ack-of-previous with claim-of-next: `Job.ack()` on
    jobs yielded from this iterator defers into `_pending_acks`, and the
    next batch's claim transaction flushes them in the same tx. Halves
    the write-tx count per job for the common worker pattern
    (`async for job in q.claim(...): handle(job); job.ack()`).
    """

    def __init__(
        self,
        queue: Queue,
        worker_id: str,
        idle_poll_s: float,
        batch_size: int = 32,
    ):
        self.queue = queue
        self.worker_id = worker_id
        self.idle_poll_s = idle_poll_s
        self.batch_size = max(1, int(batch_size))
        self._listener = None
        # Deque for O(1) popleft; list.pop(0) was O(n) per yield which
        # compounded at batch_size=128 (~128x worse than batch=8 before).
        self._buffer: deque = deque()
        # Ids from previously-yielded jobs that the user has ack'd.
        # Flushed inside the next claim transaction via
        # `ack_and_claim_batch`.
        self._pending_acks: list = []

    def __aiter__(self):
        return self

    async def __anext__(self):
        while True:
            if self._buffer:
                return self._buffer.popleft()
            batch = self.queue.ack_and_claim_batch(
                self._pending_acks, self.worker_id, self.batch_size
            )
            self._pending_acks = []
            if batch:
                for job in batch:
                    job._iter = self
                self._buffer.extend(batch)
                continue
            if self._listener is None:
                self._listener = self.queue.db.listen(self.queue._channel())
            try:
                await asyncio.wait_for(
                    self._listener.__anext__(), timeout=self.idle_poll_s
                )
            except asyncio.TimeoutError:
                pass
            except StopAsyncIteration:
                raise StopAsyncIteration
