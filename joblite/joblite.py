import asyncio
import json
import time
import traceback
from collections import deque
from typing import Any, AsyncIterator, Callable, Optional

import litenotify


class Notification:
    """A single row from `_litenotify_notifications`, as delivered to a
    `Listener`. `payload` is lazy-decoded JSON on access — matches the
    `Job.payload` convention."""

    __slots__ = ("id", "channel", "_payload_raw", "_payload", "created_at")
    _UNSET = object()

    def __init__(self, row: dict):
        self.id = row["id"]
        self.channel = row["channel"]
        self._payload_raw = row["payload"]
        self._payload = Notification._UNSET
        self.created_at = row.get("created_at", 0)

    @property
    def payload(self) -> Any:
        if self._payload is Notification._UNSET:
            raw = self._payload_raw
            if raw in (None, "", "null"):
                self._payload = None
            else:
                try:
                    self._payload = json.loads(raw)
                except (json.JSONDecodeError, TypeError):
                    # payload wasn't JSON; hand back the raw string.
                    self._payload = raw
        return self._payload

    def __repr__(self):
        return f"Notification(id={self.id}, channel={self.channel!r})"


class Listener:
    """Async iterator yielding Notifications published to `channel`.

    Semantics:
      * Starts from the CURRENT `MAX(id)` on the channel at
        construction — historical notifications are not replayed (same
        shape as `pg_notify`). Use `db.stream(...)` if you want
        durable replay with per-consumer offsets.
      * Wakes on every `.db-wal` change (any process, any writer) and
        SELECTs new rows for this channel. Cross-process wake latency
        is bounded by the 1 ms stat-poll cadence (p50 ~= 1–2 ms on
        M-series).
      * Ordered by monotonic id; no duplicates.
      * The notifications table is NOT auto-pruned. Rows accumulate
        until you call `db.prune_notifications(...)`. A listener that
        reconnects will skip any events pruned while it was offline —
        if you need durability past that window, use `db.stream(...)`.
    """

    def __init__(self, db, channel: str):
        self.db = db
        self.channel = channel
        self._buffer: deque = deque()
        # Skip anything that existed before this Listener started.
        rows = db.query(
            "SELECT COALESCE(MAX(id), 0) AS m "
            "FROM _litenotify_notifications WHERE channel=?",
            [channel],
        )
        self._last_seen = int(rows[0]["m"]) if rows else 0
        self._wal = db.wal_events()

    def __aiter__(self):
        return self

    async def __anext__(self) -> Notification:
        while True:
            if self._buffer:
                return self._buffer.popleft()
            rows = self.db.query(
                "SELECT id, channel, payload, created_at "
                "FROM _litenotify_notifications "
                "WHERE channel=? AND id > ? ORDER BY id",
                [self.channel, self._last_seen],
            )
            if rows:
                for r in rows:
                    self._last_seen = int(r["id"])
                    self._buffer.append(Notification(r))
                continue
            # No new rows — wait on WAL. 15s paranoia timeout.
            try:
                await asyncio.wait_for(
                    self._wal.__anext__(), timeout=15.0
                )
            except asyncio.TimeoutError:
                pass
            except StopAsyncIteration:
                raise StopAsyncIteration


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
        # Canonical DDL lives in litenotify-core::BOOTSTRAP_JOBLITE_SQL
        # so this binding and the SQLite loadable extension can't drift
        # on column counts. View + schema-version cleanup are
        # Python-binding-specific and stay here.
        with self.db.transaction() as tx:
            tx.bootstrap_joblite_schema()
            # Inspection view: UNION live + dead with a synthetic `state`.
            tx.execute("DROP VIEW IF EXISTS _joblite_jobs")
            tx.execute(
                """
                CREATE VIEW _joblite_jobs AS
                  SELECT id, queue, payload, state, priority, run_at,
                         worker_id, claim_expires_at,
                         attempts, max_attempts, NULL AS last_error, created_at
                    FROM _joblite_live
                  UNION ALL
                  SELECT id, queue, payload, 'dead' AS state, priority,
                         run_at, NULL, NULL,
                         attempts, max_attempts, last_error, created_at
                    FROM _joblite_dead
                """
            )
            # Clean up artifacts from previous schema versions.
            tx.execute("DROP INDEX IF EXISTS _joblite_jobs_claim")
            tx.execute("DROP INDEX IF EXISTS _joblite_jobs_claim_v2")
            tx.execute("DROP INDEX IF EXISTS _joblite_pending_claim")
            tx.execute("DROP INDEX IF EXISTS _joblite_processing_reclaim")
            tx.execute("DROP TABLE IF EXISTS _joblite_pending")
            tx.execute("DROP TABLE IF EXISTS _joblite_processing")

    def _channel(self) -> str:
        return f"joblite:{self.name}"

    def enqueue(
        self,
        payload: Any,
        tx=None,
        run_at: Optional[int] = None,
        priority: int = 0,
    ) -> None:
        """Insert one job row. When called without `tx=`, opens its
        own write transaction and fires one notify per call — so a
        hot loop of 10 k enqueues means 10 k transactions and 10 k
        wakes. For bulk inserts, pass a shared `tx` and the library
        will only fire one commit (at the block exit) with one
        cross-process wake:

            with db.transaction() as tx:
                for p in payloads:
                    q.enqueue(p, tx=tx)
                # single commit + one wake for all workers
        """
        run_at_val = int(run_at) if run_at is not None else int(time.time())
        payload_str = json.dumps(payload)
        params = [self.name, payload_str, run_at_val, int(priority), self.max_attempts]

        if tx is not None:
            tx.execute(
                """
                INSERT INTO _joblite_live (queue, payload, run_at, priority, max_attempts)
                VALUES (?, ?, ?, ?, ?)
                """,
                params,
            )
            tx.notify(self._channel(), "new")
            return

        with self.db.transaction() as own_tx:
            own_tx.execute(
                """
                INSERT INTO _joblite_live (queue, payload, run_at, priority, max_attempts)
                VALUES (?, ?, ?, ?, ?)
                """,
                params,
            )
            own_tx.notify(self._channel(), "new")

    def claim_one(self, worker_id: str) -> Optional[Job]:
        jobs = self.claim_batch(worker_id, 1)
        return jobs[0] if jobs else None

    # SQL for a single-batch claim. Single UPDATE ... RETURNING; partial
    # index `_joblite_live_claim` covers the subquery. State transition
    # pending -> processing stays inside the partial WHERE so the row
    # doesn't drop out of the index mid-UPDATE.
    _CLAIM_SQL = """
        UPDATE _joblite_live
        SET state = 'processing',
            worker_id = ?,
            claim_expires_at = unixepoch() + ?,
            attempts = attempts + 1
        WHERE id IN (
          SELECT id FROM _joblite_live
          WHERE queue = ?
            AND state IN ('pending', 'processing')
            AND ((state = 'pending' AND run_at <= unixepoch())
              OR (state = 'processing' AND claim_expires_at < unixepoch()))
          ORDER BY priority DESC, run_at ASC, id ASC
          LIMIT ?
        )
        RETURNING id, queue, payload, worker_id, attempts, claim_expires_at
    """

    def claim_batch(self, worker_id: str, n: int) -> list:
        """Atomically claim up to `n` jobs. One UPDATE ... RETURNING
        against _joblite_live via the partial claim index."""
        n = int(n)
        if n <= 0:
            return []
        with self.db.transaction() as tx:
            rows = tx.query(
                self._CLAIM_SQL,
                [worker_id, self.visibility_timeout_s, self.name, n],
            )
        return [Job(self, row) for row in rows]

    def ack_and_claim_batch(
        self, ack_ids, worker_id: str, n: int
    ) -> list:
        """One transaction: ack `ack_ids` (if any) then claim up to `n`
        new jobs. Used by the async iterator to pipeline the previous
        batch's ack with the next batch's claim.
        """
        n = int(n)
        if n <= 0 and not ack_ids:
            return []
        with self.db.transaction() as tx:
            if ack_ids:
                # ack = DELETE from live. 'done' is not a stored state;
                # the row simply stops existing in _joblite_live and
                # drops out of the partial claim index.
                tx.execute(
                    """
                    DELETE FROM _joblite_live
                    WHERE id IN (SELECT value FROM json_each(?))
                      AND worker_id = ?
                      AND claim_expires_at >= unixepoch()
                    """,
                    [json.dumps([int(i) for i in ack_ids]), worker_id],
                )
            if n <= 0:
                return []
            rows = tx.query(
                self._CLAIM_SQL,
                [worker_id, self.visibility_timeout_s, self.name, n],
            )
        return [Job(self, row) for row in rows]

    def ack_batch(self, job_ids, worker_id: str) -> int:
        """Ack multiple jobs in one tx. Returns count of jobs whose claim
        was still valid."""
        ids = [int(i) for i in job_ids]
        if not ids:
            return 0
        with self.db.transaction() as tx:
            rows = tx.query(
                """
                DELETE FROM _joblite_live
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
                DELETE FROM _joblite_live
                WHERE id=? AND worker_id=? AND claim_expires_at >= unixepoch()
                RETURNING id
                """,
                [int(job_id), worker_id],
            )
        return len(rows) > 0

    def retry(self, job_id: int, worker_id: str, delay_s: int, error: str) -> bool:
        """Put a claimed job back into pending with a delayed run_at, or
        move it to dead if attempts have reached max_attempts. Returns
        True iff the caller's claim was still valid."""
        with self.db.transaction() as tx:
            # Pull the current row so we can branch on attempts vs
            # max_attempts. Single SELECT by PK; partial index not needed.
            rows = tx.query(
                """
                SELECT id, queue, payload, priority, run_at, max_attempts,
                       attempts, created_at
                FROM _joblite_live
                WHERE id=? AND worker_id=?
                  AND claim_expires_at >= unixepoch()
                  AND state = 'processing'
                """,
                [int(job_id), worker_id],
            )
            if not rows:
                return False
            r = rows[0]
            if r["attempts"] >= r["max_attempts"]:
                # Move to dead. DELETE from live (drops out of partial
                # index) + INSERT into _joblite_dead.
                tx.execute(
                    "DELETE FROM _joblite_live WHERE id=?", [r["id"]]
                )
                tx.execute(
                    """
                    INSERT INTO _joblite_dead
                      (id, queue, payload, priority, run_at, max_attempts,
                       attempts, last_error, created_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        r["id"], r["queue"], r["payload"], r["priority"],
                        r["run_at"], r["max_attempts"], r["attempts"],
                        error, r["created_at"],
                    ],
                )
            else:
                # Flip back to 'pending', push run_at by delay_s. State
                # stays inside the partial index's WHERE so the row just
                # becomes claimable again after the delay.
                tx.execute(
                    """
                    UPDATE _joblite_live
                    SET state = 'pending',
                        run_at = unixepoch() + ?,
                        worker_id = NULL,
                        claim_expires_at = NULL
                    WHERE id = ?
                    """,
                    [int(delay_s), r["id"]],
                )
                tx.notify(self._channel(), "retry")
        return True

    def fail(self, job_id: int, worker_id: str, error: str) -> bool:
        """Move the claim straight to dead regardless of attempts."""
        with self.db.transaction() as tx:
            rows = tx.query(
                """
                DELETE FROM _joblite_live
                WHERE id=? AND worker_id=? AND claim_expires_at >= unixepoch()
                RETURNING id, queue, payload, priority, run_at,
                          max_attempts, attempts, created_at
                """,
                [int(job_id), worker_id],
            )
            if not rows:
                return False
            r = rows[0]
            tx.execute(
                """
                INSERT INTO _joblite_dead
                  (id, queue, payload, priority, run_at, max_attempts,
                   attempts, last_error, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    r["id"], r["queue"], r["payload"], r["priority"],
                    r["run_at"], r["max_attempts"], r["attempts"],
                    error, r["created_at"],
                ],
            )
        return True

    def heartbeat(
        self, job_id: int, worker_id: str, extend_s: Optional[int] = None
    ) -> bool:
        extend = int(extend_s) if extend_s is not None else self.visibility_timeout_s
        with self.db.transaction() as tx:
            rows = tx.query(
                """
                UPDATE _joblite_live
                SET claim_expires_at = unixepoch() + ?
                WHERE id = ? AND worker_id = ? AND state = 'processing'
                RETURNING id
                """,
                [extend, int(job_id), worker_id],
            )
        return len(rows) > 0


def build_worker_id(framework: str, instance_id: str, queue: str, index: int) -> str:
    """Framework-agnostic worker id format. Previously duplicated
    verbatim across the FastAPI/Django/Flask plugins.
    """
    return f"{framework}-{instance_id}-{queue}-{index}"


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
        # Set up the WAL watcher BEFORE the first read so writes during
        # "read empty" -> "start listening" can't slip through. WAL fires
        # on every commit (any process), so it covers both same-process
        # and cross-process publishers.
        self._wal = stream.db.wal_events()

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

            # Block on WAL — fires on any commit to the DB. Covers both
            # same-process and cross-process publishers. 15s fallback.
            try:
                await asyncio.wait_for(self._wal.__anext__(), timeout=15.0)
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
        return Listener(self, channel)

    def wal_events(self):
        return self._inner.wal_events()

    def query(self, sql: str, params=None):
        return self._inner.query(sql, params)

    def prune_notifications(
        self,
        older_than_s: Optional[int] = None,
        max_keep: Optional[int] = None,
    ) -> int:
        """Delete rows from `_litenotify_notifications`. Returns the
        number of rows removed.

        Provide one or both of:
          * `older_than_s`: delete rows older than this many seconds.
          * `max_keep`: delete rows beyond the most recent N.

        If BOTH are passed, a row matching EITHER condition is deleted
        (OR semantics). `max_keep` is not a floor — combined with an
        aggressive `older_than_s` it will not protect the most-recent
        N rows from age-based deletion. Typical usage is one argument
        at a time.

        Run whenever you want — on app startup, on a scheduled task,
        from a Django management command, whatever. This is a tool
        you invoke, not a background timer. `notify()` never prunes
        on its own; that's intentional.

        For anything that needs durable replay past ~seconds, use
        `db.stream(...)` rather than `tx.notify(...)`; the stream's
        consumer-offset tracking is a better fit than trying to keep
        the notifications buffer large.
        """
        conditions: list = []
        params: list = []
        if older_than_s is not None:
            conditions.append("created_at < unixepoch() - ?")
            params.append(int(older_than_s))
        if max_keep is not None:
            # Pre-compute MAX(id) once instead of a subquery the
            # planner might (or might not) hoist out of the DELETE
            # predicate. Also correctly handles an empty table and
            # a max_keep larger than the current row count — both
            # devolve to "nothing qualifies for id-based deletion."
            rows = self.query(
                "SELECT MAX(id) AS m FROM _litenotify_notifications"
            )
            max_id = rows[0]["m"] if rows and rows[0]["m"] is not None else 0
            threshold = max_id - int(max_keep)
            if threshold >= 1:
                conditions.append("id <= ?")
                params.append(threshold)
        if not conditions:
            return 0
        with self.transaction() as tx:
            rows = tx.query(
                "DELETE FROM _litenotify_notifications WHERE "
                + " OR ".join(conditions)
                + " RETURNING id",
                params,
            )
        return len(rows)

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

    Pipelines ack-of-previous with claim-of-next: `Job.ack()` on
    jobs yielded from this iterator defers into `_pending_acks`, and the
    next batch's claim transaction flushes them in the same tx. Halves
    the write-tx count per job for the common worker pattern
    (`async for job in q.claim(...): handle(job); job.ack()`).

    Wake when the queue is empty:
      1. WAL-file watcher (`db.wal_events()`): ~1ms wake when ANY
         process commits. A background stat-poll thread watches the
         `.db-wal` file's (size, mtime); the shared watcher fans out
         to every subscriber. Wake fires per commit — we re-poll the
         queue; over-triggering is cheap (indexed SELECT on a
         partial-index row set).
      2. `idle_poll_s` timeout: last-resort safety net if the WAL
         watcher can't fire (sandboxed FS, odd container mount, etc).
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
        self._wal = None
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
            # Queue drained. Block on the WAL file watcher — wakes on
            # every commit, any process. Covers in-process enqueuers
            # (their commit appends to this DB's WAL too) AND
            # cross-process enqueuers. `idle_poll_s` is a paranoia
            # fallback for sandboxed filesystems where the stat-poll
            # thread can't observe mtime changes reliably.
            if self._wal is None:
                self._wal = self.queue.db.wal_events()
            try:
                await asyncio.wait_for(
                    self._wal.__anext__(),
                    timeout=self.idle_poll_s,
                )
            except asyncio.TimeoutError:
                pass
            except StopAsyncIteration:
                raise StopAsyncIteration
