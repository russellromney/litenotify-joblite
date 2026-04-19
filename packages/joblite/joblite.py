import asyncio
import json
import time
import traceback
import uuid
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
    )

    # Sentinel distinguishing "payload decoded to None" from "not yet decoded".
    _UNSET = object()

    def __init__(self, queue: "Queue", row: dict):
        # `row` comes from the claim UPDATE's RETURNING. The narrow claim
        # path only returns hot-path fields (id, queue, payload, worker_id,
        # attempts, claim_expires_at); other columns default to sensible
        # post-claim values.
        self.queue = queue
        self.id = row["id"]
        self.queue_name = row["queue"]
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

    @property
    def payload(self) -> Any:
        if self._payload is Job._UNSET:
            self._payload = (
                json.loads(self._payload_raw) if self._payload_raw else None
            )
        return self._payload

    def ack(self) -> bool:
        """DELETE the row if the caller's claim is still valid. Returns
        True iff the claim hadn't expired. Always goes through one write
        transaction; no deferred / pipelined batching. If you want batched
        ack, call `queue.ack_batch([ids], worker_id)` directly.
        """
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
        delay: Optional[float] = None,
        priority: int = 0,
        expires: Optional[float] = None,
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

        Scheduling:
          - `run_at`: absolute unix epoch when the job becomes claimable.
          - `delay`:  seconds from now. Shorthand for
                      `run_at = time.time() + delay`. If both are
                      passed, `delay` wins.
          - `expires`: seconds from now after which this job is no
                      longer eligible for claim. Claim path filters
                      expired rows; `queue.sweep_expired()` moves
                      them into `_joblite_dead`.
        """
        if delay is not None:
            run_at_val = int(time.time() + float(delay))
        elif run_at is not None:
            run_at_val = int(run_at)
        else:
            run_at_val = int(time.time())
        expires_at_val = (
            int(time.time() + float(expires)) if expires is not None else None
        )
        payload_str = json.dumps(payload)
        params = [
            self.name,
            payload_str,
            run_at_val,
            int(priority),
            self.max_attempts,
            expires_at_val,
        ]

        if tx is not None:
            tx.execute(
                """
                INSERT INTO _joblite_live
                  (queue, payload, run_at, priority, max_attempts, expires_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                params,
            )
            tx.notify(self._channel(), "new")
            return

        with self.db.transaction() as own_tx:
            own_tx.execute(
                """
                INSERT INTO _joblite_live
                  (queue, payload, run_at, priority, max_attempts, expires_at)
                VALUES (?, ?, ?, ?, ?, ?)
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
    #
    # `expires_at IS NULL OR expires_at > unixepoch()` filters jobs that
    # have already expired out of the claim path. Expired rows stay in
    # _joblite_live until `sweep_expired()` moves them to _joblite_dead.
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
            AND (expires_at IS NULL OR expires_at > unixepoch())
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
    ) -> AsyncIterator[Job]:
        """Async iterator over this queue. Yields one claimed Job per
        `__anext__` via a single-row `claim_batch(worker_id, 1)` — one
        write transaction per job. Wakes on WAL commit from any process;
        `idle_poll_s` is a paranoia fallback for environments where the
        stat watcher can't fire.

        For batched claims, call `claim_batch(worker_id, n)` directly.
        """
        return _WorkerQueueIter(self, worker_id, idle_poll_s)

    def sweep_expired(self) -> int:
        """Move rows whose `expires_at` has passed from `_joblite_live`
        into `_joblite_dead` with `last_error='expired'`. The claim path
        already ignores expired rows, so sweep is cleanup-only — not
        correctness-critical. Call on a schedule if you enqueue jobs
        with `expires=` and want to reclaim the table space.

        Returns the number of rows moved.
        """
        with self.db.transaction() as tx:
            rows = tx.query(
                """
                DELETE FROM _joblite_live
                WHERE queue = ?
                  AND state = 'pending'
                  AND expires_at IS NOT NULL
                  AND expires_at <= unixepoch()
                RETURNING id, queue, payload, priority, run_at, max_attempts,
                          attempts, created_at
                """,
                [self.name],
            )
            if not rows:
                return 0
            for r in rows:
                tx.execute(
                    """
                    INSERT INTO _joblite_dead
                      (id, queue, payload, priority, run_at, max_attempts,
                       attempts, last_error, created_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, 'expired', ?)
                    """,
                    [
                        r["id"], r["queue"], r["payload"], r["priority"],
                        r["run_at"], r["max_attempts"], r["attempts"],
                        r["created_at"],
                    ],
                )
        return len(rows)

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
        save_every_n: int = 1000,
        save_every_s: float = 1.0,
    ) -> AsyncIterator[Event]:
        """Yield events with offset > from_offset, then live events as
        they arrive. If `consumer` is given and `from_offset` is None,
        the last saved offset for that consumer is used.

        When `consumer` is set, the iterator auto-saves offset to
        `_joblite_stream_consumers` on a cadence: at most every
        `save_every_n` yielded events or every `save_every_s` seconds,
        whichever comes first. This amortizes the offset-save write
        across many events — critical because every `save_offset` is
        an UPSERT through the single-writer slot.

        Set `save_every_n=0` and `save_every_s=0` to disable auto-save
        entirely; then call `stream.save_offset(consumer, offset)`
        yourself, optionally inside a business transaction.

        At-least-once: the save flushes BEFORE yielding the next event,
        so a crash during handler execution means the in-flight event
        is re-delivered on reconnect. Up to `save_every_n` events (or
        `save_every_s` seconds' worth) may be re-delivered after a
        crash — tune to taste.
        """
        if from_offset is None and consumer is not None:
            from_offset = self.get_offset(consumer)
        if from_offset is None:
            from_offset = 0
        return _StreamIter(
            self,
            int(from_offset),
            consumer=consumer,
            save_every_n=int(save_every_n),
            save_every_s=float(save_every_s),
        )


class _StreamIter:
    def __init__(
        self,
        stream: Stream,
        from_offset: int,
        consumer: Optional[str] = None,
        save_every_n: int = 1000,
        save_every_s: float = 1.0,
    ):
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
        # Offset auto-save bookkeeping.
        self._consumer = consumer
        self._save_every_n = max(0, save_every_n)
        self._save_every_s = max(0.0, save_every_s)
        # Highest offset yielded that hasn't been flushed to
        # _joblite_stream_consumers yet. Flushed on threshold crossing
        # inside __anext__, before yielding the next event.
        self._pending_save_offset = 0
        self._events_since_save = 0
        self._last_save_at = time.monotonic()

    def __aiter__(self):
        return self

    def _maybe_save_offset(self) -> None:
        if not self._consumer or self._pending_save_offset <= 0:
            return
        count_hit = (
            self._save_every_n > 0
            and self._events_since_save >= self._save_every_n
        )
        time_hit = (
            self._save_every_s > 0
            and (time.monotonic() - self._last_save_at) >= self._save_every_s
        )
        if count_hit or time_hit:
            self.stream.save_offset(self._consumer, self._pending_save_offset)
            self._events_since_save = 0
            self._last_save_at = time.monotonic()

    async def __anext__(self):
        while True:
            if self._buffer:
                # Flush BEFORE yielding the next event. A crash during
                # the user's handler rolls back in-progress work; the
                # saved offset is never ahead of "last handler success",
                # giving honest at-least-once semantics.
                self._maybe_save_offset()
                row = self._buffer.popleft()
                self.offset = row["offset"]
                self._pending_save_offset = self.offset
                self._events_since_save += 1
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

    def enqueue(
        self,
        payload: Any,
        tx=None,
        priority: int = 0,
        delay: Optional[float] = None,
        run_at: Optional[int] = None,
        expires: Optional[float] = None,
    ) -> None:
        self._queue.enqueue(
            payload,
            tx=tx,
            priority=priority,
            delay=delay,
            run_at=run_at,
            expires=expires,
        )

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


class LockHeld(Exception):
    """Raised when `db.lock(name).__enter__()` can't acquire the lock
    because another holder has it and the TTL hasn't elapsed. Use this
    to skip overlapping runs:

        try:
            with db.lock('nightly-backup', ttl=3600):
                do_backup()
        except joblite.LockHeld:
            # another worker is already running this; skip this run
            pass
    """


class _Lock:
    """Context manager returned by `Database.lock(name, ttl=60)`.

    Acquires the named lock in `_joblite_locks` via `INSERT OR IGNORE`.
    Expired rows (owner crashed before release) are opportunistically
    pruned on every acquire attempt, so a stale lock doesn't block
    forever — the TTL is an upper bound on how long a crashed holder
    can block others.

    Releases via `DELETE` on `__exit__`. If the TTL elapsed before
    release (holder took longer than expected and someone else acquired
    in the meantime), release is a no-op.
    """

    __slots__ = ("db", "name", "ttl", "owner", "acquired")

    def __init__(self, db: "Database", name: str, ttl: int, owner: str):
        self.db = db
        self.name = name
        self.ttl = int(ttl)
        self.owner = owner
        self.acquired = False

    def __enter__(self) -> "_Lock":
        with self.db.transaction() as tx:
            # Prune stale rows first so a crashed holder's TTL can
            # elapse before anyone else tries. Cheap — one indexed
            # DELETE.
            tx.execute(
                "DELETE FROM _joblite_locks "
                "WHERE name = ? AND expires_at <= unixepoch()",
                [self.name],
            )
            # Try to claim it. INSERT OR IGNORE is a no-op if the row
            # exists (another holder).
            tx.execute(
                """
                INSERT OR IGNORE INTO _joblite_locks
                  (name, owner, expires_at)
                VALUES (?, ?, unixepoch() + ?)
                """,
                [self.name, self.owner, self.ttl],
            )
            rows = tx.query(
                "SELECT owner FROM _joblite_locks WHERE name = ?",
                [self.name],
            )
            if not rows or rows[0]["owner"] != self.owner:
                raise LockHeld(f"lock {self.name!r} is already held")
            self.acquired = True
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        if self.acquired:
            with self.db.transaction() as tx:
                tx.execute(
                    "DELETE FROM _joblite_locks "
                    "WHERE name = ? AND owner = ?",
                    [self.name, self.owner],
                )
            self.acquired = False
        return False  # don't suppress exceptions from the with-body


class Database:
    """Wrapper over the litenotify Database that adds queue/stream/outbox."""

    def __init__(self, inner):
        self._inner = inner
        self._queues: dict = {}
        self._streams: dict = {}
        self._outboxes: dict = {}
        # Bootstrap the shared joblite schema up-front so features
        # like db.lock() (which doesn't touch Queue or Stream) find
        # their tables on first use.
        with self._inner.transaction() as tx:
            tx.bootstrap_joblite_schema()

    def transaction(self):
        return self._inner.transaction()

    def listen(self, channel: str):
        return Listener(self, channel)

    def wal_events(self):
        return self._inner.wal_events()

    def query(self, sql: str, params=None):
        return self._inner.query(sql, params)

    def lock(
        self,
        name: str,
        ttl: int = 60,
        owner: Optional[str] = None,
    ) -> _Lock:
        """Named-lock context manager backed by the `_joblite_locks`
        table. Raises `joblite.LockHeld` on `__enter__` if the lock is
        currently held by someone else.

        Typical use is around work that shouldn't overlap with itself —
        cron tasks that might run longer than their schedule interval:

            try:
                with db.lock('nightly-backup', ttl=3600):
                    do_backup()
            except joblite.LockHeld:
                # previous run still going; skip this cron tick
                pass

        `ttl` bounds how long a crashed holder can block others.
        Default 60s.
        """
        return _Lock(
            self,
            name,
            ttl=ttl,
            owner=owner or uuid.uuid4().hex,
        )

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
    """Async iterator for `queue.claim()`. Yields one job at a time via
    `claim_batch(worker_id, 1)`. One write transaction per job.

    Wake sources when the queue is empty:
      1. WAL-file watcher (`db.wal_events()`): ~1ms wake on any commit
         to this database from any process. The shared watcher fans
         out to every subscriber; over-triggering is cheap.
      2. `idle_poll_s` timeout: paranoia fallback if the WAL watcher
         can't fire (sandboxed FS, odd container mount).
    """

    def __init__(self, queue: Queue, worker_id: str, idle_poll_s: float):
        self.queue = queue
        self.worker_id = worker_id
        self.idle_poll_s = idle_poll_s
        self._wal = None

    def __aiter__(self):
        return self

    async def __anext__(self):
        while True:
            jobs = self.queue.claim_batch(self.worker_id, 1)
            if jobs:
                return jobs[0]
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
