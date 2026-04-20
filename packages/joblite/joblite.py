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
    ) -> int:
        """Insert one job row. Returns the inserted `id` (primary key
        in `_joblite_live`). Delegates to `honker_enqueue`, which handles
        run_at / delay precedence + expires computation + firing the
        wake notification atomically in Rust — same SQL path every
        binding uses.

        Scheduling precedence:
          - `delay`:   seconds from now (wins over run_at if both set)
          - `run_at`:  absolute unix epoch
          - neither:   now (claimable immediately)

        `expires`: seconds from now. Claim path filters expired rows;
        `queue.sweep_expired()` moves them into `_joblite_dead`.

        For bulk inserts with one commit + one cross-process wake,
        pass a shared `tx`:

            with db.transaction() as tx:
                for p in payloads:
                    q.enqueue(p, tx=tx)
                # single commit at block exit, one wake for all
        """
        payload_str = json.dumps(payload)
        run_at_val = int(run_at) if run_at is not None else None
        delay_val = int(delay) if delay is not None else None
        expires_val = int(expires) if expires is not None else None
        sql = "SELECT honker_enqueue(?, ?, ?, ?, ?, ?, ?) AS id"
        params = [
            self.name, payload_str, run_at_val, delay_val,
            int(priority), self.max_attempts, expires_val,
        ]
        if tx is not None:
            rows = tx.query(sql, params)
            return rows[0]["id"]
        with self.db.transaction() as own_tx:
            rows = own_tx.query(sql, params)
            return rows[0]["id"]

    def claim_one(self, worker_id: str) -> Optional[Job]:
        jobs = self.claim_batch(worker_id, 1)
        return jobs[0] if jobs else None

    def claim_batch(self, worker_id: str, n: int) -> list:
        """Atomically claim up to `n` jobs. Delegates to
        `honker_claim_batch`, which does one `UPDATE ... RETURNING` via
        the partial claim index in Rust — same SQL every binding uses.
        """
        n = int(n)
        if n <= 0:
            return []
        with self.db.transaction() as tx:
            rows = tx.query(
                "SELECT honker_claim_batch(?, ?, ?, ?) AS rows_json",
                [self.name, worker_id, n, self.visibility_timeout_s],
            )
        data = json.loads(rows[0]["rows_json"])
        return [Job(self, row) for row in data]

    def ack_batch(self, job_ids, worker_id: str) -> int:
        """Ack multiple jobs in one tx. Delegates to `honker_ack_batch`.
        Returns count of jobs whose claim was still valid."""
        ids = [int(i) for i in job_ids]
        if not ids:
            return 0
        with self.db.transaction() as tx:
            rows = tx.query(
                "SELECT honker_ack_batch(?, ?) AS n",
                [json.dumps(ids), worker_id],
            )
        return rows[0]["n"]

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
                "SELECT honker_sweep_expired(?) AS n", [self.name]
            )
        return rows[0]["n"]

    # --- result storage -------------------------------------------

    def save_result(
        self,
        job_id: int,
        value: Any,
        ttl: Optional[float] = None,
        tx=None,
    ) -> None:
        """Store the return value for a completed job under its id.
        `value` is any JSON-serializable Python value. `ttl` is the
        expiry in seconds — callers that never fetch the result can
        set a short ttl to auto-reclaim disk; `None` means no expiry
        (you're on the hook for pruning via `sweep_results()`).

        Delegates to `honker_result_save`. UPSERTs — calling twice for
        the same `job_id` replaces the first. Typical use is inside
        a worker after handler success, though callers can also call
        it manually.
        """
        value_str = json.dumps(value)
        ttl_s = int(ttl) if ttl is not None else 0
        params = [int(job_id), value_str, ttl_s]
        if tx is not None:
            tx.query("SELECT honker_result_save(?, ?, ?)", params)
            return
        with self.db.transaction() as own_tx:
            own_tx.query("SELECT honker_result_save(?, ?, ?)", params)

    def get_result(self, job_id: int) -> tuple:
        """Return `(found: bool, value: Any)` for a saved result.

        `(False, None)` means the result hasn't been saved yet OR has
        expired. `(True, value)` means we have it; `value` can still
        be `None` (task legitimately returned None). Two-tuple
        disambiguates.

        Delegates to `honker_result_get`, which returns SQL NULL for
        "absent or expired" and the stored JSON text otherwise. The
        literal string `'null'` (stored None) is distinguishable from
        SQL NULL (absent) at the rusqlite boundary.
        """
        with self.db.transaction() as tx:
            rows = tx.query(
                "SELECT honker_result_get(?) AS v", [int(job_id)]
            )
        raw = rows[0]["v"]
        if raw is None:
            return (False, None)
        return (True, json.loads(raw))

    async def wait_result(
        self,
        job_id: int,
        timeout: Optional[float] = None,
    ) -> Any:
        """Block until a result is saved for `job_id`, then return it.

        Wakes on every WAL commit (any process), so a worker in a
        different process saving the result is picked up within the
        stat-poll cadence. `timeout` is in seconds; `None` waits
        forever. Raises `asyncio.TimeoutError` on expiry.
        """
        deadline = time.time() + float(timeout) if timeout is not None else None
        # Check once before subscribing, in case it's already there.
        found, value = self.get_result(job_id)
        if found:
            return value
        wal = self.db.wal_events()
        while True:
            remaining = (
                max(0.0, deadline - time.time()) if deadline is not None else 15.0
            )
            if deadline is not None and remaining <= 0:
                raise asyncio.TimeoutError(
                    f"wait_result({job_id}) timed out"
                )
            try:
                await asyncio.wait_for(wal.__anext__(), timeout=remaining)
            except asyncio.TimeoutError:
                if deadline is None:
                    # Paranoia-poll on the 15s fallback; loop again.
                    pass
                else:
                    raise
            found, value = self.get_result(job_id)
            if found:
                return value

    def sweep_results(self) -> int:
        """Delete all expired result rows. Returns count deleted.
        Delegates to `honker_result_sweep`."""
        with self.db.transaction() as tx:
            rows = tx.query("SELECT honker_result_sweep() AS n")
        return rows[0]["n"]

    def ack(self, job_id: int, worker_id: str) -> bool:
        """Delegates to `honker_ack`."""
        with self.db.transaction() as tx:
            rows = tx.query(
                "SELECT honker_ack(?, ?) AS r",
                [int(job_id), worker_id],
            )
        return bool(rows[0]["r"])

    def retry(self, job_id: int, worker_id: str, delay_s: int, error: str) -> bool:
        """Put a claimed job back into pending with a delayed run_at, or
        move it to dead if attempts have reached max_attempts. Returns
        True iff the caller's claim was still valid. Delegates to
        `honker_retry`, which handles the attempts-vs-max-attempts branching
        + wake-notification atomically."""
        with self.db.transaction() as tx:
            rows = tx.query(
                "SELECT honker_retry(?, ?, ?, ?) AS r",
                [int(job_id), worker_id, int(delay_s), error],
            )
        return bool(rows[0]["r"])

    def fail(self, job_id: int, worker_id: str, error: str) -> bool:
        """Move the claim straight to dead regardless of attempts.
        Delegates to `honker_fail`."""
        with self.db.transaction() as tx:
            rows = tx.query(
                "SELECT honker_fail(?, ?, ?) AS r",
                [int(job_id), worker_id, error],
            )
        return bool(rows[0]["r"])

    def heartbeat(
        self, job_id: int, worker_id: str, extend_s: Optional[int] = None
    ) -> bool:
        """Delegates to `honker_heartbeat`."""
        extend = int(extend_s) if extend_s is not None else self.visibility_timeout_s
        with self.db.transaction() as tx:
            rows = tx.query(
                "SELECT honker_heartbeat(?, ?, ?) AS r",
                [int(job_id), worker_id, extend],
            )
        return bool(rows[0]["r"])


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

    Publish inside a transaction to couple the event atomically to
    your business write. Subscribe with a `from_offset` to catch up
    after a disconnect; the iterator replays rows with offset >
    from_offset, then transitions to live WAL-wake delivery.

    All SQL lives in Rust (`honker_stream_publish/read_since/
    save_offset/get_offset`); this class is a thin wrapper so every
    binding shares one storage layout.
    """

    def __init__(self, db, name: str):
        self.db = db
        self.name = name
        # _joblite_stream + _joblite_stream_consumers ship in
        # BOOTSTRAP_JOBLITE_SQL, so no per-Stream DDL needed — the
        # tables already exist by the time Database wraps the
        # connection.

    def _channel(self) -> str:
        return f"joblite:stream:{self.name}"

    def publish(
        self, payload: Any, key: Optional[str] = None, tx=None
    ) -> None:
        """Append one event. Delegates to `honker_stream_publish`.
        Pass a shared `tx` to bundle multiple publishes + a business
        write into one commit."""
        payload_str = json.dumps(payload)
        sql = "SELECT honker_stream_publish(?, ?, ?)"
        params = [self.name, key, payload_str]
        if tx is not None:
            tx.query(sql, params)
            return
        with self.db.transaction() as own_tx:
            own_tx.query(sql, params)

    def _read_since(self, offset: int, limit: int = 1000) -> list:
        with self.db.transaction() as tx:
            rows = tx.query(
                "SELECT honker_stream_read_since(?, ?, ?) AS rows_json",
                [self.name, int(offset), int(limit)],
            )
        return json.loads(rows[0]["rows_json"])

    def save_offset(self, consumer: str, offset: int) -> None:
        """Persist a consumer's high-water mark. Monotonic — never
        rewinds on duplicate deliveries. Delegates to
        `honker_stream_save_offset`."""
        with self.db.transaction() as tx:
            tx.query(
                "SELECT honker_stream_save_offset(?, ?, ?)",
                [consumer, self.name, int(offset)],
            )

    def get_offset(self, consumer: str) -> int:
        """Read a consumer's saved offset, or 0 if unknown.
        Delegates to `honker_stream_get_offset`."""
        with self.db.transaction() as tx:
            rows = tx.query(
                "SELECT honker_stream_get_offset(?, ?) AS v",
                [consumer, self.name],
            )
        return int(rows[0]["v"])

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
            rows = tx.query(
                "SELECT honker_lock_acquire(?, ?, ?) AS r",
                [self.name, self.owner, self.ttl],
            )
            if not rows[0]["r"]:
                raise LockHeld(f"lock {self.name!r} is already held")
            self.acquired = True
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        if self.acquired:
            with self.db.transaction() as tx:
                tx.query(
                    "SELECT honker_lock_release(?, ?)",
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

    def try_rate_limit(self, name: str, limit: int, per: int) -> bool:
        """Fixed-window rate limiter. Returns True if the caller is
        under `limit` invocations in the current `per`-second window
        (and records this invocation as one of those); False if the
        limit has been hit, in which case nothing is recorded.

            if db.try_rate_limit("outbound-api", limit=10, per=60):
                call_api()
            else:
                # over rate; caller decides whether to drop, retry,
                # or enqueue-with-delay.
                ...

        Uses a fixed window (`window_start = unixepoch() // per * per`).
        At window boundaries the count resets — simpler than a sliding
        window and good enough for "don't hammer this endpoint past X
        per minute" patterns. Old windows are not auto-pruned; run
        `db.sweep_rate_limits(older_than_s=...)` if the table gets
        large.

        Entire check + increment runs in one write transaction so two
        workers racing for the last slot never both see "under limit."
        """
        limit = int(limit)
        per = int(per)
        if limit <= 0 or per <= 0:
            raise ValueError("limit and per must be positive")
        with self.transaction() as tx:
            rows = tx.query(
                "SELECT honker_rate_limit_try(?, ?, ?) AS r",
                [name, limit, per],
            )
        return bool(rows[0]["r"])

    def sweep_rate_limits(self, older_than_s: int = 3600) -> int:
        """Delete rate-limit rows whose `window_start` is older than
        `older_than_s` seconds ago. Returns rows deleted. Purely a
        table-space reclaim — old windows are never consulted by
        `try_rate_limit`, so leaving them around is just disk use.
        Delegates to `honker_rate_limit_sweep`.
        """
        with self.transaction() as tx:
            rows = tx.query(
                "SELECT honker_rate_limit_sweep(?) AS n", [int(older_than_s)]
            )
        return rows[0]["n"]

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
