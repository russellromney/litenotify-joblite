// Package honker is the Go binding for Honker — a SQLite-native task
// runtime (queues, streams, pub/sub, scheduler) backed by the Honker
// SQLite loadable extension.
//
// Usage:
//
//	db, err := honker.Open("app.db", "/path/to/libhonker.dylib")
//	if err != nil { log.Fatal(err) }
//	defer db.Close()
//
//	q := db.Queue("emails", honker.QueueOptions{})
//	q.Enqueue(map[string]any{"to": "alice@example.com"}, honker.EnqueueOptions{})
//
//	job, _ := q.ClaimOne("worker-1")
//	if job != nil {
//	    sendEmail(job.Payload)
//	    job.Ack()
//	}
//
// This package wraps mattn/go-sqlite3 and registers a ConnectHook
// that loads the Honker extension on every connection in the pool.
// Requires a libhonker.dylib / libhonker.so built from the
// honker-extension crate.
package honker

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	sqlite3 "github.com/mattn/go-sqlite3"
)

// Database is a Honker handle over a SQLite file with the extension
// loaded on every pool connection. Open() bootstraps the schema on
// first use; safe to Open() the same path from multiple processes.
type Database struct {
	db      *sql.DB
	dbPath  string
	updates *updateWatcher
}

// driverCounter generates a unique sql.Register name per Open() so
// multiple databases in the same process can load different extension
// paths (or different PRAGMA configs) without colliding.
var driverCounter atomic.Int64

// deriveEntryPoint computes SQLite's default extension entry-point
// from the library's filename. Matches the logic in
// sqlite3_load_extension (ext/misc/loadextension.c) when the proc
// argument is NULL:
//
//	/path/to/libhonker_ext.dylib
//	  → basename: libhonker_ext.dylib
//	  → strip extension: libhonker_ext
//	  → strip "lib" prefix: honker_ext
//	  → strip non-alpha (ASCII): honkerext
//	  → prefix/suffix: sqlite3_honkerext_init
//
// We pass this to go-sqlite3's LoadExtension because that crate
// doesn't forward NULL for an empty entry-point string.
func deriveEntryPoint(path string) string {
	base := filepath.Base(path)
	// strip .dylib / .so / .dll
	if i := strings.LastIndex(base, "."); i > 0 {
		base = base[:i]
	}
	base = strings.TrimPrefix(base, "lib")
	var cleaned strings.Builder
	for _, r := range base {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') {
			cleaned.WriteRune(r)
		}
	}
	return "sqlite3_" + cleaned.String() + "_init"
}

const defaultPragmas = `
PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;
PRAGMA busy_timeout = 5000;
PRAGMA foreign_keys = ON;
PRAGMA cache_size = -32000;
PRAGMA temp_store = MEMORY;
PRAGMA wal_autocheckpoint = 10000;
`

// Open opens (or creates) a SQLite database at path, registers a
// fresh sql driver whose ConnectHook loads the Honker extension at
// extensionPath, applies the default PRAGMAs on every connection,
// and bootstraps the Honker schema. Caller must Close() when done.
func Open(path string, extensionPath string) (*Database, error) {
	n := driverCounter.Add(1)
	driverName := fmt.Sprintf("sqlite3_honker_%d", n)
	entryPoint := deriveEntryPoint(extensionPath)

	sql.Register(driverName, &sqlite3.SQLiteDriver{
		ConnectHook: func(conn *sqlite3.SQLiteConn) error {
			if extensionPath != "" {
				if err := conn.LoadExtension(extensionPath, entryPoint); err != nil {
					return fmt.Errorf(
						"LoadExtension(%q, %q): %w",
						extensionPath, entryPoint, err,
					)
				}
			}
			if _, err := conn.Exec(defaultPragmas, nil); err != nil {
				return fmt.Errorf("default PRAGMAs: %w", err)
			}
			return nil
		},
	})
	sqlDB, err := sql.Open(driverName, path)
	if err != nil {
		return nil, fmt.Errorf("open: %w", err)
	}
	if err := sqlDB.Ping(); err != nil {
		sqlDB.Close()
		return nil, fmt.Errorf("ping: %w", err)
	}
	if _, err := sqlDB.Exec("SELECT honker_bootstrap()"); err != nil {
		sqlDB.Close()
		return nil, fmt.Errorf("bootstrap: %w", err)
	}
	d := &Database{db: sqlDB, dbPath: path}
	d.updates, err = newUpdateWatcher(driverName, path)
	if err != nil {
		sqlDB.Close()
		return nil, fmt.Errorf("update watcher: %w", err)
	}
	return d, nil
}

// Close releases the underlying sql.DB and stops the update watcher.
func (d *Database) Close() error {
	if d.updates != nil {
		d.updates.stop()
	}
	return d.db.Close()
}

// Raw returns the underlying *sql.DB for advanced queries (e.g.
// reading _honker_live directly or integrating with Go migration
// tools).
func (d *Database) Raw() *sql.DB { return d.db }

// -------------------------------------------------------------------
// Database commit watcher (internal)
// -------------------------------------------------------------------

// fileIdentity returns a platform-specific (dev, ino) tuple for the
// given path. Used to detect when the database file has been replaced
// underneath us (atomic rename, litestream restore, volume remount).
func fileIdentity(path string) (uint64, uint64, error) {
	info, err := os.Stat(path)
	if err != nil {
		return 0, 0, err
	}
	return statDevIno(info)
}

type updateWatcher struct {
	mu     sync.Mutex
	subs   map[uint64]chan struct{}
	nextID uint64
	done   chan struct{}
	wg     sync.WaitGroup
	db     *sql.DB
	dbPath string
}

func newUpdateWatcher(driverName string, dbPath string) (*updateWatcher, error) {
	db, err := sql.Open(driverName, dbPath)
	if err != nil {
		return nil, err
	}
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, err
	}
	w := &updateWatcher{
		subs:   make(map[uint64]chan struct{}),
		done:   make(chan struct{}),
		db:     db,
		dbPath: dbPath,
	}
	w.wg.Add(1)
	go w.poll()
	return w, nil
}

func (w *updateWatcher) subscribe() (uint64, <-chan struct{}) {
	w.mu.Lock()
	defer w.mu.Unlock()
	id := w.nextID
	w.nextID++
	ch := make(chan struct{}, 1)
	w.subs[id] = ch
	return id, ch
}

func (w *updateWatcher) unsubscribe(id uint64) {
	w.mu.Lock()
	defer w.mu.Unlock()
	delete(w.subs, id)
}

func (w *updateWatcher) stop() {
	close(w.done)
	w.wg.Wait()
	w.db.Close()
}

func (w *updateWatcher) poll() {
	defer w.wg.Done()

	initDev, initIno, _ := fileIdentity(w.dbPath)
	var lastVersion uint32
	// Seed initial data_version.
	_ = w.db.QueryRow("PRAGMA data_version").Scan(&lastVersion)

	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	var tick uint64
	for {
		select {
		case <-w.done:
			return
		case <-ticker.C:
		}

		// Path 1: PRAGMA data_version (fast path)
		var version uint32
		if err := w.db.QueryRow("PRAGMA data_version").Scan(&version); err != nil {
			// Transient failure (connection pool will retry next tick).
			// Force one conservative wake.
			w.fire()
			continue
		}
		if version != lastVersion {
			lastVersion = version
			w.fire()
		}

		// Path 2: stat identity check (dead-man's switch)
		tick++
		if tick%100 == 0 {
			dev, ino, err := fileIdentity(w.dbPath)
			if err != nil {
				// File vanished — force wake, let caller recover.
				w.fire()
				continue
			}
			if dev != initDev || ino != initIno {
				panic(fmt.Sprintf(
					"honker: database file replaced: expected (dev=%d, ino=%d), found (dev=%d, ino=%d) at %s. Restart required.",
					initDev, initIno, dev, ino, w.dbPath,
				))
			}
		}
	}
}

func (w *updateWatcher) fire() {
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, ch := range w.subs {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}

// -------------------------------------------------------------------
// Transactions
// -------------------------------------------------------------------

// Transaction is an open SQLite transaction. Call Commit or Rollback
// to release; on drop without either, the transaction is rolled back
// by the database (but explicit rollback is recommended).
type Transaction struct {
	tx *sql.Tx
}

// Begin starts a new transaction.
func (d *Database) Begin() (*Transaction, error) {
	tx, err := d.db.Begin()
	if err != nil {
		return nil, err
	}
	return &Transaction{tx: tx}, nil
}

// Transaction starts a new transaction with context support.
func (d *Database) Transaction(ctx context.Context) (*Transaction, error) {
	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	return &Transaction{tx: tx}, nil
}

// Exec runs a statement on the transaction.
func (t *Transaction) Exec(sql string, args ...any) (sql.Result, error) {
	return t.tx.Exec(sql, args...)
}

// QueryRow runs a query on the transaction and returns the first row.
func (t *Transaction) QueryRow(sql string, args ...any) *sql.Row {
	return t.tx.QueryRow(sql, args...)
}

// Commit commits the transaction.
func (t *Transaction) Commit() error {
	return t.tx.Commit()
}

// Rollback rolls back the transaction.
func (t *Transaction) Rollback() error {
	return t.tx.Rollback()
}

// Raw returns the underlying *sql.Tx for advanced use.
func (t *Transaction) Raw() *sql.Tx {
	return t.tx
}

// -------------------------------------------------------------------
// Queue
// -------------------------------------------------------------------

// Queue returns a handle to a named queue. Opts are applied per-queue
// (visibility timeout, max attempts). Zero values resolve to 300s /
// 3 attempts.
func (d *Database) Queue(name string, opts QueueOptions) *Queue {
	if opts.VisibilityTimeoutS == 0 {
		opts.VisibilityTimeoutS = 300
	}
	if opts.MaxAttempts == 0 {
		opts.MaxAttempts = 3
	}
	return &Queue{db: d, name: name, opts: opts}
}

// QueueOptions holds per-queue defaults.
type QueueOptions struct {
	VisibilityTimeoutS int
	MaxAttempts        int
}

// Queue is a named work queue.
type Queue struct {
	db   *Database
	name string
	opts QueueOptions
}

// EnqueueOptions configures an enqueue call. All fields are optional.
//
//	Delay:    seconds from now before the job is claimable.
//	RunAt:    absolute unix epoch; ignored if Delay is set.
//	Priority: higher = picked first within the queue.
//	Expires:  seconds from now; job drops out of claim pool when expired.
type EnqueueOptions struct {
	Delay    *int64
	RunAt    *int64
	Priority int64
	Expires  *int64
}

// Enqueue inserts a new job. payload is marshaled via json.Marshal.
// Returns the inserted row id.
func (q *Queue) Enqueue(payload any, opts EnqueueOptions) (int64, error) {
	js, err := json.Marshal(payload)
	if err != nil {
		return 0, fmt.Errorf("marshal payload: %w", err)
	}
	var id int64
	err = q.db.db.QueryRow(
		"SELECT honker_enqueue(?, ?, ?, ?, ?, ?, ?)",
		q.name, string(js), opts.RunAt, opts.Delay, opts.Priority,
		q.opts.MaxAttempts, opts.Expires,
	).Scan(&id)
	return id, err
}

// EnqueueTx inserts a new job inside an open transaction.
func (q *Queue) EnqueueTx(tx *Transaction, payload any, opts EnqueueOptions) (int64, error) {
	js, err := json.Marshal(payload)
	if err != nil {
		return 0, fmt.Errorf("marshal payload: %w", err)
	}
	var id int64
	err = tx.tx.QueryRow(
		"SELECT honker_enqueue(?, ?, ?, ?, ?, ?, ?)",
		q.name, string(js), opts.RunAt, opts.Delay, opts.Priority,
		q.opts.MaxAttempts, opts.Expires,
	).Scan(&id)
	return id, err
}

// Job is a claimed unit of work. Payload is raw JSON bytes — unmarshal
// into your own struct via json.Unmarshal(job.Payload, &dst).
type Job struct {
	q        *Queue
	ID       int64
	Queue    string
	Payload  []byte
	WorkerID string
	Attempts int64
}

// ClaimOne atomically claims one job or returns (nil, nil) on empty.
func (q *Queue) ClaimOne(workerID string) (*Job, error) {
	jobs, err := q.ClaimBatch(workerID, 1)
	if err != nil || len(jobs) == 0 {
		return nil, err
	}
	return jobs[0], nil
}

// ClaimBatch atomically claims up to n jobs.
func (q *Queue) ClaimBatch(workerID string, n int) ([]*Job, error) {
	var rowsJSON string
	err := q.db.db.QueryRow(
		"SELECT honker_claim_batch(?, ?, ?, ?)",
		q.name, workerID, n, q.opts.VisibilityTimeoutS,
	).Scan(&rowsJSON)
	if err != nil {
		return nil, err
	}
	var raw []struct {
		ID             int64  `json:"id"`
		Queue          string `json:"queue"`
		Payload        string `json:"payload"`
		WorkerID       string `json:"worker_id"`
		Attempts       int64  `json:"attempts"`
		ClaimExpiresAt int64  `json:"claim_expires_at"`
	}
	if err := json.Unmarshal([]byte(rowsJSON), &raw); err != nil {
		return nil, fmt.Errorf("unmarshal claim result: %w", err)
	}
	jobs := make([]*Job, len(raw))
	for i, r := range raw {
		jobs[i] = &Job{
			q:        q,
			ID:       r.ID,
			Queue:    r.Queue,
			Payload:  []byte(r.Payload),
			WorkerID: r.WorkerID,
			Attempts: r.Attempts,
		}
	}
	return jobs, nil
}

// Ack deletes the claim if it's still valid. Returns true iff the
// caller's claim hadn't expired.
func (j *Job) Ack() (bool, error) {
	var n int64
	err := j.q.db.db.QueryRow(
		"SELECT honker_ack(?, ?)", j.ID, j.WorkerID,
	).Scan(&n)
	return n > 0, err
}

// Retry returns the job to pending with a delay, or moves it to
// _honker_dead if attempts reached max. Returns true iff the caller's
// claim was still valid.
func (j *Job) Retry(delaySec int64, errMsg string) (bool, error) {
	var n int64
	err := j.q.db.db.QueryRow(
		"SELECT honker_retry(?, ?, ?, ?)", j.ID, j.WorkerID, delaySec, errMsg,
	).Scan(&n)
	return n > 0, err
}

// Fail moves the claim straight to _honker_dead regardless of attempts.
func (j *Job) Fail(errMsg string) (bool, error) {
	var n int64
	err := j.q.db.db.QueryRow(
		"SELECT honker_fail(?, ?, ?)", j.ID, j.WorkerID, errMsg,
	).Scan(&n)
	return n > 0, err
}

// Heartbeat extends the claim's visibility timeout. Useful for
// long-running jobs that might exceed the default visibility window.
func (j *Job) Heartbeat(extendSec int64) (bool, error) {
	var n int64
	err := j.q.db.db.QueryRow(
		"SELECT honker_heartbeat(?, ?, ?)", j.ID, j.WorkerID, extendSec,
	).Scan(&n)
	return n > 0, err
}

// AckBatch acks multiple jobs in one call. Returns count acked.
func (q *Queue) AckBatch(ids []int64, workerID string) (int64, error) {
	if len(ids) == 0 {
		return 0, nil
	}
	idsJSON, err := json.Marshal(ids)
	if err != nil {
		return 0, fmt.Errorf("marshal ids: %w", err)
	}
	var n int64
	err = q.db.db.QueryRow(
		"SELECT honker_ack_batch(?, ?)", string(idsJSON), workerID,
	).Scan(&n)
	return n, err
}

// SweepExpired moves expired pending rows to dead. Returns count moved.
func (q *Queue) SweepExpired() (int64, error) {
	var n int64
	err := q.db.db.QueryRow(
		"SELECT honker_sweep_expired(?)", q.name,
	).Scan(&n)
	return n, err
}

// ClaimWaker returns a waker that blocks on database updates until a job
// is claimable.
func (q *Queue) ClaimWaker() *ClaimWaker {
	subID, ch := q.db.updates.subscribe()
	return &ClaimWaker{
		q:        q,
		subID:    subID,
		updateCh: ch,
	}
}

// ClaimWaker blocks until a job is claimable, waking on database updates.
type ClaimWaker struct {
	q        *Queue
	subID    uint64
	updateCh <-chan struct{}
}

// Next blocks until a job is claimable, then claims and returns it.
// Returns (nil, nil) if ctx is cancelled.
func (w *ClaimWaker) Next(ctx context.Context, workerID string) (*Job, error) {
	for {
		job, err := w.q.ClaimOne(workerID)
		if err != nil {
			return nil, err
		}
		if job != nil {
			return job, nil
		}
		nextAt, err := w.q.nextClaimAt()
		if err != nil {
			return nil, err
		}
		if nextAt > 0 && nextAt <= time.Now().Unix() {
			continue
		}
		if !waitForUpdateOrDeadline(ctx, w.updateCh, nextAt) {
			return nil, nil
		}
	}
}

// Close unsubscribes from the update watcher.
func (w *ClaimWaker) Close() {
	w.q.db.updates.unsubscribe(w.subID)
}

func (q *Queue) nextClaimAt() (int64, error) {
	var ts int64
	err := q.db.db.QueryRow(
		"SELECT honker_queue_next_claim_at(?)", q.name,
	).Scan(&ts)
	return ts, err
}

// -------------------------------------------------------------------
// Stream
// -------------------------------------------------------------------

// Stream returns a handle to a named stream.
func (d *Database) Stream(name string) *Stream {
	return &Stream{db: d, name: name}
}

// Stream is a named append-only event log with per-consumer offsets.
type Stream struct {
	db   *Database
	name string
}

// StreamEvent is a single event in a stream.
type StreamEvent struct {
	Offset    int64
	Topic     string
	Key       *string
	Payload   []byte
	CreatedAt int64
}

// Publish inserts an event. Returns the assigned offset.
func (s *Stream) Publish(payload any) (int64, error) {
	return s.publishImpl(nil, payload)
}

// PublishWithKey publishes with a partition key.
func (s *Stream) PublishWithKey(key string, payload any) (int64, error) {
	return s.publishImpl(&key, payload)
}

// PublishTx publishes inside an open transaction.
func (s *Stream) PublishTx(tx *Transaction, payload any) (int64, error) {
	return s.publishTxImpl(tx, nil, payload)
}

func (s *Stream) publishImpl(key *string, payload any) (int64, error) {
	js, err := json.Marshal(payload)
	if err != nil {
		return 0, fmt.Errorf("marshal payload: %w", err)
	}
	var offset int64
	err = s.db.db.QueryRow(
		"SELECT honker_stream_publish(?, ?, ?)",
		s.name, key, string(js),
	).Scan(&offset)
	return offset, err
}

func (s *Stream) publishTxImpl(tx *Transaction, key *string, payload any) (int64, error) {
	js, err := json.Marshal(payload)
	if err != nil {
		return 0, fmt.Errorf("marshal payload: %w", err)
	}
	var offset int64
	err = tx.tx.QueryRow(
		"SELECT honker_stream_publish(?, ?, ?)",
		s.name, key, string(js),
	).Scan(&offset)
	return offset, err
}

// ReadSince reads up to limit events with offset strictly greater
// than offset.
func (s *Stream) ReadSince(offset int64, limit int) ([]StreamEvent, error) {
	var rowsJSON string
	err := s.db.db.QueryRow(
		"SELECT honker_stream_read_since(?, ?, ?)",
		s.name, offset, limit,
	).Scan(&rowsJSON)
	if err != nil {
		return nil, err
	}
	return parseStreamEvents(rowsJSON)
}

// ReadFromConsumer reads from the saved offset of consumer.
func (s *Stream) ReadFromConsumer(consumer string, limit int) ([]StreamEvent, error) {
	offset, err := s.GetOffset(consumer)
	if err != nil {
		return nil, err
	}
	return s.ReadSince(offset, limit)
}

// SaveOffset persists a consumer's offset. Monotonic: lower offsets
// are ignored. Returns true if the offset was advanced.
func (s *Stream) SaveOffset(consumer string, offset int64) (bool, error) {
	return s.saveOffsetImpl(consumer, offset, s.db.db)
}

// SaveOffsetTx saves offset inside an open transaction.
func (s *Stream) SaveOffsetTx(tx *Transaction, consumer string, offset int64) (bool, error) {
	return s.saveOffsetImpl(consumer, offset, tx.tx)
}

func (s *Stream) saveOffsetImpl(consumer string, offset int64, executor queryRower) (bool, error) {
	var n int64
	err := executor.QueryRow(
		"SELECT honker_stream_save_offset(?, ?, ?)",
		consumer, s.name, offset,
	).Scan(&n)
	return n > 0, err
}

// GetOffset returns the current saved offset for consumer, or 0.
func (s *Stream) GetOffset(consumer string) (int64, error) {
	var offset int64
	err := s.db.db.QueryRow(
		"SELECT honker_stream_get_offset(?, ?)",
		consumer, s.name,
	).Scan(&offset)
	return offset, err
}

// Subscribe returns a channel of stream events for a named consumer.
// Resumes from saved offset, wakes on database updates, auto-saves offset
// every 1000 events. Close on ctx cancel.
func (s *Stream) Subscribe(ctx context.Context, consumer string) <-chan StreamEvent {
	ch := make(chan StreamEvent, 16)
	go s.subscribeLoop(ctx, consumer, ch)
	return ch
}

func (s *Stream) subscribeLoop(ctx context.Context, consumer string, ch chan<- StreamEvent) {
	defer close(ch)
	offset, err := s.GetOffset(consumer)
	if err != nil {
		return
	}
	lastSaved := offset
	subID, updateCh := s.db.updates.subscribe()
	defer s.db.updates.unsubscribe(subID)

	for {
		events, err := s.ReadSince(offset, 100)
		if err != nil {
			return
		}
		for _, ev := range events {
			select {
			case <-ctx.Done():
				return
			case ch <- ev:
			}
			offset = ev.Offset
			if offset-lastSaved >= 1000 {
				_, _ = s.SaveOffset(consumer, offset)
				lastSaved = offset
			}
		}
		if len(events) > 0 {
			continue
		}
		select {
		case <-ctx.Done():
			return
		case <-updateCh:
			for {
				select {
				case <-updateCh:
				default:
					goto drained
				}
			}
		drained:
		}
	}
}

func parseStreamEvents(rowsJSON string) ([]StreamEvent, error) {
	var raw []struct {
		Offset    int64   `json:"offset"`
		Topic     string  `json:"topic"`
		Key       *string `json:"key"`
		Payload   string  `json:"payload"`
		CreatedAt int64   `json:"created_at"`
	}
	if err := json.Unmarshal([]byte(rowsJSON), &raw); err != nil {
		return nil, fmt.Errorf("unmarshal stream events: %w", err)
	}
	events := make([]StreamEvent, len(raw))
	for i, r := range raw {
		events[i] = StreamEvent{
			Offset:    r.Offset,
			Topic:     r.Topic,
			Key:       r.Key,
			Payload:   []byte(r.Payload),
			CreatedAt: r.CreatedAt,
		}
	}
	return events, nil
}

// -------------------------------------------------------------------
// Scheduler
// -------------------------------------------------------------------

// Scheduler returns a scheduler facade.
func (d *Database) Scheduler() *Scheduler {
	return &Scheduler{db: d}
}

// Scheduler manages time-triggered tasks with leader election.
type Scheduler struct {
	db *Database
}

// ScheduledTask is a periodic task registration.
type ScheduledTask struct {
	Name     string
	Queue    string
	// Schedule is the canonical recurring schedule expression.
	// It accepts:
	//   - 5-field cron
	//   - 6-field cron
	//   - @every <n><unit> like "@every 1s"
	Schedule string
	// Cron is a backward-compatible alias for Schedule.
	Cron     string
	Payload  any
	Priority int64
	ExpiresS *int64
}

// ScheduledFire is one firing of a scheduled task.
type ScheduledFire struct {
	Name   string `json:"name"`
	Queue  string `json:"queue"`
	FireAt int64  `json:"fire_at"`
	JobID  int64  `json:"job_id"`
}

// Add registers a recurring task. Idempotent by name.
func (s *Scheduler) Add(task ScheduledTask) error {
	payloadJSON, err := json.Marshal(task.Payload)
	if err != nil {
		return fmt.Errorf("marshal payload: %w", err)
	}
	expr := task.Schedule
	if expr == "" {
		expr = task.Cron
	}
	_, err = s.db.db.Exec(
		"SELECT honker_scheduler_register(?, ?, ?, ?, ?, ?)",
		task.Name, task.Queue, expr, string(payloadJSON),
		task.Priority, task.ExpiresS,
	)
	return err
}

// Remove unregisters a task by name. Returns rows deleted.
func (s *Scheduler) Remove(name string) (int64, error) {
	var n int64
	err := s.db.db.QueryRow(
		"SELECT honker_scheduler_unregister(?)", name,
	).Scan(&n)
	return n, err
}

// Tick fires due boundaries and returns what was enqueued.
func (s *Scheduler) Tick() ([]ScheduledFire, error) {
	now := time.Now().Unix()
	var rowsJSON string
	err := s.db.db.QueryRow(
		"SELECT honker_scheduler_tick(?)", now,
	).Scan(&rowsJSON)
	if err != nil {
		return nil, err
	}
	var fires []ScheduledFire
	if err := json.Unmarshal([]byte(rowsJSON), &fires); err != nil {
		return nil, fmt.Errorf("unmarshal fires: %w", err)
	}
	return fires, nil
}

// Soonest returns the soonest next_fire_at across all tasks, or 0.
func (s *Scheduler) Soonest() (int64, error) {
	var ts int64
	err := s.db.db.QueryRow(
		"SELECT honker_scheduler_soonest()",
	).Scan(&ts)
	return ts, err
}

// Run is a blocking leader-elected loop. Only the lock holder fires
// ticks. Exits when ctx is cancelled.
func (s *Scheduler) Run(ctx context.Context, owner string) error {
	const lockTTL = 60
	const heartbeatInterval = 20 * time.Second
	subID, updateCh := s.db.updates.subscribe()
	defer s.db.updates.unsubscribe(subID)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		lock, err := s.db.TryLock("honker-scheduler", owner, lockTTL)
		if err != nil {
			return err
		}
		if lock == nil {
			if !waitForUpdateOrDuration(ctx, updateCh, 5*time.Second) {
				return ctx.Err()
			}
			continue
		}

		err = s.leaderLoop(ctx, lock, owner, lockTTL, heartbeatInterval, updateCh)
		_ = lock.Release()
		if err != nil {
			return err
		}
	}
}

func (s *Scheduler) leaderLoop(ctx context.Context, lock *Lock, owner string, lockTTL int64, heartbeatInterval time.Duration, updateCh <-chan struct{}) error {
	lastHeartbeat := time.Now()

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		_, err := s.Tick()
		if err != nil {
			return err
		}

		if time.Since(lastHeartbeat) >= heartbeatInterval {
			ok, err := lock.Heartbeat(lockTTL)
			if err != nil {
				return err
			}
			if !ok {
				// Lost the lock — exit leader loop and re-contest.
				return nil
			}
			lastHeartbeat = time.Now()
		}

		waitFor := heartbeatInterval
		soonest, err := s.Soonest()
		if err != nil {
			return err
		}
		if soonest > 0 {
			untilSoonest := time.Until(time.Unix(soonest, 0))
			if untilSoonest < 0 {
				untilSoonest = 0
			}
			if untilSoonest < waitFor {
				waitFor = untilSoonest
			}
		}
		if !waitForUpdateOrDuration(ctx, updateCh, waitFor) {
			return nil
		}
	}
}

func waitForUpdateOrDeadline(ctx context.Context, updateCh <-chan struct{}, unixSec int64) bool {
	if unixSec <= 0 {
		return waitForUpdateOrDuration(ctx, updateCh, 0)
	}
	until := time.Until(time.Unix(unixSec, 0))
	if until < 0 {
		until = 0
	}
	return waitForUpdateOrDuration(ctx, updateCh, until)
}

func waitForUpdateOrDuration(ctx context.Context, updateCh <-chan struct{}, waitFor time.Duration) bool {
	if waitFor <= 0 {
		select {
		case <-ctx.Done():
			return false
		default:
			return true
		}
	}

	timer := time.NewTimer(waitFor)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return false
	case <-updateCh:
		drainUpdateCh(updateCh)
		return true
	case <-timer.C:
		return true
	}
}

func drainUpdateCh(updateCh <-chan struct{}) {
	for {
		select {
		case <-updateCh:
		default:
			return
		}
	}
}

// -------------------------------------------------------------------
// Locks
// -------------------------------------------------------------------

// TryLock attempts to acquire an advisory lock. Returns nil if not
// acquired.
func (d *Database) TryLock(name, owner string, ttlSec int64) (*Lock, error) {
	var n int64
	err := d.db.QueryRow(
		"SELECT honker_lock_acquire(?, ?, ?)", name, owner, ttlSec,
	).Scan(&n)
	if err != nil {
		return nil, err
	}
	if n != 1 {
		return nil, nil
	}
	lock := &Lock{db: d, name: name, owner: owner}
	runtime.SetFinalizer(lock, (*Lock).finalize)
	return lock, nil
}

// Lock is a held advisory lock.
type Lock struct {
	db       *Database
	name     string
	owner    string
	released bool
}

// Release releases the lock. Idempotent.
func (l *Lock) Release() error {
	if l.released {
		return nil
	}
	l.released = true
	runtime.SetFinalizer(l, nil)
	_, err := l.db.db.Exec(
		"SELECT honker_lock_release(?, ?)", l.name, l.owner,
	)
	return err
}

// Heartbeat extends the lock's TTL. Returns true if we still own it.
// Uses a direct UPDATE because honker_lock_acquire uses INSERT OR
// IGNORE and won't extend an existing row.
func (l *Lock) Heartbeat(ttlSec int64) (bool, error) {
	tx, err := l.db.db.Begin()
	if err != nil {
		return false, err
	}
	defer tx.Rollback()

	_, err = tx.Exec(
		"UPDATE _honker_locks SET expires_at = unixepoch() + ? WHERE name = ? AND owner = ?",
		ttlSec, l.name, l.owner,
	)
	if err != nil {
		return false, err
	}
	var changes int64
	if err := tx.QueryRow("SELECT changes()").Scan(&changes); err != nil {
		return false, err
	}
	if err := tx.Commit(); err != nil {
		return false, err
	}
	return changes > 0, nil
}

func (l *Lock) finalize() {
	if l.released {
		return
	}
	l.released = true
	if l.db != nil && l.db.db != nil {
		_, _ = l.db.db.Exec(
			"SELECT honker_lock_release(?, ?)", l.name, l.owner,
		)
	}
}

// -------------------------------------------------------------------
// Rate limiting
// -------------------------------------------------------------------

// TryRateLimit performs a fixed-window rate limit check. Returns true
// if the request is allowed.
func (d *Database) TryRateLimit(name string, limit, perSec int64) (bool, error) {
	var n int64
	err := d.db.QueryRow(
		"SELECT honker_rate_limit_try(?, ?, ?)", name, limit, perSec,
	).Scan(&n)
	return n == 1, err
}

// -------------------------------------------------------------------
// Results
// -------------------------------------------------------------------

// SaveResult persists a job result.
func (d *Database) SaveResult(jobID int64, value string, ttlSec int64) error {
	_, err := d.db.Exec(
		"SELECT honker_result_save(?, ?, ?)", jobID, value, ttlSec,
	)
	return err
}

// GetResult fetches a stored result. Returns nil if absent or expired.
func (d *Database) GetResult(jobID int64) (*string, error) {
	var value *string
	err := d.db.QueryRow(
		"SELECT honker_result_get(?)", jobID,
	).Scan(&value)
	return value, err
}

// SweepResults drops expired results. Returns rows deleted.
func (d *Database) SweepResults() (int64, error) {
	var n int64
	err := d.db.QueryRow(
		"SELECT honker_result_sweep()",
	).Scan(&n)
	return n, err
}

// -------------------------------------------------------------------
// Notify / Listen
// -------------------------------------------------------------------

// Notify fires a pub/sub signal on channel. Payload is marshaled via
// json.Marshal. Returns the notification id.
func (d *Database) Notify(channel string, payload any) (int64, error) {
	js, err := json.Marshal(payload)
	if err != nil {
		return 0, fmt.Errorf("marshal payload: %w", err)
	}
	var id int64
	err = d.db.QueryRow("SELECT notify(?, ?)", channel, string(js)).Scan(&id)
	return id, err
}

// NotifyTx fires a notification inside an open transaction.
func (d *Database) NotifyTx(tx *Transaction, channel string, payload any) (int64, error) {
	js, err := json.Marshal(payload)
	if err != nil {
		return 0, fmt.Errorf("marshal payload: %w", err)
	}
	var id int64
	err = tx.tx.QueryRow("SELECT notify(?, ?)", channel, string(js)).Scan(&id)
	return id, err
}

// Notification is a pub/sub message.
type Notification struct {
	ID      int64
	Channel string
	Payload []byte
}

// Listen returns a channel of notifications on channel. Starts at
// MAX(id) at attach time; historical notifications are not replayed.
// Close on ctx cancel.
func (d *Database) Listen(ctx context.Context, channel string) <-chan Notification {
	ch := make(chan Notification, 16)
	go d.listenLoop(ctx, channel, ch)
	return ch
}

func (d *Database) listenLoop(ctx context.Context, channel string, ch chan<- Notification) {
	defer close(ch)
	var lastID int64
	row := d.db.QueryRow("SELECT COALESCE(MAX(id), 0) FROM _honker_notifications")
	if err := row.Scan(&lastID); err != nil {
		return
	}

	subID, updateCh := d.updates.subscribe()
	defer d.updates.unsubscribe(subID)

	for {
		rows, err := d.db.Query(
			"SELECT id, channel, payload FROM _honker_notifications WHERE id > ? AND channel = ? ORDER BY id ASC LIMIT 1000",
			lastID, channel,
		)
		if err != nil {
			return
		}
		for rows.Next() {
			var n Notification
			if err := rows.Scan(&n.ID, &n.Channel, &n.Payload); err != nil {
				rows.Close()
				return
			}
			lastID = n.ID
			select {
			case <-ctx.Done():
				rows.Close()
				return
			case ch <- n:
			}
		}
		rows.Close()

		select {
		case <-ctx.Done():
			return
		case <-updateCh:
			for {
				select {
				case <-updateCh:
				default:
					goto drained
				}
			}
		drained:
		}
	}
}

// -------------------------------------------------------------------
// Helpers
// -------------------------------------------------------------------

type queryRower interface {
	QueryRow(query string, args ...any) *sql.Row
}
