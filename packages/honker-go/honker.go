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
	"database/sql"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"
	"sync/atomic"

	sqlite3 "github.com/mattn/go-sqlite3"
)

// Database is a Honker handle over a SQLite file with the extension
// loaded on every pool connection. Open() bootstraps the schema on
// first use; safe to Open() the same path from multiple processes.
type Database struct {
	db *sql.DB
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
	// go-sqlite3's `LoadExtension(path, "")` passes an empty cstring
	// as the entry-point name to SQLite's `sqlite3_load_extension`,
	// not NULL — so SQLite's default "derive sqlite3_<extname>_init
	// from filename" path doesn't trigger. We replicate the derivation
	// in Go and pass the explicit name.
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
	return &Database{db: sqlDB}, nil
}

// Close releases the underlying sql.DB.
func (d *Database) Close() error { return d.db.Close() }

// Raw returns the underlying *sql.DB for advanced queries (e.g.
// reading _honker_live directly or integrating with Go migration
// tools).
func (d *Database) Raw() *sql.DB { return d.db }

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
