package honker

import (
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

// findExtension locates libhonker.{dylib,so} under the repo's
// target/release/ dir so the test doesn't need to hardcode a path.
func findExtension(t *testing.T) string {
	t.Helper()
	_, thisFile, _, _ := runtime.Caller(0)
	// thisFile = .../packages/honker-go/honker_test.go
	repo := filepath.Clean(filepath.Join(filepath.Dir(thisFile), "..", ".."))
	candidates := []string{
		filepath.Join(repo, "target/release/libhonker_ext.dylib"),
		filepath.Join(repo, "target/release/libhonker_ext.so"),
		filepath.Join(repo, "target/release/libhonker_extension.dylib"),
		filepath.Join(repo, "target/release/libhonker_extension.so"),
	}
	for _, p := range candidates {
		if _, err := os.Stat(p); err == nil {
			return p
		}
	}
	t.Skipf(
		"honker extension not built — run `cargo build -p honker-extension --release` first. "+
			"looked for: %v",
		candidates,
	)
	return ""
}

func TestEnqueueClaimAck(t *testing.T) {
	extPath := findExtension(t)
	dbPath := filepath.Join(t.TempDir(), "t.db")

	db, err := Open(dbPath, extPath)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	q := db.Queue("emails", QueueOptions{})

	id, err := q.Enqueue(map[string]any{"to": "alice@example.com"}, EnqueueOptions{})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	if id <= 0 {
		t.Fatalf("expected positive id, got %d", id)
	}

	job, err := q.ClaimOne("worker-1")
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if job == nil {
		t.Fatal("expected a claimed job, got nil")
	}
	if job.ID != id {
		t.Fatalf("expected id=%d, got %d", id, job.ID)
	}

	var payload map[string]any
	if err := json.Unmarshal(job.Payload, &payload); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}
	if payload["to"] != "alice@example.com" {
		t.Fatalf("payload round-trip failed: %+v", payload)
	}

	ok, err := job.Ack()
	if err != nil {
		t.Fatalf("ack: %v", err)
	}
	if !ok {
		t.Fatal("expected ack=true for a fresh claim")
	}

	// Queue empty after ack.
	next, err := q.ClaimOne("worker-1")
	if err != nil {
		t.Fatalf("second claim: %v", err)
	}
	if next != nil {
		t.Fatalf("expected empty queue after ack, got job %d", next.ID)
	}
}

func TestRetryAndFail(t *testing.T) {
	extPath := findExtension(t)
	dbPath := filepath.Join(t.TempDir(), "t.db")
	db, err := Open(dbPath, extPath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	q := db.Queue("retries", QueueOptions{MaxAttempts: 2})

	if _, err := q.Enqueue(map[string]any{"i": 1}, EnqueueOptions{}); err != nil {
		t.Fatal(err)
	}

	job, _ := q.ClaimOne("w")
	if job == nil {
		t.Fatal("no job")
	}

	// retry with 0 delay → claimable again immediately
	ok, err := job.Retry(0, "first attempt failed")
	if err != nil || !ok {
		t.Fatalf("retry: ok=%v err=%v", ok, err)
	}

	// claim again; attempts should now be 2 (max_attempts)
	job2, _ := q.ClaimOne("w")
	if job2 == nil {
		t.Fatal("retry didn't flip back to pending")
	}
	if job2.Attempts != 2 {
		t.Fatalf("expected attempts=2, got %d", job2.Attempts)
	}

	// second retry hits max; should move to dead
	if _, err := job2.Retry(0, "second attempt failed"); err != nil {
		t.Fatal(err)
	}
	// queue empty
	next, _ := q.ClaimOne("w")
	if next != nil {
		t.Fatalf("expected dead-letter path, got job %d", next.ID)
	}

	// dead row visible
	var count int
	row := db.Raw().QueryRow("SELECT COUNT(*) FROM _honker_dead WHERE queue='retries'")
	if err := row.Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("expected 1 dead row, got %d", count)
	}
}

func TestNotify(t *testing.T) {
	extPath := findExtension(t)
	dbPath := filepath.Join(t.TempDir(), "t.db")
	db, err := Open(dbPath, extPath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	id, err := db.Notify("orders", map[string]any{"id": 42})
	if err != nil {
		t.Fatal(err)
	}
	if id <= 0 {
		t.Fatalf("expected positive notification id, got %d", id)
	}

	var channel, payload string
	row := db.Raw().QueryRow(
		"SELECT channel, payload FROM _honker_notifications WHERE id = ?",
		id,
	)
	if err := row.Scan(&channel, &payload); err != nil {
		t.Fatal(err)
	}
	if channel != "orders" {
		t.Fatalf("channel: got %q", channel)
	}
	var parsed map[string]any
	if err := json.Unmarshal([]byte(payload), &parsed); err != nil {
		t.Fatalf("payload unmarshal: %v (raw=%s)", err, payload)
	}
	if int(parsed["id"].(float64)) != 42 {
		t.Fatalf("payload mismatch: %+v", parsed)
	}
}
