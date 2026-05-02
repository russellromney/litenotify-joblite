package honker

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"testing"
)

func repoRoot(t *testing.T) string {
	t.Helper()
	_, thisFile, _, _ := runtime.Caller(0)
	return filepath.Clean(filepath.Join(filepath.Dir(thisFile), "..", ".."))
}

func findInteropPython(t *testing.T) string {
	t.Helper()
	repo := repoRoot(t)
	if p := os.Getenv("HONKER_INTEROP_PYTHON"); p != "" {
		cmd := exec.Command(p, "-c", pythonProbeScript)
		cmd.Env = interopPythonEnv(t, "")
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("HONKER_INTEROP_PYTHON cannot import honker: %v\n%s", err, string(out))
		}
		return p
	}
	candidates := []string{
		filepath.Join(repo, ".venv", "bin", "python"),
		filepath.Join(repo, ".venv", "Scripts", "python.exe"),
		"python3",
		"python",
	}
	for _, p := range candidates {
		cmd := exec.Command(p, "-c", pythonProbeScript)
		cmd.Env = interopPythonEnv(t, "")
		if err := cmd.Run(); err == nil {
			return p
		}
	}
	t.Skipf(
		"Python honker binding unavailable; build it first, or set HONKER_INTEROP_PYTHON. tried %v",
		candidates,
	)
	return ""
}

const pythonProbeScript = `
import os
import tempfile
import honker

p = tempfile.mktemp(prefix="honker-probe-", suffix=".db")
db = honker.open(p)
db.query("SELECT 1")
db = None
try:
    os.remove(p)
except OSError:
    pass
`

func interopPythonEnv(t *testing.T, dbPath string) []string {
	t.Helper()
	repo := repoRoot(t)
	paths := []string{
		filepath.Join(repo, "packages", "honker", "python"),
		filepath.Join(repo, "packages"),
	}
	env := os.Environ()
	if old := os.Getenv("PYTHONPATH"); old != "" {
		paths = append(paths, old)
	}
	env = append(env, "PYTHONPATH="+strings.Join(paths, string(os.PathListSeparator)))
	if dbPath != "" {
		env = append(env, "DB_PATH="+dbPath)
	}
	return env
}

func runInteropPython(t *testing.T, python string, dbPath string, script string) string {
	t.Helper()
	cmd := exec.Command(python, "-c", script)
	cmd.Env = interopPythonEnv(t, dbPath)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("python interop failed: %v\n%s", err, string(out))
	}
	return string(out)
}

func TestGoPythonInteropQueueStreamAndNotify(t *testing.T) {
	extPath := findExtension(t)
	python := findInteropPython(t)
	dbPath := filepath.Join(t.TempDir(), "go-python.db")

	db, err := Open(dbPath, extPath)
	if err != nil {
		t.Fatalf("open go db: %v", err)
	}
	defer db.Close()

	goQueue := db.Queue("go-to-python", QueueOptions{})
	for i := 0; i < 25; i++ {
		if _, err := goQueue.Enqueue(map[string]any{
			"source": "go",
			"seq":    i,
			"key":    "go-" + string(rune('a'+i)),
		}, EnqueueOptions{}); err != nil {
			t.Fatalf("go enqueue %d: %v", i, err)
		}
	}
	if _, err := db.Notify("from-go", map[string]any{"source": "go", "count": 25}); err != nil {
		t.Fatalf("go notify: %v", err)
	}
	if _, err := db.Stream("interop").Publish(map[string]any{"source": "go", "kind": "stream"}); err != nil {
		t.Fatalf("go stream publish: %v", err)
	}

	out := runInteropPython(t, python, dbPath, `
import json
import os
import honker

db = honker.open(os.environ["DB_PATH"])

go_jobs = db.queue("go-to-python").claim_batch("python-worker", 50)
go_payloads = [job.payload for job in go_jobs]
acked = db.queue("go-to-python").ack_batch(
    [job.id for job in go_jobs],
    "python-worker",
)

go_note = db.query(
    "SELECT payload FROM _honker_notifications "
    "WHERE channel='from-go' ORDER BY id DESC LIMIT 1"
)
go_events = db.stream("interop")._read_since(0, 10)

py_q = db.queue("python-to-go")
for i in range(25):
    py_q.enqueue({"source": "python", "seq": i, "key": f"py-{i:02d}"})
with db.transaction() as tx:
    tx.notify("from-python", {"source": "python", "count": len(go_jobs)})
db.stream("interop").publish({"source": "python", "kind": "stream"})

print(json.dumps({
    "acked": acked,
    "go_payloads": go_payloads,
    "go_note": json.loads(go_note[0]["payload"]),
    "go_events": [ev["payload"] for ev in go_events],
}))
`)

	var pyObserved struct {
		Acked      int64             `json:"acked"`
		GoPayloads []map[string]any  `json:"go_payloads"`
		GoNote     map[string]any    `json:"go_note"`
		GoEvents   []json.RawMessage `json:"go_events"`
	}
	if err := json.Unmarshal([]byte(out), &pyObserved); err != nil {
		t.Fatalf("unmarshal python output %q: %v", out, err)
	}
	if pyObserved.Acked != 25 {
		t.Fatalf("python acked %d go jobs, want 25", pyObserved.Acked)
	}
	if len(pyObserved.GoPayloads) != 25 {
		t.Fatalf("python claimed %d go jobs, want 25", len(pyObserved.GoPayloads))
	}
	goSeqs := make([]int, 0, 25)
	for _, payload := range pyObserved.GoPayloads {
		if payload["source"] != "go" {
			t.Fatalf("unexpected go payload source: %#v", payload)
		}
		goSeqs = append(goSeqs, int(payload["seq"].(float64)))
	}
	sort.Ints(goSeqs)
	for i, seq := range goSeqs {
		if seq != i {
			t.Fatalf("go seqs = %v, want 0..24", goSeqs)
		}
	}
	if pyObserved.GoNote["source"] != "go" || int(pyObserved.GoNote["count"].(float64)) != 25 {
		t.Fatalf("python saw wrong go notification: %#v", pyObserved.GoNote)
	}
	if len(pyObserved.GoEvents) != 1 {
		t.Fatalf("python saw %d go stream events, want 1", len(pyObserved.GoEvents))
	}

	pyQueue := db.Queue("python-to-go", QueueOptions{})
	pyJobs, err := pyQueue.ClaimBatch("go-worker", 50)
	if err != nil {
		t.Fatalf("go claim python jobs: %v", err)
	}
	if len(pyJobs) != 25 {
		t.Fatalf("go claimed %d python jobs, want 25", len(pyJobs))
	}
	pySeqs := make([]int, 0, 25)
	ids := make([]int64, 0, 25)
	for _, job := range pyJobs {
		ids = append(ids, job.ID)
		var payload map[string]any
		if err := json.Unmarshal(job.Payload, &payload); err != nil {
			t.Fatalf("unmarshal python job: %v", err)
		}
		if payload["source"] != "python" {
			t.Fatalf("unexpected python payload source: %#v", payload)
		}
		pySeqs = append(pySeqs, int(payload["seq"].(float64)))
	}
	sort.Ints(pySeqs)
	for i, seq := range pySeqs {
		if seq != i {
			t.Fatalf("python seqs = %v, want 0..24", pySeqs)
		}
	}
	acked, err := pyQueue.AckBatch(ids, "go-worker")
	if err != nil {
		t.Fatalf("go ack python jobs: %v", err)
	}
	if acked != 25 {
		t.Fatalf("go acked %d python jobs, want 25", acked)
	}

	var notePayload string
	if err := db.Raw().QueryRow(
		"SELECT payload FROM _honker_notifications WHERE channel='from-python' ORDER BY id DESC LIMIT 1",
	).Scan(&notePayload); err != nil {
		t.Fatalf("read python notification: %v", err)
	}
	var note map[string]any
	if err := json.Unmarshal([]byte(notePayload), &note); err != nil {
		t.Fatalf("unmarshal python notification: %v", err)
	}
	if note["source"] != "python" || int(note["count"].(float64)) != 25 {
		t.Fatalf("go saw wrong python notification: %#v", note)
	}

	events, err := db.Stream("interop").ReadSince(0, 10)
	if err != nil {
		t.Fatalf("go read stream: %v", err)
	}
	if len(events) != 2 {
		t.Fatalf("go saw %d stream events, want 2", len(events))
	}
}
