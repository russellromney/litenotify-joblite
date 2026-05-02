package honker

import (
	"context"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"
)

func TestTransactionAtomicity(t *testing.T) {
	extPath := findExtension(t)
	dbPath := filepath.Join(t.TempDir(), "t.db")
	db, err := Open(dbPath, extPath)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	q := db.Queue("txq", QueueOptions{})

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if _, err := tx.Exec("CREATE TABLE test (id INTEGER PRIMARY KEY)"); err != nil {
		t.Fatalf("create table: %v", err)
	}
	if _, err := q.EnqueueTx(tx, map[string]any{"x": 1}, EnqueueOptions{}); err != nil {
		t.Fatalf("enqueue tx: %v", err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatalf("rollback: %v", err)
	}

	job, _ := q.ClaimOne("w")
	if job != nil {
		t.Fatalf("expected no job after rollback, got %d", job.ID)
	}

	tx2, err := db.Begin()
	if err != nil {
		t.Fatalf("begin2: %v", err)
	}
	id, err := q.EnqueueTx(tx2, map[string]any{"x": 2}, EnqueueOptions{})
	if err != nil {
		t.Fatalf("enqueue tx2: %v", err)
	}
	if err := tx2.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}

	job2, _ := q.ClaimOne("w")
	if job2 == nil || job2.ID != id {
		t.Fatalf("expected job %d after commit, got %v", id, job2)
	}
	job2.Ack()
}

func TestStream(t *testing.T) {
	extPath := findExtension(t)
	dbPath := filepath.Join(t.TempDir(), "t.db")
	db, err := Open(dbPath, extPath)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	s := db.Stream("events")

	off1, err := s.Publish(map[string]any{"a": 1})
	if err != nil {
		t.Fatalf("publish: %v", err)
	}
	off2, err := s.PublishWithKey("k1", map[string]any{"a": 2})
	if err != nil {
		t.Fatalf("publish with key: %v", err)
	}

	events, err := s.ReadSince(0, 10)
	if err != nil {
		t.Fatalf("read since: %v", err)
	}
	if len(events) != 2 {
		t.Fatalf("expected 2 events, got %d", len(events))
	}
	if events[0].Offset != off1 {
		t.Fatalf("expected offset %d, got %d", off1, events[0].Offset)
	}
	if events[1].Key == nil || *events[1].Key != "k1" {
		t.Fatalf("expected key k1, got %v", events[1].Key)
	}

	ok, err := s.SaveOffset("c1", off1)
	if err != nil {
		t.Fatalf("save offset: %v", err)
	}
	if !ok {
		t.Fatal("expected save offset to succeed")
	}

	offset, err := s.GetOffset("c1")
	if err != nil {
		t.Fatalf("get offset: %v", err)
	}
	if offset != off1 {
		t.Fatalf("expected offset %d, got %d", off1, offset)
	}

	events2, err := s.ReadFromConsumer("c1", 10)
	if err != nil {
		t.Fatalf("read from consumer: %v", err)
	}
	if len(events2) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events2))
	}
	if events2[0].Offset != off2 {
		t.Fatalf("expected offset %d, got %d", off2, events2[0].Offset)
	}
}

func TestStreamSaveOffsetTxRollback(t *testing.T) {
	extPath := findExtension(t)
	dbPath := filepath.Join(t.TempDir(), "t.db")
	db, err := Open(dbPath, extPath)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	s := db.Stream("txstream")
	off, _ := s.Publish(map[string]any{"a": 1})

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	ok, err := s.SaveOffsetTx(tx, "c1", off)
	if err != nil {
		t.Fatalf("save offset tx: %v", err)
	}
	if !ok {
		t.Fatal("expected save to succeed")
	}
	if err := tx.Rollback(); err != nil {
		t.Fatalf("rollback: %v", err)
	}

	offset, _ := s.GetOffset("c1")
	if offset != 0 {
		t.Fatalf("expected offset 0 after rollback, got %d", offset)
	}
}

func TestStreamPublishTxRollback(t *testing.T) {
	extPath := findExtension(t)
	dbPath := filepath.Join(t.TempDir(), "t.db")
	db, err := Open(dbPath, extPath)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	s := db.Stream("txstream2")
	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	_, err = s.PublishTx(tx, map[string]any{"x": 1})
	if err != nil {
		t.Fatalf("publish tx: %v", err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatalf("rollback: %v", err)
	}

	events, _ := s.ReadSince(0, 10)
	if len(events) != 0 {
		t.Fatalf("expected 0 events after rollback, got %d", len(events))
	}
}

func TestStreamSubscribe(t *testing.T) {
	extPath := findExtension(t)
	dbPath := filepath.Join(t.TempDir(), "t.db")
	db, err := Open(dbPath, extPath)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	s := db.Stream("sub")

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	ch := s.Subscribe(ctx, "c1")

	time.Sleep(50 * time.Millisecond)
	off, err := s.Publish(map[string]any{"x": 1})
	if err != nil {
		t.Fatalf("publish: %v", err)
	}

	select {
	case ev := <-ch:
		if ev.Offset != off {
			t.Fatalf("expected offset %d, got %d", off, ev.Offset)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for stream event")
	}
}

func TestListen(t *testing.T) {
	extPath := findExtension(t)
	dbPath := filepath.Join(t.TempDir(), "t.db")
	db, err := Open(dbPath, extPath)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	ch := db.Listen(ctx, "orders")

	time.Sleep(50 * time.Millisecond)

	go func() {
		if _, err := db.Notify("orders", map[string]any{"id": 42}); err != nil {
			t.Errorf("notify: %v", err)
		}
	}()

	select {
	case n := <-ch:
		if n.Channel != "orders" {
			t.Fatalf("expected channel orders, got %s", n.Channel)
		}
		var payload map[string]any
		if err := json.Unmarshal(n.Payload, &payload); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		if int(payload["id"].(float64)) != 42 {
			t.Fatalf("expected id 42, got %v", payload["id"])
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("timed out waiting for notification")
	}
}

func TestListenFilter(t *testing.T) {
	extPath := findExtension(t)
	dbPath := filepath.Join(t.TempDir(), "t.db")
	db, err := Open(dbPath, extPath)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	ch := db.Listen(ctx, "ch-a")

	time.Sleep(50 * time.Millisecond)

	if _, err := db.Notify("ch-b", map[string]any{"x": 1}); err != nil {
		t.Fatalf("notify b: %v", err)
	}
	if _, err := db.Notify("ch-a", map[string]any{"y": 2}); err != nil {
		t.Fatalf("notify a: %v", err)
	}

	select {
	case n := <-ch:
		var payload map[string]any
		json.Unmarshal(n.Payload, &payload)
		if payload["y"] == nil {
			t.Fatalf("expected y payload, got %s", string(n.Payload))
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("timed out waiting for filtered notification")
	}
}

func TestScheduler(t *testing.T) {
	extPath := findExtension(t)
	dbPath := filepath.Join(t.TempDir(), "t.db")
	db, err := Open(dbPath, extPath)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	sched := db.Scheduler()

	if err := sched.Add(ScheduledTask{
		Name: "test", Queue: "q", Schedule: "0 * * * *",
		Payload: map[string]any{"x": 1}, Priority: 0,
	}); err != nil {
		t.Fatalf("add: %v", err)
	}

	soonest, err := sched.Soonest()
	if err != nil {
		t.Fatalf("soonest: %v", err)
	}
	if soonest == 0 {
		t.Fatal("expected non-zero soonest")
	}

	if _, err := sched.Tick(); err != nil {
		t.Fatalf("tick: %v", err)
	}

	n, err := sched.Remove("test")
	if err != nil {
		t.Fatalf("remove: %v", err)
	}
	if n != 1 {
		t.Fatalf("expected 1 removal, got %d", n)
	}

	soonest2, _ := sched.Soonest()
	if soonest2 != 0 {
		t.Fatalf("expected 0 soonest after remove, got %d", soonest2)
	}
}

func TestSchedulerRun(t *testing.T) {
	extPath := findExtension(t)
	dbPath := filepath.Join(t.TempDir(), "t.db")
	db, err := Open(dbPath, extPath)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	sched := db.Scheduler()
	if err := sched.Add(ScheduledTask{
		Name: "runtest", Queue: "q", Schedule: "0 * * * *",
		Payload: map[string]any{"x": 1},
	}); err != nil {
		t.Fatalf("add: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	err = sched.Run(ctx, "owner-1")
	if err != context.DeadlineExceeded {
		t.Fatalf("expected DeadlineExceeded, got %v", err)
	}
}

func TestSchedulerAcceptsLegacyCronAlias(t *testing.T) {
	extPath := findExtension(t)
	dbPath := filepath.Join(t.TempDir(), "t.db")
	db, err := Open(dbPath, extPath)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	sched := db.Scheduler()
	if err := sched.Add(ScheduledTask{
		Name: "legacy", Queue: "q", Cron: "@every 1s",
		Payload: map[string]any{"ok": true},
	}); err != nil {
		t.Fatalf("add: %v", err)
	}

	soonest, err := sched.Soonest()
	if err != nil {
		t.Fatalf("soonest: %v", err)
	}
	if soonest == 0 {
		t.Fatal("expected non-zero soonest")
	}
}

func TestLock(t *testing.T) {
	extPath := findExtension(t)
	dbPath := filepath.Join(t.TempDir(), "t.db")
	db, err := Open(dbPath, extPath)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	lock1, err := db.TryLock("mylock", "owner-1", 60)
	if err != nil {
		t.Fatalf("try lock: %v", err)
	}
	if lock1 == nil {
		t.Fatal("expected lock acquired")
	}

	lock2, err := db.TryLock("mylock", "owner-2", 60)
	if err != nil {
		t.Fatalf("try lock 2: %v", err)
	}
	if lock2 != nil {
		t.Fatal("expected lock denied")
	}

	if err := lock1.Release(); err != nil {
		t.Fatalf("release: %v", err)
	}
	if err := lock1.Release(); err != nil {
		t.Fatalf("idempotent release: %v", err)
	}

	lock3, err := db.TryLock("mylock", "owner-2", 60)
	if err != nil {
		t.Fatalf("try lock 3: %v", err)
	}
	if lock3 == nil {
		t.Fatal("expected re-acquire")
	}
	lock3.Release()
}

func TestLockHeartbeat(t *testing.T) {
	extPath := findExtension(t)
	dbPath := filepath.Join(t.TempDir(), "t.db")
	db, err := Open(dbPath, extPath)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	lock, err := db.TryLock("hb-lock", "owner-1", 2)
	if err != nil {
		t.Fatalf("try lock: %v", err)
	}
	if lock == nil {
		t.Fatal("expected lock")
	}
	defer lock.Release()

	ok, err := lock.Heartbeat(60)
	if err != nil {
		t.Fatalf("heartbeat: %v", err)
	}
	if !ok {
		t.Fatal("expected heartbeat success")
	}
}

func TestLockHeartbeatKeepsOwnership(t *testing.T) {
	extPath := findExtension(t)
	dbPath := filepath.Join(t.TempDir(), "t.db")
	db, err := Open(dbPath, extPath)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	lock, err := db.TryLock("hbtest", "o1", 1)
	if err != nil {
		t.Fatalf("try lock: %v", err)
	}
	if lock == nil {
		t.Fatal("expected lock")
	}
	defer lock.Release()

	ok, err := lock.Heartbeat(60)
	if err != nil {
		t.Fatalf("heartbeat: %v", err)
	}
	if !ok {
		t.Fatal("expected heartbeat ok")
	}

	time.Sleep(2 * time.Second)

	lock2, err := db.TryLock("hbtest", "o2", 60)
	if err != nil {
		t.Fatalf("try lock 2: %v", err)
	}
	if lock2 != nil {
		t.Fatal("expected lock still held after heartbeat extension")
	}
}

func TestRateLimit(t *testing.T) {
	extPath := findExtension(t)
	dbPath := filepath.Join(t.TempDir(), "t.db")
	db, err := Open(dbPath, extPath)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	for i := 0; i < 3; i++ {
		ok, err := db.TryRateLimit("api", 3, 60)
		if err != nil {
			t.Fatalf("rate limit %d: %v", i+1, err)
		}
		if !ok {
			t.Fatalf("expected allow on attempt %d", i+1)
		}
	}

	ok, err := db.TryRateLimit("api", 3, 60)
	if err != nil {
		t.Fatalf("rate limit 4: %v", err)
	}
	if ok {
		t.Fatal("expected block on 4th attempt")
	}
}

func TestResults(t *testing.T) {
	extPath := findExtension(t)
	dbPath := filepath.Join(t.TempDir(), "t.db")
	db, err := Open(dbPath, extPath)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	if err := db.SaveResult(1, `{"ok":true}`, 3600); err != nil {
		t.Fatalf("save result: %v", err)
	}

	val, err := db.GetResult(1)
	if err != nil {
		t.Fatalf("get result: %v", err)
	}
	if val == nil || *val != `{"ok":true}` {
		t.Fatalf("expected result, got %v", val)
	}

	missing, err := db.GetResult(999)
	if err != nil {
		t.Fatalf("get missing: %v", err)
	}
	if missing != nil {
		t.Fatalf("expected nil for missing, got %v", missing)
	}

	n, err := db.SweepResults()
	if err != nil {
		t.Fatalf("sweep: %v", err)
	}
	if n != 0 {
		t.Fatalf("expected 0 sweeps (ttl 3600), got %d", n)
	}
}

func TestSweepResultsExpired(t *testing.T) {
	extPath := findExtension(t)
	dbPath := filepath.Join(t.TempDir(), "t.db")
	db, err := Open(dbPath, extPath)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	if err := db.SaveResult(1, "val", 1); err != nil {
		t.Fatalf("save result: %v", err)
	}
	time.Sleep(2 * time.Second)

	n, err := db.SweepResults()
	if err != nil {
		t.Fatalf("sweep: %v", err)
	}
	if n != 1 {
		t.Fatalf("expected 1 swept, got %d", n)
	}

	missing, _ := db.GetResult(1)
	if missing != nil {
		t.Fatal("expected nil after sweep")
	}
}

func TestClaimWaker(t *testing.T) {
	extPath := findExtension(t)
	dbPath := filepath.Join(t.TempDir(), "t.db")
	db, err := Open(dbPath, extPath)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	q := db.Queue("waker", QueueOptions{})
	waker := q.ClaimWaker()
	defer waker.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	jobCh := make(chan *Job, 1)
	go func() {
		job, err := waker.Next(ctx, "worker-1")
		if err != nil {
			t.Errorf("waker next: %v", err)
		}
		jobCh <- job
	}()

	time.Sleep(100 * time.Millisecond)
	if _, err := q.Enqueue(map[string]any{"x": 1}, EnqueueOptions{}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	select {
	case job := <-jobCh:
		if job == nil {
			t.Fatal("expected job from waker")
		}
		job.Ack()
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for waker")
	}
}

func TestClaimWakerWakesOnRunAtDeadline(t *testing.T) {
	extPath := findExtension(t)
	dbPath := filepath.Join(t.TempDir(), "t.db")
	db, err := Open(dbPath, extPath)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	q := db.Queue("runat", QueueOptions{})
	runAt := time.Now().Add(2 * time.Second).Unix()
	if _, err := q.Enqueue(map[string]any{"x": 1}, EnqueueOptions{RunAt: &runAt}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	waker := q.ClaimWaker()
	defer waker.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	start := time.Now()
	job, err := waker.Next(ctx, "worker-1")
	if err != nil {
		t.Fatalf("waker next: %v", err)
	}
	if job == nil {
		t.Fatal("expected job from waker")
	}
	elapsed := time.Since(start)
	expected := time.Until(time.Unix(runAt, 0))
	minElapsed := expected - 250*time.Millisecond
	if minElapsed < 0 {
		minElapsed = 0
	}
	if elapsed < minElapsed {
		t.Fatalf("run_at wake came too early: %v (expected about %v)", elapsed, expected)
	}
	if elapsed > expected+2500*time.Millisecond {
		t.Fatalf("run_at wake came too late: %v (expected about %v)", elapsed, expected)
	}
	job.Ack()
}

func TestSchedulerAcceptsEverySecondExpression(t *testing.T) {
	extPath := findExtension(t)
	dbPath := filepath.Join(t.TempDir(), "t.db")
	db, err := Open(dbPath, extPath)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	sched := db.Scheduler()
	if err := sched.Add(ScheduledTask{
		Name:     "fast",
		Queue:    "beats",
		Schedule: "@every 1s",
		Payload: map[string]any{"ok": true},
	}); err != nil {
		t.Fatalf("add: %v", err)
	}

	soonest, err := sched.Soonest()
	if err != nil {
		t.Fatalf("soonest: %v", err)
	}
	if soonest == 0 {
		t.Fatal("expected non-zero soonest")
	}

	var rowsJSON string
	if err := db.Raw().QueryRow("SELECT honker_scheduler_tick(?)", soonest).Scan(&rowsJSON); err != nil {
		t.Fatalf("tick: %v", err)
	}
	var fires []ScheduledFire
	if err := json.Unmarshal([]byte(rowsJSON), &fires); err != nil {
		t.Fatalf("unmarshal fires: %v", err)
	}
	if len(fires) != 1 {
		t.Fatalf("expected 1 fire, got %d", len(fires))
	}
}

func TestAckBatch(t *testing.T) {
	extPath := findExtension(t)
	dbPath := filepath.Join(t.TempDir(), "t.db")
	db, err := Open(dbPath, extPath)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	q := db.Queue("batch", QueueOptions{})
	id1, _ := q.Enqueue(map[string]any{"i": 1}, EnqueueOptions{})
	id2, _ := q.Enqueue(map[string]any{"i": 2}, EnqueueOptions{})

	jobs, _ := q.ClaimBatch("w", 2)
	if len(jobs) != 2 {
		t.Fatalf("expected 2 jobs, got %d", len(jobs))
	}

	n, err := q.AckBatch([]int64{id1, id2}, "w")
	if err != nil {
		t.Fatalf("ack batch: %v", err)
	}
	if n != 2 {
		t.Fatalf("expected ack 2, got %d", n)
	}

	next, _ := q.ClaimOne("w")
	if next != nil {
		t.Fatal("expected empty queue")
	}
}

func TestSweepExpired(t *testing.T) {
	extPath := findExtension(t)
	dbPath := filepath.Join(t.TempDir(), "t.db")
	db, err := Open(dbPath, extPath)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	q := db.Queue("sweep", QueueOptions{})
	delay := int64(0)
	expires := int64(1)
	_, err = q.Enqueue(map[string]any{"x": 1}, EnqueueOptions{Delay: &delay, Expires: &expires})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	time.Sleep(2 * time.Second)

	n, err := q.SweepExpired()
	if err != nil {
		t.Fatalf("sweep expired: %v", err)
	}
	if n != 1 {
		t.Fatalf("expected 1 swept, got %d", n)
	}
}

func TestNotifyTx(t *testing.T) {
	extPath := findExtension(t)
	dbPath := filepath.Join(t.TempDir(), "t.db")
	db, err := Open(dbPath, extPath)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	id, err := db.NotifyTx(tx, "ch", map[string]any{"x": 1})
	if err != nil {
		t.Fatalf("notify tx: %v", err)
	}
	if id <= 0 {
		t.Fatal("expected positive id")
	}

	var count int
	row := db.Raw().QueryRow("SELECT COUNT(*) FROM _honker_notifications WHERE id = ?", id)
	if err := row.Scan(&count); err != nil {
		t.Fatalf("count before commit: %v", err)
	}
	if count != 0 {
		t.Fatalf("expected 0 visible before commit, got %d", count)
	}

	if err := tx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}

	row = db.Raw().QueryRow("SELECT COUNT(*) FROM _honker_notifications WHERE id = ?", id)
	if err := row.Scan(&count); err != nil {
		t.Fatalf("count after commit: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected 1 visible after commit, got %d", count)
	}
}
