// Atomic business-write + enqueue in one transaction.
//
// The typed honker.Queue.Enqueue API goes through the sql.DB pool
// rather than an explicit *sql.Tx, so for atomic-with-your-write
// we drop to raw SQL + database/sql's *sql.Tx, which pins a single
// underlying connection for the whole transaction. Adding a typed
// tx-aware Enqueue is on the roadmap.
//
//   go run -tags sqlite_load_extension examples/atomic/main.go
package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime"

	honker "github.com/russellromney/honker-go"
)

func findExtension() string {
	_, thisFile, _, _ := runtime.Caller(0)
	// .../packages/honker-go/examples/atomic/main.go
	repo := filepath.Clean(filepath.Join(filepath.Dir(thisFile), "..", "..", "..", ".."))
	for _, rel := range []string{
		"target/release/libhonker_ext.dylib",
		"target/release/libhonker_ext.so",
	} {
		p := filepath.Join(repo, rel)
		if _, err := os.Stat(p); err == nil {
			return p
		}
	}
	return ""
}

func main() {
	ext := findExtension()
	if ext == "" {
		log.Fatal("extension not built — run `cargo build -p honker-extension --release`")
	}
	tmp, err := os.MkdirTemp("", "honker-")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(tmp)

	db, err := honker.Open(filepath.Join(tmp, "app.db"), ext)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Plain business table.
	if _, err := db.Raw().Exec(
		"CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER, total INTEGER)",
	); err != nil {
		log.Fatal(err)
	}

	// Success path — INSERT + enqueue in one tx.
	{
		tx, err := db.Raw().Begin()
		if err != nil {
			log.Fatal(err)
		}
		if _, err := tx.Exec(
			"INSERT INTO orders (user_id, total) VALUES (?, ?)", 42, 9900,
		); err != nil {
			log.Fatal(err)
		}
		payload, _ := json.Marshal(map[string]any{"to": "alice@example.com", "order_id": 42})
		var id int64
		if err := tx.QueryRow(
			"SELECT honker_enqueue(?, ?, ?, ?, ?, ?, ?)",
			"emails", string(payload), nil, nil, 0, 3, nil,
		).Scan(&id); err != nil {
			log.Fatal(err)
		}
		if err := tx.Commit(); err != nil {
			log.Fatal(err)
		}
	}

	orders := countRows(db, "SELECT COUNT(*) FROM orders")
	queued := countRows(db, "SELECT COUNT(*) FROM _honker_live WHERE queue='emails'")
	fmt.Printf("committed: %d order(s), %d job(s)\n", orders, queued)
	if orders != 1 || queued != 1 {
		log.Fatalf("expected 1/1, got %d/%d", orders, queued)
	}

	// Rollback path.
	{
		tx, err := db.Raw().Begin()
		if err != nil {
			log.Fatal(err)
		}
		if _, err := tx.Exec(
			"INSERT INTO orders (user_id, total) VALUES (?, ?)", 43, 5000,
		); err != nil {
			log.Fatal(err)
		}
		payload, _ := json.Marshal(map[string]any{"to": "bob@example.com", "order_id": 43})
		var id int64
		if err := tx.QueryRow(
			"SELECT honker_enqueue(?, ?, ?, ?, ?, ?, ?)",
			"emails", string(payload), nil, nil, 0, 3, nil,
		).Scan(&id); err != nil {
			log.Fatal(err)
		}
		// Simulate business logic failure.
		if err := tx.Rollback(); err != nil {
			log.Fatal(err)
		}
		fmt.Println("rolled back: simulated payment-processing failure")
	}

	orders = countRows(db, "SELECT COUNT(*) FROM orders")
	queued = countRows(db, "SELECT COUNT(*) FROM _honker_live WHERE queue='emails'")
	fmt.Printf("after rollback: %d order(s), %d job(s)\n", orders, queued)
	if orders != 1 || queued != 1 {
		log.Fatalf("expected 1/1, got %d/%d", orders, queued)
	}
	fmt.Println("atomic enqueue + rollback both work as expected")
}

func countRows(db *honker.Database, sql string) int64 {
	var n int64
	if err := db.Raw().QueryRow(sql).Scan(&n); err != nil {
		log.Fatal(err)
	}
	return n
}
