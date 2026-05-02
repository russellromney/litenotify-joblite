// Basic example: enqueue a few jobs, claim them, ack each.
//
//	go run -tags sqlite_load_extension main.go
package main

import (
	"encoding/json"
	"fmt"
	"log"

	"github.com/russellromney/honker-go"
)

func main() {
	db, err := honker.Open("demo.db", "./libhonker_extension.dylib")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	q := db.Queue("emails", honker.QueueOptions{})

	for i := 0; i < 3; i++ {
		if _, err := q.Enqueue(
			map[string]any{"to": fmt.Sprintf("user-%d@example.com", i)},
			honker.EnqueueOptions{},
		); err != nil {
			log.Fatal(err)
		}
	}

	for {
		job, err := q.ClaimOne("worker-1")
		if err != nil {
			log.Fatal(err)
		}
		if job == nil {
			break
		}
		var body map[string]any
		_ = json.Unmarshal(job.Payload, &body)
		fmt.Printf("processing job %d: to=%s\n", job.ID, body["to"])
		if _, err := job.Ack(); err != nil {
			log.Fatal(err)
		}
	}
}
