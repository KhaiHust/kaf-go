// cmd/client/main.go
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

func main() {
	client, err := kgo.NewClient(
		kgo.SeedBrokers("localhost:9092"),
		kgo.WithLogger(kgo.BasicLogger(os.Stderr, kgo.LogLevelDebug, nil)),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	adm := kadm.NewClient(client)
	defer adm.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Second)
	defer cancel()

	resp, err := adm.CreateTopics(ctx, 3, 1, nil, "orders")
	if err != nil {
		log.Fatalf("CreateTopics: %v", err)
	}

	for _, t := range resp.Sorted() {
		if t.Err != nil {
			fmt.Printf("FAIL  topic=%s  error=%v\n", t.Topic, t.Err)
		} else {
			fmt.Printf("OK    topic=%s  id=%s\n", t.Topic, t.ID)
		}
	}

	// Send messages to the topic
	record := &kgo.Record{
		Topic: "orders",
		Key:   []byte("order-0"),
		Value: []byte(`{"order_id": "12345", "product": "laptop", "quantity": 1}`),
	}

	results := client.ProduceSync(ctx, record)
	for _, pr := range results {
		if pr.Err != nil {
			fmt.Printf("FAIL  produce error=%v\n", pr.Err)
		} else {
			fmt.Printf("OK    produced to topic=%s partition=%d offset=%d\n",
				pr.Record.Topic, pr.Record.Partition, pr.Record.Offset)
		}
	}
}
