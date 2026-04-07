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
		//kgo.ProducerBatchCompression(kgo.NoCompression()),
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
	records := make([]*kgo.Record, 100)
	for idx := 0; idx < 100; idx++ {
		record := &kgo.Record{
			Topic: "orders",
			Key:   []byte(fmt.Sprintf("%d", idx)),
			Value: []byte(`{"order_id": "12345", "product": "laptop", "quantity": 1}`),
		}
		records[idx] = record
	}

	results := client.ProduceSync(ctx, records...)
	for _, pr := range results {
		if pr.Err != nil {
			fmt.Printf("FAIL  produce error=%v\n", pr.Err)
		} else {
			fmt.Printf("OK    produced to topic=%s partition=%d offset=%d\n",
				pr.Record.Topic, pr.Record.Partition, pr.Record.Offset)
		}
	}

	consumer, err := kgo.NewClient(
		kgo.SeedBrokers("localhost:9092"),
		kgo.WithLogger(kgo.BasicLogger(os.Stderr, kgo.LogLevelDebug, nil)),
		kgo.ConsumerGroup("orders-group"),                 // ← triggers FindCoordinator
		kgo.Balancers(kgo.RoundRobinBalancer()),           // ← default
		kgo.ConsumeTopics("orders"),                       // ← subscribe
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()), // ← from offset 0
	)
	if err != nil {
		log.Fatal(err)
	}
	defer consumer.Close()
	//Fetch data from topic
	fetchCtx, fetchCancel := context.WithTimeout(context.Background(), 1000*time.Second)
	defer fetchCancel()

	target := 100
	received := 0
	for received < target {
		fetches := consumer.PollFetches(fetchCtx)

		if fetches.IsClientClosed() {
			log.Println("client closed while fetching")
			break
		}
		if err := fetchCtx.Err(); err != nil {
			log.Printf("fetch timeout: %v", err)
			break
		}

		fetches.EachError(func(topic string, partition int32, err error) {
			log.Printf("fetch error topic=%s partition=%d err=%v", topic, partition, err)
		})

		fetches.EachRecord(func(r *kgo.Record) {
			fmt.Printf("FETCH topic=%s partition=%d offset=%d key=%s value=%s\n",
				r.Topic, r.Partition, r.Offset, string(r.Key), string(r.Value))
			received++
		})
	}

	fmt.Printf("done fetching: %d records\n", received)
}
