// cmd/client/main.go
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

func main() {
	client, err := kgo.NewClient(
		kgo.SeedBrokers("localhost:9092"),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	adm := kadm.NewClient(client)
	defer adm.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	resp, err := adm.CreateTopics(ctx, 3, 1, nil, "orders")
	if err != nil {
		log.Fatalf("CreateTopics: %v", err)
	}

	for _, t := range resp.Sorted() {
		if t.Err != nil {
			fmt.Printf("FAIL  topic=%s  error=%v\n", t.Topic, t.Err)
		} else {
			fmt.Printf("OK    topic=%s  id=%s\n", t.Topic, t.Topic)
		}
	}
}
