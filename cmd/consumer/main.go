// cmd/consumer/main.go
// Runs N consumers in the same group concurrently to test group rebalancing.
// Usage: go run ./cmd/consumer [--consumers 3] [--topic orders] [--group orders-group]
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

func main() {
	numConsumers := flag.Int("consumers", 3, "number of concurrent consumers")
	topic := flag.String("topic", "orders", "topic to consume")
	group := flag.String("group", "orders-group", "consumer group id")
	flag.Parse()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Cancel on SIGINT/SIGTERM so all consumers shut down cleanly.
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sig
		fmt.Println("\nshutting down...")
		cancel()
	}()

	var wg sync.WaitGroup
	for i := 0; i < *numConsumers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			runConsumer(ctx, id, *topic, *group)
		}(i)
		// Stagger starts slightly so join-group log lines don't interleave.
		time.Sleep(200 * time.Millisecond)
	}

	wg.Wait()
	fmt.Println("all consumers stopped")
}

func runConsumer(ctx context.Context, id int, topic, group string) {
	prefix := fmt.Sprintf("[consumer-%d]", id)

	client, err := kgo.NewClient(
		kgo.SeedBrokers("localhost:9092"),
		kgo.WithLogger(kgo.BasicLogger(os.Stderr, kgo.LogLevelInfo, func() string {
			return prefix + " "
		})),
		kgo.ConsumerGroup(group),
		kgo.Balancers(kgo.RoundRobinBalancer()),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		log.Printf("%s new client error: %v", prefix, err)
		return
	}
	defer client.Close()

	log.Printf("%s started, group=%s topic=%s", prefix, group, topic)

	for {
		fetches := client.PollFetches(ctx)

		if fetches.IsClientClosed() {
			log.Printf("%s client closed", prefix)
			return
		}
		if ctx.Err() != nil {
			return
		}

		fetches.EachError(func(t string, partition int32, err error) {
			log.Printf("%s fetch error topic=%s partition=%d: %v", prefix, t, partition, err)
		})

		fetches.EachRecord(func(r *kgo.Record) {
			fmt.Printf("%s topic=%s partition=%d offset=%d key=%s value=%s\n",
				prefix, r.Topic, r.Partition, r.Offset, string(r.Key), string(r.Value))
		})
	}
}
