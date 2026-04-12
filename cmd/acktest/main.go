// cmd/acktest/main.go — exercise produce + fetch under acks=0, acks=1, acks=-1.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

func main() {
	brokersFlag := flag.String("brokers", "localhost:9092,localhost:9093", "comma-separated seed brokers")
	acksFlag := flag.Int("acks", 1, "0, 1, or -1")
	topicFlag := flag.String("topic", "ack-test", "topic name")
	count := flag.Int("count", 50, "number of records to produce")
	flag.Parse()
	seeds := strings.Split(*brokersFlag, ",")

	var requiredAcks kgo.Acks
	switch *acksFlag {
	case 0:
		requiredAcks = kgo.NoAck()
	case 1:
		requiredAcks = kgo.LeaderAck()
	case -1:
		requiredAcks = kgo.AllISRAcks()
	default:
		log.Fatalf("invalid acks=%d (need 0, 1, or -1)", *acksFlag)
	}

	// Admin client uses default settings (only used for CreateTopics).
	admClient, err := kgo.NewClient(kgo.SeedBrokers(seeds...))
	if err != nil {
		log.Fatal(err)
	}
	adm := kadm.NewClient(admClient)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if _, err = adm.CreateTopics(ctx, 3, 2, nil, *topicFlag); err != nil {
		// ignore "already exists"
		fmt.Fprintf(os.Stderr, "CreateTopics warning: %v\n", err)
	}
	adm.Close()
	admClient.Close()

	// Producer with the selected acks setting.
	producer, err := kgo.NewClient(
		kgo.SeedBrokers(seeds...),
		kgo.RequiredAcks(requiredAcks),
		kgo.DisableIdempotentWrite(),
		kgo.RecordRetries(2),
		kgo.ProduceRequestTimeout(5*time.Second),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer producer.Close()

	records := make([]*kgo.Record, *count)
	for i := 0; i < *count; i++ {
		records[i] = &kgo.Record{
			Topic: *topicFlag,
			Key:   []byte(fmt.Sprintf("k-%d", i)),
			Value: []byte(fmt.Sprintf("v-%d", i)),
		}
	}

	produceCtx, produceCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer produceCancel()

	t0 := time.Now()
	results := producer.ProduceSync(produceCtx, records...)
	elapsed := time.Since(t0)

	produced, failed := 0, 0
	for _, r := range results {
		if r.Err != nil {
			failed++
		} else {
			produced++
		}
	}
	fmt.Printf("acks=%d  produced=%d  failed=%d  elapsed=%v\n", *acksFlag, produced, failed, elapsed)

	if *acksFlag == 0 {
		// acks=0 has no response, so fire-and-forget; give broker a moment to write.
		time.Sleep(500 * time.Millisecond)
	}

	// Verify by consuming.
	consumer, err := kgo.NewClient(
		kgo.SeedBrokers(seeds...),
		kgo.ConsumeTopics(*topicFlag),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer consumer.Close()

	fetchCtx, fetchCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer fetchCancel()

	received := 0
	deadline := time.Now().Add(10 * time.Second)
	for received < *count && time.Now().Before(deadline) {
		fetches := consumer.PollFetches(fetchCtx)
		if fetches.IsClientClosed() {
			break
		}
		fetches.EachError(func(t string, p int32, err error) {
			fmt.Fprintf(os.Stderr, "fetch error topic=%s partition=%d err=%v\n", t, p, err)
		})
		fetches.EachRecord(func(r *kgo.Record) {
			received++
		})
	}
	fmt.Printf("acks=%d  consumed=%d (target %d)\n", *acksFlag, received, *count)
}
