// loadgen — franz-go produce/consume workload generator.
//
// Targets either kaf-go or Apache Kafka. Same binary, different --brokers list.
// Emits Prometheus metrics on --metrics-port for client-side latency / throughput.
package main

import (
	"context"
	"crypto/rand"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Latency buckets: 100 µs → 10 s, log-spaced enough for produce hot path.
var latencyBuckets = []float64{
	100e-6, 250e-6, 500e-6,
	1e-3, 2.5e-3, 5e-3, 10e-3, 25e-3,
	50e-3, 100e-3, 250e-3, 500e-3,
	1.0, 2.5, 5.0, 10.0,
}

var (
	sentRecords = promauto.NewCounter(prometheus.CounterOpts{
		Name: "loadgen_sent_records_total",
		Help: "Records successfully acknowledged by the broker.",
	})
	failedRecords = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "loadgen_failed_records_total",
		Help: "Records that failed to produce, by error class.",
	}, []string{"error"})
	sentBytes = promauto.NewCounter(prometheus.CounterOpts{
		Name: "loadgen_sent_bytes_total",
		Help: "Bytes (record values) successfully acknowledged.",
	})
	clientLatency = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "loadgen_produce_latency_seconds",
		Help:    "End-to-end produce latency observed by the client (Produce → ack callback).",
		Buckets: latencyBuckets,
	}, []string{"acks"})
	inFlight = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "loadgen_inflight_records",
		Help: "Records produced but not yet acknowledged.",
	})
)

// phaseStats holds local atomic counters so we can print a summary at exit
// without re-walking the Prometheus registry.
type phaseStats struct {
	sent     int64
	failed   int64
	bytes    int64
	mu       sync.Mutex
	errClass map[string]int64
	firstErr string
}

func (s *phaseStats) recordErr(class string, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.errClass == nil {
		s.errClass = make(map[string]int64)
	}
	s.errClass[class]++
	if s.firstErr == "" && err != nil {
		s.firstErr = err.Error()
	}
}

func main() {
	var (
		brokers     = flag.String("brokers", "kaf1:9092,kaf2:9092,kaf3:9092", "comma-separated broker list")
		topic       = flag.String("topic", "loadtest", "target topic (must exist; create with cmd/client first)")
		workers     = flag.Int("workers", 8, "concurrent producer goroutines")
		recordSize  = flag.Int("record-size", 1024, "record value size in bytes")
		acks        = flag.Int("acks", -1, "0 | 1 | -1 (all)")
		idempotent  = flag.Bool("idempotent", true, "enable idempotent producer (PID + sequence dedup)")
		duration    = flag.Duration("duration", 60*time.Second, "steady-state duration")
		warmup      = flag.Duration("warmup", 10*time.Second, "warm-up duration (metrics suppressed)")
		batchBytes  = flag.Int("batch-bytes", 1<<20, "kgo.ProducerBatchMaxBytes")
		bufRecords  = flag.Int("buffered-records", 100_000, "kgo.MaxBufferedRecords")
		metricsPort = flag.Int("metrics-port", 8090, "Prometheus /metrics port")
		createTopic = flag.Bool("create-topic", true, "create the target topic if it does not exist (idempotent)")
		partitions  = flag.Int("partitions", 3, "partition count when creating topic")
		replication = flag.Int("replication-factor", 3, "replication factor when creating topic")
	)
	flag.Parse()

	startMetricsServer(*metricsPort)

	cl, err := newClient(*brokers, *topic, *acks, *idempotent, *batchBytes, *bufRecords)
	if err != nil {
		log.Fatalf("client init: %v", err)
	}
	defer cl.Close()

	if *createTopic {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		if err := ensureTopic(ctx, cl, *topic, int32(*partitions), int16(*replication)); err != nil {
			cancel()
			log.Fatalf("create topic: %v", err)
		}
		cancel()
	}

	payload := make([]byte, *recordSize)
	if _, err := rand.Read(payload); err != nil {
		log.Fatalf("rand payload: %v", err)
	}

	rootCtx, cancel := context.WithCancel(context.Background())
	go func() {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
		<-sigCh
		log.Printf("loadgen: signal received, cancelling")
		cancel()
	}()

	acksLabel := fmt.Sprintf("%d", *acks)

	if *warmup > 0 {
		log.Printf("loadgen: warm-up %s", *warmup)
		runPhase(rootCtx, cl, payload, *workers, *warmup, false, acksLabel, nil)
	}

	log.Printf("loadgen: steady-state %s × %d workers, record-size=%dB, acks=%s, idempotent=%v",
		*duration, *workers, *recordSize, acksLabel, *idempotent)
	stats := &phaseStats{}
	start := time.Now()
	runPhase(rootCtx, cl, payload, *workers, *duration, true, acksLabel, stats)
	elapsed := time.Since(start)

	if err := cl.Flush(context.Background()); err != nil {
		log.Printf("loadgen: flush error: %v", err)
	}

	report(elapsed, *recordSize, stats)
}

func newClient(brokers, topic string, acks int, idempotent bool, batchBytes, bufRecords int) (*kgo.Client, error) {
	opts := []kgo.Opt{
		kgo.SeedBrokers(strings.Split(brokers, ",")...),
		kgo.DefaultProduceTopic(topic),
		kgo.ProducerBatchMaxBytes(int32(batchBytes)),
		kgo.MaxBufferedRecords(bufRecords),
		kgo.RecordPartitioner(kgo.StickyKeyPartitioner(nil)),
	}
	switch acks {
	case 0:
		opts = append(opts, kgo.RequiredAcks(kgo.NoAck()), kgo.DisableIdempotentWrite())
	case 1:
		opts = append(opts, kgo.RequiredAcks(kgo.LeaderAck()), kgo.DisableIdempotentWrite())
	case -1:
		opts = append(opts, kgo.RequiredAcks(kgo.AllISRAcks()))
		if !idempotent {
			opts = append(opts, kgo.DisableIdempotentWrite())
		}
	default:
		return nil, fmt.Errorf("invalid acks %d (want 0, 1, -1)", acks)
	}
	return kgo.NewClient(opts...)
}

func runPhase(parent context.Context, cl *kgo.Client, payload []byte, workers int, dur time.Duration, record bool, acksLabel string, stats *phaseStats) {
	ctx, cancel := context.WithTimeout(parent, dur)
	defer cancel()

	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for ctx.Err() == nil {
				start := time.Now()
				inFlight.Inc()
				cl.Produce(ctx, &kgo.Record{Value: payload}, func(r *kgo.Record, err error) {
					inFlight.Dec()
					if !record {
						return
					}
					if err != nil {
						class := classifyErr(err)
						failedRecords.WithLabelValues(class).Inc()
						if stats != nil {
							atomic.AddInt64(&stats.failed, 1)
							stats.recordErr(class, err)
						}
						return
					}
					clientLatency.WithLabelValues(acksLabel).Observe(time.Since(start).Seconds())
					sentRecords.Inc()
					sentBytes.Add(float64(len(r.Value)))
					if stats != nil {
						atomic.AddInt64(&stats.sent, 1)
						atomic.AddInt64(&stats.bytes, int64(len(r.Value)))
					}
				})
			}
		}()
	}
	wg.Wait()
}

func classifyErr(err error) string {
	if err == nil {
		return "none"
	}
	s := err.Error()
	switch {
	case strings.Contains(s, "context"):
		return "context_cancelled"
	case strings.Contains(s, "timed out"):
		return "timeout"
	case strings.Contains(s, "NOT_ENOUGH_REPLICAS_AFTER_APPEND"):
		return "not_enough_replicas_after_append"
	case strings.Contains(s, "NOT_ENOUGH_REPLICAS"):
		return "not_enough_replicas"
	case strings.Contains(s, "NOT_LEADER"):
		return "not_leader"
	default:
		return "other"
	}
}

func ensureTopic(ctx context.Context, cl *kgo.Client, topic string, partitions int32, replicationFactor int16) error {
	adm := kadm.NewClient(cl)
	resp, err := adm.CreateTopic(ctx, partitions, replicationFactor, nil, topic)
	if err != nil {
		return fmt.Errorf("CreateTopic RPC: %w", err)
	}
	if resp.Err != nil && !errors.Is(resp.Err, kerr.TopicAlreadyExists) {
		return fmt.Errorf("CreateTopic %q: %w", topic, resp.Err)
	}
	if errors.Is(resp.Err, kerr.TopicAlreadyExists) {
		log.Printf("loadgen: topic %q already exists", topic)
	} else {
		log.Printf("loadgen: created topic %q (partitions=%d, RF=%d)", topic, partitions, replicationFactor)
	}
	return nil
}

func startMetricsServer(port int) {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("ok\n"))
	})
	addr := fmt.Sprintf(":%d", port)
	go func() {
		log.Printf("loadgen: metrics on %s/metrics", addr)
		if err := http.ListenAndServe(addr, mux); err != nil {
			log.Printf("metrics server: %v", err)
		}
	}()
}

func report(elapsed time.Duration, recordSize int, s *phaseStats) {
	sent := atomic.LoadInt64(&s.sent)
	failed := atomic.LoadInt64(&s.failed)
	bytes := atomic.LoadInt64(&s.bytes)
	secs := elapsed.Seconds()
	if secs <= 0 {
		secs = 1
	}
	fmt.Printf(`
═══ loadgen summary ═══
duration         %s
records sent     %d
records failed   %d
bytes sent       %d
throughput       %.0f msg/s   %.2f MiB/s
record size      %d B
`, elapsed.Round(time.Millisecond), sent, failed, bytes,
		float64(sent)/secs, float64(bytes)/(1024*1024)/secs, recordSize)
	s.mu.Lock()
	if len(s.errClass) > 0 {
		fmt.Println("failure breakdown:")
		for k, v := range s.errClass {
			fmt.Printf("  %-30s %d\n", k, v)
		}
		if s.firstErr != "" {
			fmt.Printf("first error      %s\n", s.firstErr)
		}
	}
	s.mu.Unlock()
}
