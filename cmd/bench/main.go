// cmd/bench/main.go
// End-to-end benchmark with detailed metrics and multiple test scenarios.
//
// Single run (existing behaviour):
//
//	go run ./cmd/bench -msgs 5000 -size 4096 -workers 4
//
// Full scenario suite:
//
//	go run ./cmd/bench -suite -brokers localhost:9092,localhost:9093,localhost:9094 -partitions 3
package main

import (
	"context"
	"flag"
	"fmt"
	"math"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// ─── scenario definition ──────────────────────────────────────────────────────

type Scenario struct {
	Name        string
	Msgs        int
	Size        int // bytes
	Workers     int
	Partitions  int
	Replication int
}

// predefined suite; brokers are injected at runtime.
func defaultSuite(partitions, replication int) []Scenario {
	return []Scenario{
		{Name: "latency      ", Msgs: 2_000, Size: 256, Workers: 1, Partitions: partitions, Replication: replication},
		{Name: "thru-small   ", Msgs: 5_000, Size: 256, Workers: 4, Partitions: partitions, Replication: replication},
		{Name: "thru-medium  ", Msgs: 5_000, Size: 4_096, Workers: 4, Partitions: partitions, Replication: replication},
		{Name: "thru-large   ", Msgs: 500, Size: 65_536, Workers: 2, Partitions: partitions, Replication: replication},
		{Name: "scale-1w     ", Msgs: 3_000, Size: 1_024, Workers: 1, Partitions: partitions, Replication: replication},
		{Name: "scale-2w     ", Msgs: 3_000, Size: 1_024, Workers: 2, Partitions: partitions, Replication: replication},
		{Name: "scale-4w     ", Msgs: 3_000, Size: 1_024, Workers: 4, Partitions: partitions, Replication: replication},
		{Name: "scale-8w     ", Msgs: 3_000, Size: 1_024, Workers: 8, Partitions: partitions, Replication: replication},
	}
}

// ─── result types ─────────────────────────────────────────────────────────────

type PartStat struct {
	Leader      int32
	Count       int64
	StartOffset int64
}

type ProduceResult struct {
	Total     int64
	Errors    int64
	Elapsed   time.Duration
	Latencies []int64 // nanoseconds, unsorted
	PartStats map[int32]*PartStat
}

type ConsumeResult struct {
	Received int64
	Expected int64
	Elapsed  time.Duration
	Stalled  bool
}

type BenchResult struct {
	Scenario Scenario
	Topic    string
	Produce  ProduceResult
	Consume  ConsumeResult
}

// ─── main ─────────────────────────────────────────────────────────────────────

func main() {
	brokersFlag := flag.String("brokers", "localhost:9092", "comma-separated seed brokers")
	topicFlag := flag.String("topic", "bench", "base topic name")
	msgsFlag := flag.Int("msgs", 5_000, "messages per run (single mode)")
	sizeFlag := flag.Int("size", 1_024, "message size in bytes (single mode)")
	workersFlag := flag.Int("workers", 4, "producer goroutines (single mode)")
	partFlag := flag.Int("partitions", 1, "topic partitions")
	replFlag := flag.Int("replication", 1, "replication factor")
	suiteFlag := flag.Bool("suite", false, "run full scenario suite")
	flag.Parse()

	seeds := splitTrim(*brokersFlag)
	fmt.Printf("brokers : %v\n", seeds)
	fmt.Printf("topic   : %s\n\n", *topicFlag)

	var scenarios []Scenario
	if *suiteFlag {
		scenarios = defaultSuite(*partFlag, *replFlag)
	} else {
		scenarios = []Scenario{{
			Name:        "single",
			Msgs:        *msgsFlag,
			Size:        *sizeFlag,
			Workers:     *workersFlag,
			Partitions:  *partFlag,
			Replication: *replFlag,
		}}
	}

	var results []BenchResult
	for _, s := range scenarios {
		topicName := fmt.Sprintf("%s-%s", *topicFlag, strings.TrimSpace(s.Name))
		r := runScenario(seeds, topicName, s)
		printScenarioResult(r)
		results = append(results, r)
	}

	if len(results) > 1 {
		printSummaryTable(results)
	}
}

// ─── scenario runner ──────────────────────────────────────────────────────────

func runScenario(seeds []string, topicName string, s Scenario) BenchResult {
	// Create topic (ignore already-exists).
	partLeaders := setupTopic(seeds, topicName, int32(s.Partitions), int16(s.Replication))

	// Produce.
	pr := runProduce(seeds, topicName, s)

	// Annotate partition stats with leader info.
	for p, ps := range pr.PartStats {
		if leader, ok := partLeaders[p]; ok {
			ps.Leader = leader
		}
	}

	// Consume.
	startOffsets := make(map[int32]kgo.Offset, len(pr.PartStats))
	for p, ps := range pr.PartStats {
		startOffsets[p] = kgo.NewOffset().At(ps.StartOffset)
	}
	cr := runConsume(seeds, topicName, startOffsets, pr.Total)

	return BenchResult{Scenario: s, Topic: topicName, Produce: pr, Consume: cr}
}

func setupTopic(seeds []string, topic string, partitions int32, replication int16) map[int32]int32 {
	adm := kadm.NewClient(mustClient(seeds))
	defer adm.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	adm.CreateTopics(ctx, partitions, replication, nil, topic) //nolint — ignore already-exists
	cancel()

	// Fetch partition→leader mapping.
	dctx, dcancel := context.WithTimeout(context.Background(), 5*time.Second)
	details, err := adm.ListTopics(dctx, topic)
	dcancel()

	leaders := make(map[int32]int32)
	if err == nil {
		for _, td := range details {
			for _, p := range td.Partitions.Sorted() {
				leaders[p.Partition] = p.Leader
			}
		}
	}
	return leaders
}

// ─── produce ─────────────────────────────────────────────────────────────────

func runProduce(seeds []string, topic string, s Scenario) ProduceResult {
	client := mustClient(seeds, kgo.DefaultProduceTopic(topic))
	defer client.Close()

	payload := make([]byte, s.Size)
	for i := range payload {
		payload[i] = 'x'
	}

	var (
		produced  atomic.Int64
		errCount  atomic.Int64
		latMu     sync.Mutex
		latencies = make([]int64, 0, s.Msgs)
		psMu      sync.Mutex
		partStats = make(map[int32]*PartStat)
		wg        sync.WaitGroup
	)

	perWorker := s.Msgs / s.Workers
	remainder := s.Msgs - perWorker*s.Workers

	start := time.Now()
	for w := 0; w < s.Workers; w++ {
		count := perWorker
		if w == 0 {
			count += remainder
		}
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			lats := make([]int64, 0, n)
			for i := 0; i < n; i++ {
				t0 := time.Now()
				res := client.ProduceSync(context.Background(), &kgo.Record{Value: payload})
				ns := time.Since(t0).Nanoseconds()
				if res[0].Err != nil {
					errCount.Add(1)
					continue
				}
				produced.Add(1)
				lats = append(lats, ns)

				r := res[0].Record
				psMu.Lock()
				if _, seen := partStats[r.Partition]; !seen {
					partStats[r.Partition] = &PartStat{StartOffset: r.Offset}
				}
				partStats[r.Partition].Count++
				psMu.Unlock()
			}
			latMu.Lock()
			latencies = append(latencies, lats...)
			latMu.Unlock()
		}(count)
	}
	wg.Wait()
	elapsed := time.Since(start)

	return ProduceResult{
		Total:     produced.Load(),
		Errors:    errCount.Load(),
		Elapsed:   elapsed,
		Latencies: latencies,
		PartStats: partStats,
	}
}

// ─── consume ─────────────────────────────────────────────────────────────────

const stallTimeout = 2 * time.Second

func runConsume(seeds []string, topic string, startOffsets map[int32]kgo.Offset, expected int64) ConsumeResult {
	partMap := map[string]map[int32]kgo.Offset{topic: startOffsets}
	client := mustClient(seeds, kgo.ConsumePartitions(partMap))
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	var received int64
	lastProgress := time.Now()
	var lastRecordAt time.Time
	stalled := false

	start := time.Now()
	for received < expected {
		pollCtx, pollCancel := context.WithTimeout(ctx, stallTimeout)
		fetches := client.PollFetches(pollCtx)
		pollCancel()

		if ctx.Err() != nil {
			break
		}

		before := received
		fetches.EachError(func(t string, p int32, err error) {
			if strings.Contains(err.Error(), "context deadline exceeded") {
				return
			}
			fmt.Fprintf(os.Stderr, "  fetch error topic=%s partition=%d: %v\n", t, p, err)
		})
		fetches.EachRecord(func(_ *kgo.Record) {
			if received < expected {
				received++
			}
		})

		if received > before {
			lastProgress = time.Now()
			lastRecordAt = lastProgress
		} else if time.Since(lastProgress) >= stallTimeout {
			stalled = true
			break
		}
	}

	elapsed := lastRecordAt.Sub(start)
	if lastRecordAt.IsZero() {
		elapsed = time.Since(start)
	}

	return ConsumeResult{
		Received: received,
		Expected: expected,
		Elapsed:  elapsed,
		Stalled:  stalled,
	}
}

// ─── reporting ────────────────────────────────────────────────────────────────

func printScenarioResult(r BenchResult) {
	s := r.Scenario
	pr := r.Produce
	cr := r.Consume

	sort.Slice(pr.Latencies, func(i, j int) bool { return pr.Latencies[i] < pr.Latencies[j] })

	sep := strings.Repeat("─", 62)
	fmt.Printf("\n%s\n", sep)
	fmt.Printf(" SCENARIO : %s\n", strings.TrimSpace(s.Name))
	fmt.Printf(" Config   : %d msgs × %d B  workers=%d  partitions=%d  replication=%d\n",
		s.Msgs, s.Size, s.Workers, s.Partitions, s.Replication)
	fmt.Printf(" Topic    : %s\n", r.Topic)
	fmt.Printf("%s\n", sep)

	// ── produce ──
	fmt.Println("\n PRODUCE")
	if pr.Elapsed.Seconds() > 0 {
		fmt.Printf("   throughput : %s msg/s   %.2f MB/s\n",
			commaf(float64(pr.Total)/pr.Elapsed.Seconds()),
			float64(pr.Total)*float64(s.Size)/pr.Elapsed.Seconds()/1e6)
	}
	fmt.Printf("   elapsed    : %v\n", pr.Elapsed.Round(time.Millisecond))
	fmt.Printf("   sent/error : %d / %d\n", pr.Total, pr.Errors)

	if len(pr.Latencies) > 0 {
		fmt.Println("   latency")
		fmt.Printf("     min  %-10v  p50  %-10v  p95  %-10v\n",
			dur(pr.Latencies[0]),
			dur(pct(pr.Latencies, 50)),
			dur(pct(pr.Latencies, 95)))
		fmt.Printf("     p99  %-10v  p999 %-10v  max  %v\n",
			dur(pct(pr.Latencies, 99)),
			dur(pct(pr.Latencies, 99.9)),
			dur(pr.Latencies[len(pr.Latencies)-1]))
		fmt.Println("   histogram")
		printLatHist(pr.Latencies)
	}

	// partition breakdown
	if len(pr.PartStats) > 0 {
		fmt.Println("   per-partition")
		parts := sortedParts(pr.PartStats)
		for _, p := range parts {
			ps := pr.PartStats[p]
			pct100 := float64(ps.Count) / float64(pr.Total) * 100
			fmt.Printf("     p%-2d  leader=broker-%-2d  records=%-6d (%.1f%%)  start-offset=%d\n",
				p, ps.Leader, ps.Count, pct100, ps.StartOffset)
		}
	}

	// ── consume ──
	fmt.Println("\n CONSUME")
	recvPct := 0.0
	if cr.Expected > 0 {
		recvPct = float64(cr.Received) / float64(cr.Expected) * 100
	}
	status := "OK"
	if cr.Stalled {
		status = "STALLED (broker fetch issue)"
	}
	fmt.Printf("   received   : %d / %d (%.1f%%)  %s\n",
		cr.Received, cr.Expected, recvPct, status)
	if cr.Elapsed > 0 && cr.Received > 0 {
		fmt.Printf("   throughput : %s msg/s   %.2f MB/s\n",
			commaf(float64(cr.Received)/cr.Elapsed.Seconds()),
			float64(cr.Received)*float64(s.Size)/cr.Elapsed.Seconds()/1e6)
		fmt.Printf("   elapsed    : %v\n", cr.Elapsed.Round(time.Millisecond))
	}
	fmt.Println()
}

func printSummaryTable(results []BenchResult) {
	fmt.Println("\n" + strings.Repeat("═", 100))
	fmt.Println(" SUMMARY")
	fmt.Println(strings.Repeat("═", 100))
	fmt.Printf(" %-14s  %4s %6s %4s  %10s %8s  %8s %8s %8s  %8s %6s\n",
		"scenario", "wkrs", "size", "part",
		"prod msg/s", "prod MB/s",
		"p50", "p99", "max",
		"cons msg/s", "recv%")
	fmt.Println(" " + strings.Repeat("─", 98))

	for _, r := range results {
		s := r.Scenario
		pr := r.Produce
		cr := r.Consume

		prodMsgS, prodMBs := 0.0, 0.0
		if pr.Elapsed.Seconds() > 0 {
			prodMsgS = float64(pr.Total) / pr.Elapsed.Seconds()
			prodMBs = prodMsgS * float64(s.Size) / 1e6
		}
		consMsgS := 0.0
		if cr.Elapsed.Seconds() > 0 && cr.Received > 0 {
			consMsgS = float64(cr.Received) / cr.Elapsed.Seconds()
		}
		recvPct := 0.0
		if cr.Expected > 0 {
			recvPct = float64(cr.Received) / float64(cr.Expected) * 100
		}
		p50 := dur(pct(pr.Latencies, 50))
		p99 := dur(pct(pr.Latencies, 99))
		max := time.Duration(0)
		if len(pr.Latencies) > 0 {
			max = dur(pr.Latencies[len(pr.Latencies)-1])
		}

		fmt.Printf(" %-14s  %4d %6s %4d  %10s %8.2f  %8v %8v %8v  %8s %5.1f%%\n",
			strings.TrimSpace(s.Name),
			s.Workers,
			fmtBytes(s.Size),
			s.Partitions,
			commaf(prodMsgS), prodMBs,
			p50, p99, max,
			commaf(consMsgS),
			recvPct)
	}
	fmt.Println(strings.Repeat("═", 100))
}

// ─── histogram ───────────────────────────────────────────────────────────────

var histBuckets = []struct {
	label string
	limit int64 // nanoseconds; 0 = catch-all
}{
	{"< 100µs  ", 100_000},
	{"< 500µs  ", 500_000},
	{"< 1ms    ", 1_000_000},
	{"< 5ms    ", 5_000_000},
	{"< 10ms   ", 10_000_000},
	{"≥ 10ms   ", 0},
}

func printLatHist(sorted []int64) {
	counts := make([]int, len(histBuckets))
	bi := 0
	for _, v := range sorted {
		for bi < len(histBuckets)-1 && histBuckets[bi].limit > 0 && v >= histBuckets[bi].limit {
			bi++
		}
		counts[bi]++
	}
	total := len(sorted)
	const barWidth = 30
	for i, b := range histBuckets {
		frac := float64(counts[i]) / float64(total)
		filled := int(math.Round(frac * barWidth))
		bar := strings.Repeat("█", filled) + strings.Repeat("░", barWidth-filled)
		fmt.Printf("     %s %s %5.1f%%  (%d)\n", b.label, bar, frac*100, counts[i])
	}
}

// ─── helpers ─────────────────────────────────────────────────────────────────

func mustClient(seeds []string, opts ...kgo.Opt) *kgo.Client {
	all := append([]kgo.Opt{kgo.SeedBrokers(seeds...)}, opts...)
	c, err := kgo.NewClient(all...)
	if err != nil {
		fmt.Fprintf(os.Stderr, "kgo.NewClient: %v\n", err)
		os.Exit(1)
	}
	return c
}

func splitTrim(s string) []string {
	parts := strings.Split(s, ",")
	for i, p := range parts {
		parts[i] = strings.TrimSpace(p)
	}
	return parts
}

func pct(sorted []int64, p float64) int64 {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(math.Ceil(p/100*float64(len(sorted)))) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

func dur(ns int64) time.Duration {
	return time.Duration(ns)
}

func fmtBytes(n int) string {
	switch {
	case n >= 1<<20:
		return fmt.Sprintf("%dMB", n>>20)
	case n >= 1<<10:
		return fmt.Sprintf("%dKB", n>>10)
	default:
		return fmt.Sprintf("%dB", n)
	}
}

// commaf formats a float with thousands separators, no decimals.
func commaf(f float64) string {
	s := fmt.Sprintf("%.0f", f)
	out := make([]byte, 0, len(s)+len(s)/3)
	for i, c := range s {
		if i > 0 && (len(s)-i)%3 == 0 {
			out = append(out, ',')
		}
		out = append(out, byte(c))
	}
	return string(out)
}

func sortedParts(m map[int32]*PartStat) []int32 {
	keys := make([]int32, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
	return keys
}
