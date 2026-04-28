// Package metrics centralises every Prometheus metric exported by the broker.
// Instrumentation sites import this package and call the exported helpers; nobody
// else should hold prometheus.* objects directly.
package metrics

import (
	"strconv"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Histogram bucket layout shared by produce, fetch, and purgatory wait
// (per docs/load_test_plan.md §4.2). Tail extends to 10s so we can spot
// the rare acks=-1 timeout.
var latencyBuckets = []float64{
	100e-6, 250e-6, 500e-6,
	1e-3, 2.5e-3, 5e-3, 10e-3, 25e-3,
	50e-3, 100e-3, 250e-3, 500e-3, 1.0,
	2.5, 5.0, 10.0,
}

// Counters.
var (
	ProduceRequests = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "kaf_produce_requests_total",
		Help: "Produce requests received, by acks setting, topic, and outcome",
	}, []string{"acks", "topic", "error_code"})

	ProduceRecords = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "kaf_produce_records_total",
		Help: "Records appended (post-dedup) by topic",
	}, []string{"topic"})

	ProduceBytes = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "kaf_produce_bytes_total",
		Help: "Record bytes appended (post-dedup) by topic",
	}, []string{"topic"})

	FetchRequests = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "kaf_fetch_requests_total",
		Help: "Fetch requests received, by topic and caller role (consumer|replica)",
	}, []string{"topic", "role"})

	FetchBytes = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "kaf_fetch_bytes_total",
		Help: "Bytes returned to fetchers, by topic and caller role",
	}, []string{"topic", "role"})

	ISRShrink = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "kaf_isr_shrink_total",
		Help: "ISR shrink events (replica dropped from ISR)",
	}, []string{"topic", "partition"})

	ISRExpand = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "kaf_isr_expand_total",
		Help: "ISR expand events (replica re-added to ISR)",
	}, []string{"topic", "partition"})

	IdempotenceDup = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "kaf_idempotence_dup_total",
		Help: "Idempotent producer retries that hit the dedup ring (no re-append)",
	}, []string{"topic", "partition"})

	IdempotenceValidateErrors = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "kaf_idempotence_validate_errors_total",
		Help: "Idempotence Validate errors by Kafka error code (45/46/59/90)",
	}, []string{"error_code"})

	SnapshotWrites = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "kaf_producer_snapshot_writes_total",
		Help: "Producer-state snapshot writes triggered by segment roll",
	}, []string{"topic", "partition"})

	PIDBlocksLeased = promauto.NewCounter(prometheus.CounterOpts{
		Name: "kaf_pid_blocks_leased_total",
		Help: "Producer-id blocks leased from the metadata log",
	})

	PurgatoryFailAll = promauto.NewCounter(prometheus.CounterOpts{
		Name: "kaf_purgatory_fail_all_total",
		Help: "Purgatory FailAll firings (e.g. NOT_ENOUGH_REPLICAS_AFTER_APPEND)",
	})
)

// Histograms.
var (
	ProduceLatency = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "kaf_produce_latency_seconds",
		Help:    "Server-side produce request latency (entry to response written)",
		Buckets: latencyBuckets,
	}, []string{"acks", "topic"})

	FetchLatency = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "kaf_fetch_latency_seconds",
		Help:    "Server-side fetch request latency, by caller role",
		Buckets: latencyBuckets,
	}, []string{"role"})

	PurgatoryWait = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "kaf_purgatory_wait_seconds",
		Help:    "Time a parked acks=-1 waiter spent in PartitionPurgatory before resolution",
		Buckets: latencyBuckets,
	})
)

// Gauges.
var (
	PartitionLEO = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "kaf_partition_leo",
		Help: "Leader log-end offset (next write offset)",
	}, []string{"topic", "partition"})

	PartitionHWM = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "kaf_partition_hwm",
		Help: "Partition high-watermark (first uncommitted offset)",
	}, []string{"topic", "partition"})

	PartitionISRSize = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "kaf_partition_isr_size",
		Help: "Number of replicas currently in ISR",
	}, []string{"topic", "partition"})

	PartitionReplicaLag = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "kaf_partition_replica_lag_offsets",
		Help: "LEO - ReplicaLEO[r] for each replica (leader-side view)",
	}, []string{"topic", "partition", "replica"})

	PurgatoryDepth = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "kaf_purgatory_depth",
		Help: "Number of acks=-1 waiters parked in purgatory",
	}, []string{"topic", "partition"})

	OpenConnections = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "kaf_open_connections",
		Help: "Currently open client TCP connections to this broker",
	})

	ActiveSegments = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "kaf_active_segments",
		Help: "Active commitlog segments (rolls increment this)",
	}, []string{"topic", "partition"})
)

// FormatPartition is a small helper so callers don't reach for strconv themselves.
func FormatPartition(p int32) string {
	return strconv.FormatInt(int64(p), 10)
}

// FormatErrorCode renders an int16 Kafka error code as a label value.
func FormatErrorCode(code int16) string {
	return strconv.FormatInt(int64(code), 10)
}
