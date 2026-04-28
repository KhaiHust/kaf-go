# Load Test Plan — kaf-go vs Apache Kafka

**Goal:** produce credible benchmark numbers by running kaf-go side-by-side against Apache Kafka on identical hardware, identical workload, identical client. Output: a results table of the form *"kaf-go achieves X% of Kafka's throughput at p99=Y ms"* across a matrix of `acks`, partitions, record size, and idempotence settings.

**Hardware target:** Apple M4 MacBook Pro, 24 GB unified memory, macOS. All work runs in Docker Desktop with native ARM64 images (no Rosetta).

**Output artefacts:**
1. `docker-compose.kaf.yml` — 3-broker kaf-go cluster + monitoring stack
2. `docker-compose.kafka.yml` — 3-broker Apache Kafka KRaft cluster + monitoring stack
3. `cmd/loadgen` — franz-go-based workload generator with Prometheus metrics
4. Grafana dashboards (5 panels: throughput, latency, replication, idempotence, system)
5. Final results report in this repo (`docs/load_test_results.md`) with screenshots

---

## Table of Contents

1. [Why side-by-side](#1-why-side-by-side)
2. [Resource budget on M4 / 24 GB](#2-resource-budget-on-m4--24-gb)
3. [Stack overview](#3-stack-overview)
4. [Metrics to add to kaf-go](#4-metrics-to-add-to-kaf-go)
5. [Docker Compose layouts](#5-docker-compose-layouts)
6. [Apache Kafka equivalent config](#6-apache-kafka-equivalent-config)
7. [Workload generator (`cmd/loadgen`)](#7-workload-generator-cmdloadgen)
8. [Test matrix](#8-test-matrix)
9. [Grafana dashboards](#9-grafana-dashboards)
10. [Failure / chaos scenarios](#10-failure--chaos-scenarios)
11. [Result tabulation](#11-result-tabulation)
12. [Milestone plan](#12-milestone-plan)
13. [Risks and gotchas](#13-risks-and-gotchas)

---

## 1. Why side-by-side

Standalone numbers are uninterpretable. *"kaf-go achieves 35,949 msg/s at p99=319 µs"* tells the reader nothing — they have no baseline. Compare on identical hardware with the **same client library** (franz-go), the **same workload generator**, the **same docker resource limits**, and the report becomes:

> *"kaf-go reaches 62% of Apache Kafka's throughput at acks=-1 with idempotent producers, RF=3, 1 KB messages, on 3-broker clusters with identical 2 vCPU / 2 GB limits. p99 produce latency: 1.4 ms (kaf-go) vs 0.9 ms (Kafka)."*

That sentence is the entire point of this exercise. Everything else is plumbing.

**Non-goals:** "beat Kafka." kaf-go is a study project — even reaching 50–70% of Kafka's throughput is a strong result. Reaching 100% is unrealistic without years of JVM-equivalent JIT/GC tuning, log-cleaner threading, and zero-copy sendfile fast paths.

---

## 2. Resource budget on M4 / 24 GB

Docker Desktop on macOS runs containers inside a Linux VM. Default memory cap is often 8 GB — bump it to **16 GB** in Docker Desktop → Settings → Resources before starting.

Per-container budget (each cluster runs alone, never both at once):

| Component | Count | RAM | CPU |
|---|---|---|---|
| kaf-go broker | 3 | 256 MiB each | 2 vCPU each |
| Apache Kafka broker | 3 | 2 GiB each (JVM heap 1.5 GiB) | 2 vCPU each |
| Prometheus | 1 | 512 MiB | 1 vCPU |
| Grafana | 1 | 256 MiB | 0.5 vCPU |
| cAdvisor | 1 | 256 MiB | 0.5 vCPU |
| node_exporter | 1 | 64 MiB | 0.25 vCPU |
| loadgen | 1 | 512 MiB | 2 vCPU |
| **Total (kaf-go cluster)** | | **~2.4 GiB** | **~9.25 vCPU** |
| **Total (Kafka cluster)** | | **~7.7 GiB** | **~9.25 vCPU** |

M4 has 10 cores; Docker Desktop typically exposes 6–8 to the VM. The CPU oversubscription (9.25 requested vs 8 available) is fine for benchmarking — the workload is bottlenecked on brokers, not loadgen.

**Critical:** apply identical `--cpus` and `--memory` limits to both clusters' brokers. A fair comparison requires identical CPU caps; a "naked" kaf-go on the host with no limit beating a 2-vCPU-capped Kafka would be meaningless.

---

## 3. Stack overview

```
                                ┌──────────────┐
                                │   loadgen    │  franz-go client,
                                │   (cmd/      │  exposes /metrics
                                │   loadgen)   │
                                └──────┬───────┘
                                       │  Produce / Fetch
                       ┌───────────────┼───────────────┐
                       │               │               │
                  ┌────▼────┐     ┌────▼────┐     ┌────▼────┐
                  │ broker1 │◄───►│ broker2 │◄───►│ broker3 │
                  │  :9092  │     │  :9093  │     │  :9094  │
                  └────┬────┘     └────┬────┘     └────┬────┘
                       │ /metrics      │ /metrics      │ /metrics
                       └───────┬───────┴───────────────┘
                               │ scrape
                       ┌───────▼────────┐    ┌──────────┐
                       │   Prometheus   │◄───┤cAdvisor  │ container CPU/RAM/IO
                       │     :9090      │    └──────────┘
                       │                │◄───┤ node_exp │ host metrics
                       └───────┬────────┘    └──────────┘
                               │
                       ┌───────▼────────┐
                       │    Grafana     │  dashboards
                       │     :3000      │
                       └────────────────┘
```

Same diagram applies to the Kafka cluster — just swap the three broker boxes and add a JMX-exporter sidecar to each Kafka broker (Kafka exposes JMX, not Prometheus natively).

---

## 4. Metrics to add to kaf-go

Add `github.com/prometheus/client_golang` and expose `/metrics` on a separate HTTP port (default `9100`, configurable via `--metrics-port`). Don't multiplex on the Kafka TCP port — keep them isolated.

### 4.1 Counters

```go
var (
    produceRequests = promauto.NewCounterVec(prometheus.CounterOpts{
        Name: "kaf_produce_requests_total",
        Help: "Produce requests received, by acks setting and outcome",
    }, []string{"acks", "topic", "error_code"})

    produceRecords = promauto.NewCounterVec(prometheus.CounterOpts{
        Name: "kaf_produce_records_total",
    }, []string{"topic"})

    produceBytes = promauto.NewCounterVec(prometheus.CounterOpts{
        Name: "kaf_produce_bytes_total",
    }, []string{"topic"})

    fetchRequests = promauto.NewCounterVec(prometheus.CounterOpts{
        Name: "kaf_fetch_requests_total",
    }, []string{"topic", "role"}) // role=consumer|replica

    fetchBytes = promauto.NewCounterVec(prometheus.CounterOpts{
        Name: "kaf_fetch_bytes_total",
    }, []string{"topic", "role"})

    isrShrink = promauto.NewCounterVec(prometheus.CounterOpts{
        Name: "kaf_isr_shrink_total",
    }, []string{"topic", "partition"})

    isrExpand = promauto.NewCounterVec(prometheus.CounterOpts{
        Name: "kaf_isr_expand_total",
    }, []string{"topic", "partition"})

    idempotenceDup = promauto.NewCounterVec(prometheus.CounterOpts{
        Name: "kaf_idempotence_dup_total",
        Help: "Idempotent producer retries that hit the dedup ring (no re-append)",
    }, []string{"topic", "partition"})

    idempotenceValidateErrors = promauto.NewCounterVec(prometheus.CounterOpts{
        Name: "kaf_idempotence_validate_errors_total",
    }, []string{"error_code"}) // 45/46/59/90

    snapshotWrites = promauto.NewCounterVec(prometheus.CounterOpts{
        Name: "kaf_producer_snapshot_writes_total",
    }, []string{"topic", "partition"})

    pidBlocksLeased = promauto.NewCounter(prometheus.CounterOpts{
        Name: "kaf_pid_blocks_leased_total",
    })

    purgatoryFailAll = promauto.NewCounter(prometheus.CounterOpts{
        Name: "kaf_purgatory_fail_all_total",
        Help: "NOT_ENOUGH_REPLICAS_AFTER_APPEND fail-all events",
    })
)
```

### 4.2 Histograms

```go
var produceLatencyBuckets = []float64{
    100e-6, 250e-6, 500e-6,                 // µs range
    1e-3, 2.5e-3, 5e-3, 10e-3, 25e-3,        // ms range
    50e-3, 100e-3, 250e-3, 500e-3, 1.0,      // up to 1 s
    2.5, 5.0, 10.0,                          // tail
}

produceLatency = promauto.NewHistogramVec(prometheus.HistogramOpts{
    Name:    "kaf_produce_latency_seconds",
    Buckets: produceLatencyBuckets,
}, []string{"acks", "topic"})

fetchLatency = promauto.NewHistogramVec(prometheus.HistogramOpts{
    Name:    "kaf_fetch_latency_seconds",
    Buckets: produceLatencyBuckets,
}, []string{"role"})

purgatoryWait = promauto.NewHistogram(prometheus.HistogramOpts{
    Name:    "kaf_purgatory_wait_seconds",
    Buckets: produceLatencyBuckets,
})
```

### 4.3 Gauges

```go
partitionLEO  = promauto.NewGaugeVec(/*…*/, []string{"topic", "partition"})
partitionHWM  = promauto.NewGaugeVec(/*…*/, []string{"topic", "partition"})
partitionISR  = promauto.NewGaugeVec(prometheus.GaugeOpts{
    Name: "kaf_partition_isr_size",
}, []string{"topic", "partition"})
partitionReplicaLag = promauto.NewGaugeVec(prometheus.GaugeOpts{
    Name: "kaf_partition_replica_lag_offsets",
    Help: "LEO - ReplicaLEO[r] for each replica r",
}, []string{"topic", "partition", "replica"})
purgatoryDepth = promauto.NewGaugeVec(/*…*/, []string{"topic", "partition"})
openConnections = promauto.NewGauge(/*…*/)
activeSegments  = promauto.NewGaugeVec(/*…*/, []string{"topic", "partition"})
```

### 4.4 Where to install (instrumentation sites)

| Metric | Site |
|---|---|
| `produce_requests`, `produce_records`, `produce_bytes`, `produce_latency` | `api/produce_api.go` — wrap the handler with `time.Now()` + defer observe |
| `fetch_requests`, `fetch_bytes`, `fetch_latency` | `api/fetch_api.go` — same wrapping |
| `isr_shrink`, `isr_expand` | `server/isr_manager.go` — increment in the changed-branch |
| `partition_leo`, `partition_hwm`, `partition_replica_lag` | `coordinator/partition_state.go` `AdvanceHWM` and `UpdateReplicaLEO` |
| `idempotence_dup` | `coordinator/idempotence_state.go` `Validate` dup-return path |
| `idempotence_validate_errors` | same file, error-return path |
| `snapshot_writes` | `coordinator/producer_state_manager.go` `OnSegmentRoll` after successful write |
| `pid_blocks_leased` | `coordinator/pid_manager.go` `leaseNextBlockLocked` |
| `purgatory_depth` | `coordinator/partition_purgatory.go` Add/CompleteUpTo |
| `purgatory_wait` | `api/produce_api.go` acks=-1 path — observe (now - addTime) on Done |
| `purgatory_fail_all` | `coordinator/partition_purgatory.go` FailAll |
| `open_connections` | `server/conn.go` accept loop and connection close |
| `active_segments` | `storage/commitlog/commitlog.go` segment-roll hook |

### 4.5 Metrics endpoint

```go
// server/server.go Start()
go func() {
    mux := http.NewServeMux()
    mux.Handle("/metrics", promhttp.Handler())
    log.Printf("metrics: listening on :%d", s.metricsPort)
    if err := http.ListenAndServe(fmt.Sprintf(":%d", s.metricsPort), mux); err != nil {
        log.Printf("metrics server error: %v", err)
    }
}()
```

---

## 5. Docker Compose layouts

### 5.1 `docker-compose.kaf.yml` (kaf-go cluster)

```yaml
version: "3.9"

x-kaf-broker: &kaf-broker
  build:
    context: .
    dockerfile: Dockerfile.kaf-go
  deploy:
    resources:
      limits:
        cpus: "2.0"
        memory: 256M
  restart: unless-stopped
  networks: [kaf-net]

services:
  kaf1:
    <<: *kaf-broker
    container_name: kaf1
    command:
      - /kaf-broker
      - -broker-id=1
      - -port=9092
      - -metrics-port=9100
      - -peers=2=kaf2:9092,3=kaf3:9092
      - -log-dir=/data
    ports: ["9092:9092", "9100:9100"]
    volumes: [kaf1-data:/data]

  kaf2:
    <<: *kaf-broker
    container_name: kaf2
    command:
      - /kaf-broker
      - -broker-id=2
      - -port=9092
      - -metrics-port=9100
      - -peers=1=kaf1:9092,3=kaf3:9092
      - -log-dir=/data
    ports: ["9093:9092", "9101:9100"]
    volumes: [kaf2-data:/data]

  kaf3:
    <<: *kaf-broker
    container_name: kaf3
    command:
      - /kaf-broker
      - -broker-id=3
      - -port=9092
      - -metrics-port=9100
      - -peers=1=kaf1:9092,2=kaf2:9092
      - -log-dir=/data
    ports: ["9094:9092", "9102:9100"]
    volumes: [kaf3-data:/data]

  prometheus:
    image: prom/prometheus:latest
    container_name: prometheus
    volumes:
      - ./monitoring/prometheus-kaf.yml:/etc/prometheus/prometheus.yml:ro
      - prom-data:/prometheus
    ports: ["9090:9090"]
    networks: [kaf-net]
    deploy:
      resources:
        limits: { cpus: "1.0", memory: 512M }

  grafana:
    image: grafana/grafana:latest
    container_name: grafana
    environment:
      - GF_SECURITY_ADMIN_PASSWORD=admin
      - GF_AUTH_ANONYMOUS_ENABLED=true
      - GF_AUTH_ANONYMOUS_ORG_ROLE=Viewer
    volumes:
      - ./monitoring/grafana/dashboards:/etc/grafana/provisioning/dashboards:ro
      - ./monitoring/grafana/datasources:/etc/grafana/provisioning/datasources:ro
      - grafana-data:/var/lib/grafana
    ports: ["3000:3000"]
    networks: [kaf-net]
    deploy:
      resources:
        limits: { cpus: "0.5", memory: 256M }

  cadvisor:
    image: gcr.io/cadvisor/cadvisor:latest
    container_name: cadvisor
    privileged: true
    volumes:
      - /:/rootfs:ro
      - /var/run:/var/run:ro
      - /sys:/sys:ro
      - /var/lib/docker/:/var/lib/docker:ro
    ports: ["8080:8080"]
    networks: [kaf-net]

  node_exporter:
    image: prom/node-exporter:latest
    container_name: node_exporter
    pid: host
    volumes: ["/:/host:ro,rslave"]
    command: ["--path.rootfs=/host"]
    ports: ["9101:9100"]   # NB: clashes with kaf2's metrics-port — pick a free one
    networks: [kaf-net]

volumes:
  kaf1-data:
  kaf2-data:
  kaf3-data:
  prom-data:
  grafana-data:

networks:
  kaf-net:
    driver: bridge
```

**Port-collision fix:** `node_exporter` defaults to 9100; map it to `9200:9100` instead. Pick free host ports for everything (`9092-9094` for Kafka APIs, `9100-9102` for kaf-go metrics, `9200` for node_exporter, `8080` for cAdvisor, `9090` for Prometheus, `3000` for Grafana).

### 5.2 `Dockerfile.kaf-go`

```dockerfile
FROM golang:1.23-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /kaf-broker ./broker/

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /kaf-broker /kaf-broker
EXPOSE 9092 9100
USER nonroot:nonroot
ENTRYPOINT ["/kaf-broker"]
```

`distroless/static` is ~2 MB and ARM64-native. Image build time on M4: ~30 s.

### 5.3 `monitoring/prometheus-kaf.yml`

```yaml
global:
  scrape_interval: 5s
  evaluation_interval: 15s

scrape_configs:
  - job_name: kaf-brokers
    static_configs:
      - targets: ["kaf1:9100", "kaf2:9100", "kaf3:9100"]
        labels: { cluster: kaf-go }

  - job_name: cadvisor
    static_configs:
      - targets: ["cadvisor:8080"]

  - job_name: node
    static_configs:
      - targets: ["node_exporter:9100"]
```

5 s scrape interval is aggressive but fine for short benchmark runs. For 30-minute soak tests, drop to 15 s.

### 5.4 `monitoring/grafana/datasources/prometheus.yml`

```yaml
apiVersion: 1
datasources:
  - name: Prometheus
    type: prometheus
    url: http://prometheus:9090
    access: proxy
    isDefault: true
```

---

## 6. Apache Kafka equivalent config

### 6.1 `docker-compose.kafka.yml` (KRaft mode, 3 brokers)

```yaml
version: "3.9"

x-kafka-broker: &kafka-broker
  image: apache/kafka:3.8.0
  deploy:
    resources:
      limits: { cpus: "2.0", memory: 2G }
  environment: &kafka-env
    KAFKA_PROCESS_ROLES: broker,controller
    KAFKA_LISTENERS: PLAINTEXT://:9092,CONTROLLER://:9093
    KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT
    KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT
    KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
    KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka1:9093,2@kafka2:9093,3@kafka3:9093
    KAFKA_NUM_PARTITIONS: 6
    KAFKA_DEFAULT_REPLICATION_FACTOR: 3
    KAFKA_MIN_INSYNC_REPLICAS: 2
    KAFKA_LOG_SEGMENT_BYTES: 104857600         # 100 MiB — match kaf-go
    KAFKA_LOG_RETENTION_BYTES: -1              # disable retention for benchmark
    KAFKA_HEAP_OPTS: "-Xmx1536m -Xms1536m"     # cap JVM heap to make memory limit effective
    CLUSTER_ID: "MkU3OEVBNTcwNTJENDM2Qk"       # any valid base64 UUID; keep stable
  restart: unless-stopped
  networks: [kafka-net]

services:
  kafka1:
    <<: *kafka-broker
    container_name: kafka1
    environment:
      <<: *kafka-env
      KAFKA_NODE_ID: 1
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka1:9092
    ports: ["19092:9092"]
    volumes: [kafka1-data:/var/lib/kafka/data]

  kafka2:
    <<: *kafka-broker
    container_name: kafka2
    environment:
      <<: *kafka-env
      KAFKA_NODE_ID: 2
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka2:9092
    ports: ["19093:9092"]
    volumes: [kafka2-data:/var/lib/kafka/data]

  kafka3:
    <<: *kafka-broker
    container_name: kafka3
    environment:
      <<: *kafka-env
      KAFKA_NODE_ID: 3
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka3:9092
    ports: ["19094:9092"]
    volumes: [kafka3-data:/var/lib/kafka/data]

  jmx-exporter-1:
    image: bitnami/jmx-exporter:latest
    container_name: jmx-exporter-1
    command: ["5556", "/etc/jmx-exporter/kafka.yml"]
    volumes: ["./monitoring/jmx-kafka.yml:/etc/jmx-exporter/kafka.yml:ro"]
    environment:
      - JMX_HOST=kafka1
      - JMX_PORT=9999
    networks: [kafka-net]

  # repeat jmx-exporter-2 / -3 with JMX_HOST=kafka2 / kafka3, ports 5557 / 5558

  prometheus:
    image: prom/prometheus:latest
    volumes:
      - ./monitoring/prometheus-kafka.yml:/etc/prometheus/prometheus.yml:ro
    ports: ["9090:9090"]
    networks: [kafka-net]

  grafana:
    image: grafana/grafana:latest
    # … same as kaf-go compose

  cadvisor:
    # … same as kaf-go compose

  node_exporter:
    # … same as kaf-go compose

volumes:
  kafka1-data:
  kafka2-data:
  kafka3-data:

networks:
  kafka-net:
    driver: bridge
```

**Critical config parity** (mirror these exactly between clusters):

| Setting | Both clusters |
|---|---|
| brokers | 3 |
| CPU limit per broker | 2.0 |
| memory limit per broker | (kaf-go 256 MiB; Kafka 2 GiB — JVM floor is unavoidable, document the asymmetry) |
| replication factor | 3 |
| min.insync.replicas | 2 |
| segment size | 100 MiB |
| log retention | unlimited (off) |
| number of partitions | matches test row |

The memory asymmetry is unavoidable — Kafka cannot run in 256 MiB. Note this in the report: *"kaf-go runs in 1/8 the memory; throughput comparison is at equal CPU."*

### 6.2 JMX exporter config (`monitoring/jmx-kafka.yml`)

Use the [official Confluent JMX exporter sample for Kafka](https://github.com/prometheus/jmx_exporter/blob/main/example_configs/kafka-2_0_0.yml) — covers `MessagesInPerSec`, `BytesInPerSec`, request handler queue size, request latency percentiles, replica lag.

---

## 7. Workload generator (`cmd/loadgen`)

A new binary using franz-go. Targets either cluster transparently via `--brokers`. Emits its own Prometheus metrics so you measure latency from the **client** side too (broker-side latency excludes network and queueing).

### 7.1 CLI surface

```
loadgen \
  --brokers kaf1:9092,kaf2:9092,kaf3:9092 \
  --topic loadtest \
  --workers 8 \
  --records 1000000 \
  --record-size 1024 \
  --acks -1 \
  --idempotent \
  --partitions 6 \
  --duration 60s \
  --metrics-port 8090 \
  --warmup 10s
```

### 7.2 Skeleton

```go
package main

import (
    "context"
    "flag"
    "github.com/twmb/franz-go/pkg/kgo"
    "github.com/prometheus/client_golang/prometheus/promauto"
    // …
)

func main() {
    var (
        brokers     = flag.String("brokers", "", "")
        topic       = flag.String("topic", "loadtest", "")
        workers     = flag.Int("workers", 8, "")
        recordSize  = flag.Int("record-size", 1024, "")
        acks        = flag.Int("acks", -1, "")
        idempotent  = flag.Bool("idempotent", true, "")
        duration    = flag.Duration("duration", 60*time.Second, "")
        warmup      = flag.Duration("warmup", 10*time.Second, "")
    )
    flag.Parse()

    opts := []kgo.Opt{
        kgo.SeedBrokers(strings.Split(*brokers, ",")...),
        kgo.DefaultProduceTopic(*topic),
        kgo.ProducerBatchMaxBytes(1 << 20),  // 1 MiB
        kgo.MaxBufferedRecords(100_000),
    }
    switch *acks {
    case 0:  opts = append(opts, kgo.RequiredAcks(kgo.NoAck()))
    case 1:  opts = append(opts, kgo.RequiredAcks(kgo.LeaderAck()))
    case -1: opts = append(opts, kgo.RequiredAcks(kgo.AllISRAcks()))
    }
    if *idempotent {
        // idempotence is on by default in franz-go when acks=all
    } else {
        opts = append(opts, kgo.DisableIdempotentWrite())
    }
    cl, _ := kgo.NewClient(opts...)
    defer cl.Close()

    payload := make([]byte, *recordSize)
    rand.Read(payload)

    // Prometheus metrics
    sentRecords := promauto.NewCounter(/*…*/)
    failedRecords := promauto.NewCounter(/*…*/)
    clientLatency := promauto.NewHistogram(/*…*/)

    // Warm-up
    runWorkers(cl, payload, *workers, *warmup, /*record metrics?*/ false)
    // Real run
    runWorkers(cl, payload, *workers, *duration, true)
}

func runWorkers(cl *kgo.Client, payload []byte, n int, dur time.Duration, record bool) {
    ctx, cancel := context.WithTimeout(context.Background(), dur)
    defer cancel()
    var wg sync.WaitGroup
    for i := 0; i < n; i++ {
        wg.Add(1)
        go func() {
            defer wg.Done()
            for ctx.Err() == nil {
                start := time.Now()
                cl.Produce(ctx, &kgo.Record{Value: payload}, func(_ *kgo.Record, err error) {
                    if record {
                        clientLatency.Observe(time.Since(start).Seconds())
                        if err != nil { failedRecords.Inc() } else { sentRecords.Inc() }
                    }
                })
            }
        }()
    }
    wg.Wait()
    cl.Flush(context.Background())
}
```

### 7.3 Why warm-up matters

JIT (Kafka), file-cache fill (both), Idempotence ring miss → segment-roll first-snapshot — all hit cold. Discard the first 10 s. Without this, kaf-go looks slower than it is because every PID is freshly leased.

### 7.4 Run as a container

```yaml
# add to either compose file
loadgen:
  build: { context: ., dockerfile: Dockerfile.loadgen }
  command: ["--brokers=kaf1:9092,kaf2:9092,kaf3:9092", "--duration=60s", "--workers=8"]
  ports: ["8090:8090"]
  depends_on: [kaf1, kaf2, kaf3]
  networks: [kaf-net]   # or kafka-net
```

---

## 8. Test matrix

Run each row twice — once on kaf-go, once on Kafka. Record the result table for each.

### 8.1 Phase 1 — Baseline (1 row)

| acks | partitions | record size | idempotent | RF | minISR | workers | duration |
|---|---|---|---|---|---|---|---|
| -1 | 6 | 1 KiB | yes | 3 | 2 | 8 | 60 s + 10 s warmup |

Sanity check: confirm both clusters produce + consume cleanly before scaling out the matrix. If kaf-go drops records or Kafka rejects with `NOT_ENOUGH_REPLICAS`, fix config before continuing.

### 8.2 Phase 2 — `acks` sweep (3 rows)

| acks | rest |
|---|---|
| 0 | baseline |
| 1 | baseline |
| -1 | baseline |

**Hypothesis:** kaf-go's `acks=0` and `acks=1` should be within 80–90% of Kafka. `acks=-1` may diverge more — Kafka's purgatory is more sophisticated.

### 8.3 Phase 3 — partition sweep (5 rows)

| partitions |
|---|
| 1, 3, 6, 12, 24 |

**Hypothesis:** kaf-go scales close to linear up to ~12 partitions; beyond that, per-partition fetcher goroutines start to show overhead. Kafka's per-partition costs are amortised by sharing a request handler thread pool.

### 8.4 Phase 4 — record size sweep (4 rows)

| size |
|---|
| 100 B, 1 KiB, 10 KiB, 100 KiB |

**Hypothesis:** at 100 B both are CPU-bound on protocol parsing; at 100 KiB both are network-bound and gap closes. The most flattering size for kaf-go is probably ~10 KiB.

### 8.5 Phase 5 — idempotence on/off (2 rows)

| idempotent |
|---|
| no |
| yes |

**Hypothesis:** kaf-go's idempotence overhead per produce is ~1 mutex acquire + ring slot write. Should be < 5% throughput cost. If higher, contention on `IdempotenceState.mu`.

### 8.6 Phase 6 — soak (1 long run)

Baseline config × 30 minutes. Watch for:
- memory leak (RSS climbing without bound)
- segment-roll behaviour (every ~N minutes, brief snapshot-write spike)
- ISR flapping under steady load (should be zero shrink/expand events)

**Total runs:** 1 + 3 + 5 + 4 + 2 + 1 = **16 rows × 2 clusters = 32 runs**, each ~70 s + setup. Roughly 2 hours of pure run time + dashboards review.

---

## 9. Grafana dashboards

Five dashboards, each ~6–8 panels. Save as JSON under `monitoring/grafana/dashboards/`.

### 9.1 Throughput

- Records/sec produced: `rate(kaf_produce_records_total[30s])` (or `kafka_server_brokertopicmetrics_messagesin_total` for Kafka)
- Bytes/sec produced: `rate(kaf_produce_bytes_total[30s])`
- Records/sec consumed: `rate(kaf_fetch_bytes_total{role="consumer"}[30s])`
- Replication bytes/sec: `rate(kaf_fetch_bytes_total{role="replica"}[30s])`
- Active producers: `kaf_open_connections`
- Per-broker breakdown (single-stat × 3)

### 9.2 Latency

- Produce p50/p95/p99 by acks: `histogram_quantile(0.99, sum(rate(kaf_produce_latency_seconds_bucket[30s])) by (le, acks))`
- Fetch p50/p95/p99 by role
- Purgatory wait time: `histogram_quantile(0.99, sum(rate(kaf_purgatory_wait_seconds_bucket[30s])) by (le))`
- Loadgen-side client latency (from cmd/loadgen) — overlaid for sanity check

### 9.3 Replication

- ISR size per partition: `kaf_partition_isr_size{topic="loadtest"}`
- LEO − HWM gap per partition (timeseries)
- Replica lag in offsets: `kaf_partition_replica_lag_offsets`
- ISR shrink/expand events: `increase(kaf_isr_shrink_total[5m])` (annotation overlay)
- Min-ISR violations: `increase(kaf_purgatory_fail_all_total[5m])`

### 9.4 Idempotence

- Dup short-circuit rate: `rate(kaf_idempotence_dup_total[30s])`
- Validate errors by code (stacked area): `rate(kaf_idempotence_validate_errors_total[30s])`
- Snapshot writes/min: `rate(kaf_producer_snapshot_writes_total[1m]) * 60`
- PID blocks leased (cumulative): `kaf_pid_blocks_leased_total`

### 9.5 System

- CPU% per container (cAdvisor): `rate(container_cpu_usage_seconds_total{name=~"kaf.*"}[30s])`
- RAM per container: `container_memory_working_set_bytes`
- Disk write IOPS / MB/s: `rate(container_fs_writes_bytes_total[30s])`
- Network RX/TX per container

Pre-build these with [Grafana JSON model](https://grafana.com/docs/grafana/latest/dashboards/build-dashboards/import-dashboards/) so they ship with the repo.

---

## 10. Failure / chaos scenarios

These scenarios prove the system is **correct under failure**, not just fast on the happy path. The Grafana captures from these runs are the highest-leverage evidence the harness produces.

### 10.1 Broker kill mid-load

```
# while loadgen is running steady-state at acks=-1
docker kill kaf2
```

Observe in Grafana:
- ISR shrink event for every partition led by a broker that was replicating to kaf2
- Some `acks=-1` produces fail with `NOT_ENOUGH_REPLICAS_AFTER_APPEND` (if minISR=2 and one died)
- Throughput dip, then recovery
- HWM stalls on partitions where kaf2 was the leader (no failover yet — known gap)

```
docker start kaf2
```

Observe:
- ReplicaFetcher catches up
- `LastCaughtUp` refreshes
- ISR expand event
- Throughput restored

**Capture**: Grafana screenshot of the full sequence — this is your "the system gracefully degrades and recovers" evidence.

### 10.2 Broker pause (network-equivalent)

```
docker pause kaf3 ; sleep 15 ; docker unpause kaf3
```

Tests the 10 s `LastCaughtUp` shrink threshold cleanly. The paused broker's TCP connection stays open but no fetches arrive → ISR shrinks at ~10 s mark, expands again ~1 s after unpause.

### 10.3 Slow disk simulation

Hard on macOS Docker (no native cgroup IO throttling). Skip unless using Linux. If you want to try:

```
docker run --device-write-bps /dev/sda:1mb …
```

But Docker Desktop's VM doesn't expose this cleanly. Document as out-of-scope.

### 10.4 Producer retry storm

Run loadgen with `--retries=10` and kill kaf1 (assuming it's leader for some partitions). Idempotent producers will retry the in-flight batches; verify `kaf_idempotence_dup_total` rises (proving dedup catches retries) and no duplicate records appear when you `cmd/client` consume the topic afterward.

**This is the single best demo of the idempotent producer working end-to-end.**

---

## 11. Result tabulation

For each test row, record:

```
Row: acks=-1, partitions=6, size=1KiB, idempotent=yes, RF=3, minISR=2, 60s steady-state

                        kaf-go      Kafka       ratio (kaf/kafka)
─────────────────────────────────────────────────────────────────
Throughput (msg/s)      35,949      57,800      0.62
Throughput (MiB/s)      35.1        56.4        0.62
Produce p50 (ms)        0.086       0.41        —
Produce p95 (ms)        0.21        0.71        —
Produce p99 (ms)        0.32        0.94        —
Produce p99.9 (ms)      1.4         2.8         —
CPU per broker (%)      ~140        ~165        —      (ratio of 2-vCPU cap)
RAM per broker (MiB)    180         1,720       0.10
Disk write (MiB/s)      36          58          —
Errors                  0           0           —
ISR shrinks             0           0           —
Idempotence dups        0           0           —
```

Save as a markdown table in `docs/load_test_results.md` with one section per phase. Embed Grafana screenshots as PNG (export from dashboard panel → "Share" → "Direct link rendered image").

**The headline number is Phase 1's throughput ratio.** Everything else is supporting evidence.

---

## 12. Milestone plan

Ordered list of milestones. Each is a self-contained deliverable; later milestones depend on the artefacts produced earlier. Status as of 2026-04-28 marked inline.

| # | Milestone | Work | Deliverable | Status |
|---|---|---|---|---|
| **M1** | kaf-go cluster scaffold | Write `Dockerfile.kaf-go` + `Dockerfile.loadgen`. Write `docker-compose.kaf.yml` with monitoring stack. Validate compose syntax. | kaf-go cluster topology defined | ✅ landed (`load-test/`) |
| **M2** | Advertised-host plumbing | Add `-advertised-host` flag to broker so Metadata/FindCoordinator/BrokerRegistration responses use container hostnames instead of `"localhost"`. Required for any cross-container client. | Broker reachable from peer/client containers | ✅ landed (5-file patch, see §13.4) |
| **M3** | Kafka comparison cluster | Write `docker-compose.kafka.yml`. Configure JMX exporter sidecars. Verify Kafka brokers form a quorum, JMX metrics flow into Prometheus. | Kafka comparison cluster runs | ✅ landed (compose written; live verification pending first run) |
| **M4** | Workload generator | Write `cmd/loadgen` with franz-go. Run it against both clusters at baseline config; eyeball that records are being produced and consumed. Verify warm-up vs steady-state separation. | Workload generator works end-to-end | ✅ landed and verified 2026-04-28; baseline 500k–700k msg/s (see `load-test/README.md` issues #4–#7 for the four bugs fixed during bring-up) |
| **M5** | Broker metrics + dashboards | Add `prometheus/client_golang` dependency to kaf-go. Wire all counters / histograms / gauges per §4. Build the broker-side Grafana dashboards (throughput, latency, replication, idempotence). Smoke test: `curl localhost:9100/metrics`. | Broker-side metrics + 4 dashboards | ✅ landed 2026-04-28 (`metrics/metrics.go`, `--metrics-port` flag default 9100, four dashboards under `load-test/monitoring/grafana/dashboards/`) |
| **M6** | Baseline + acks sweep | Run Phase 1 (baseline) and Phase 2 (acks sweep). Capture Grafana screenshots. Tabulate. **Commit interim results to git.** | First half of the matrix complete | ✅ landed 2026-04-28 (`docs/load_test_results.md`) — runs at partitions=3 (deviation from §8.1's `partitions=6`); see results doc for details |
| **M7** | Full matrix + chaos + report | Run Phase 3 (partitions), Phase 4 (record size), Phase 5 (idempotence), Phase 6 (30-min soak), and the chaos scenarios in §10. Write `docs/load_test_results.md` with screenshots and tables. Update README.md with the headline number. | Final report | 🟡 partial — Phases 3/4/5 ✅, Phase 6 kaf-go ✅ (30-min clean soak, 1.005 B records, zero invariant violations; surfaced + fixed a hot-path INFO logging bug in `server/conn.go`). Phase 6 Kafka partial (~4 min, all 3 brokers SIGSEGV on ARM64 JVM). Chaos scenarios §10 still ⏳ |

Sequencing note: metrics instrumentation (M5) consistently takes ~2× the time you budget for it. If M5 slips, downstream milestones slip with it — don't shortcut the metrics work, every later dashboard and result table depends on it.

---

## 13. Risks and gotchas

### 13.1 Docker Desktop memory

If the cluster OOMs (containers restart, no clear error in compose logs), bump Docker Desktop memory cap. M4's 24 GB is plenty but Docker defaults to 8.

### 13.2 ARM image availability

All images called out (apache/kafka, prom/prometheus, grafana/grafana, gcr.io/cadvisor/cadvisor, prom/node-exporter, distroless) have native ARM64 builds as of 2024+. If you see "platform mismatch" warnings, you're on an old version — `docker pull` again.

### 13.3 ~~Localhost listener trap~~ — non-issue

The broker's `net.Listen` already binds `:%d` (= `0.0.0.0:port`), so other containers can reach it. Verified in `broker/main.go:47` and `server/server.go:85`. No fix needed.

### 13.4 ~~franz-go advertised-listener handling~~ — **FIXED 2026-04-28**

Was a hard blocker: the broker hardcoded `"localhost"` as its self-advertised host, so `Metadata` / `FindCoordinator` / `BrokerRegistration` responses said *"broker N is at localhost:9092"* — unreachable from any other container. Now resolved by adding `-advertised-host` to `broker/main.go`, plumbed through `server.NewServer` → `Server.AdvertisedHost()` → self-`RegisterBroker`, `BrokerRegistrar.ownHostPort`, and `FindCoordinator`. Default empty value still yields `"localhost"` for single-host dev workflows.

Compose passes `-advertised-host=kafN` per broker. End-to-end loadgen → kaf-go traffic now works inside the bridge network without any workaround.

Files touched: `broker/main.go`, `server/server.go`, `server/broker_register.go`, `api/findcoordinator_api.go`, `server/conn.go`. Net +35 / -10 LOC.

### 13.5 Memory comparison is unfair

JVM Kafka cannot run in 256 MiB. Kafka in 2 GiB vs kaf-go in 256 MiB at equal CPU is the fairest comparison available. **State this explicitly in the report** — don't pretend the memory column is apples-to-apples.

### 13.6 Disk variance on Docker Desktop

Docker Desktop on macOS uses VirtIOFS or gRPC-FUSE for volume mounts depending on version — both have variable performance. Use **named volumes** (`kaf1-data:/data`) not bind mounts (`./data:/data`) for the broker logs. Bind mounts on macOS can be 5–10× slower than named volumes and would tank both clusters equally but unpredictably.

### 13.7 First-run Idempotence segment-roll

The first segment roll triggers a snapshot write — a brief latency blip. If your test run is shorter than one segment roll (< 100 MiB at 1 KiB messages = 100,000 records), you never see the snapshot path. For a meaningful soak you want **at least 3 rolls** = 300,000+ records produced. At baseline 35K msg/s × 60 s = 2.1M records → 21 rolls. Fine.

### 13.8 Don't run both clusters at once

8 GB combined RAM, 18+ vCPU = fine on M4 in absolute terms, but they will *contend for IO bandwidth* in unpredictable ways. **Always tear down one before bringing up the other.** Save state via `docker volume`s if you want to compare side-by-side images later.

```bash
# end of kaf-go run
docker compose -f docker-compose.kaf.yml down

# start Kafka run
docker compose -f docker-compose.kafka.yml up -d
```

### 13.9 cAdvisor on OrbStack — pin v0.47.2

`gcr.io/cadvisor/cadvisor:latest` is currently v0.55+, which dropped direct Docker API support and looks for containerd at `/run/containerd/containerd.sock`. OrbStack doesn't expose containerd at that path, so newer cAdvisor only emits the `id` cgroup-path label and dashboard queries with `{name=~"kaf.*"}` match nothing. Compose pins to **v0.47.2** plus an explicit `/var/run/docker.sock:/var/run/docker.sock:ro` mount. Docker Desktop / Lima / Colima happen to work without the explicit socket; OrbStack needs it.

### 13.10 `compose run --rm` strips network aliases

`docker compose run --rm loadgen` creates a *one-off* container — by design it isn't a long-lived service, and compose does **not** attach the service's network aliases. So Prometheus's `static_configs.targets: ["loadgen:8090"]` fails DNS lookup, scrape target stays DOWN, and the loadgen-side panels are empty even while the test runs. Pass `--use-aliases --service-ports` to opt back in. Both `make loadgen` and `make kafka-loadgen` do this now.

---

## Appendix A — quick-start commands

```bash
# Build kaf-go image
docker build -t kaf-go:local -f Dockerfile.kaf-go .

# Bring up the kaf-go cluster + monitoring
docker compose -f docker-compose.kaf.yml up -d

# Wait for cluster to be ready (~10 s)
sleep 10

# Run baseline load test
docker compose -f docker-compose.kaf.yml run --rm loadgen \
  --brokers kaf1:9092,kaf2:9092,kaf3:9092 \
  --topic loadtest --partitions 6 \
  --workers 8 --record-size 1024 --acks -1 --idempotent \
  --duration 60s --warmup 10s

# Inspect dashboards
open http://localhost:3000     # admin / admin

# Tear down
docker compose -f docker-compose.kaf.yml down -v   # -v wipes volumes
```

Repeat with `docker-compose.kafka.yml` for the Kafka run. Compare Grafana panels side-by-side (Grafana time-range picker → "Last 5 minutes" works well after each run).

---

## Appendix B — what to commit to git

```
docker-compose.kaf.yml
docker-compose.kafka.yml
Dockerfile.kaf-go
Dockerfile.loadgen
cmd/loadgen/main.go
monitoring/
  prometheus-kaf.yml
  prometheus-kafka.yml
  jmx-kafka.yml
  grafana/
    datasources/prometheus.yml
    dashboards/
      throughput.json
      latency.json
      replication.json
      idempotence.json
      system.json
docs/
  load_test_plan.md       (this file)
  load_test_results.md    (filled in at M7, with PNG screenshots in docs/img/)
```

Don't commit volume contents (`kaf*-data/`, `kafka*-data/`, `prom-data/`, `grafana-data/`) — add to `.gitignore`.
