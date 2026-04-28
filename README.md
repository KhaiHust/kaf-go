# kaf-go

> **Personal learning project.** A Kafka-compatible broker in Go, built from scratch. Speaks the real binary wire protocol; driven by an unmodified [franz-go](https://github.com/twmb/franz-go) client across multiple nodes.
>
> **Long-term target: [KRaft](https://cwiki.apache.org/confluence/display/KAFKA/KIP-500%3A+Replace+ZooKeeper+with+a+Self-Managed+Metadata+Quorum)** — Kafka's Raft-based metadata quorum that replaced ZooKeeper in 3.3. Everything below is the broker-internals foundation needed to get there: a real wire protocol, a real commit log, real replication, real durability semantics. Once those hold under load, the next milestone is implementing Raft (`Vote` / `BeginQuorumEpoch` / `EndQuorumEpoch` / `FetchSnapshot`) and lifting the current single-controller setup to a controller quorum.

---

## What is implemented

- [x] **Wire protocol** — binary framing, compact types, tagged fields, flexible API versions
- [x] **17 Kafka APIs** — Produce, Fetch, ListOffsets, Metadata, ApiVersions, CreateTopics, the full consumer-group set, InitProducerId, BrokerRegistration, BrokerHeartbeat
- [x] **Producer durability** — full `acks` contract: `0` / `1` / `-1` via per-partition purgatory + HWM-driven release
- [x] **Idempotent producer** — `InitProducerId`, per-`(PID, partition)` 5-slot dedup ring, segment-roll snapshots, durable PID block lease in `__cluster_metadata`
- [x] **`min.insync.replicas`** — pre-append `NOT_ENOUGH_REPLICAS` (19); ISR shrink fails parked waiters with `NOT_ENOUGH_REPLICAS_AFTER_APPEND` (20)
- [x] **ISR / HWM tracking** — leader-side ISRManager (500 ms ticker, 10 s lag threshold); HWM = `min(LEO across ISR)`; fetch clips to HWM for consumers, LEO for replica fetchers
- [x] **Consumer groups** — full lifecycle: FindCoordinator → JoinGroup → SyncGroup → Heartbeat → OffsetFetch → OffsetCommit → LeaveGroup
- [x] **Zero-copy fetch** — `io.Copy` from file descriptor straight to `*net.TCPConn`
- [x] **Multi-broker** — peer config, lowest-ID controller, `__cluster_metadata` log streamed by followers, replica fetcher with lazy peer dial, broker registration (62) + heartbeat (63)
- [x] **Persistent commit log** — log segments with offset-indexed lookup, topic recovery on restart

Not yet:

- [ ] Leader failover
- [ ] Transactions / exactly-once (`__transaction_state`, `AddPartitionsToTxn` / `EndTxn` / `WriteTxnMarkers`, read-committed LSO)
- [ ] Log retention / compaction
- [ ] Persistent consumer offsets
- [ ] **KRaft / Raft consensus** — long-term target

---

## Architecture

```
broker/         entry point, starts TCP server
server/         TCP listener + per-connection handler (conn.go dispatches by API key)
                MetadataFetcher / ReplicaFetcher / ReplicaFetcherManager
                BrokerRegistrar / BrokerRegistry / ISRManager
api/            one handler file per Kafka API
protocol/       binary codec + per-API request/response structs
                admin · broker · consumer · fetch · metadata · producer · topic · types
coordinator/    consumer group state, metadata log, partition state,
                purgatory, idempotence ring, PID manager, producer state manager
storage/commitlog/   log segments, offset index, HWM/LEO clip
constant/       API keys, error codes, group states, ack semantics
cmd/{client,consumer,acktest,bench}   test + benchmark drivers
```

Deeper walkthrough: [`docs/full_flow.md`](docs/full_flow.md) (boot → produce → consume across all subsystems).

### Implemented APIs

| Key | Name | Version |
|---|---|---|
| 0 | Produce | 13 |
| 1 | Fetch | 18 |
| 2 | ListOffsets | 10 |
| 3 | Metadata | 13 |
| 8 | OffsetCommit | 8 |
| 9 | OffsetFetch | 8 |
| 10 | FindCoordinator | 6 |
| 11 | JoinGroup | 9 |
| 12 | Heartbeat | 4 |
| 13 | LeaveGroup | 5 |
| 14 | SyncGroup | 5 |
| 18 | ApiVersions | 3 |
| 19 | CreateTopics | 7 |
| 22 | InitProducerId | 5 |
| 62 | BrokerRegistration | 0 |
| 63 | BrokerHeartbeat | 0 |

Keys 62 and 63 are internal KRaft APIs (broker→controller, not in public client docs).

### Multi-broker metadata flow

```
Controller (broker 1)                    Follower (broker 2)
       │                                         │
       │  ◄── BrokerRegistration ────────────────│
       │  append BrokerRecord to                 │
       │  __cluster_metadata log                 │
       │                                         │
       │  ◄── CreateTopics (client) ─────────────│
       │  append TopicRecord +                   │
       │  PartitionRecord × N                    │
       │                                         │
       │  ◄── Fetch(__cluster_metadata) ─────────│
       │  ──► records ───────────────────────────│
       │                                         │  apply:
       │                                         │  TopicRecord     → register topic
       │                                         │  PartitionRecord  leader=2
       │                                         │    → create commitlog (I'm leader)
       │                                         │  PartitionRecord  leader=1
       │                                         │    → start ReplicaFetcher
```

### Produce path (`acks=-1`)

```
Producer                  Leader                            Follower
   │── Produce(acks=-1) ──►│
   │                       │ if len(ISR) < minISR:
   │◄── NOT_ENOUGH_REPLICAS (19)
   │                       │ AppendRaw → LEO = baseOffset+N
   │                       │ purgatory.Add(waiter, requiredOffset = LEO)
   │                       │
   │                       │◄── Fetch(replica) ─────────────│
   │                       │ update ReplicaLEO[follower]
   │                       │ AdvanceHWM = min(LEO across ISR)
   │                       │ purgatory.CompleteUpTo(HWM) → wake waiter
   │◄── BaseOffset ────────│
   │
   │ ISRManager tick (500 ms):
   │   shrink: drop followers older than 10 s
   │   if ISR < minISR: purgatory.FailAll → NOT_ENOUGH_REPLICAS_AFTER_APPEND (20)
   │   on change: append PartitionRecord to __cluster_metadata
```

### Idempotent producer path

```
Client (enable.idempotence=true)         Controller
   │── InitProducerId(txnId=null) ──────►│ PidManager.AllocateOne:
   │                                     │   if block exhausted:
   │                                     │     append ProducerIdsRecord{NextPid+=1000}
   │                                     │     to __cluster_metadata BEFORE reply
   │◄── (PID=N, epoch=0) ────────────────│
   │
   │── Produce(PID=N, epoch=0, seq=0..K) ──► Idempotence.Validate:
   │                                          unknown PID + seq=0 → accept
   │                                          AppendRaw → baseOffset=B
   │                                          Idempotence.Record(...)  (ring slot stamped)
   │◄── BaseOffset=B ────────────────────────
   │
   │── Produce(retry same batch) ──────────► Validate: seq ≤ LastSeq AND in ring
   │◄── BaseOffset=B  (dup short-circuit)    NO second AppendRaw

Segment roll → ProducerStateManager writes <newBase>.producer.snapshot (off the produce path).
Restart → load latest snapshot, walk active-segment tail, replay Idempotence.Record per batch.
```

### Consumer group state machine

```
Empty ──► PreparingRebalance ──► CompletingRebalance ──► Stable
              ▲                                           │
              └──────────── member join/leave ────────────┘
                                  │
                                Dead (all members left)
```

Join barrier blocks JoinGroup responses until every known member has re-joined; session timeout evicts unresponsive members and triggers rebalance; round-robin partition assignment.

---

## Getting started

```bash
# Single broker on :9092
go run ./broker

# Two-broker cluster
go build -o /tmp/kaf-broker ./broker/
/tmp/kaf-broker -broker-id 1 -port 9092 -peers "2=localhost:9093" -log-dir /tmp/kaf-data1
/tmp/kaf-broker -broker-id 2 -port 9093 -peers "1=localhost:9092" -log-dir /tmp/kaf-data2

# Drive it
go run ./cmd/client    --brokers localhost:9092
go run ./cmd/bench     -brokers "localhost:9092,localhost:9093" -msgs 10000 -workers 4 -partitions 3
```

---

## Load test

3-broker kaf-go vs 3-broker Apache Kafka 3.8 KRaft on Apple M4 / OrbStack. Plan: `docs/load_test_plan.md` · Numbers: `docs/load_test_results.md` · Infra: `load-test/`.

```
       ┌────────────┐
       │  loadgen   │  franz-go workload, exposes /metrics
       └─────┬──────┘
             │ Produce / Fetch (acks=-1, idempotent)
   ┌─────────┼─────────┐
   ▼         ▼         ▼
┌─────┐   ┌─────┐   ┌─────┐
│kaf1 │◄─►│kaf2 │◄─►│kaf3 │   3-broker cluster, RF=3, min.ISR=2
│:9092│   │:9092│   │:9092│   each exposes :9100 /metrics
└──┬──┘   └──┬──┘   └──┬──┘
   └─────────┼─────────┘
             │ scrape (kaf_* + cAdvisor + loadgen_*)
        ┌────▼─────┐
        │Prometheus│──►  Grafana :3000
        └──────────┘     (throughput / latency / replication / idempotence)
```

**30-min soak** (1 KiB records, 8 workers, partitions=6, `acks=-1`, idempotent):

| Metric | kaf-go (clean 30 min) | Kafka 3.8 (partial ~4 min)\* |
|---|---:|---:|
| Throughput | **558 K msg/s** (545 MiB/s) | ~2.14 M msg/s |
| Records appended | **1,005,353,500** | — |
| Broker p50 / p95 / p99 | 5.4 / 10.1 / 23.2 ms | (JMX unavailable) |
| RSS per broker | 57 – 148 MiB | — |
| ISR shrinks / dedup dups / min-ISR violations | **0 / 0 / 0** | — |

\* Kafka 3.8 SIGSEGV'd on ARM64 ~5 min in. **1 billion records, zero invariant violations** on the kaf-go side. Sweeps land kaf-go at **0.18× – 0.21× of Kafka** throughput; full breakdown + the hot-path-logging cliff that surfaced under sustained load are in `docs/load_test_results.md`.
