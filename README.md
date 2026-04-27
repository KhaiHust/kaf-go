# kaf-go

> **Personal learning project.** I use Kafka at work every day but only ever as a black box. This is my attempt to actually understand it — by building it from scratch.

---

## What I am learning

I started with a simple question: *what actually happens when a producer sends a message?*

Answering it properly meant implementing the binary wire protocol, then the commit log, then consumer groups, then multi-broker coordination, then the durability semantics that make Kafka an actual log instead of a queue — each piece revealing why Kafka is designed the way it is. The broker now speaks real Kafka protocol and can be driven by an unmodified franz-go client across multiple nodes, including an idempotent producer.

The long-term target is **KRaft** ([KIP-500](https://cwiki.apache.org/confluence/display/KAFKA/KIP-500%3A+Replace+ZooKeeper+with+a+Self-Managed+Metadata+Quorum)) — Kafka's Raft-based consensus layer that replaced ZooKeeper in Kafka 3.3. Getting there requires mastering all the broker internals first, which is what this project is building toward.

### Lessons so far

- **Binary protocol** — writing the codec by hand (framing, compact types, tagged fields, version negotiation) makes the wire format concrete. You can't fake understanding it.
- **Consumer group rebalancing** — implementing the join barrier and session-timeout eviction made it immediately clear why rebalances are disruptive and what actually triggers them in production.
- **Commit log** — append-only segments with an offset index explain the sequential-write performance and how consumers can seek without scanning.
- **Zero-copy fetch** — routing records straight from a file descriptor to the socket via `io.Copy` on `*net.TCPConn` shows exactly what the OS-level optimization is and why it matters for fetch throughput.
- **Multi-broker metadata propagation** — implementing `__cluster_metadata` showed why Kafka needs an internal topic to broadcast cluster state. Every broker continuously polls the controller's metadata log so it knows which partitions it owns, which broker is the leader for each partition, and where to route replica fetch requests — without querying the controller on every client request.
- **`acks` semantics & purgatory** — implementing `acks=-1` made HWM (high-water mark) tangible: the leader can't acknowledge a record until every in-sync follower has fetched past it. Producer requests park in a per-partition purgatory queue and are only released when HWM advances, which ties together append, replication, and ISR shrink/expand into one feedback loop.
- **Idempotent producer** — building the per-`(PID, partition)` 5-slot dedup ring and the durable PID-block lease shows why `enable.idempotence=true` is non-trivial: the broker has to recognize retries by sequence number, return the *original* offset on duplicates without writing a second copy, and survive a restart without re-issuing already-handed-out PIDs.
- **Protocol debugging** — finding a single misplaced tagged-field read by decoding raw TCP bytes against the Kafka spec, or tracking down an endianness bug in `numRecords` that caused fetch offsets to jump by 16 million, teaches you more than any documentation would.

---

## What is implemented (v4)

- **Kafka wire protocol** — binary framing, compact types, tagged fields, flexible API versions
- **Producer** — record batches, Snappy compression
- **Producer durability semantics** — full `acks` contract: `0` (fire-and-forget), `1` (leader-local), `-1` (wait for ISR via per-partition purgatory; HWM-driven release)
- **Idempotent producer** — `InitProducerId` with block-based PID allocation, per-`(PID, partition)` 5-slot dedup ring, retry short-circuit returns the original `BaseOffset`, per-segment-roll snapshot files (`<base>.producer.snapshot`) plus tail-replay on startup, durable PID lease in `__cluster_metadata`
- **`min.insync.replicas`** — per-topic config; pre-append rejects with `NOT_ENOUGH_REPLICAS` (19); ISR shrink fails parked waiters with `NOT_ENOUGH_REPLICAS_AFTER_APPEND` (20)
- **ISR / HWM tracking** — leader-side ISRManager (500ms ticker, 10s lag threshold) shrinks/expands ISR; HWM = `min(LEO across ISR)`; fetch responses clip to HWM for consumers, LEO for replica fetchers
- **Consumer groups** — full lifecycle: FindCoordinator → JoinGroup → SyncGroup → Heartbeat → OffsetFetch → OffsetCommit → LeaveGroup
- **Zero-copy fetch** — record data sent directly from disk to socket via `io.Copy` on `*net.TCPConn`
- **Persistent commit log** — log segments with offset-indexed lookup, topic recovery on restart
- **Multi-broker coordination** — multiple brokers with peer configuration, lowest-ID controller election
- **Metadata log** — `__cluster_metadata` commitlog backed by the standard Fetch API; followers stream topic/partition/broker records from the controller on startup
- **Broker registration & heartbeat** — followers register with the controller (API 62), report metadata offset every 3s (API 63)
- **Replica fetcher** — non-leader partitions continuously fetch from the leader to stay in sync; lazy peer dial handles peer-up-late ordering
- **Benchmark tool** — end-to-end produce+consume throughput with p50/p95/p99 latency breakdown
- **17 Kafka APIs** implemented

### Implemented APIs

| API Key | Name | Version |
|---------|------|---------|
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

---

## Getting Started

### Prerequisites

- Go 1.21+

### Run a single broker

```bash
go run ./broker
```

Listens on `:9092`, stores log data under `var/log/`.

### Run a two-broker cluster

```bash
go build -o /tmp/kaf-broker ./broker/

# Terminal 1 — broker 1 (controller, lowest ID)
/tmp/kaf-broker -broker-id 1 -port 9092 -peers "2=localhost:9093" -log-dir /tmp/kaf-data1

# Terminal 2 — broker 2 (follower)
/tmp/kaf-broker -broker-id 2 -port 9093 -peers "1=localhost:9092" -log-dir /tmp/kaf-data2
```

Broker 2 registers with broker 1, starts streaming `__cluster_metadata`, and begins heartbeating every 3 seconds. Topics created on broker 1 are automatically propagated to broker 2.

### Run the test client

```bash
# Produce 100 messages, then consume with a consumer group
go run ./cmd/client --brokers localhost:9092

# Multi-consumer group test
go run ./cmd/consumer --consumers 3 --topic orders --group orders-group
```

### Run the benchmark

```bash
go run ./cmd/bench \
  -brokers "localhost:9092,localhost:9093" \
  -msgs 10000 \
  -size 1024 \
  -workers 4 \
  -partitions 3
```

Sample output:
```
=== PRODUCE ===
  messages  : 10000
  elapsed   : 1.243s
  throughput: 8045 msg/s   8.24 MB/s
  latency   : p50=381µs    p95=1.2ms    p99=3.1ms    max=18.4ms

=== CONSUME ===
  messages  : 10000
  elapsed   : 892ms
  throughput: 11211 msg/s  11.48 MB/s
```

---

## Project Structure

```
.
├── broker/             # Entry point
├── api/                # Kafka API handlers (one file per API)
├── protocol/           # Wire protocol encoding/decoding
│   ├── admin/          # ApiVersions, Metadata, ListOffsets
│   ├── broker/         # BrokerRegistration, BrokerHeartbeat
│   ├── consumer/       # JoinGroup, SyncGroup, Heartbeat, Offset*
│   ├── fetch/          # Fetch request/response
│   ├── metadata/       # __cluster_metadata record types + RecordBatch codec
│   ├── producer/       # Produce, InitProducerId, record batches
│   ├── topic/          # CreateTopics request/response
│   └── types/          # Compact types, varints, compression
├── coordinator/        # Consumer group state, metadata log, partition state,
│                       #   purgatory, idempotence ring, PID manager,
│                       #   producer state manager (segment-roll snapshots)
├── storage/
│   └── commitlog/      # Log segments, offset index, HWM/LEO clip
├── server/             # TCP server, MetadataFetcher, ReplicaFetcher,
│                       #   ReplicaFetcherManager, BrokerRegistrar, ISRManager
├── constant/           # API keys, error codes, group states, ack semantics
└── cmd/
    ├── client/         # Test producer + consumer
    ├── consumer/       # Multi-consumer group test tool
    ├── acktest/        # Multi-broker acks=0/1/-1 integration test
    └── bench/          # Produce+consume throughput benchmark
```

---

## Consumer Group State Machine

```
Empty ──► PreparingRebalance ──► CompletingRebalance ──► Stable
                 ▲                                          │
                 └──────────── member join/leave ───────────┘
                                      │
                                    Dead (all members left)
```

Key mechanisms:
- **Join barrier** — server blocks all JoinGroup responses until every known member has re-joined
- **Session timeout eviction** — unresponsive members are removed and a rebalance is triggered
- **Round-robin assignment** — partitions distributed evenly across group members

---

## Multi-Broker Metadata Flow

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
       │                                         │  TopicRecord   → register topic
       │                                         │  PartitionRecord leader=2
       │                                         │    → create commitlog (I'm leader)
       │                                         │  PartitionRecord leader=1
       │                                         │    → start ReplicaFetcher
```

---

## Produce Path (`acks=-1`)

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
   │                       │ purgatory.CompleteUpTo(HWM)
   │                       │   → wake waiter
   │◄── BaseOffset ────────│
   │
   │ ISRManager tick (every 500ms):
   │   shrink: drop followers older than 10s
   │   if ISR < minISR: purgatory.FailAll → NOT_ENOUGH_REPLICAS_AFTER_APPEND (20)
   │   on change: append PartitionRecord to __cluster_metadata
```

---

## Idempotent Producer Path

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

When the active segment fills:
   commitLog rolls to <newBase>.log
     → SegmentRollHook fires (off the produce path)
     → ProducerStateManager writes <newBase>.producer.snapshot
     → prunes older snapshots, keeping the last 2

On broker restart (per partition):
   ProducerStateManager.Recover:
     → load latest <base>.producer.snapshot ≤ activeBase
     → WalkBatchHeadersFrom(scanFrom, ...) replays the active segment tail,
       calling Idempotence.Record for each batch with PID ≥ 0
```

---

## Compatibility

Tested with [franz-go](https://github.com/twmb/franz-go) v1.20.7.

---

## Roadmap

Things to study and implement in future versions, roughly ordered by learning value:

### Storage
- [ ] **Log retention** — delete or compact old segments by size/time (`log.retention.bytes`, `log.retention.ms`)
- [ ] **Log compaction** — keep only the latest record per key (how Kafka implements compacted topics)
- [ ] **Index file recovery** — rebuild offset index from segment data on unclean shutdown

### Protocol & API
- [ ] **DeleteTopics API** (key 20)
- [ ] **DeleteRecords API** (key 21) — truncate a partition to a given offset
- [ ] **DescribeGroups API** (key 15) — inspect group state and member assignments
- [ ] **ListGroups API** (key 16)
- [ ] **Fetch sessions** (KIP-227) — broker caches partition metadata across fetches to reduce bandwidth
- [ ] **Incremental fetch** — only send changed partitions in subsequent fetch requests

### Replication
- [x] **ISR tracking** — leader-side ticker shrinks/expands ISR by replica fetch lag; min-ISR enforced on produce path
- [ ] **Leader failover** — when the leader broker dies, elect a new leader from the ISR
- [ ] **Follower-side idempotence state** — mirror per-PID dedup ring on followers so failover preserves dedup
- [ ] **OffsetCommit persistence** — survive broker restart without losing consumer group offsets

### Transactions / Exactly-once (Phase C)
- [ ] **`__transaction_state` topic** — durable transaction coordinator state
- [ ] **AddPartitionsToTxn / EndTxn / WriteTxnMarkers** — transaction control APIs
- [ ] **Read-committed isolation** — consumers clip fetches to LSO (last stable offset), skip aborted batches

### KRaft (Kafka Raft Metadata — [KIP-500](https://cwiki.apache.org/confluence/display/KAFKA/KIP-500%3A+Replace+ZooKeeper+with+a+Self-Managed+Metadata+Quorum))

KRaft is the most architecturally interesting part of modern Kafka. Instead of relying on ZooKeeper, the cluster elects a **controller quorum** that runs the Raft consensus algorithm over a replicated **metadata log**. Every broker streams this log to stay up to date on topic, partition, and ISR changes.

- [ ] **Raft consensus** — implement leader election, log replication, and commitment (Raft paper §3–5)
- [ ] **Controller quorum** — a subset of brokers act as Raft voters; one becomes the active controller
- [ ] **Metadata APIs** — `Vote` (key 52), `BeginQuorumEpoch` (key 53), `EndQuorumEpoch` (key 54), `FetchSnapshot` (key 59)
- [ ] **Controller failover** — if the active controller dies, the quorum elects a new one without ZooKeeper

### Consumer Groups
- [ ] **Sticky assignor** — minimize partition movement across rebalances (KIP-54)
- [ ] **Static membership** — avoid unnecessary rebalances on consumer restart (KIP-345)
- [ ] **Group instance ID** — consumer rejoins with the same assignment after restart

### Observability
- [ ] **Prometheus metrics** — `messages_in_total`, `bytes_in_total`, `consumer_lag`, `active_connections`
- [ ] **Structured request logging** — log each API request with latency, error code, client ID

### Security
- [ ] **TLS** — encrypt client-broker connections
- [ ] **SASL PLAIN** — username/password authentication
