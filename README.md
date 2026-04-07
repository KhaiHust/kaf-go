# kaf-go

> **Personal learning project.** I use Kafka at work every day but only ever as a black box. This is my attempt to actually understand it — by building it from scratch.

---

## What I am learning

I started with a simple question: *what actually happens when a producer sends a message?*

Answering it properly meant implementing the binary wire protocol, then the commit log, then consumer groups, and so on — each piece revealing why Kafka is designed the way it is. The broker now speaks real Kafka protocol and can be driven by an unmodified franz-go client.

The long-term target is **KRaft** ([KIP-500](https://cwiki.apache.org/confluence/display/KAFKA/KIP-500%3A+Replace+ZooKeeper+with+a+Self-Managed+Metadata+Quorum)) — Kafka's Raft-based consensus layer that replaced ZooKeeper in Kafka 3.3. Getting there requires mastering all the broker internals first, which is what this project is building toward.

### Lessons so far

- **Binary protocol** — writing the codec by hand (framing, compact types, tagged fields, version negotiation) makes the wire format concrete. You can't fake understanding it.
- **Consumer group rebalancing** — implementing the join barrier and session-timeout eviction made it immediately clear why rebalances are disruptive and what actually triggers them in production.
- **Commit log** — append-only segments with an offset index explain the sequential-write performance and how consumers can seek without scanning.
- **Zero-copy fetch** — routing records straight from a file descriptor to the socket via `io.Copy` on `*net.TCPConn` shows exactly what the OS-level optimization is and why it matters for fetch throughput.
- **Protocol debugging** — decoding raw TCP bytes against the Kafka spec to find a single misplaced tagged-field read is slow, tedious, and teaches you more than any documentation would.

---

## What is implemented (v1)

- **Kafka wire protocol** — binary framing, compact types, tagged fields, flexible API versions
- **Producer** — record batches, Snappy compression
- **Consumer groups** — full lifecycle: FindCoordinator → JoinGroup → SyncGroup → Heartbeat → OffsetFetch → OffsetCommit → LeaveGroup
- **Zero-copy fetch** — record data sent directly from disk to socket via `io.Copy` on `*net.TCPConn`
- **Persistent commit log** — log segments with offset-indexed lookup, topic recovery on restart
- **13 Kafka APIs** implemented

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

---

## Getting Started

### Prerequisites

- Go 1.21+

### Run the broker

```bash
go run ./broker
```

Listens on `:9092`, stores log data under `var/log/`.

### Run the test client

```bash
# Produce 100 messages, then consume with a group of 3
go run ./cmd/client
go run ./cmd/consumer --consumers 3 --topic orders --group orders-group
```

---

## Project Structure

```
.
├── broker/             # Entry point
├── api/                # Kafka API handlers (one file per API)
├── protocol/           # Wire protocol encoding/decoding
│   ├── admin/          # ApiVersions, Metadata, ListOffsets
│   ├── consumer/       # JoinGroup, SyncGroup, Heartbeat, Offset*
│   ├── fetch/          # Fetch request/response
│   ├── producer/       # Produce request/response, record batches
│   ├── topic/          # CreateTopics request/response
│   └── types/          # Compact types, varints, compression
├── coordinator/        # Consumer group state machine
├── storage/
│   └── commitlog/      # Log segments, offset index, message encoding
├── server/             # TCP server, connection handler
├── constant/           # API keys, error codes, group states
└── cmd/
    ├── client/         # Test producer + consumer
    └── consumer/       # Multi-consumer group test tool
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
- [ ] **Multi-broker setup** — multiple broker nodes, each with a unique broker ID
- [ ] **Leader election** — elect a partition leader among replicas
- [ ] **ISR (In-Sync Replicas)** — followers replicate from leader, track lag, join/leave ISR
- [ ] **Follower fetch** — replicas use the Fetch API internally to replicate data

### KRaft (Kafka Raft Metadata — [KIP-500](https://cwiki.apache.org/confluence/display/KAFKA/KIP-500%3A+Replace+ZooKeeper+with+a+Self-Managed+Metadata+Quorum))

KRaft is the most architecturally interesting part of modern Kafka. Instead of relying on ZooKeeper, the cluster elects a **controller quorum** that runs the Raft consensus algorithm over a replicated **metadata log**. Every broker streams this log to stay up to date on topic, partition, and ISR changes.

- [ ] **Raft consensus** — implement leader election, log replication, and commitment (Raft paper §3–5)
- [ ] **Metadata log** — a special internal topic (`__cluster_metadata`) that stores all cluster state as records
- [ ] **Controller quorum** — a subset of brokers act as Raft voters; one becomes the active controller
- [ ] **Metadata APIs** — `Vote` (key 52), `BeginQuorumEpoch` (key 53), `EndQuorumEpoch` (key 54), `FetchSnapshot` (key 59)
- [ ] **Broker registration** — brokers register with the active controller on startup (`BrokerRegistration` key 62)
- [ ] **Metadata fetch** — brokers fetch incremental metadata updates from the controller (`FetchSnapshot`, `Fetch` on metadata log)
- [ ] **Controller failover** — if the active controller dies, the quorum elects a new one without ZooKeeper

### Consumer Groups
- [ ] **Sticky assignor** — minimize partition movement across rebalances (KIP-54)
- [ ] **Static membership** — avoid unnecessary rebalances on consumer restart (KIP-345)
- [ ] **Group instance ID** — consumer rejoins with the same assignment after restart

### Reliability
- [ ] **Integration test suite** — produce N messages, consume in group of M, assert zero message loss
- [ ] **Benchmark** — measure produce/consume throughput and p99 latency
- [ ] **Graceful rebalance on shutdown** — send LeaveGroup before closing so the group rebalances immediately

### Observability
- [ ] **Prometheus metrics** — `messages_in_total`, `bytes_in_total`, `consumer_lag`, `active_connections`
- [ ] **Structured request logging** — log each API request with latency, error code, client ID

### Security
- [ ] **TLS** — encrypt client-broker connections
- [ ] **SASL PLAIN** — username/password authentication
