# kaf-go — Full System Flow

A complete walkthrough of every subsystem and how they connect, from the moment you start a broker to a consumer receiving a message across two brokers.

**Milestone:** v5 (post-2026-04-27). Covers v2 (multi-broker baseline), v3 (full `acks` semantics + ISR + `min.insync.replicas`), v4 (idempotent producer + durable PID block lease), and v5 (segment-roll producer state snapshots, snapshot-free hot path). Companion specs: `docs/produce_semantics_impl.md`, `docs/producer_snapshot_redesign.md`.

---

## Table of Contents

1. [Architecture Overview](#1-architecture-overview)
2. [Binary Protocol Layer](#2-binary-protocol-layer)
3. [Broker Startup](#3-broker-startup)
4. [Multi-Broker Setup: Peers and Controller Election](#4-multi-broker-setup-peers-and-controller-election)
5. [Broker Registration and Heartbeat](#5-broker-registration-and-heartbeat)
6. [Metadata Log: How Followers Learn Cluster State](#6-metadata-log-how-followers-learn-cluster-state)
7. [Topic Creation](#7-topic-creation)
8. [Producing Messages — `acks` semantics](#8-producing-messages--acks-semantics)
9. [ISR, HWM, Purgatory](#9-isr-hwm-purgatory)
10. [Idempotent Producer (v4)](#10-idempotent-producer-v4)
11. [Producer State Snapshot (v5)](#11-producer-state-snapshot-v5)
12. [Consumer Groups](#12-consumer-groups)
13. [Fetching Messages (Zero-Copy)](#13-fetching-messages-zero-copy)
14. [Storage: CommitLog](#14-storage-commitlog)
15. [Replica Fetching](#15-replica-fetching)
16. [End-to-End: Two Brokers, 100 Records](#16-end-to-end-two-brokers-100-records)

---

## 1. Architecture Overview

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                                kaf-go broker                                 │
│                                                                              │
│  ┌──────────┐    ┌──────────────────────────────────────────────────────┐    │
│  │ TCP :9092│───►│ conn.go: per-connection loop                         │    │
│  └──────────┘    │   ReadFraming → decode RequestHeader                 │    │
│                  │   switch(apiKey) → api handler                       │    │
│                  └─────────────┬────────────────────────────────────────┘    │
│                                │                                             │
│   ┌────────────────────────────┼─────────────────────────────────────────┐   │
│   │                            │                                         │   │
│  ┌▼────────┐  ┌────────────────▼─────┐ ┌──────────┐ ┌──────────────────┐ │   │
│  │TopicStore│ │ PartitionStateStore  │ │GroupStore│ │  BrokerRegistry  │ │   │
│  │          │ │ "topic-N" →          │ │groupId → │ │  brokerId →      │ │   │
│  │topicId → │ │  PartitionState{     │ │GroupState│ │  host:port,epoch │ │   │
│  │CommitLog │ │   LEO,HWM,           │ └──────────┘ └──────────────────┘ │   │
│  │+meta.json│ │   ReplicaLEO,        │                                   │   │
│  └──────────┘ │   LastCaughtUp,      │ ┌────────────────┐                │   │
│               │   ISR,Replicas,      │ │  PidManager    │                │   │
│               │   Idempotence  ──────┼─┤(controller-only│                │   │
│               │   ProducerStateMgr   │ │ block leases   │                │   │
│               │   ProducerStateMgr ──┼─┤ to __cluster_  │                │   │
│               │ }                    │ │ metadata)      │                │   │
│               └──────────┬───────────┘ └────────────────┘                │   │
│                          │                                               │   │
│   ┌──────────────────────┴───────────────────────────────────────────┐   │   │
│   │                                                                  │   │   │
│   │  PartitionPurgatory  ◄── Produce(acks=-1) parks here             │   │   │
│   │   per-partition wait queue, woken by AdvanceHWM                  │   │   │
│   │   FailAll(NOT_ENOUGH_REPLICAS_AFTER_APPEND) on min-ISR violation │   │   │
│   └──────────────────────────────────────────────────────────────────┘   │   │
│                                                                          │   │
│  ┌──────────────┐  ┌────────────────────┐  ┌──────────────────────┐      │   │
│  │ MetadataLog  │  │ ISRManager         │  │ ReplicaFetcherMgr    │      │   │
│  │ commitlog-   │  │ 500 ms ticker      │  │  per-broker          │      │   │
│  │ backed       │  │  shrink (10 s lag) │  │  BrokerClient TCP    │      │   │
│  │ __cluster_   │  │  expand (caught up)│  │  per-partition       │      │   │
│  │ metadata-0/  │  │  publish Partition-│  │  ReplicaFetcher      │      │   │
│  │ +Replay()    │  │  Record on change  │  │  goroutines          │      │   │
│  └──────────────┘  └────────────────────┘  └──────────────────────┘      │   │
│                                                                          │   │
│  Follower-only: BrokerRegistrar (heartbeat 3s) + MetadataFetcher (poll)  │   │
└──────────────────────────────────────────────────────────────────────────────┘
```

### Package map

| Package | What it does |
|---|---|
| `broker/` | Entry point. Parses flags, creates `Server`, calls `Start()`. |
| `server/` | TCP listener, per-connection handler (`conn.go`), server state, ISRManager, ReplicaFetcher(Manager), MetadataFetcher, BrokerRegistrar/Registry. |
| `api/` | One handler per Kafka API key — including `produce_api.go` (acks + idempotence), `init_producer_id_api.go` (key 22). |
| `protocol/` | Binary codec — `Reader`, `Writer`, per-API request/response structs. `protocol/metadata/` defines `BrokerRecord`, `TopicRecord` (v1, with `MinInsyncReplicas`), `PartitionRecord`, `ProducerIdsRecord`. |
| `coordinator/` | `partition_state.go` (LEO/HWM/ReplicaLEO/Idempotence/ProducerStateManager), `partition_purgatory.go`, `idempotence_state.go` (5-slot ring dedup), `pid_manager.go`, `producer_snapshot.go`, `producer_state_manager.go`, `metadata_log.go`, `group_store.go`. |
| `storage/commitlog/` | Append-only log segments + sparse offset index. `clip.go` for HWM/LEO Fetch clipping. `commitlog.go` exposes `SetSegmentRollHook`, `WalkBatchHeadersFrom`, `ActiveSegmentBaseOffset`, `Dir`. |
| `storage/` | `TopicStore`, `TopicMeta` persistence (`meta.json`), `metas` sidecar map for `MinInsyncReplicas`. |
| `constant/` | API keys (incl. 22, 62, 63), error codes (incl. 19/20/45/46/59/90/116). |
| `common/` | `IsFlexible(apiKey, version)` flexible-encoding map. |

---

## 2. Binary Protocol Layer

Every Kafka message over the wire follows the same framing:

```
┌─────────────────────────────────────────────────────────┐
│  [4 bytes]  message_size  (big-endian int32)            │
│  [N bytes]  payload                                     │
│      ├─ RequestHeader  (API key, version, correlation)  │
│      └─ Request body   (API-specific)                   │
└─────────────────────────────────────────────────────────┘
```

The response follows the same framing:

```
┌────────────────────────────────────────────────────────────┐
│  [4 bytes]  message_size                                   │
│  [N bytes]  payload                                        │
│      ├─ ResponseHeader  (correlation_id, [tagged fields]) │
│      └─ Response body                                      │
└────────────────────────────────────────────────────────────┘
```

### Request Header

```
ApiKey          int16
ApiVersion      int16
CorrelationId   int32
ClientId        nullable string
[tagged fields] (only for flexible APIs)
```

### Flexible vs. Non-Flexible Encoding

APIs at or above the version in `common.apiFlexibleVersions` use **compact types**:

- **Compact string**: uvarint(len+1) + bytes — not the usual int16 length
- **Compact array**: uvarint(N+1) — N=0 is sent as `0x01`
- **Tagged fields**: every struct ends with uvarint(count); each field is `(tag_key varint)(length varint)(bytes)`

`common.IsFlexible(apiKey, apiVersion)` returns true when encoding should use compact types.

Almost all implemented APIs are flexible (version ≥ some threshold). The most important ones:

| API | Key | Min flexible version |
|---|---|---|
| Fetch | 1 | 12 |
| Produce | 0 | 9 |
| Metadata | 3 | 9 |
| CreateTopics | 19 | 5 |
| ApiVersions | 18 | 3 |
| InitProducerId | 22 | 2 |
| BrokerRegistration (internal) | 62 | 0 (always flexible) |
| BrokerHeartbeat (internal) | 63 | 0 (always flexible) |

### ResponseHeader and Tagged Fields

The server's `ResponseHeader.Encode` writes an empty tagged-fields byte (`0x00`) for flexible APIs. The client's `ResponseHeader.Decode` must consume it, or every response body is read 1 byte off. `ResponseHeader` carries `ApiKey` and `ApiVersion` so `IsFlexible` can decide whether to read the byte:

```go
func (h *ResponseHeader) Decode(r *Reader) error {
    h.CorrelationId, _ = r.ReadInt32()
    if common.IsFlexible(h.ApiKey, h.ApiVersion) {
        return r.ReadTaggedFields()  // reads 0x00 and discards
    }
    return nil
}
```

### How `conn.go` dispatches requests

```
conn.handle() loop:
  1. ReadFraming(conn) → read 4-byte length prefix, read that many bytes
  2. Decode RequestHeader (apiKey, apiVersion, correlationId, clientId)
  3. switch(apiKey) → call api.HandleXxxApi(conn, server, header, reader)
  4. Each handler decodes request body, does work, calls protocol.WriteFraming(conn, responseHeader, response)
```

---

## 3. Broker Startup

`broker/main.go` parses flags and calls `server.NewServer(...)`, then `server.Start()`.

### `NewServer`

```go
s := &Server{
    addr:                addr,        // ":9092"
    logDir:              logDir,      // "var/log"
    brokerID:            brokerID,    // 1
    topicStore:          NewTopicStore(),
    groupStore:          NewGroupStore(),
    partitionStateStore: NewPartitionStateStore(),
    brokerRegistry:      NewBrokerRegistry(),
    metadataLog:         NewMetadataLog(),
}
s.replicaFetcherManager = NewReplicaFetcherManager(s)
// Register self so IsController() works immediately. AdvertisedHost() returns
// the value of the `-advertised-host` flag, falling back to "localhost".
s.brokerRegistry.RegisterBroker(brokerID, s.AdvertisedHost(), parsePort(addr))
```

`NewServer` takes `(addr, logDir, brokerID, advertisedHost)` — the trailing string is the host this broker tells clients/peers to reach it on. Empty string preserves the legacy `"localhost"` default for single-host dev. In Docker, `broker/main.go` plumbs `-advertised-host=kafN` so `Metadata` / `FindCoordinator` / `BrokerRegistration` responses use the container hostname (reachable across the bridge network).

### `Start()`

```
Start():
  1. initMetadataLog()       ← create __cluster_metadata commitlog + register in TopicStore;
                               wire PartitionState (controller as leader) so Fetch handler
                               accepts replica polls
  2. recoverPidManager()     ← MetadataLog.Replay → scan ProducerIdsRecords → SeedFromLog(maxNextPid)
                               → Configure(brokerID, brokerEpoch, metaLog)
  3. recoverTopic()          ← scan logDir, rebuild TopicStore from disk; for each partition:
                               open commitlog, install segment-roll hook,
                               ProducerStateManager.Recover (load latest <base>.producer.snapshot
                               + walk active segment tail to repopulate IdempotenceState)
  4. net.Listen(tcp, addr)
  5. go acceptLoop()         ← spawns goroutine per connection
  6. go isrManager.Run()     ← 500 ms ticker, shrink/expand ISR for partitions led by self
  7. if !IsController():
       go brokerRegistrar.RegisterWithRetry()
       go metadataFetcher.Run()
```

### `recoverTopic()`

On restart, all previous topics would be lost from memory. `recoverTopic` scans the log directory:

```
logDir/
  orders/
    meta.json                ← TopicMeta: name, topicId, numPartitions
    orders-0/                ← partition 0 commitlog
    orders-1/
    orders-2/
```

For each topic directory it reads `meta.json`, registers the topic in `TopicStore`, opens each partition's `CommitLog`, and calls `partitionStateStore.SetLeader` (assumes self is leader for recovered partitions).

### `initMetadataLog()`

Every broker (controller and follower alike) initializes the metadata commitlog:

```go
dir := filepath.Join(logDir, "__cluster_metadata", "__cluster_metadata-0")
cl := commitlog.NewCommitLog(dir, config)
metadataLog.Init(cl)

// Register __cluster_metadata as a topic in TopicStore so the Fetch handler
// can serve it to followers without any special-casing.
topicStore.RegisterTopic(ClusterMetadataTopicID, "__cluster_metadata", 1)
topicStore.AddCommitLog(ClusterMetadataTopicID, 0, cl)
```

The controller appends to this log. Followers read from it via a standard Fetch request.

---

## 4. Multi-Broker Setup: Peers and Controller Election

```bash
# Broker 1 — controller
/tmp/kaf-broker -broker-id 1 -port 9092 -peers "2=localhost:9093"

# Broker 2 — follower
/tmp/kaf-broker -broker-id 2 -port 9093 -peers "1=localhost:9092"
```

`--peers` is parsed into `map[int32]string` (brokerID → addr). `server.SetPeers(peers)` is called after `NewServer`:

```go
func (s *Server) SetPeers(peers map[int32]string) {
    for id, addr := range peers {
        // Register in BrokerRegistry so IsController() can compute the lowest ID
        s.brokerRegistry.RegisterBroker(id, host, port)
        // Dial for replica fetching (best-effort)
        s.replicaFetcherManager.RegisterBrokerClient(id, addr)
    }

    if !s.IsController() {
        controllerID := s.brokerRegistry.LowestBrokerID()
        controllerAddr := s.brokerRegistry.Addr(controllerID)
        s.brokerRegistrar = NewBrokerRegistrar(s, controllerAddr)
        s.metadataFetcher = NewMetadataFetcher(s, controllerAddr)
    }
}
```

### Controller Election

There is no Raft or ZooKeeper election — the **broker with the lowest ID** is always the controller:

```go
func (s *Server) IsController() bool {
    return s.brokerID == s.brokerRegistry.LowestBrokerID()
}
```

With `--peers "2=localhost:9093"`, broker 1 has IDs {1, 2} in its registry; `LowestBrokerID()` returns 1 = self → broker 1 is the controller.

With `--peers "1=localhost:9092"`, broker 2 has IDs {1, 2}; `LowestBrokerID()` returns 1 ≠ self (2) → broker 2 is a follower.

---

## 5. Broker Registration and Heartbeat

When a follower starts, it needs to tell the controller it exists. This lets the controller write a `BrokerRecord` to the metadata log so all other brokers learn the new broker's address.

### Registration flow

```
Broker 2                                    Broker 1 (controller)
  │                                                  │
  │── TCP connect ──────────────────────────────────►│
  │                                                  │
  │── BrokerRegistration (API key 62) ──────────────►│
  │   { BrokerId: 2, ClusterId: "...",               │
  │     Listeners: [{PLAINTEXT, <adv-host>, 9093}],  │  1. RegisterBroker(2, <adv-host>, 9093)
  │     IncarnationId: <random UUID> }               │  2. epoch = existing.epoch + 1
  │                                                  │  3. MetadataLog.Append(BrokerRecord{2,...}.Encode())
  │◄── BrokerRegistrationResponse ─────────────────-│
  │   { ErrorCode: 0, BrokerEpoch: 2 }              │
  │                                                  │
  │  atomic.StoreInt64(&brokerEpoch, 2)              │
  │  go heartbeatLoop()                              │
```

`<adv-host>` comes from `BrokerRegistrar.ownHostPort()` — when the listener binds `0.0.0.0` (the default), it returns `Server.AdvertisedHost()`, which is the value of `-advertised-host` (or `"localhost"` when unset). In single-host dev `<adv-host>` = `"localhost"`; in the Docker compose setup it's the container hostname (`kaf2`). The controller persists whatever value was sent into the `BrokerRecord` it appends to `__cluster_metadata`, and every follower's `MetadataFetcher` learns the same host string.

`BrokerRegistrar.sendRequest` uses its own raw TCP connection (not the standard `BrokerClient`). It manually frames the request (4-byte length prefix + payload) and reads the response, because the registration happens before the standard client infrastructure is set up.

### Heartbeat loop

Every 3 seconds the follower sends a `BrokerHeartbeat` (API key 63):

```go
req := &BrokerHeartbeatRequest{
    BrokerId:              server.brokerID,
    BrokerEpoch:           atomic.LoadInt64(&server.brokerEpoch),
    CurrentMetadataOffset: metadataFetcher.CurrentOffset(),
    WantFence:             false,
    WantShutDown:          false,
}
```

The controller validates `BrokerEpoch`, updates the broker's `LastSeen` and `MetadataOffset` in the `BrokerRegistry`, and responds with `IsFenced`, `IsCaughtUp`, `ShouldShutDown`.

---

## 6. Metadata Log: How Followers Learn Cluster State

The metadata log is the central mechanism for propagating cluster state from the controller to followers. It uses the exact same storage and fetch path as regular topic data.

### What gets written and when

| Event | Record written |
|---|---|
| Broker registers | `BrokerRecord` (type=0) |
| Topic created | `TopicRecord` (type=1, **wire v1** with `MinInsyncReplicas`) + one `PartitionRecord` per partition (type=2) |
| ISR shrink/expand | `PartitionRecord` (type=2) — leader appends, followers re-apply |
| PID block leased (controller) | `ProducerIdsRecord` (type=3) — written **before** replying to `InitProducerId` so a controller crash cannot re-issue an already-handed-out PID |

### Record encoding

Each record value is a custom binary format (not Kafka's native KRaft format):

```
BrokerRecord (type=0):
  [0x00][0x00]  type + version
  [4]  BrokerId (int32 big-endian)
  [8]  Epoch    (int64)
  [2]  HostLen  (uint16)
  [N]  Host     (UTF-8)
  [4]  Port     (int32)

TopicRecord (type=1, wire v1):
  [0x01][0x01]                      ← version 1 (v0 still decodes; default MinISR=1)
  [16] TopicId  (UUID bytes)
  [4]  NumPartitions (int32)
  [2]  NameLen
  [N]  Name
  [2]  MinInsyncReplicas (int16)    ← v1 trailing field

PartitionRecord (type=2):
  [0x02][0x00]
  [16] TopicId
  [4]  PartitionId
  [4]  Leader         (broker ID)
  [4]  LeaderEpoch
  [4]  NumReplicas
  [4*] Replicas[]
  [4]  NumISR
  [4*] ISR[]

ProducerIdsRecord (type=3):
  [0x03][0x00]
  [4]  BrokerId       (int32)       ← broker that leased the block
  [8]  BrokerEpoch    (int64)
  [8]  NextProducerId (int64)       ← end-exclusive; block is [prevNext, NextProducerId)
```

### RecordBatch wrapping

`MetadataLog.Append(value []byte)` calls `EncodeAsRecordBatch(value)` before writing to the commitlog. This wraps the raw value in a valid Kafka RecordBatch (magic=2, CRC32C, zigzag varint record), so the standard Fetch handler can serve it to followers with zero changes.

```
MetadataLog.Append(value)
  → EncodeAsRecordBatch(value)   // produces valid Kafka RecordBatch bytes
  → commitLog.AppendRaw(batch, 1)
```

### MetadataFetcher on the follower

`MetadataFetcher.Run()` dials the controller, then loops:

```
fetchLoop():
  1. Build FetchRequest for ClusterMetadataTopicID, partition 0, at fetchOffset
  2. Send to controller (same Fetch API as regular data)
  3. Receive FetchResponse
  4. For each non-empty partition response:
       values = DecodeRecordValues(partResp.Records)
       for value in values:
           server.ApplyMetadataRecord(value)
       count = countRecordsInRecordBatches(partResp.Records)
       fetchOffset += count
       currentOffset.Store(fetchOffset)
```

`fetchOffset` starts at 0 and advances by the number of records received. `currentOffset` is read atomically by the heartbeat to report progress to the controller.

### ApplyMetadataRecord on the follower

```go
func (s *Server) ApplyMetadataRecord(value []byte) {
    rec := metadatapkg.Decode(value)  // dispatches by type byte
    switch r := rec.(type) {

    case *BrokerRecord:
        // Learn that broker r.BrokerId exists at r.Host:r.Port
        s.brokerRegistry.RegisterBroker(r.BrokerId, r.Host, r.Port)

    case *TopicRecord:
        // Register the topic in memory (idempotent — no error if already exists)
        s.topicStore.RegisterTopic(r.TopicId, r.Name, r.NumPartitions)
        // Cache MinInsyncReplicas in the sidecar so the produce path can pre-check ISR floor
        s.topicStore.SetTopicMeta(r.TopicId, TopicMeta{MinInsyncReplicas: r.MinInsyncReplicas})
        // Persist meta.json so the topic survives a restart
        storage.SaveTopicMeta(topicDir, TopicMeta{...})

    case *PartitionRecord:
        // Update full partition state: leader, epoch, replicas, ISR
        // Lazy-inits IdempotenceState + ProducerStateManager on the leader-side path
        s.partitionStateStore.SetPartitionState(
            topicName, r.PartitionId, r.Leader, r.LeaderEpoch, r.Replicas, r.ISR,
        )
        if r.Leader == s.brokerID {
            // This broker is the leader: create the commitlog directory,
            // install segment-roll hook, run ProducerStateManager.Recover
            s.createPartitionLog(topicName, r.TopicId, r.PartitionId)
        } else {
            // This broker is a replica: start fetching from the leader
            // Lazy-dials BrokerClient if peer wasn't registered at startup
            s.replicaFetcherManager.AddFetcher(topicName, r.PartitionId, r.Leader)
        }

    case *ProducerIdsRecord:
        // Forward-compat for B4 follower-side dedup: track the high-water PID
        // so a follower promoted to leader doesn't re-issue an already-leased block
        s.pidManager.SeedFromLog(r.NextProducerId)
    }
}
```

### Ordering guarantee

Records are appended in order: `TopicRecord` always appears in the log before its `PartitionRecord`s. When `ApplyMetadataRecord` processes a `PartitionRecord`, the `TopicRecord` has already been applied, so `topicStore.GetTopic(topicId)` always succeeds.

---

## 7. Topic Creation

Topic creation runs only on the controller. The follower learns about the new topic via the metadata log (Section 6).

### Flow

```
Client                              Broker 1 (controller)
  │                                         │
  │── CreateTopics v7 ───────────────────►  │
  │   { name:"orders", numPartitions:3,     │
  │     replicationFactor:2,                │
  │     configs:[                           │
  │       {min.insync.replicas: "2"} ] }    │
  │                                         │
  │                       validateTopics()  │ ← regex check, partition count > 0
  │                       topicId = uuid.NewV7()
  │                       parseTopicConfig: minISR = 2  (default 1)
  │                                         │
  │            AssignReplicas("orders", 3, 2, [1,2]):
  │              orders-0: leader=1, replicas=[1,2], ISR=[1,2]
  │              orders-1: leader=2, replicas=[2,1], ISR=[2,1]
  │              orders-2: leader=1, replicas=[1,2], ISR=[1,2]
  │                                         │
  │                       createTopicStorage(logDir):
  │                         SaveTopicMeta → <logDir>/orders/meta.json
  │                                       (includes MinInsyncReplicas=2)
  │                         topicStore.SetTopicMeta(...) sidecar
  │                         for each partition led by self:
  │                           NewCommitLog(<logDir>/orders/orders-N/)
  │                           SetSegmentRollHook(...)         ← PSM hook
  │                           PartitionState.ProducerStateManager wired
  │                           TopicStore.AddCommitLog(topicId, N, cl)
  │                                         │
  │                       StartFollowerFetch:
  │                         for each partition not led by self:
  │                           ReplicaFetcherManager.AddFetcher(...)
  │                                         │
  │              Append to metadata log:    │
  │                TopicRecord{orders, 3, MinISR=2}     ← offset N
  │                PartitionRecord{orders-0, leader=1, ISR=[1,2]}
  │                PartitionRecord{orders-1, leader=2, ISR=[2,1]}
  │                PartitionRecord{orders-2, leader=1, ISR=[1,2]}
  │                each Append → ps.OnLeaderAppend(baseOffset)
  │                              → AdvanceHWM → followers can fetch
  │                                         │
  │◄── CreateTopicsResponse ───────────────-│
  │    { TopicId: ..., ErrorCode: 0 }       │
```

**Ordering bug fixed in v3:** `topicStore.AddTopic` + `createTopicStorage` MUST run **before** `StartFollowerFetch` — earlier ordering broke `ReplicaFetcherManager.AddFetcher`'s `GetTopicByName` lookup. (See `docs/produce_semantics_impl.md` Phase A "Multi-broker infrastructure fixes".)

### Replica assignment: round-robin with shift

```go
for partition := 0; partition < numPartitions; partition++ {
    replicas := make([]int32, replicationFactor)
    for i := 0; i < replicationFactor; i++ {
        brokerID := brokerIDs[(partition + i) % len(brokerIDs)]
        replicas[i] = brokerID
    }
    // replicas[0] is the leader
}
```

With brokers [1, 2] and 3 partitions:
- partition 0: `(0+0)%2 = 0` → broker 1 (leader)
- partition 1: `(1+0)%2 = 1` → broker 2 (leader)
- partition 2: `(2+0)%2 = 0` → broker 1 (leader)

### On-disk layout after CreateTopics

```
var/log/
  orders/
    meta.json               { name, topicId, numPartitions:3, replicationFactor:1 }
    orders-0/
      00000000000000000000.log
      00000000000000000000.index
    orders-1/
    orders-2/
  __cluster_metadata/
    __cluster_metadata-0/
      00000000000000000000.log    ← contains TopicRecord + 3 PartitionRecords
```

---

## 8. Producing Messages — `acks` semantics

The Produce handler honours all three `acks` values and (for idempotent producers) dedups retries before they reach the log. The full hot path:

```
HandleProduceApiKeys (api/produce_api.go):
  for each topic, partition:
    ps = partitionStateStore.GetPartitionState(topic, partition)
    if ps == nil || ps.LeaderBrokerID != myBrokerID:
        → NOT_LEADER_FOR_PARTITION (6)

    batch       = DecodeRecordBatch(rawBytes)
    pid         = batch.ProducerId            // -1 if non-idempotent
    epoch       = batch.ProducerEpoch
    firstSeq    = batch.BaseSequence
    lastSeq     = firstSeq + batch.LastOffsetDelta

    # ─── Idempotence pre-check (v4) ────────────────────────────
    if pid >= 0 && ps.Idempotence != nil:
        cachedBase, isDup, err = ps.Idempotence.Validate(
            pid, epoch, firstSeq, lastSeq)
        if err  → return GetErrorId(err)      // 45/46/59/90
        if isDup → return BaseOffset = cachedBase   // NO append, dup short-circuit

    # ─── min.insync.replicas pre-check (acks=-1 only) ──────────
    if acks == -1:
        minISR = topicStore.GetMinInsyncReplicas(topicId)
        if len(ps.ISR) < minISR:
            → NOT_ENOUGH_REPLICAS (19)

    # ─── Local append ──────────────────────────────────────────
    cl = topicStore.GetCommitLog(topicId, partition)
    baseOffset, lastOffset = cl.AppendRaw(rawBytes, recordCount)
    ps.OnLeaderAppend(lastOffset)             // bumps LEO, AdvanceHWM
    if pid >= 0:
        ps.Idempotence.Record(pid, epoch, firstSeq, lastSeq, baseOffset)

    # ─── Reply per `acks` ──────────────────────────────────────
    switch acks:
      case 0:    suppress response entirely
      case 1:    return BaseOffset = baseOffset (immediately)
      case -1:
        waiter = PartitionAppendWaiter{
                   RequiredOffset: lastOffset+1,
                   ISRSnapshot:    ps.ISR,
                   Deadline:       now + RequestTimeout,
                 }
        ps.Purgatory.Add(waiter)
        ps.Purgatory.CompleteUpTo(ps.GetHWM())   # tryCompleteElseWatch race-drain
        select {
          case <-waiter.Done:    return BaseOffset = baseOffset
          case <-deadline:        return REQUEST_TIMED_OUT (7)
          # FailAll(NOT_ENOUGH_REPLICAS_AFTER_APPEND=20) on ISR-shrink-below-minISR
        }
```

### `acks` value summary

| `acks` | Reply when | Failure modes |
|---|---|---|
| `0` | never (fire-and-forget) | client never knows |
| `1` | leader has written locally | leader crash before replication = data loss |
| `-1` / `all` | every replica in current ISR has fetched past `lastOffset` | `REQUEST_TIMED_OUT` (7), `NOT_ENOUGH_REPLICAS` (19) pre-check, `NOT_ENOUGH_REPLICAS_AFTER_APPEND` (20) post-park |

### The `tryCompleteElseWatch` race

Between `ps.OnLeaderAppend(lastOffset)` (which advances HWM) and `Purgatory.Add(waiter)`, all replicas might have *already* fetched past `lastOffset`. The waiter would never wake without an explicit drain. `Purgatory.CompleteUpTo(ps.GetHWM())` immediately after `Add` closes this race. Without it, single-broker `acks=-1` always times out.

### Why min-ISR-after-append doesn't delete records

`NOT_ENOUGH_REPLICAS_AFTER_APPEND` (20) revokes the **acknowledgement** but the bytes stay on disk at `baseOffset`. The producer's idempotent retry (same PID, same `BaseSequence`) hits `Idempotence.Validate` → returns the cached `BaseOffset` without re-appending. That's why this two-phase semantic is safe: data + idempotence dedup together preserve correctness across the broken acknowledgement.

### Leader check

Before any of the above runs, the handler verifies `ps.LeaderBrokerID == myBrokerID`. If not, it returns `NOT_LEADER_FOR_PARTITION` (6); the client fetches new metadata and retries against the correct broker.

### `AppendRaw` semantics

The produce request carries `Records` as raw bytes (a full Kafka RecordBatch as sent by the client). `AppendRaw` writes those bytes directly to the segment file and patches the `BaseOffset` field (bytes 0–7) to reflect the committed offset in the log. **Followers also call `AppendRaw`** with the bytes they fetched from the leader — preserving the leader's exact CRC, BaseSequence, ProducerId, etc., so idempotence headers remain valid for failover.

---

## 9. ISR, HWM, Purgatory

This section deepens the moving parts behind `acks=-1`. The fixed point: **the leader is the only writer of HWM**, advanced as a function of follower fetch progress.

### 9.1 PartitionState fields (`coordinator/partition_state.go`)

```
LEO            int64                       // next offset to write (exclusive)
HWM            int64                       // first uncommitted offset; consumer-visible boundary
Replicas       []int32                     // full replica set
ISR            []int32                     // in-sync subset
ReplicaLEO     map[int32]int64             // last fetchOffset reported by each replica
LastCaughtUp   map[int32]time.Time         // last moment replica was at LEO
Idempotence    *IdempotenceState           // per-(PID, partition) dedup ring
ProducerStateManager *ProducerStateManager // segment-roll snapshot writer
```

### 9.2 HWM advancement formula

```go
func (ps *PartitionState) AdvanceHWM() {
    newHWM := ps.LEO
    for _, replicaID := range ps.ISR {
        if replicaID == ps.LeaderBrokerID { continue }
        if leo, ok := ps.ReplicaLEO[replicaID]; ok && leo < newHWM {
            newHWM = leo
        }
    }
    if newHWM > ps.HWM {                       // monotonic — never decreases
        ps.HWM = newHWM
        ps.Purgatory.CompleteUpTo(ps.HWM)      // wake acks=-1 waiters
    }
}
```

**Why monotonic?** An ISR shrink that drops a slow replica must not retroactively *raise* HWM in a way that changes durability semantics; conversely a follower briefly leaving and rejoining must never *lower* HWM (would un-commit acked records).

**Triggers:**
- `OnLeaderAppend(lastOffset)` after producer write — bumps LEO, then `AdvanceHWM`.
- `UpdateReplicaLEO` from a replica fetch — `AdvanceHWM`.
- `ISRManager` shrink/expand — recomputes after ISR mutation.

### 9.3 Leader-side accounting on each replica fetch (`api/fetch_api.go`)

```go
isReplicaFetch := req.ReplicaState.ReplicaId > 0
if isReplicaFetch {
    ps.UpdateReplicaLEO(replicaId, fetchOffset)   // 1) record this replica's LEO
    maxVisible = ps.LEO                           //   followers may see uncommitted
} else {
    maxVisible = ps.HWM                           //   consumers see only committed
}
records = ClipRegionToOffset(region, maxVisible)
```

`UpdateReplicaLEO` stamps `LastCaughtUp[replicaId] = now()` iff `fetchOffset >= LEO`. The act of fetching offset N is the proof "I have through N-1" — there is no separate ACK message.

### 9.4 ISRManager (`server/isr_manager.go`)

500 ms ticker on the leader. For each partition `p` led by self:

```
shrink:
    for replica in ISR(p):
        if now - LastCaughtUp[replica] > 10s:           # replica.lag.time.max.ms
            ISR(p) -= replica;  changed = true

expand:
    for replica in Replicas(p) \ ISR(p):
        if ReplicaLEO[replica] >= HWM:
            ISR(p) += replica;  changed = true

if changed:
    metadataLog.Append(PartitionRecord{ISR: new})       # propagates via MetadataFetcher
    AdvanceHWM(p)
    if len(ISR) < minInSyncReplicas:
        Purgatory.FailAll(NOT_ENOUGH_REPLICAS_AFTER_APPEND)  # 20
```

**Asymmetry:** shrink uses **time** (offset lag is bursty under GC/large batches; "are you talking?" is the robust signal). Expand uses **offset** (only re-add when actually caught up to HWM).

### 9.5 PartitionPurgatory (`coordinator/partition_purgatory.go`)

Per-partition wait queue for `acks=-1` produce requests:

```
Add(waiter)                  → enqueue PartitionAppendWaiter{RequiredOffset, ISRSnapshot, Deadline, Done}
CompleteUpTo(hwm)            → close(waiter.Done) for every waiter where RequiredOffset ≤ hwm
FailAll(err)                 → close every Done with err set; used on min-ISR violation
```

Generalises to Kafka's `DelayedProduce` operation. Each waiter carries its own deadline; a goroutine reaper not implemented — handlers race their own `time.After` against `<-waiter.Done`.

---

## 10. Idempotent Producer (v4)

The Produce path dedups retries from idempotent clients (`enable.idempotence=true` in franz-go), backed by per-partition state plus a durable PID lease in `__cluster_metadata`. Full spec: `docs/produce_semantics_impl.md` §4.

### 10.1 Per-(PID, partition) dedup state (`coordinator/idempotence_state.go`)

One `IdempotenceState` per partition; lazy-init in `SetPartitionState` / `AssignReplicas` / `SetLeader`. `Producers map[int64]*ProducerBatchLog` keyed by PID.

```go
type ProducerBatchLog struct {
    LastSeq         int32
    LastOffset      int64
    LastEpoch       int16
    LastLeaderEpoch int32        // KIP-101 placeholder
    LastTimestamp   int64
    Ring            [5]ProducerRingSlot   // most recent 5 batches: (FirstSeq, LastSeq, BaseOffset)
}
```

5 slots matches `max.in.flight.requests.per.connection ≤ 5` — the maximum unacked batches per producer/partition.

### 10.2 `Validate(pid, epoch, firstSeq, lastSeq)` rules

| Condition | Outcome |
|---|---|
| Unknown PID + `firstSeq != 0` | `UNKNOWN_PRODUCER_ID` (59) — KIP-360 trigger to re-`InitProducerId` |
| `epoch < LastEpoch` | `INVALID_PRODUCER_EPOCH` (46) — fenced |
| `epoch > LastEpoch` requires `firstSeq == 0` | else error 90; on accept, reset entry |
| `firstSeq == LastSeq + 1` | fresh, proceed |
| `firstSeq == 0 AND LastSeq == math.MaxInt32` | sequence wrap, proceed |
| `firstSeq <= LastSeq` AND in ring | **dup, return cached BaseOffset, no append** |
| `firstSeq <= LastSeq` AND not in ring | `DUPLICATE_SEQUENCE_NUMBER` (45) |
| `firstSeq > LastSeq + 1` | `OUT_OF_ORDER_SEQUENCE_NUMBER` (90) |

Lazy 1-hour expiration (`producer.id.expiration.ms`): every `Validate` checks `now - LastTimestamp > 1h` and drops the entry if so, falling into the unknown-PID path.

### 10.3 `InitProducerId` (API key 22) and PidManager

`coordinator/pid_manager.go` is **block-based**: each broker leases blocks of `DefaultPidBlockSize = 1000`. `AllocateOne` increments within the block; on exhaustion `leaseNextBlockLocked` appends a `ProducerIdsRecord` to `__cluster_metadata` **before** returning the new block. Persistence-before-reply guarantees a controller crash cannot re-issue an already-handed-out PID.

`api/init_producer_id_api.go`:
- Returns `NOT_CONTROLLER` (41) on non-controllers; client retries against the controller.
- `TransactionalId != nil` → `INVALID_REQUEST` (Phase C will route to txn coordinator).

### 10.4 Recovery on startup (`server.recoverPidManager`)

Runs after `initMetadataLog`:

```
MetadataLog.Replay(applyFn):
    walk __cluster_metadata via FindRecords + DecodeRecordValues
    for each ProducerIdsRecord r:
        maxSeen = max(maxSeen, r.NextProducerId)

pidManager.SeedFromLog(maxSeen)
pidManager.Configure(brokerID, brokerEpoch, metaLog)
```

Followers also seed via `ApplyMetadataRecord` — forward-compat for B4 follower-side dedup once leader election lands.

### 10.5 End-to-end flow

```
Client (idempotent franz-go)              Broker (controller)
  │── InitProducerId(txnId=null) ─────────►│ leaseNextBlockLocked (if needed):
  │                                        │   append ProducerIdsRecord{NextPid+=1000}
  │                                        │   to __cluster_metadata BEFORE reply
  │◄── (PID=N, epoch=0) ───────────────────│
  │                                        │
  │── Produce(PID=N, seq=0..K) ───────────►│ Validate: unknown PID + seq=0 → accept
  │                                        │ AppendRaw → baseOffset=B
  │                                        │ Idempotence.Record (ring slot stamped)
  │                                        │ OnLeaderAppend (LEO bump, AdvanceHWM)
  │◄── BaseOffset=B ───────────────────────│
  │                                        │
  │── Produce(retry same batch) ──────────►│ Validate: seq=0 ≤ LastSeq AND in ring
  │◄── BaseOffset=B (dup short-circuit) ───│ NO second AppendRaw

[broker restart]
                                            recoverPidManager: seed from __cluster_metadata
                                            recoverTopic / createPartitionLog:
                                              ProducerStateManager.Recover (see §11)
```

---

## 11. Producer State Snapshot (v5)

Source of truth for `IdempotenceState` is the **RecordBatches on disk** (every batch carries `ProducerId`, `ProducerEpoch`, `BaseSequence`, `LastOffsetDelta`). Snapshots are a recovery cache, written async on segment roll. v4's per-batch write-through scheme is retired; the produce hot path is now snapshot-free. Full spec: `docs/producer_snapshot_redesign.md`.

### 11.1 Trigger — segment roll

```go
// storage/commitlog/commitlog.go AppendRaw / Append
rolled := false
var rolledBase int64
if c.activeSegment.IsFull() {
    newSegment, _ = NewSegment(c.dir, c.activeSegment.NextOffset(), c.config)
    c.segments = append(c.segments, newSegment)
    c.activeSegment = newSegment
    rolled = true; rolledBase = newSegment.BaseOffset
}
// … append, update nextOffset … (still under c.mu)

if rolled && c.onSegmentRoll != nil {
    hook := c.onSegmentRoll
    go hook(rolledBase)            // run AFTER releasing c.mu — never block produce
}
```

`ProducerStateManager.OnSegmentRoll(newBase)`:
1. `state.Snapshot()` — copy current `Producers` map.
2. `WriteProducerSnapshot(dir, newBase, entries)` — open, write, **`f.Sync()`**, close, `os.Rename`.
3. `pruneOldSnapshots` — keep `defaultRetainSnapshots = 2` newest.

### 11.2 File format

Filename: `<20-digit-base>.producer.snapshot` (e.g. `00000000000001000000.producer.snapshot`).

```
[magic:    uint32 = 0x4B41464B "KAFK"]
[version:  uint16 = 1]
[snapshot_offset: int64]                     ← cross-check against filename
[count:    uint32]
per entry × count:
  [PID:int64][LastEpoch:int16][LastSeq:int32]
  [LastOffset:int64][LastLeaderEpoch:int32][LastTimestamp:int64]
  [RingLen:uint8]
  RingLen × {FirstSeq:int32, LastSeq:int32, BaseOffset:int64, LeaderEpoch:int32}
[crc32c:   uint32]                           ← Castagnoli, over magic..last ring slot
```

Atomic write via `.tmp` + `f.Sync()` + `os.Rename`. Half-written files are never visible to a Restore. Trailing CRC32C catches torn-write corruption.

### 11.3 Recovery (`coordinator/producer_state_manager.go:Recover`)

```
activeBase = cl.ActiveSegmentBaseOffset()
snapBase, entries = LoadLatestProducerSnapshot(dir, activeBase)   # picks highest base ≤ activeBase
if entries != nil: state.Restore(entries)

scanFrom = max(snapBase, activeBase)
cl.WalkBatchHeadersFrom(scanFrom, func(h SnapshotBatchHeader):
    if h.ProducerId < 0: skip                    # non-idempotent batch
    firstSeq = h.BaseSequence
    lastSeq  = firstSeq + h.LastOffsetDelta
    state.Record(h.ProducerId, h.ProducerEpoch, firstSeq, lastSeq, h.BaseOffset))
```

`WalkBatchHeadersFrom` reads a fixed **57-byte** prefix per batch via `ReadAt` (BaseOffset, BatchLength, LastOffsetDelta, ProducerId, ProducerEpoch, BaseSequence) — no record-body decode. Stops cleanly when `pos+totalLen > fileSize` (torn tail-write tolerance).

### 11.4 Crash safety

| Crash window | Outcome |
|---|---|
| Between segment roll and snapshot write | Recover loads previous snapshot, tail-replay covers the gap. No data loss; longer scan. |
| Mid-snapshot-write (before rename) | `.tmp` file may dangle; canonical file never half-formed. Next roll overwrites tmp. |
| Power loss between rename metadata commit and data writeback | `f.Sync()` before rename + CRC32C trailer = corrupt file rejected by Load, fall back to older snapshot. |
| Mid-write of active segment (torn last batch) | `walkBatchHeaders` ignores trailing bytes < 57 + body; followers re-fetch and re-append on restart. |

### 11.5 Performance (validated 2026-04-27)

- 35,949 msg/s produce throughput, p50 = 86 µs, p99 = 319 µs (5000 × 1 KiB × 4 workers × 3 partitions, 100 MB segments — zero `*.producer.snapshot` files written, no roll triggered).
- v4 → v5 produce path: **−2 syscalls per batch**, **−1 lock-acquire per batch** (was Validate+Record+Snapshot, now Validate+Record), **0** O(N) map-walk encodes.

### 11.6 Migration from v4

Legacy `producer_state.snapshot` (single file, no numeric prefix) is silently ignored by `LoadLatestProducerSnapshot` (only matches `*.producer.snapshot` with a numeric prefix). On v5 first boot: tail-rebuild fully repopulates state from the active segment. No manual ops step.

---

## 12. Consumer Groups

Consumer groups allow multiple consumers to share the work of reading a topic. The broker coordinates which consumer reads which partition.

### Full state machine

```
Client                              Broker (coordinator)
  │                                         │
  │── FindCoordinator ──────────────────── ►│ returns NodeId = 1 (hardcoded: self)
  │                                         │
  │── JoinGroup (memberId="") ────────────► │ → MEMBER_ID_REQUIRED + generated memberId
  │── JoinGroup (memberId=<generated>) ──► │
  │                                         │ group state: Empty → PreparingRebalance
  │                                         │ wait for all members (rebalanceTimeout)
  │                                         │ first member = leader
  │◄── JoinGroupResponse ──────────────────-│
  │    { leaderId, generationId,            │
  │      members: [...] (leader only),      │  group state: CompletingRebalance
  │      protocolName: "roundrobin" }       │
  │                                         │
  │── SyncGroup ──────────────────────────► │ leader sends assignments
  │   (leader sends partition assignments)  │ followers send empty assignments
  │◄── SyncGroupResponse ──────────────────-│ all members get their assignment
  │    { assignment: [partition 0, 2] }     │  group state: Stable
  │                                         │
  │── Heartbeat every 3s ─────────────────► │ → OK
  │                                         │   if REBALANCE_IN_PROGRESS → new member joined
  │                                         │
  │── OffsetFetch ────────────────────────► │ → committed offsets (-1 if none)
  │                                         │
  │── Fetch (loop) ───────────────────────► │ → records
  │                                         │
  │── OffsetCommit ───────────────────────► │ → persists offset in GroupStore
  │                                         │
  │── LeaveGroup ─────────────────────────► │ → triggers rebalance for remaining members
```

### GroupStore

```go
type GroupStore struct {
    groups  map[string]*GroupState    // groupId → state
    offsets map[string]map[string]int64  // groupId → "topic:partition" → offset
}
```

Group state machine:
- `Empty` → `PreparingRebalance` (first JoinGroup)
- `PreparingRebalance` → `CompletingRebalance` (all members joined or timeout)
- `CompletingRebalance` → `Stable` (all SyncGroup received)
- `Stable` → `PreparingRebalance` (member joined or left, heartbeat timeout)

### JoinGroup blocking

`HandleJoinGroupApi` blocks the handler goroutine until either all expected members have joined or `rebalanceTimeout` elapses. Other connections are served on their own goroutines so this doesn't block the server.

### OffsetCommit / OffsetFetch

Offsets are stored in memory under `groupId → "topicName:partition" → offset`. There is no persistence to disk yet (restarting the broker resets all consumer offsets to -1, causing consumers to restart from the beginning).

---

## 13. Fetching Messages (Zero-Copy)

The Fetch handler is the most performance-critical path. It avoids copying record data from disk into Go heap memory by using `io.Copy` with a `SectionReader` directly from the segment file to the TCP socket.

### Fetch handler flow

```
HandleFetchApiKeys:
  isReplicaFetch = req.ReplicaState.ReplicaId > 0
  for each topic/partition requested:
    ps = partitionStateStore.GetPartitionState(topic, partition)
    cl = topicStore.GetCommitLog(topicId, partition)
    region = cl.FindRecords(fetchOffset, maxBytes)
    ← region is a {file, fileOffset, fileSize, LEO, LogStart}

    if isReplicaFetch:
        ps.UpdateReplicaLEO(replicaId, fetchOffset)   # leader-side accounting
        maxVisible = ps.LEO                            # followers see uncommitted
    else:
        maxVisible = ps.HWM                            # consumers see only committed
    region = ClipRegionToOffset(region, maxVisible)    # storage/commitlog/clip.go

  Build response in segments:
    [header bytes] [in-memory]
    [partition meta bytes — incl. HighWatermark, LogStartOffset] [in-memory]
    [record bytes] [file region — zero-copy]
    [partition post-records bytes] [in-memory]
    ...

  Write:
    [4-byte frame size]
    [ResponseHeader bytes]
    for each segment:
      if file: io.Copy(tcpConn, SectionReader(file, offset, size))
      if bytes: conn.Write(bytes)
```

`ClipRegionToOffset` walks the 61-byte RecordBatch headers (no body decode) and trims `region.Size` to include only batches whose `lastOffset < maxVisible` — strict less-than because HWM is the **first uncommitted** offset.

### Why zero-copy matters

Without zero-copy, the path is: disk → kernel buffer → Go heap → kernel socket buffer → NIC. With `io.Copy` on a `TCPConn`, the OS can use `sendfile(2)` on Linux (or `sendfile` on macOS), reducing it to: disk → kernel buffer → NIC, with no userspace copy.

### `FindRecords(fetchOffset, maxBytes)`

```
commitLog.FindRecords(offset, maxBytes):
  1. Binary-search the sparse offset index to find the segment that contains `offset`
  2. Within the segment, scan forward from the indexed position to find the exact
     batch boundary for `offset`
  3. Return RecordsRegion{
         File:       segment file descriptor,
         FileOffset: byte position in file,
         Size:       min(remaining bytes in segment, maxBytes),
         LEO:        log end offset,
         LogStart:   oldest offset in log,
     }
```

The caller sends `region.Size` bytes starting at `region.FileOffset` of `region.File` directly over the network.

---

## 14. Storage: CommitLog

### On-disk layout

```
orders-0/
  00000000000000000000.log    ← segment starting at offset 0
  00000000000000000000.index  ← sparse offset index for this segment
  00000000000000000134.log    ← next segment (if first is full)
  00000000000000000134.index
```

Segment file names are the **base offset** of the first record in the segment, zero-padded to 20 digits.

### Segment

Each `.log` file is a sequence of Kafka RecordBatches written back-to-back:

```
[RecordBatch 0]   ← baseOffset=0,  batchLength=..., records...
[RecordBatch 1]   ← baseOffset=34, batchLength=..., records...
[RecordBatch 2]   ← baseOffset=67, ...
```

`AppendRaw` patches the `baseOffset` field (bytes 0–7 of the batch) to the current `nextOffset` before writing.

### Sparse offset index

The `.index` file stores `(logicalOffset, fileByteOffset)` pairs. An entry is written every `IndexInterval` bytes (default 4 KB). This means finding a specific offset requires:

1. Binary-search the index for the largest entry ≤ target offset
2. Seek to that file position in the `.log` file
3. Scan forward batch-by-batch until the target offset is found

This keeps index files small (sparse) while still giving O(log N + scan) reads.

### Segment rotation

When the active segment exceeds `SegmentMaxBytes` (default 100 MB), `AppendRaw` creates a new segment named after `nextOffset`.

---

## 15. Replica Fetching

When a broker's `ApplyMetadataRecord` finds a `PartitionRecord` where `r.Leader != s.brokerID`, it calls `ReplicaFetcherManager.AddFetcher(topicName, partitionId, leaderID)`.

### ReplicaFetcherManager — lazy peer dialing

`brokerClients map[int32]*BrokerClient` is keyed by leader broker ID. `AddFetcher`:

1. Look up `brokerClients[leaderID]`.
2. If missing, `dialBrokerClientLocked` — `net.Dial` to the peer's `host:port` from `BrokerRegistry`, store the client.
3. Spawn a `ReplicaFetcher` goroutine bound to that BrokerClient + this partition.

This handles the **peer-up-late ordering**: if broker B starts before broker A and a topic is created on A first, B's `AddFetcher` initially has no BrokerClient for A. Lazy dial fixes it without a startup barrier.

### ReplicaFetcher

Each `ReplicaFetcher` runs in its own goroutine (one per partition this broker follows) and continuously:

```
for:
  req = FetchRequest{
    topic: topicId, partition: partition,
    fetchOffset: rf.fetchOffset,
    logStartOffset: commitLog.OldestOffset(),
    MaxWaitMs:    500,                          # long-poll
    ReplicaState: {ReplicaId: myBrokerID, ReplicaEpoch: -1},
  }
  resp = client.Fetch(req)             ← blocks up to 500 ms on leader
  if err: sleep 500 ms; continue
  for each partition response:
    if records not empty:
      count = countRecordsInRecordBatches(records)
      baseOffset = commitLog.AppendRaw(records, count)        # bytes-as-is, no re-encode
      rf.fetchOffset = baseOffset + count
      rf.highWatermark = partResponse.HighWatermark            # advisory; not used for visibility
```

Two subtleties:

1. **`AppendRaw` not `Append`** — followers preserve the leader's exact byte layout (CRC, BaseOffset, ProducerId, BaseSequence). Re-encoding would invalidate idempotence headers and break dedup after failover.
2. **Followers compute no HWM.** The leader is the single writer of HWM. Followers receive HWM in fetch responses purely as advisory state (used for log truncation in real Kafka; not yet implemented here).

### Leader side — what makes the loop work

The Fetch handler on the leader (Section 13) is what closes the replication loop:

- `ps.UpdateReplicaLEO(replicaId, fetchOffset)` — records the follower's LEO.
- If `fetchOffset >= LEO`: stamp `LastCaughtUp[replicaId] = now()` (only place liveness is refreshed; ISRManager reads this).
- `AdvanceHWM` — recompute `HWM = min(LEO across ISR)`; if HWM moved, drain Purgatory.

`ReplicaState.ReplicaId > 0` is the discriminator for "this is a follower fetch, not a consumer fetch" — drives `maxVisible = LEO` (vs `HWM` for consumers) and the LEO bookkeeping above.

### countRecordsInRecordBatches

Parses raw RecordBatch bytes to count total records across all batches. Needed because `AppendRaw` requires `recordCount` to advance `nextOffset` correctly.

```
Batch layout:
  [0..7]   BaseOffset  (int64)
  [8..11]  BatchLength (int32)
  ...
  [57..60] NumRecords  (int32, big-endian)

pos = 0
while pos < len(raw):
  batchLen   = BigEndian.Uint32(raw[pos+8 : pos+12])
  numRecords += BigEndian.Uint32(raw[pos+57 : pos+61])
  pos += 12 + batchLen   # 12 = BaseOffset(8) + BatchLength(4)
```

### Two replication pipelines side-by-side

| Aspect | Control plane — `MetadataFetcher` | Data plane — `ReplicaFetcher` |
|---|---|---|
| Source partition | `__cluster_metadata` (controller-led) | each user partition (leader-led) |
| Goroutines | 1 per follower broker | **1 per partition** the broker follows |
| Payload | `BrokerRecord`, `TopicRecord`, `PartitionRecord`, `ProducerIdsRecord` | raw RecordBatches (user data) |
| On apply | mutate `BrokerRegistry`, `TopicStore`, `PartitionStateStore`, `pidManager` | `commitLog.AppendRaw(rawBytes, count)` |
| Leader-side accounting | none (read-only) | `UpdateReplicaLEO` + `AdvanceHWM` |
| Triggered by | non-controller startup | `PartitionRecord` apply where `leader != self` |

**Why per-partition fetchers (not per-broker)?** Backpressure isolation. One slow partition (large batches, slow disk) cannot stall replication of unrelated partitions sharing the same leader.

### countRecordsInRecordBatches

This function parses raw RecordBatch bytes to count the total number of records across all batches. It's needed because `AppendRaw` requires a `recordCount` parameter to advance `nextOffset` correctly.

```
Batch layout:
  [0..7]   BaseOffset  (int64)
  [8..11]  BatchLength (int32)
  ...
  [57..60] NumRecords  (int32, big-endian)

pos = 0
while pos < len(raw):
  batchLen = BigEndian.Uint32(raw[pos+8 : pos+12])
  numRecords += BigEndian.Uint32(raw[pos+57 : pos+61])
  pos += 12 + batchLen   // 12 = BaseOffset(8) + BatchLength(4)
```

The `NumRecords` field is at a fixed offset within the batch header (bytes 57–60 from the batch start). Reading it correctly requires big-endian byte order.

---

## 16. End-to-End: Two Brokers, 100 Records

This section traces the complete flow of the two-broker smoke test.

### Setup

```
Broker 1  (brokerID=1, port=19092)  ← controller (lowest ID)
Broker 2  (brokerID=2, port=19093)  ← follower
```

### Phase 1: Startup

```
t=0  Broker 1 starts
     initMetadataLog() → __cluster_metadata commitlog
     BrokerRegistry = {1: localhost:19092, 2: localhost:19093}
     IsController() = true → no MetadataFetcher started

t=0  Broker 2 starts
     initMetadataLog() → __cluster_metadata commitlog (empty, will be filled by fetching)
     BrokerRegistry = {1: localhost:19092, 2: localhost:19093}
     IsController() = false
     → BrokerRegistrar.RegisterWithRetry() goroutine
     → MetadataFetcher.Run() goroutine
```

### Phase 2: Registration

```
t=0.1  Broker 2 dials broker 1 port 19092
       sends BrokerRegistration{BrokerId:2, Host:localhost, Port:19093}

t=0.1  Broker 1 receives BrokerRegistration
       → RegisterBroker(2, localhost, 19093) → epoch=2
       → MetadataLog.Append(BrokerRecord{2, localhost, 19093, epoch=2}.Encode())
       → __cluster_metadata offset 0 now contains BrokerRecord for broker 2
       → responds {BrokerEpoch: 2}

t=0.1  Broker 2 stores brokerEpoch=2, starts heartbeatLoop()
```

### Phase 3: MetadataFetcher catches up

```
t=0.1  Broker 2 MetadataFetcher sends Fetch for __cluster_metadata offset 0
t=0.1  Broker 1 Fetch handler:
         cl = topicStore.GetCommitLog(ClusterMetadataTopicID, 0)
         region = cl.FindRecords(0, 10MB)
         → returns region pointing to the BrokerRecord batch
       → FetchResponse with 1 record batch

t=0.1  Broker 2 MetadataFetcher receives response:
         DecodeRecordValues(raw) → [BrokerRecord{2,...} bytes]
         ApplyMetadataRecord(value):
           BrokerRegistry.RegisterBroker(2, localhost, 19093)
         fetchOffset = 0 + 1 = 1
         currentOffset.Store(1)
       LOG: "metadata fetcher: applied records count=1 fetchOffset=1"
```

### Phase 4: Topic Creation

```
t=1  Client connects to broker 1 port 19092
     sends CreateTopics{name:"orders", numPartitions:3, replicationFactor:1}

t=1  Broker 1:
     knownBrokers = [1, 2]
     AssignReplicas("orders", 3, 1, [1,2]):
       orders-0: leader=1
       orders-1: leader=2
       orders-2: leader=1

     createTopicStorage():
       SaveTopicMeta → var/log/orders/meta.json
       NewCommitLog(orders-0/) → AddCommitLog(topicId, 0, cl0)
       NewCommitLog(orders-1/) → AddCommitLog(topicId, 1, cl1)
       NewCommitLog(orders-2/) → AddCommitLog(topicId, 2, cl2)

     MetadataLog.Append(TopicRecord{orders, 3}.Encode())    → offset 1
     MetadataLog.Append(PartitionRecord{0, leader=1}.Encode()) → offset 2
     MetadataLog.Append(PartitionRecord{1, leader=2}.Encode()) → offset 3
     MetadataLog.Append(PartitionRecord{2, leader=1}.Encode()) → offset 4

     → CreateTopicsResponse{TopicId: ..., ErrorCode: 0}
```

### Phase 5: Broker 2 applies topic records

```
t=1.1  Broker 2 MetadataFetcher sends Fetch for __cluster_metadata offset 1
t=1.1  Broker 1 responds with 4 record batches (offsets 1–4)

t=1.1  Broker 2 applies each:
         TopicRecord{orders, 3}:
           TopicStore.RegisterTopic(topicId, "orders", 3)
           storage.SaveTopicMeta(var/log/orders/meta.json)
         PartitionRecord{0, leader=1}:
           SetPartitionState("orders", 0, 1, ...)
           leader=1 ≠ myBrokerID(2) → ReplicaFetcherManager.AddFetcher("orders", 0, 1)
         PartitionRecord{1, leader=2}:
           SetPartitionState("orders", 1, 2, ...)
           leader=2 == myBrokerID(2) → createPartitionLog("orders", topicId, 1)
             NewCommitLog(var/log/orders/orders-1/)
             AddCommitLog(topicId, 1, cl)
             SetLeader("orders", 1, 2)
         PartitionRecord{2, leader=1}:
           SetPartitionState("orders", 2, 1, ...)
           leader=1 ≠ myBrokerID(2) → ReplicaFetcherManager.AddFetcher("orders", 2, 1)

       fetchOffset = 1 + 4 = 5
       LOG: "metadata fetcher: applied records count=4 fetchOffset=5"
       LOG: "metadata: partition log created (this broker is leader) topic=orders partition=1"
```

### Phase 6: Client produces 100 records (idempotent, acks=-1, RF=2, minISR=2)

```
t=2.0  Client sends InitProducerId(txnId=null) → broker 1 (controller)
       PidManager.AllocateOne:
         block exhausted? leaseNextBlockLocked:
           append ProducerIdsRecord{NextPid=1000} to __cluster_metadata
           BEFORE replying
         return PID=0, epoch=0

t=2.1  Client sends Metadata for "orders" → broker 1
       responds:
         brokers: [{1, localhost:19092}, {2, localhost:19093}]
         topics:  [{orders, MinISR=2,
                    part0: leader=1, ISR=[1,2],
                    part1: leader=2, ISR=[2,1],
                    part2: leader=1, ISR=[1,2]}]

t=2.2  Client routes (idempotent client uses sticky partitioner):
         ~34 records → broker 1 (partitions 0, 2)
         ~33 records → broker 2 (partition 1)

t=2.3  Broker 1 receives Produce(acks=-1) for part0:
         Idempotence.Validate(pid=0, epoch=0, seq=0..33) → unknown PID + seq=0 → OK
         len(ISR=[1,2]) >= MinISR=2 → OK
         cl0.AppendRaw → baseOffset=0, lastOffset=33
         OnLeaderAppend(33) → LEO=34, HWM still 0 (replica 2 hasn't caught up)
         Idempotence.Record({0..33, base=0})
         Purgatory.Add(waiter{RequiredOffset:34})
         Purgatory.CompleteUpTo(0)  # tryCompleteElseWatch race-drain

t=2.4  Broker 2's ReplicaFetcher for part0 polls broker 1 at offset 0
       Broker 1 Fetch handler:
         maxVisible = ps.LEO = 34  (replica fetch)
         UpdateReplicaLEO(2, 0)
       returns 34 records with BaseOffset=0

t=2.5  Broker 2 ReplicaFetcher: AppendRaw(records, 34); fetchOffset=34
       polls broker 1 again at offset 34:
       Broker 1 Fetch handler:
         UpdateReplicaLEO(2, 34) → 34 >= LEO=34 → LastCaughtUp[2]=now
         AdvanceHWM: min(LEO=34, ReplicaLEO[2]=34) = 34 → HWM=34
         Purgatory.CompleteUpTo(34) → wakes waiter (RequiredOffset=34)
       (long-poll returns empty)

t=2.6  Broker 1 sends Produce response for part0:
         BaseOffset=0 → client receives ack

t=2.7  Same flow for part2 on broker 1, part1 on broker 2 (with broker 1 as the
       follower for part1). All three partitions ack within ~milliseconds.

t=2.8  ISRManager (running on each leader every 500 ms):
         shrink: LastCaughtUp[2] is fresh → no change
         expand: nothing to add
         (no PartitionRecord written)

t=3.0  Producer flushes & exits. PID=0 entry stays in Idempotence map (1h TTL).
```

**On-disk state after Phase 6:**
```
<logDir>/orders/orders-0/00000000000000000000.log     (34 records, broker 1 + 2)
<logDir>/orders/orders-1/00000000000000000000.log     (33 records, broker 2 + 1)
<logDir>/orders/orders-2/00000000000000000000.log     (33 records, broker 1 + 2)
                                                       (no producer.snapshot files —
                                                        no segment roll triggered)
<logDir>/__cluster_metadata/__cluster_metadata-0/00000000000000000000.log
   contains: BrokerRecord(2), TopicRecord(orders, MinISR=2),
             PartitionRecord ×3, ProducerIdsRecord(NextPid=1000)
```

### Phase 7: Consumer fetches all records

```
t=3  Consumer client:
     FindCoordinator → broker 1 (NodeId=1)
     JoinGroup → gets assignment [part0, part2] (or [part1] etc. depending on balance)
     SyncGroup → confirmed assignment
     OffsetFetch → {part0: -1, part1: -1, part2: -1} (no committed offsets)

     Fetch loop:
       Fetch(part0, offset=0) → broker 1 → 34 records (zero-copy from orders-0/)
       Fetch(part1, offset=0) → broker 2 → 33 records (zero-copy from orders-1/)
       Fetch(part2, offset=0) → broker 1 → 33 records (zero-copy from orders-2/)
       received = 100

     OffsetCommit → stores {part0: 34, part1: 33, part2: 33}
     LeaveGroup

     Output: "done fetching: 100 records"
```

---

## Key design decisions

### Why is the controller just the broker with the lowest ID?

No consensus protocol (Raft, Paxos) is implemented. Determining the controller requires every broker to agree — and since every broker knows all other broker IDs from `--peers` at startup, the "lowest ID wins" rule produces the same answer on every node without any coordination messages. In real Kafka KRaft, the controller is elected via Raft. Adding Raft is a long-term study target.

### Why is the metadata log backed by a commitlog?

The Fetch API handler reads directly from the `TopicStore`'s commitlog map. Registering `__cluster_metadata` in the topic store with `ClusterMetadataTopicID` means followers can fetch metadata records using the same standard Fetch request that clients use for regular data — no special handler needed.

### Why encode metadata records as RecordBatches?

The Fetch API sends raw bytes from the commitlog to the client. If metadata records weren't wrapped in proper RecordBatch framing, the client (MetadataFetcher) would receive malformed data. Wrapping them in `EncodeAsRecordBatch` makes the bytes valid on both ends.

### Why zero-copy for Fetch?

The Fetch path is on the critical latency/throughput path. Copying data from disk into Go heap to then write it back to the socket wastes CPU and memory bandwidth. `io.Copy(tcpConn, SectionReader(...))` lets the OS use `sendfile` to transfer directly from the page cache to the socket buffer.

### Why tagged fields at the end of every struct?

Kafka's flexible encoding appends a tagged-fields section to every message and array element. This allows future versions to add new optional fields without breaking older decoders. Even if there are no tagged fields, a single `0x00` byte is written to indicate an empty tag map. Forgetting to read this byte offsets all subsequent field reads by 1 byte — which was the bug in `ResponseHeader.Decode`.

### Why is HWM advancement monotonic?

A committed record cannot become uncommitted. ISR shrinks (slow follower dropped) must not retroactively raise HWM in a way that breaks durability semantics; conversely a follower briefly leaving and rejoining must not lower HWM. `HWM = max(HWM, min(LEO across ISR))` is the correct invariant.

### Why does ISRManager use time-based shrink and offset-based expand?

Offset lag is bursty (large batches, GC pauses, transient slowdowns) — using it to shrink would flap the ISR. "Are you still talking to me?" (LastCaughtUp time) is the robust liveness signal. For expand, only re-add when the follower is *actually* caught up to HWM — re-adding a not-yet-caught-up replica would let HWM drop on the next AdvanceHWM call, violating monotonicity.

### Why does the leader expose LEO (not HWM) to followers on Fetch?

Followers must mirror everything the leader has, including not-yet-committed records. If the leader served only HWM-clipped data to followers, no follower could ever catch up to LEO, so HWM would never advance — deadlock. Replica fetches see LEO; consumer fetches see HWM.

### Why does `min.insync.replicas` violation revoke the ack but not delete records?

Two-phase semantics: data is durable as soon as `AppendRaw` returns; the acknowledgement is a separate signal carried by the produce response. Decoupling lets the producer retry without losing the on-disk write. Idempotence dedup (Section 10) ensures the retry returns the original `BaseOffset` without re-appending — preserving exactly-once-per-partition correctness even across `NOT_ENOUGH_REPLICAS_AFTER_APPEND` errors.

### Why is `InitProducerId` block-leased and persisted before reply?

Per-PID allocation would require a metadata-log append on every PID handout — too expensive. Blocks of 1000 amortise the persistence cost. Persistence-before-reply is non-negotiable: a controller crash between reply and append would re-issue an already-handed-out PID on next startup, violating the unique-PID invariant idempotence relies on.

### Why a 5-slot dedup ring per (PID, partition)?

Matches `max.in.flight.requests.per.connection ≤ 5` — the maximum unacked batches per producer/partition. A larger ring wastes memory; a smaller ring would mis-classify legitimate retries as `DUPLICATE_SEQUENCE_NUMBER` (45) under high in-flight depth.

### Why `UNKNOWN_PRODUCER_ID` (59) instead of `INVALID_PRODUCER_EPOCH` for unknown PIDs?

Per KIP-360, an unknown PID with `firstSeq != 0` is a recoverable error: it means the broker has no state for this producer (broker restart with snapshot loss, log retention dropped the producer's history, etc.). Returning 59 tells the client to re-run `InitProducerId` and start a fresh sequence — non-fatal. `INVALID_PRODUCER_EPOCH` (46) is fatal and means a newer producer instance has fenced this one.

### Why is the producer state snapshot written on segment roll, not per batch?

The RecordBatches on disk already carry every field needed to rebuild `IdempotenceState` (`ProducerId`, `ProducerEpoch`, `BaseSequence`, `LastOffsetDelta`). The snapshot is purely a recovery cache, not the source of truth. Writing per batch (v4 design) cost 2 syscalls + O(N active PIDs) map walk + O(N) re-encode on every produce — pure overhead, since the data could always be reconstructed by tail-walking the active segment. v5 writes async on segment roll only, with `f.Sync() + Rename` for crash safety and CRC32C for torn-write detection.

### Why `f.Sync()` before `os.Rename` for the snapshot?

`os.Rename` is atomic on POSIX for *metadata* — the rename either happens or doesn't. But the file *contents* may not yet be flushed to stable storage at the time of rename. After power loss between rename-metadata-commit and data-writeback, the rename is durable but the file body is zeros. `f.Sync()` before rename forces the data out first; CRC32C in the trailer is the second line of defence against filesystem-level torn writes.

### Why per-partition ReplicaFetcher goroutines, not per-broker?

Backpressure isolation. One slow partition (large batch, slow disk on the leader, GC pause) cannot stall replication of unrelated partitions sharing the same leader. Per-broker fetchers would batch all partitions into one Fetch RPC — head-of-line blocking by the slowest partition.

---

## What is NOT implemented (v5)

Be honest about the boundaries — these are deliberate study-scope limits, not bugs.

- **Leader failover.** When a leader broker dies, no new leader is elected from the ISR; the partition becomes unavailable. Real Kafka uses controller-driven leader election (`Replicas[0]` preferred from ISR; falls back per `unclean.leader.election.enable`).
- **Follower-side `IdempotenceState` (B4).** Leader-only today. Once partition leader election lands, the replication path must mirror `Idempotence.Record` on followers (see `// TODO(failover)` in `server/replica_fetcher.go`) or dedup state evaporates on failover.
- **Transactions / exactly-once (Phase C).** No `__transaction_state`, no `AddPartitionsToTxn` / `EndTxn` / `WriteTxnMarkers`, no `read_committed` LSO clipping. `InitProducerId` rejects `TransactionalId != nil` with `INVALID_REQUEST`.
- **Truncation on log divergence.** If a follower's tail diverges from the leader (would happen after unclean election), no truncation logic. Needs KIP-101 leader-epoch-based offset-for-leader-epoch lookup.
- **OffsetCommit persistence.** Consumer offsets are in-memory only; lost on broker restart.
- **Follower-side `__cluster_metadata` persistence.** Followers apply records to in-memory state but don't write their own metadata commitlog. On restart they re-sync from offset 0.
- **Deterministic `AssignReplicas` ordering.** `GetAllBrokerIDs()` iterates a map (random order); partition→leader assignment is non-deterministic between runs.
- **Log retention and compaction.**
- **SASL / TLS.**
- **Fetch sessions (KIP-227).**
- **KRaft / Raft consensus** (long-term study target — controller is currently lowest-broker-ID).
- **Snapshot-on-shutdown.** A clean shutdown doesn't fire `OnSegmentRoll` for the active segment, so restart does a longer tail scan than strictly necessary. Correctness is unaffected; pure recovery latency cost.
