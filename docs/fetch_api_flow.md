# Fetch API — Detailed Flow

## Overview

The Fetch API (ApiKey = 1) is how a Kafka consumer reads messages from a broker.
This document traces every step from a raw TCP byte arriving at the broker to
record bytes being sent back to the client via `sendfile(2)`.

```
Client                              Broker
  │                                    │
  │── TCP: [4-byte len][payload] ──────▶│  1. Network framing
  │                                    │  2. Request header decode
  │                                    │  3. Fetch request decode
  │                                    │  4. CommitLog lookup per partition
  │                                    │  5. OffsetIndex lookup (mmap binary search)
  │                                    │  6. FindRawRegion (KAFKA-18989 algorithm)
  │                                    │  7. Build ioSegment list
  │◀─ TCP: [4-byte len][payload] ───────│  8. Stream response (sendfile for records)
```

---

## Step 1 — Network Framing (`server/conn.go`, `protocol/wireframe.go`)

```
[4 bytes: payload length (big-endian int32)]
[N bytes: payload]
```

`Conn.handle()` loops forever on the connection, calling `ReadFraming` each iteration:

```go
// server/conn.go
for {
    framing, err := protocol.ReadFraming(c.conn)  // blocks until full frame arrives
    reader := protocol.NewReader(framing)
    requestHeader.Decode(reader)
    c.HandleRequest(apiKey, &requestHeader, reader)
}
```

`ReadFraming` (`protocol/wireframe.go`):
1. Reads exactly 4 bytes → parses as `uint32` payload length
2. Allocates `[]byte` of that length
3. Reads exactly that many bytes with `io.ReadFull`
4. Returns the complete payload — all subsequent parsing is done in memory

**Goroutine model**: each accepted TCP connection gets its own goroutine
(`server/server.go` `acceptLoop`). Requests on the same connection are
processed sequentially within that goroutine.

---

## Step 2 — Request Header Decode (`protocol/header.go`)

The first bytes of every Kafka request are the request header:

```
[ApiKey:       int16  (2 bytes)]
[ApiVersion:   int16  (2 bytes)]
[CorrelationId: int32 (4 bytes)]
[ClientId:     nullable string (2-byte len + bytes, -1 = null)]
[TAG_BUFFER:   uvarint (only if flexible version)]   ← Fetch v12+ is flexible
```

```go
// protocol/header.go
func (h *RequestHeader) Decode(r *Reader) error {
    h.ApiKey        = r.ReadInt16()
    h.ApiVersion    = r.ReadInt16()
    h.CorrelationId = r.ReadInt32()
    h.ClientId      = r.ReadNullableString()
    if common.IsFlexible(h.ApiKey, h.ApiVersion) {
        r.ReadTaggedFields()   // skip TAG_BUFFER (uvarint count + each tag)
    }
}
```

`IsFlexible` (`common/version.go`) checks the `apiFlexibleVersions` map:
Fetch becomes flexible at **v12**. Since we serve **v18**, the header always
includes a TAG_BUFFER.

After decode, `apiKey = 1` routes to `HandleFetchApiKeys`.

---

## Step 3 — Fetch Request Decode (`protocol/fetch/fetch_request.go`)

The remaining bytes in the reader are the Fetch request body (v18 wire format):

```
[MaxWaitMs:       int32]
[MinBytes:        int32]
[MaxBytes:        int32]
[IsolationLevel:  int8 ]
[SessionId:       int32]
[SessionEpoch:    int32]
[Topics:          compact array]
  [TopicId:       UUID  (16 bytes)]
  [Partitions:    compact array]
    [Partition:           int32]
    [CurrentLeaderEpoch:  int32]
    [FetchOffset:         int64]   ← "start reading from here"
    [LastFetchedEpoch:    int32]
    [LogStartOffset:      int64]
    [PartitionMaxBytes:   int32]   ← max bytes per partition
    [TAG_BUFFER]
      [tag 0: ReplicaDirectoryId  UUID ]
      [tag 1: HighWatermark       int64]
  [TAG_BUFFER]  ← topic level
[ForgottenTopics: compact array]
[RackId:          compact string]
[TAG_BUFFER]      ← request level
  [tag 0: ClusterId     compact nullable string]
  [tag 1: ReplicaState  int32 + int64          ]
```

**Compact array encoding**: length is `uvarint(N+1)` — e.g. an array of 3 items
encodes the length as `uvarint(4)`. Empty array = `uvarint(1)`. Null = `uvarint(0)`.

The two key fields per partition:
- `FetchOffset` — the consumer's current position; broker reads from here
- `PartitionMaxBytes` — upper bound on record bytes for this partition

---

## Step 4 — CommitLog Lookup (`api/fetch_api.go`, `storage/topic.go`)

```go
// api/fetch_api.go
cl := topicStore.GetCommitLog(t.TopicId, part.Partition)
```

`TopicStore.GetCommitLog` (`storage/topic.go`) is a simple map lookup:

```go
key := fmt.Sprintf("%s-%d", topicId.String(), partition)
return t.commitLogs[key]   // map[string]commitlog.ICommitLog
```

- Returns `nil` if the topic/partition is unknown → `ErrorCode = 3`
  (`UNKNOWN_TOPIC_OR_PARTITION`)
- Returns the `ICommitLog` instance for that partition otherwise

Each `ICommitLog` corresponds to one physical directory on disk:
`{logDir}/{topicName}/{topicName}-{partitionIndex}/`

---

## Step 5 — FindRecords (`storage/commitlog/commitlog.go`)

```go
region, err := cl.FindRecords(part.FetchOffset, int64(part.PartitionMaxBytes))
```

```go
// storage/commitlog/commitlog.go
func (c *commitLog) FindRecords(fetchOffset int64, maxBytes int64) (*RecordsRegion, error) {
    c.mu.RLock()
    defer c.mu.RUnlock()

    if fetchOffset >= c.nextOffset {
        return nil, nil   // consumer is caught up — nothing to return
    }

    segIdx := c.findSegmentByOffset(fetchOffset)   // binary search over segments
    seg := c.segments[segIdx]

    filePos, size, err := seg.FindRawRegion(fetchOffset, maxBytes)

    return &RecordsRegion{
        File:       seg.logFile,
        FileOffset: filePos,
        Size:       size,
        LEO:        c.nextOffset,       // Log End Offset
        LogStart:   c.segments[0].BaseOffset,
    }, nil
}
```

### `findSegmentByOffset` — segment binary search

Each `Segment` has a `BaseOffset`. The segments slice is always sorted by
`BaseOffset`. Binary search finds the rightmost segment whose `BaseOffset <=
fetchOffset`:

```go
func (c *commitLog) findSegmentByOffset(offset int64) int {
    left, right := 0, len(c.segments)-1
    result := -1
    for left <= right {
        mid := (left + right) / 2
        if c.segments[mid].BaseOffset <= offset {
            result = mid
            left = mid + 1
        } else {
            right = mid - 1
        }
    }
    return result
}
```

Example: segments with BaseOffsets `[0, 1000, 2000]`, fetchOffset = `1500`
→ returns index `1` (segment starting at offset 1000).

---

## Step 6 — FindRawRegion (`storage/commitlog/segment.go`)

This is the KAFKA-18989 algorithm. It locates the starting batch within the
segment file and accumulates subsequent batches up to `maxBytes`, **without
reading record data into userspace**.

### Phase A — OffsetIndex lookup (mmap binary search)

```go
indexPos, err := s.offsetIndex.Lookup(fetchOffset)
```

`OffsetIndex` (`storage/commitlog/index.go`) is memory-mapped with `mmap(2)`.
Each entry is 8 bytes:

```
[relativeOffset: int32 (4 bytes)]   = absoluteOffset - segmentBaseOffset
[filePosition:   int32 (4 bytes)]   = byte position in the .log file
```

Binary search finds the floor entry (largest `relativeOffset <= fetchOffset`).
Returns a file position that is at or before the target batch — not necessarily
exact because the index is sparse (one entry per `indexInterval` bytes, default 4 KB).

### Phase B — Batch header scan (KAFKA-18989 fast path + slow path)

Starting from the file position returned by the index, scan forward reading
only **17 bytes** (fast path) or **27 bytes** (slow path) per batch — never
the record data itself.

**RecordBatch wire layout**:

```
[BaseOffset:      int64   8 bytes]  ← byte 0
[BatchLength:     int32   4 bytes]  ← byte 8   (= total bytes after this field)
[PartLeaderEpoch: int32   4 bytes]  ← byte 12
[Magic:           int8    1 byte ]  ← byte 16
[CRC:             int32   4 bytes]  ← byte 17
[Attributes:      int16   2 bytes]  ← byte 21
[LastOffsetDelta: int32   4 bytes]  ← byte 23  ← slow path reads up to here
[FirstTimestamp:  int64   8 bytes]
[MaxTimestamp:    int64   8 bytes]
...
[Records]
```

`totalLen = 8 (BaseOffset) + 4 (BatchLength field) + BatchLength value`

```go
// Fast path: read 17 bytes
curr := s.readBatchMeta17(pos)   // reads BaseOffset + BatchLength + Magic

if curr.baseOffset >= fetchOffset {
    // fetchOffset is within the previous batch (preMeta) or this batch
    startPos = preMeta.filePos OR curr.filePos
    break
}

// Slow path: read 10 more bytes to get LastOffsetDelta
lastOffset := s.readLastOffset(curr)   // reads CRC + Attributes + LastOffsetDelta
// lastOffset = baseOffset + LastOffsetDelta

if lastOffset >= fetchOffset {
    startPos = curr.filePos
    break
}

preMeta = curr
pos = curr.filePos + curr.totalLen   // advance to next batch
```

**Why preMeta?** The fast path triggers when `nextBatch.baseOffset >= fetchOffset`.
This means `fetchOffset` is inside `preMeta` (the batch before `nextBatch`).
So `startPos = preMeta.filePos`.

### Phase C — Accumulate batches up to maxBytes

```go
pos = startPos
accumulated := int64(0)
for pos+17 <= fileSize {
    curr := s.readBatchMeta17(pos)

    // Always include at least one batch (even if it alone exceeds maxBytes)
    if maxBytes > 0 && accumulated > 0 && accumulated+curr.totalLen > maxBytes {
        break
    }

    accumulated += curr.totalLen
    pos += curr.totalLen
}
```

Returns `(startPos, accumulated, nil)` — a byte range in the log file, no data read.

---

## Step 7 — Build ioSegment List (`api/fetch_api.go`)

The response is assembled as an ordered list of `ioSegment`s:

```go
type ioSegment struct {
    data       []byte   // in-memory bytes
    file       *os.File // file region (zero-copy)
    fileOffset int64
    fileSize   int64
}
```

A single `protocol.Writer w` encodes all non-record bytes. Each time a file
region needs to be inserted, `snapshot()` is called:

```go
snapshot := func() {
    cp := make([]byte, len(w.Bytes()))
    copy(cp, w.Bytes())
    segs = append(segs, ioSegment{data: cp})
    w.Reset()
}
```

**Segment sequence for one topic, one partition with records**:

```
segs[0]: bytes  → [response header fields] + [uvarint(N+1)]   ← records length prefix
segs[1]: file   → [N bytes in log file]                        ← sendfile
segs[2]: bytes  → [0x00 partition tagged fields]
                  [0x00 topic tagged fields]
                  [response footer tagged fields]
```

For a partition with **no records** (caught up, or unknown):
everything stays in a single bytes segment (`uvarint(0)` for null records).

### Total frame size computation

Before writing anything to the socket, the total payload is computed by summing
all segment sizes. This is required because the Kafka framing protocol needs the
4-byte length prefix before any payload bytes:

```go
totalPayload := int64(0)
for i := range segs { totalPayload += segs[i].size() }

binary.BigEndian.PutUint32(frameSz[:],
    uint32(int64(len(rhdrBytes)) + totalPayload))
```

---

## Step 8 — Stream Response to Client (`api/fetch_api.go`)

### Response wire format (Fetch v18)

```
[4 bytes:  frame size (big-endian uint32)]
─── Kafka Response Header ───────────────
[4 bytes:  CorrelationId]
[1 byte:   TAG_BUFFER (0x00, empty)]      ← v18 is flexible → header v1
─── FetchResponse body ──────────────────
[4 bytes:  ThrottleTimeMs]
[2 bytes:  ErrorCode]
[4 bytes:  SessionId]
[uvarint:  topics array length (N+1)]
  [16 bytes: TopicId (UUID)]
  [uvarint:  partitions array length (N+1)]
    [4 bytes:  PartitionIndex]
    [2 bytes:  ErrorCode]
    [8 bytes:  HighWatermark]
    [8 bytes:  LastStableOffset]
    [8 bytes:  LogStartOffset]
    [uvarint:  AbortedTransactions length (1 = empty)]
    [4 bytes:  PreferredReadReplica]
    [uvarint:  COMPACT_RECORDS length (N+1)]  ◄── snapshot() before here
    [N bytes:  record data]                   ◄── sendfile(2)
    [1 byte:   partition TAG_BUFFER (0x00)]
  [1 byte:   topic TAG_BUFFER (0x00)]
[response-level TAG_BUFFER]
  (NodeEndpoints tagged field, if any)
```

### Sending segments

```go
tc, isTCP := conn.(*net.TCPConn)
for _, s := range segs {
    if s.file != nil {
        if isTCP {
            sr := io.NewSectionReader(s.file, s.fileOffset, s.fileSize)
            io.Copy(tc, sr)   // → sendfile(2) syscall
        } else {
            // non-TCP fallback: read into buf then write
        }
    } else {
        conn.Write(s.data)
    }
}
```

`io.Copy(TCPConn, SectionReader)` triggers the Go runtime to call `sendfile(2)`:
the kernel copies bytes directly from the file's page cache to the socket send
buffer. No userspace allocation, no copy through `read()` + `write()`.

---

## Data Flow Diagram

```
TCP bytes arrive
      │
      ▼
ReadFraming()           ← 4-byte length prefix → allocate + io.ReadFull
      │
      ▼
RequestHeader.Decode()  ← ApiKey=1, ApiVersion=18, CorrelationId, ClientId
      │
      ▼
FetchRequest.Decode()   ← MaxWaitMs, MinBytes, MaxBytes, Topics[], ForgottenTopics[], ...
      │                    per partition: FetchOffset, PartitionMaxBytes
      │
      ▼
topicStore.GetCommitLog(topicId, partition)
      │
      ├── nil → ErrorCode=3 (UNKNOWN_TOPIC_OR_PARTITION), skip
      │
      └── ICommitLog
            │
            ▼
      FindRecords(fetchOffset, maxBytes)
            │
            ├── fetchOffset >= nextOffset → nil (nothing to read)
            │
            └── findSegmentByOffset()     ← binary search over segments[]
                  │
                  ▼
            FindRawRegion(fetchOffset, maxBytes)
                  │
                  ├── offsetIndex.Lookup()    ← mmap binary search → file position
                  │
                  ├── Phase B: scan batch headers (17 or 27 bytes per batch)
                  │     fast path:  nextBatch.baseOffset >= fetchOffset
                  │     slow path:  read LastOffsetDelta → compute lastOffset
                  │
                  └── Phase C: accumulate batches up to maxBytes
                        │
                        └── RecordsRegion{File, FileOffset, Size, LEO, LogStart}
                              │
                              ▼
                        ioSegment list:
                          [bytes: header + uvarint(N+1)]
                          [file:  log file @ offset, size N]
                          [bytes: tagged fields + footer]
                              │
                              ▼
                        conn.Write(frameSz)
                        conn.Write(responseHdrBytes)
                        conn.Write(segs[0].data)
                        sendfile(2): segs[1].file → socket
                        conn.Write(segs[2].data)
```

---

## Key Design Decisions

### Why `snapshot()` instead of writing records into `pr.Records`?

Loading records into memory requires allocating `region.Size` bytes per partition.
For a consumer fetching 1 MB batches, this means 1 MB heap allocation per fetch
request. With `sendfile(2)`, the kernel handles the transfer directly from the
page cache — no heap allocation for the records at all.

### Why split `EncodePartitionPreRecords` / `EncodePartitionPostRecords`?

The wire format requires the record bytes to be **inside** the partition response,
between the `COMPACT_RECORDS` length prefix and the partition tagged fields.
This means we cannot encode the full partition response as one contiguous buffer
if we want zero-copy for the records. Splitting the encode at the records
boundary lets the segments interleave correctly.

### Why `io.NewSectionReader`?

`sendfile(2)` needs `ReadFrom` or `WriteTo` on the connection side plus an
`io.Reader` that supports the transfer. `io.NewSectionReader` wraps the log
file with bounds (`FileOffset`, `Size`), ensuring sendfile transfers exactly
the right bytes without needing `lseek` or separate length tracking.

### Why mmap for the offset index?

The offset index is read on every fetch to locate the starting file position.
`mmap(2)` maps the index file into the process address space — binary search
over it accesses the OS page cache directly without any `read()` syscalls,
giving ~nanosecond-level index lookup latency.

---

## References

### Kafka Protocol

- **Fetch API request/response schema (all versions)** — <https://kafka.apache.org/protocol.html#The_Messages_Fetch>
- **Record Batch binary format** — <https://kafka.apache.org/documentation/#recordbatch>
- **KIP-482** — Tagged fields and flexible versions — <https://cwiki.apache.org/confluence/display/KAFKA/KIP-482%3A+The+Kafka+Protocol+should+Support+Optional+Tagged+Fields>
- **KIP-595** — Fetch v13, introduces `COMPACT_RECORDS` — <https://cwiki.apache.org/confluence/display/KAFKA/KIP-595%3A+A+Raft+Protocol+for+the+Metadata+Quorum>
- **KAFKA-18989** — Lazy batch scanning algorithm (fast/slow path) — <https://issues.apache.org/jira/browse/KAFKA-18989>
- **Kafka Log Storage Internals** — <https://kafka.apache.org/documentation/#log>

### Go Standard Library

- **`io.Copy`** — triggers `sendfile` when src/dst support it — <https://pkg.go.dev/io#Copy>
- **`io.NewSectionReader`** — bounds-limited `ReadAt` wrapper — <https://pkg.go.dev/io#NewSectionReader>
- **`io.ReadFull`** — used in `ReadFraming` to guarantee complete reads — <https://pkg.go.dev/io#ReadFull>
- **`net.TCPConn`** — type assertion target for sendfile path — <https://pkg.go.dev/net#TCPConn>
- **`encoding/binary`** — big-endian frame size and index entry parsing — <https://pkg.go.dev/encoding/binary>
- **`sync.RWMutex`** — shared read lock on CommitLog and Segment during fetch — <https://pkg.go.dev/sync#RWMutex>

### Linux / OS Syscalls

- **`sendfile(2)`** — zero-copy file-to-socket — <https://man7.org/linux/man-pages/man2/sendfile.2.html>
- **`mmap(2)`** — memory-mapped index files — <https://man7.org/linux/man-pages/man2/mmap.2.html>
- **`msync(2)`** — flush mmap changes on index close — <https://man7.org/linux/man-pages/man2/msync.2.html>
- **Go runtime sendfile (Linux)** — <https://github.com/golang/go/blob/master/src/net/sendfile_linux.go>

### Related Source Files

- `server/conn.go` — `Conn.handle()`, request loop, routing by ApiKey
- `server/server.go` — `acceptLoop`, goroutine-per-connection, `recoverTopic`
- `protocol/wireframe.go` — `ReadFraming` / `WriteFraming`
- `protocol/header.go` — `RequestHeader.Decode`, `ResponseHeader.Encode`, flexible version logic
- `common/version.go` — `IsFlexible`, `apiFlexibleVersions` map
- `api/fetch_api.go` — `HandleFetchApiKeys`, `ioSegment`, `snapshot()`
- `protocol/fetch/fetch_request.go` — `FetchRequest.Decode` (v18)
- `protocol/fetch/fetch_response.go` — `EncodePartitionPreRecords`, `EncodePartitionPostRecords`
- `storage/commitlog/commitlog.go` — `FindRecords`, `findSegmentByOffset`
- `storage/commitlog/segment.go` — `FindRawRegion`, `readBatchMeta17`, `readLastOffset`
- `storage/commitlog/index.go` — `OffsetIndex` (mmap), `TimeIndex` (mmap)
- `storage/topic.go` — `TopicStore.GetCommitLog`
