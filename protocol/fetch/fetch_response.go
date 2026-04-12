package fetch

import (
	"github.com/KhaiHust/kaf-go/constant"
	"github.com/KhaiHust/kaf-go/protocol"
	"github.com/KhaiHust/kaf-go/protocol/types"
	"github.com/gofrs/uuid/v5"
)

type FetchResponse struct {
	ThrottleTimeMs int32
	ErrorCode      int16
	SessionId      int32
	Responses      []FetchTopicResponse

	// tag: 0
	NodeEndpoints []NodeEndpoint
}

func (f *FetchResponse) Decode(r *protocol.Reader) error {
	var err error
	if f.ThrottleTimeMs, err = r.ReadInt32(); err != nil {
		return err
	}
	if f.ErrorCode, err = r.ReadInt16(); err != nil {
		return err
	}
	if f.SessionId, err = r.ReadInt32(); err != nil {
		return err
	}

	numTopics, err := r.ReadCompactArrayLen()
	if err != nil {
		return err
	}
	f.Responses = make([]FetchTopicResponse, numTopics)
	for i := range f.Responses {
		if err := f.Responses[i].decode(r); err != nil {
			return err
		}
	}

	// response-level tagged fields (may include NodeEndpoints — skip them)
	return r.ReadTaggedFields()
}

func (t *FetchTopicResponse) decode(r *protocol.Reader) error {
	var err error
	if t.TopicId, err = r.ReadUUID(); err != nil {
		return err
	}
	numPartitions, err := r.ReadCompactArrayLen()
	if err != nil {
		return err
	}
	t.Partitions = make([]FetchPartitionResponse, numPartitions)
	for i := range t.Partitions {
		if err := t.Partitions[i].decode(r); err != nil {
			return err
		}
	}
	return r.ReadTaggedFields()
}

func (p *FetchPartitionResponse) decode(r *protocol.Reader) error {
	var err error
	if p.PartitionIndex, err = r.ReadInt32(); err != nil {
		return err
	}
	if p.ErrorCode, err = r.ReadInt16(); err != nil {
		return err
	}
	if p.HighWatermark, err = r.ReadInt64(); err != nil {
		return err
	}
	if p.LastStableOffset, err = r.ReadInt64(); err != nil {
		return err
	}
	if p.LogStartOffset, err = r.ReadInt64(); err != nil {
		return err
	}

	numAborted, err := r.ReadCompactArrayLen()
	if err != nil {
		return err
	}
	for i := 0; i < numAborted; i++ {
		var at AbortedTransaction
		if at.ProducerId, err = r.ReadInt64(); err != nil {
			return err
		}
		if at.FirstOffset, err = r.ReadInt64(); err != nil {
			return err
		}
		if err = r.ReadTaggedFields(); err != nil {
			return err
		}
		p.AbortedTransactions = append(p.AbortedTransactions, at)
	}

	if p.PreferredReadReplica, err = r.ReadInt32(); err != nil {
		return err
	}
	if p.Records, err = r.ReadCompactRecords(); err != nil {
		return err
	}
	return r.ReadTaggedFields()
}

func (f *FetchResponse) ApiKey() int16 {
	return constant.ApiKeyFetch
}

type FetchTopicResponse struct {
	TopicId    uuid.UUID
	Partitions []FetchPartitionResponse
}

type FetchPartitionResponse struct {
	PartitionIndex       int32
	ErrorCode            int16
	HighWatermark        int64
	LastStableOffset     int64
	LogStartOffset       int64
	AbortedTransactions  []AbortedTransaction
	PreferredReadReplica int32
	Records              types.CompactRecords // raw record bytes (COMPACT_RECORDS v13+), nil = null

	// tag: 0
	DivergingEpoch *DivergingEpoch
	// tag: 1
	CurrentLeader *CurrentLeader
	// tag: 2
	SnapshotId *SnapshotId
}

type AbortedTransaction struct {
	ProducerId  int64
	FirstOffset int64
}

type DivergingEpoch struct {
	Epoch     int32
	EndOffset int64
}

type CurrentLeader struct {
	LeaderId    int32
	LeaderEpoch int32
}

type SnapshotId struct {
	EndOffset int64
	Epoch     int32
}

type NodeEndpoint struct {
	NodeId int32
	Host   types.CompactString
	Port   int32
	Rack   types.CompactNullableString
}

// --- Encode ---

func (f *FetchResponse) Encode(w *protocol.Writer) error {
	w.WriteInt32(f.ThrottleTimeMs)
	w.WriteInt16(f.ErrorCode)
	w.WriteInt32(f.SessionId)

	// responses
	w.WriteCompactArrayLen(len(f.Responses))
	for i := range f.Responses {
		if err := f.Responses[i].encode(w); err != nil {
			return err
		}
	}

	// response-level tagged fields
	return f.encodeTaggedFields(w)
}

// EncodeHeader writes the response-level fields and opens the topics compact array.
// topicsLen must equal the number of FetchTopicResponse items that will follow.
func (f *FetchResponse) EncodeHeader(w *protocol.Writer, topicsLen int) {
	w.WriteInt32(f.ThrottleTimeMs)
	w.WriteInt16(f.ErrorCode)
	w.WriteInt32(f.SessionId)
	w.WriteCompactArrayLen(topicsLen)
}

// EncodeFooter writes the response-level tagged fields (node_endpoints).
func (f *FetchResponse) EncodeFooter(w *protocol.Writer) error {
	return f.encodeTaggedFields(w)
}

// EncodePartitionPreRecords writes all partition fields up to and including the
// COMPACT_RECORDS length prefix. recordsSize < 0 encodes a null records field.
// The actual record bytes must follow immediately in the stream.
func (p *FetchPartitionResponse) EncodePartitionPreRecords(w *protocol.Writer, recordsSize int64) {
	w.WriteInt32(p.PartitionIndex)
	w.WriteInt16(p.ErrorCode)
	w.WriteInt64(p.HighWatermark)
	w.WriteInt64(p.LastStableOffset)
	w.WriteInt64(p.LogStartOffset)

	w.WriteCompactArrayLen(len(p.AbortedTransactions))
	for _, at := range p.AbortedTransactions {
		w.WriteInt64(at.ProducerId)
		w.WriteInt64(at.FirstOffset)
		w.WriteEmptyTaggedFields()
	}

	w.WriteInt32(p.PreferredReadReplica)

	// COMPACT_RECORDS length prefix: uvarint(N+1), or uvarint(0) for null
	if recordsSize < 0 {
		w.WriteUVarInt(0)
	} else {
		w.WriteUVarInt(uint64(recordsSize + 1))
	}
}

// EncodePartitionPostRecords writes the partition-level tagged fields that
// follow the records data in the wire format.
func (p *FetchPartitionResponse) EncodePartitionPostRecords(w *protocol.Writer) error {
	return p.encodeTaggedFields(w)
}

func (f *FetchResponse) encodeTaggedFields(w *protocol.Writer) error {
	if len(f.NodeEndpoints) == 0 {
		w.WriteEmptyTaggedFields()
		return nil
	}

	// 1 tagged field: node_endpoints (tag: 0)
	w.WriteUVarInt(1)
	w.WriteUVarInt(0) // tag key

	// encode node_endpoints into tmp buffer to get size first
	tmp := protocol.NewWriter(50 * len(f.NodeEndpoints))
	tmp.WriteCompactArrayLen(len(f.NodeEndpoints))
	for i := range f.NodeEndpoints {
		f.NodeEndpoints[i].encode(tmp)
	}

	payload := tmp.Bytes()
	w.WriteUVarInt(uint64(len(payload)))
	w.WriteRaw(payload)
	return nil
}

// --- FetchTopicResponse ---

func (t *FetchTopicResponse) encode(w *protocol.Writer) error {
	w.WriteUUID(t.TopicId)

	w.WriteCompactArrayLen(len(t.Partitions))
	for i := range t.Partitions {
		if err := t.Partitions[i].encode(w); err != nil {
			return err
		}
	}

	w.WriteEmptyTaggedFields()
	return nil
}

// --- FetchPartitionResponse ---

func (p *FetchPartitionResponse) encode(w *protocol.Writer) error {
	w.WriteInt32(p.PartitionIndex)
	w.WriteInt16(p.ErrorCode)
	w.WriteInt64(p.HighWatermark)
	w.WriteInt64(p.LastStableOffset)
	w.WriteInt64(p.LogStartOffset)

	// aborted_transactions
	w.WriteCompactArrayLen(len(p.AbortedTransactions))
	for _, at := range p.AbortedTransactions {
		w.WriteInt64(at.ProducerId)
		w.WriteInt64(at.FirstOffset)
		w.WriteEmptyTaggedFields()
	}

	w.WriteInt32(p.PreferredReadReplica)

	// records — COMPACT_RECORDS (v13+): uvarint(N+1), or uvarint(0) for null
	w.WriteCompactRecords(p.Records)

	return p.encodeTaggedFields(w)
}

func (p *FetchPartitionResponse) encodeTaggedFields(w *protocol.Writer) error {
	count := 0
	if p.DivergingEpoch != nil {
		count++
	}
	if p.CurrentLeader != nil {
		count++
	}
	if p.SnapshotId != nil {
		count++
	}

	w.WriteUVarInt(uint64(count))

	if p.DivergingEpoch != nil {
		w.WriteUVarInt(0)             // tag key
		tmp := protocol.NewWriter(12) // int32 + int64
		tmp.WriteInt32(p.DivergingEpoch.Epoch)
		tmp.WriteInt64(p.DivergingEpoch.EndOffset)
		payload := tmp.Bytes()
		w.WriteUVarInt(uint64(len(payload)))
		w.WriteRaw(payload)
	}

	if p.CurrentLeader != nil {
		w.WriteUVarInt(1) // tag key
		tmp := protocol.NewWriter(8)
		tmp.WriteInt32(p.CurrentLeader.LeaderId)
		tmp.WriteInt32(p.CurrentLeader.LeaderEpoch)
		payload := tmp.Bytes()
		w.WriteUVarInt(uint64(len(payload)))
		w.WriteRaw(payload)
	}

	if p.SnapshotId != nil {
		w.WriteUVarInt(2) // tag key
		tmp := protocol.NewWriter(12)
		tmp.WriteInt64(p.SnapshotId.EndOffset)
		tmp.WriteInt32(p.SnapshotId.Epoch)
		payload := tmp.Bytes()
		w.WriteUVarInt(uint64(len(payload)))
		w.WriteRaw(payload)
	}

	return nil
}

// --- NodeEndpoint ---

func (n *NodeEndpoint) encode(w *protocol.Writer) {
	w.WriteInt32(n.NodeId)
	w.WriteCompactString(n.Host)
	w.WriteInt32(n.Port)
	w.WriteCompactNullableString(n.Rack)
	w.WriteEmptyTaggedFields()
}
