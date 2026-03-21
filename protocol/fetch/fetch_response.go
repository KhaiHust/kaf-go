package fetch

import (
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
	Records              []byte // raw COMPACT_RECORDS bytes, nil = null records

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
	tmp.WriteEmptyTaggedFields() // node_endpoint array has no own tagged fields

	payload := tmp.Bytes()
	w.WriteUVarInt(uint64(len(payload)))
	w.WriteBytes(payload)
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

	// records — COMPACT_RECORDS: UVarInt(len+1) then raw bytes; null = 0x00
	if p.Records == nil {
		w.WriteUVarInt(0)
	} else {
		w.WriteUVarInt(uint64(len(p.Records)) + 1)
		w.WriteBytes(p.Records)
	}

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
		w.WriteBytes(payload)
	}

	if p.CurrentLeader != nil {
		w.WriteUVarInt(1) // tag key
		tmp := protocol.NewWriter(8)
		tmp.WriteInt32(p.CurrentLeader.LeaderId)
		tmp.WriteInt32(p.CurrentLeader.LeaderEpoch)
		payload := tmp.Bytes()
		w.WriteUVarInt(uint64(len(payload)))
		w.WriteBytes(payload)
	}

	if p.SnapshotId != nil {
		w.WriteUVarInt(2) // tag key
		tmp := protocol.NewWriter(12)
		tmp.WriteInt64(p.SnapshotId.EndOffset)
		tmp.WriteInt32(p.SnapshotId.Epoch)
		payload := tmp.Bytes()
		w.WriteUVarInt(uint64(len(payload)))
		w.WriteBytes(payload)
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
