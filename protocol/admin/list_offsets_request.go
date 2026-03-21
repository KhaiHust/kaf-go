package admin

import (
	"github.com/KhaiHust/kaf-go/protocol"
	"github.com/KhaiHust/kaf-go/protocol/types"
)

// ListOffsets timestamps — sent in Partition.Timestamp
const (
	ListOffsetsLatest   = int64(-1) // return LEO (next offset to be written)
	ListOffsetsEarliest = int64(-2) // return log start offset
)

type ListOffsetsRequest struct {
	ReplicaId      int32
	IsolationLevel int8
	Topics         []ListOffsetsTopic
	TimeoutMs      int32 // added in v11
}

type ListOffsetsTopic struct {
	Name       types.CompactString
	Partitions []ListOffsetsPartition
}

type ListOffsetsPartition struct {
	PartitionIndex     int32
	CurrentLeaderEpoch int32
	Timestamp          int64
}

func (l *ListOffsetsRequest) Decode(r *protocol.Reader) error {
	var err error

	if l.ReplicaId, err = r.ReadInt32(); err != nil {
		return err
	}
	if l.IsolationLevel, err = r.ReadInt8(); err != nil {
		return err
	}

	topicsLen, err := r.ReadCompactArrayLen()
	if err != nil {
		return err
	}
	l.Topics = make([]ListOffsetsTopic, topicsLen)
	for i := range l.Topics {
		if err = l.Topics[i].decode(r); err != nil {
			return err
		}
	}

	// timeout_ms added in v11 — comes AFTER topics
	if l.TimeoutMs, err = r.ReadInt32(); err != nil {
		return err
	}

	return r.ReadTaggedFields()
}

func (t *ListOffsetsTopic) decode(r *protocol.Reader) error {
	var err error
	if t.Name, err = r.ReadCompactString(); err != nil {
		return err
	}
	partLen, err := r.ReadCompactArrayLen()
	if err != nil {
		return err
	}
	t.Partitions = make([]ListOffsetsPartition, partLen)
	for i := range t.Partitions {
		if err = t.Partitions[i].decode(r); err != nil {
			return err
		}
	}
	return r.ReadTaggedFields()
}

func (p *ListOffsetsPartition) decode(r *protocol.Reader) error {
	var err error
	if p.PartitionIndex, err = r.ReadInt32(); err != nil {
		return err
	}
	if p.CurrentLeaderEpoch, err = r.ReadInt32(); err != nil {
		return err
	}
	if p.Timestamp, err = r.ReadInt64(); err != nil {
		return err
	}
	return r.ReadTaggedFields()
}
