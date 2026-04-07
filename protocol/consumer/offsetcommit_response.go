package consumer

import (
	"github.com/KhaiHust/kaf-go/constant"
	"github.com/KhaiHust/kaf-go/protocol"
	"github.com/KhaiHust/kaf-go/protocol/types"
)

type OffsetCommitResponsePartition struct {
	PartitionIndex int32
	ErrorCode      int16
}

type OffsetCommitResponseTopic struct {
	Topic      types.CompactString
	Partitions []OffsetCommitResponsePartition
}

type OffsetCommitResponse struct {
	ThrottleTimeMs int32
	Topics         []OffsetCommitResponseTopic
}

func (o *OffsetCommitResponse) ApiKey() int16 {
	return constant.ApiKeyOffsetCommit
}

func (o *OffsetCommitResponse) Encode(w *protocol.Writer) error {
	w.WriteInt32(o.ThrottleTimeMs)
	w.WriteCompactArrayLen(len(o.Topics))
	for _, t := range o.Topics {
		if err := t.Encode(w); err != nil {
			return err
		}
	}
	w.WriteEmptyTaggedFields()
	return nil
}

func (t *OffsetCommitResponseTopic) Encode(w *protocol.Writer) error {
	w.WriteCompactString(t.Topic)
	w.WriteCompactArrayLen(len(t.Partitions))
	for _, p := range t.Partitions {
		if err := p.Encode(w); err != nil {
			return err
		}
	}
	w.WriteEmptyTaggedFields()
	return nil
}

func (p *OffsetCommitResponsePartition) Encode(w *protocol.Writer) error {
	w.WriteInt32(p.PartitionIndex)
	w.WriteInt16(p.ErrorCode)
	w.WriteEmptyTaggedFields()
	return nil
}
