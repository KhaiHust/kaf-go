package consumer

import (
	"github.com/KhaiHust/kaf-go/constant"
	"github.com/KhaiHust/kaf-go/protocol"
	"github.com/KhaiHust/kaf-go/protocol/types"
)

type OffsetFetchResponsePartition struct {
	PartitionIndex       int32
	CommittedOffset      int64
	CommittedLeaderEpoch int32
	Metadata             types.CompactNullableString
	ErrorCode            int16
}

type OffsetFetchResponseTopic struct {
	Topic      types.CompactString
	Partitions []OffsetFetchResponsePartition
}

type OffsetFetchResponseGroup struct {
	GroupID   types.CompactString
	Topics    []OffsetFetchResponseTopic
	ErrorCode int16
}

type OffsetFetchResponse struct {
	ThrottleTimeMs int32
	Groups         []OffsetFetchResponseGroup
}

func (o *OffsetFetchResponse) ApiKey() int16 {
	return constant.ApiKeyOffsetFetch
}

func (o *OffsetFetchResponse) Encode(w *protocol.Writer) error {
	w.WriteInt32(o.ThrottleTimeMs)
	w.WriteCompactArrayLen(len(o.Groups))
	for _, g := range o.Groups {
		if err := g.Encode(w); err != nil {
			return err
		}
	}
	w.WriteEmptyTaggedFields()
	return nil
}

func (g *OffsetFetchResponseGroup) Encode(w *protocol.Writer) error {
	w.WriteCompactString(g.GroupID)
	w.WriteCompactArrayLen(len(g.Topics))
	for _, t := range g.Topics {
		if err := t.Encode(w); err != nil {
			return err
		}
	}
	w.WriteInt16(g.ErrorCode)
	w.WriteEmptyTaggedFields()
	return nil
}

func (t *OffsetFetchResponseTopic) Encode(w *protocol.Writer) error {
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

func (p *OffsetFetchResponsePartition) Encode(w *protocol.Writer) error {
	w.WriteInt32(p.PartitionIndex)
	w.WriteInt64(p.CommittedOffset)
	w.WriteInt32(p.CommittedLeaderEpoch)
	w.WriteCompactNullableString(p.Metadata)
	w.WriteInt16(p.ErrorCode)
	w.WriteEmptyTaggedFields()
	return nil
}
