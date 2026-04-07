package consumer

import (
	"github.com/KhaiHust/kaf-go/constant"
	"github.com/KhaiHust/kaf-go/protocol"
	"github.com/KhaiHust/kaf-go/protocol/types"
)

const OffsetCommitRequestVersion = 8

type OffsetCommitPartition struct {
	PartitionIndex       int32
	CommittedOffset      int64
	CommittedLeaderEpoch int32
	CommittedMetadata    types.CompactNullableString
}

type OffsetCommitTopic struct {
	Topic      types.CompactString
	Partitions []OffsetCommitPartition
}

type OffsetCommitRequest struct {
	GroupID                   types.CompactString
	GenerationIDOrMemberEpoch int32
	MemberID                  types.CompactString
	GroupInstanceID           types.CompactNullableString
	Topics                    []OffsetCommitTopic
}

func (o *OffsetCommitRequest) ApiKey() int64 {
	return constant.ApiKeyOffsetCommit
}

func (o *OffsetCommitRequest) ApiVersion() int16 {
	return OffsetCommitRequestVersion
}

func (o *OffsetCommitRequest) CorrelationID() int32 {
	panic("implement me")
}

func (o *OffsetCommitRequest) Decode(r *protocol.Reader) error {
	var err error

	if o.GroupID, err = r.ReadCompactString(); err != nil {
		return err
	}

	if o.GenerationIDOrMemberEpoch, err = r.ReadInt32(); err != nil {
		return err
	}

	if o.MemberID, err = r.ReadCompactString(); err != nil {
		return err
	}

	if o.GroupInstanceID, err = r.ReadCompactNullableString(); err != nil {
		return err
	}

	topicsLen, err := r.ReadCompactArrayLen()
	if err != nil {
		return err
	}

	o.Topics = make([]OffsetCommitTopic, topicsLen)
	for i := range o.Topics {
		if o.Topics[i].Topic, err = r.ReadCompactString(); err != nil {
			return err
		}

		partitionsLen, err := r.ReadCompactArrayLen()
		if err != nil {
			return err
		}

		o.Topics[i].Partitions = make([]OffsetCommitPartition, partitionsLen)
		for j := range o.Topics[i].Partitions {
			p := &o.Topics[i].Partitions[j]
			if p.PartitionIndex, err = r.ReadInt32(); err != nil {
				return err
			}
			if p.CommittedOffset, err = r.ReadInt64(); err != nil {
				return err
			}
			if p.CommittedLeaderEpoch, err = r.ReadInt32(); err != nil {
				return err
			}
			if p.CommittedMetadata, err = r.ReadCompactNullableString(); err != nil {
				return err
			}
			if err = r.ReadTaggedFields(); err != nil {
				return err
			}
		}

		if err = r.ReadTaggedFields(); err != nil {
			return err
		}
	}

	return r.ReadTaggedFields()
}
