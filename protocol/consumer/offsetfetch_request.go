package consumer

import (
	"github.com/KhaiHust/kaf-go/constant"
	"github.com/KhaiHust/kaf-go/protocol"
	"github.com/KhaiHust/kaf-go/protocol/types"
)

const OffsetFetchRequestVersion = 8

type OffsetFetchTopic struct {
	Topic            types.CompactString
	PartitionIndexes []int32
}

type OffsetFetchGroup struct {
	GroupID types.CompactString
	Topics  []OffsetFetchTopic
}

type OffsetFetchRequest struct {
	Groups        []OffsetFetchGroup
	RequireStable bool
}

func (o *OffsetFetchRequest) ApiKey() int64 {
	return constant.ApiKeyOffsetFetch
}

func (o *OffsetFetchRequest) ApiVersion() int16 {
	return OffsetFetchRequestVersion
}

func (o *OffsetFetchRequest) CorrelationID() int32 {
	panic("implement me")
}

func (o *OffsetFetchRequest) Decode(r *protocol.Reader) error {
	var err error

	groupsLen, err := r.ReadCompactArrayLen()
	if err != nil {
		return err
	}

	o.Groups = make([]OffsetFetchGroup, groupsLen)
	for i := range o.Groups {
		g := &o.Groups[i]

		if g.GroupID, err = r.ReadCompactString(); err != nil {
			return err
		}

		topicsLen, err := r.ReadCompactArrayLen()
		if err != nil {
			return err
		}

		g.Topics = make([]OffsetFetchTopic, topicsLen)
		for j := range g.Topics {
			t := &g.Topics[j]

			if t.Topic, err = r.ReadCompactString(); err != nil {
				return err
			}

			partitionsLen, err := r.ReadCompactArrayLen()
			if err != nil {
				return err
			}

			t.PartitionIndexes = make([]int32, partitionsLen)
			for k := range t.PartitionIndexes {
				if t.PartitionIndexes[k], err = r.ReadInt32(); err != nil {
					return err
				}
			}

			if err = r.ReadTaggedFields(); err != nil {
				return err
			}
		}

		if err = r.ReadTaggedFields(); err != nil {
			return err
		}
	}

	if o.RequireStable, err = r.ReadBool(); err != nil {
		return err
	}

	return r.ReadTaggedFields()
}
