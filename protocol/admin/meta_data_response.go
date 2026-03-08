package admin

import (
	"github.com/KhaiHust/kaf-go/constant"
	"github.com/KhaiHust/kaf-go/protocol"
	"github.com/KhaiHust/kaf-go/protocol/types"
	"github.com/gofrs/uuid/v5"
)

const (
	MetaDatResponseVersion = 13
)

type MetadataResponse struct {
	ThrottleTimeMs int32
	Brokers        []MetadataResponseBroker
	ClusterID      types.CompactNullableString
	ControllerID   int32
	Topics         []MetadataResponseTopic
	ErrorCode      int16
}

func (m *MetadataResponse) Decode(r *protocol.Reader) error {
	//TODO implement me
	panic("implement me")
}

func (m *MetadataResponse) ApiKey() int16 {
	return constant.ApiKeyMetaData
}

func (m *MetadataResponse) Encode(w *protocol.Writer) error {
	w.WriteInt32(m.ThrottleTimeMs)

	w.WriteCompactArrayLen(len(m.Brokers))
	for _, broker := range m.Brokers {
		if err := broker.encode(w); err != nil {
			return err
		}
	}

	w.WriteCompactNullableString(m.ClusterID)
	w.WriteInt32(m.ControllerID)

	w.WriteCompactArrayLen(len(m.Topics))
	for _, topic := range m.Topics {
		if err := topic.encode(w); err != nil {
			return err
		}
	}

	w.WriteInt16(m.ErrorCode)
	w.WriteEmptyTaggedFields()
	return nil
}

type MetadataResponseBroker struct {
	NodeID int32
	Host   types.CompactString
	Port   int32
	Rack   types.CompactNullableString
}

func (b *MetadataResponseBroker) encode(w *protocol.Writer) error {
	w.WriteInt32(b.NodeID)
	w.WriteCompactString(b.Host)
	w.WriteInt32(b.Port)
	w.WriteCompactNullableString(b.Rack)
	w.WriteEmptyTaggedFields()
	return nil
}

type MetadataResponsePartition struct {
	ErrorCode       int16
	PartitionIndex  int32
	LeaderID        int32
	LeaderEpoch     int32
	ReplicaNodes    []int32
	IsrNodes        []int32
	OfflineReplicas []int32
}

func (p *MetadataResponsePartition) encode(w *protocol.Writer) error {
	w.WriteInt16(p.ErrorCode)
	w.WriteInt32(p.PartitionIndex)
	w.WriteInt32(p.LeaderID)
	w.WriteInt32(p.LeaderEpoch)

	w.WriteCompactArrayLen(len(p.ReplicaNodes))
	for _, node := range p.ReplicaNodes {
		w.WriteInt32(node)
	}

	w.WriteCompactArrayLen(len(p.IsrNodes))
	for _, node := range p.IsrNodes {
		w.WriteInt32(node)
	}

	w.WriteCompactArrayLen(len(p.OfflineReplicas))
	for _, node := range p.OfflineReplicas {
		w.WriteInt32(node)
	}

	w.WriteEmptyTaggedFields()
	return nil
}

type MetadataResponseTopic struct {
	ErrorCode                 int16
	Name                      types.CompactNullableString
	TopicID                   uuid.UUID
	IsInternal                bool
	Partitions                []MetadataResponsePartition
	TopicAuthorizedOperations int32
}

func (t *MetadataResponseTopic) encode(w *protocol.Writer) error {
	w.WriteInt16(t.ErrorCode)
	w.WriteCompactNullableString(t.Name)
	w.WriteUUID(t.TopicID)
	w.WriteBool(t.IsInternal)

	w.WriteCompactArrayLen(len(t.Partitions))
	for _, partition := range t.Partitions {
		if err := partition.encode(w); err != nil {
			return err
		}
	}

	w.WriteInt32(t.TopicAuthorizedOperations)
	w.WriteEmptyTaggedFields()
	return nil
}
