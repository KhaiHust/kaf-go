package metadata

import (
	"encoding/binary"

	"github.com/gofrs/uuid/v5"
)

// ClusterMetadataTopicID is the well-known UUID for the __cluster_metadata internal topic.
var ClusterMetadataTopicID = uuid.UUID{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1}

const (
	RecordTypeBroker      = byte(0)
	RecordTypeTopic       = byte(1)
	RecordTypePartition   = byte(2)
	RecordTypeProducerIds = byte(3)
)

// BrokerRecord is written when a broker registers with the controller.
// Wire: [type:1][version:1][BrokerId:4][Epoch:8][HostLen:2][Host:N][Port:4]
type BrokerRecord struct {
	BrokerId int32
	Host     string
	Port     int32
	Epoch    int64
}

func (r *BrokerRecord) Encode() []byte {
	b := make([]byte, 2+4+8+2+len(r.Host)+4)
	b[0] = RecordTypeBroker
	b[1] = 0
	off := 2
	binary.BigEndian.PutUint32(b[off:], uint32(r.BrokerId))
	off += 4
	binary.BigEndian.PutUint64(b[off:], uint64(r.Epoch))
	off += 8
	binary.BigEndian.PutUint16(b[off:], uint16(len(r.Host)))
	off += 2
	copy(b[off:], r.Host)
	off += len(r.Host)
	binary.BigEndian.PutUint32(b[off:], uint32(r.Port))
	return b
}

func DecodeBrokerRecord(payload []byte) *BrokerRecord {
	off := 0
	brokerId := int32(binary.BigEndian.Uint32(payload[off:]))
	off += 4
	epoch := int64(binary.BigEndian.Uint64(payload[off:]))
	off += 8
	hostLen := int(binary.BigEndian.Uint16(payload[off:]))
	off += 2
	host := string(payload[off : off+hostLen])
	off += hostLen
	port := int32(binary.BigEndian.Uint32(payload[off:]))
	return &BrokerRecord{BrokerId: brokerId, Host: host, Port: port, Epoch: epoch}
}

// TopicRecord is written when a topic is created.
// Wire v0: [type:1][version:1][TopicId:16][NumPartitions:4][NameLen:2][Name:N]
// Wire v1: v0 + [MinInsyncReplicas:2]
type TopicRecord struct {
	TopicId           uuid.UUID
	Name              string
	NumPartitions     int32
	MinInsyncReplicas int16
}

func (r *TopicRecord) Encode() []byte {
	b := make([]byte, 2+16+4+2+len(r.Name)+2)
	b[0] = RecordTypeTopic
	b[1] = 1 // version
	off := 2
	copy(b[off:], r.TopicId[:])
	off += 16
	binary.BigEndian.PutUint32(b[off:], uint32(r.NumPartitions))
	off += 4
	binary.BigEndian.PutUint16(b[off:], uint16(len(r.Name)))
	off += 2
	copy(b[off:], r.Name)
	off += len(r.Name)
	binary.BigEndian.PutUint16(b[off:], uint16(r.MinInsyncReplicas))
	return b
}

func DecodeTopicRecord(payload []byte, version byte) *TopicRecord {
	off := 0
	var topicId uuid.UUID
	copy(topicId[:], payload[off:off+16])
	off += 16
	numPartitions := int32(binary.BigEndian.Uint32(payload[off:]))
	off += 4
	nameLen := int(binary.BigEndian.Uint16(payload[off:]))
	off += 2
	name := string(payload[off : off+nameLen])
	off += nameLen

	rec := &TopicRecord{TopicId: topicId, Name: name, NumPartitions: numPartitions, MinInsyncReplicas: 1}
	if version >= 1 && len(payload)-off >= 2 {
		rec.MinInsyncReplicas = int16(binary.BigEndian.Uint16(payload[off:]))
	}
	return rec
}

// PartitionRecord is written for each partition when a topic is created.
// Wire: [type:1][version:1][TopicId:16][PartitionId:4][Leader:4][LeaderEpoch:4]
//
//	[NumReplicas:4][Replicas...][NumISR:4][ISR...]
type PartitionRecord struct {
	TopicId     uuid.UUID
	PartitionId int32
	Leader      int32
	LeaderEpoch int32
	Replicas    []int32
	ISR         []int32
}

func (r *PartitionRecord) Encode() []byte {
	size := 2 + 16 + 4 + 4 + 4 + 4 + 4*len(r.Replicas) + 4 + 4*len(r.ISR)
	b := make([]byte, size)
	b[0] = RecordTypePartition
	b[1] = 0
	off := 2
	copy(b[off:], r.TopicId[:])
	off += 16
	binary.BigEndian.PutUint32(b[off:], uint32(r.PartitionId))
	off += 4
	binary.BigEndian.PutUint32(b[off:], uint32(r.Leader))
	off += 4
	binary.BigEndian.PutUint32(b[off:], uint32(r.LeaderEpoch))
	off += 4
	binary.BigEndian.PutUint32(b[off:], uint32(len(r.Replicas)))
	off += 4
	for _, rep := range r.Replicas {
		binary.BigEndian.PutUint32(b[off:], uint32(rep))
		off += 4
	}
	binary.BigEndian.PutUint32(b[off:], uint32(len(r.ISR)))
	off += 4
	for _, isr := range r.ISR {
		binary.BigEndian.PutUint32(b[off:], uint32(isr))
		off += 4
	}
	return b
}

func DecodePartitionRecord(payload []byte) *PartitionRecord {
	off := 0
	var topicId uuid.UUID
	copy(topicId[:], payload[off:off+16])
	off += 16
	partId := int32(binary.BigEndian.Uint32(payload[off:]))
	off += 4
	leader := int32(binary.BigEndian.Uint32(payload[off:]))
	off += 4
	leaderEpoch := int32(binary.BigEndian.Uint32(payload[off:]))
	off += 4
	numReplicas := int(binary.BigEndian.Uint32(payload[off:]))
	off += 4
	replicas := make([]int32, numReplicas)
	for i := range replicas {
		replicas[i] = int32(binary.BigEndian.Uint32(payload[off:]))
		off += 4
	}
	numISR := int(binary.BigEndian.Uint32(payload[off:]))
	off += 4
	isr := make([]int32, numISR)
	for i := range isr {
		isr[i] = int32(binary.BigEndian.Uint32(payload[off:]))
		off += 4
	}
	return &PartitionRecord{
		TopicId: topicId, PartitionId: partId, Leader: leader,
		LeaderEpoch: leaderEpoch, Replicas: replicas, ISR: isr,
	}
}

// ProducerIdsRecord is appended every time a broker leases a fresh PID block
// from the controller. The NextProducerId field is the *exclusive* end of the
// new lease — the broker may issue PIDs up to but not including it.
//
// On controller failover, scanning all ProducerIdsRecords in __cluster_metadata
// and taking the max(NextProducerId) tells the new controller where to resume
// PID allocation safely.
//
// Wire: [type:1][version:1][BrokerId:4][BrokerEpoch:8][NextProducerId:8]
type ProducerIdsRecord struct {
	BrokerId       int32
	BrokerEpoch    int64
	NextProducerId int64
}

func (r *ProducerIdsRecord) Encode() []byte {
	b := make([]byte, 2+4+8+8)
	b[0] = RecordTypeProducerIds
	b[1] = 0
	off := 2
	binary.BigEndian.PutUint32(b[off:], uint32(r.BrokerId))
	off += 4
	binary.BigEndian.PutUint64(b[off:], uint64(r.BrokerEpoch))
	off += 8
	binary.BigEndian.PutUint64(b[off:], uint64(r.NextProducerId))
	return b
}

func DecodeProducerIdsRecord(payload []byte) *ProducerIdsRecord {
	off := 0
	brokerId := int32(binary.BigEndian.Uint32(payload[off:]))
	off += 4
	brokerEpoch := int64(binary.BigEndian.Uint64(payload[off:]))
	off += 8
	nextPid := int64(binary.BigEndian.Uint64(payload[off:]))
	return &ProducerIdsRecord{BrokerId: brokerId, BrokerEpoch: brokerEpoch, NextProducerId: nextPid}
}

// Decode dispatches based on the type byte and returns the decoded record.
// Returns nil if the type is unknown or the payload is too short.
func Decode(b []byte) interface{} {
	if len(b) < 2 {
		return nil
	}
	payload := b[2:]
	switch b[0] {
	case RecordTypeBroker:
		return DecodeBrokerRecord(payload)
	case RecordTypeTopic:
		return DecodeTopicRecord(payload, b[1])
	case RecordTypePartition:
		return DecodePartitionRecord(payload)
	case RecordTypeProducerIds:
		return DecodeProducerIdsRecord(payload)
	}
	return nil
}
