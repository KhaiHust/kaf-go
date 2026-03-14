package producer

import (
	"github.com/KhaiHust/kaf-go/constant"
	"github.com/KhaiHust/kaf-go/protocol"
	"github.com/KhaiHust/kaf-go/protocol/types"
)

type ProduceRequest struct {
	TransactionId types.CompactNullableString
	Acks          int16
	TimeoutMs     int32
	TopicData     []ProduceRequestTopicData
}

func (p *ProduceRequest) ApiKey() int16 {
	return constant.ApiKeyProduce
}

func (p *ProduceRequest) Decode(r *protocol.Reader) error {
	var err error
	if p.TransactionId, err = r.ReadCompactNullableString(); err != nil {
		return err
	}
	if p.Acks, err = r.ReadInt16(); err != nil {
		return err
	}
	if p.TimeoutMs, err = r.ReadInt32(); err != nil {
		return err
	}
	topicDataLength, err := r.ReadCompactArrayLen()
	if err != nil {
		return err
	}
	p.TopicData = make([]ProduceRequestTopicData, topicDataLength)
	for i := 0; i < int(topicDataLength); i++ {
		if err = p.TopicData[i].decode(r); err != nil {
			return err
		}
	}
	return r.ReadTaggedFields()
}

type ProduceRequestTopicData struct {
	Name types.CompactString
	//TopicId       uuid.UUID //version 13
	PartitionData []ProduceRequestPartitionData
}

func (t *ProduceRequestTopicData) decode(r *protocol.Reader) error {
	var err error
	if t.Name, err = r.ReadCompactString(); err != nil {
		return err
	}
	//if t.TopicId, err = r.ReadUUID(); err != nil {
	//	return err
	//}
	partitionDataLength, err := r.ReadCompactArrayLen()
	if err != nil {
		return err
	}
	t.PartitionData = make([]ProduceRequestPartitionData, partitionDataLength)
	for i, _ := range t.PartitionData {
		if err = t.PartitionData[i].decode(r); err != nil {
			return err
		}
	}
	return r.ReadTaggedFields()

}

type ProduceRequestPartitionData struct {
	Index   int32
	Records types.CompactRecords
}

func (p *ProduceRequestPartitionData) decode(r *protocol.Reader) error {
	var err error
	if p.Index, err = r.ReadInt32(); err != nil {
		return err
	}
	if p.Records, err = r.ReadCompactRecords(); err != nil {
		return err
	}
	return r.ReadTaggedFields()
}
