package admin

import (
	"github.com/KhaiHust/kaf-go/protocol"
	"github.com/KhaiHust/kaf-go/protocol/types"
	"github.com/gofrs/uuid/v5"
)

const (
	MetaDataRequestVersion = 13
)

type MetaDataRequest struct {
	Topics                           []TopicMetadataRequest
	AllowAutoTopicCreation           bool
	IncludeTopicAuthorizedOperations bool
	version                          int16
}

func (req *MetaDataRequest) Decode(r *protocol.Reader) error {
	topicLen, err := r.ReadCompactArrayLen()
	if err != nil {
		return err
	}
	if topicLen == -1 {
		req.Topics = nil
	} else {
		topics := make([]TopicMetadataRequest, topicLen)
		for i := 0; i < topicLen; i++ {
			if err = topics[i].decode(r); err != nil {
				return err
			}
		}
		req.Topics = topics
	}
	if req.AllowAutoTopicCreation, err = r.ReadBool(); err != nil {
		return err
	}
	if req.IncludeTopicAuthorizedOperations, err = r.ReadBool(); err != nil {
		return err
	}
	req.version = MetaDataRequestVersion

	return r.ReadTaggedFields()
}

type TopicMetadataRequest struct {
	TopicId uuid.UUID
	Name    types.CompactNullableString
}

func (t *TopicMetadataRequest) decode(r *protocol.Reader) error {
	var err error
	if t.TopicId, err = r.ReadUUID(); err != nil {
		return err
	}
	if t.Name, err = r.ReadCompactNullableString(); err != nil {
		return err
	}

	return r.ReadTaggedFields()
}
