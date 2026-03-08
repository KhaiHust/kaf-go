package topic

import (
	"github.com/KhaiHust/kaf-go/constant"
	"github.com/KhaiHust/kaf-go/protocol"
	"github.com/KhaiHust/kaf-go/protocol/types"
	"github.com/gofrs/uuid/v5"
)

type CreateTopicsResponse struct {
	ThrottleTimeMs int32
	Topics         []CreateTopicResponseData
}

func (c *CreateTopicsResponse) ApiKey() int16 {
	return constant.ApiKeyCreateTopics
}

func (c *CreateTopicsResponse) Encode(w *protocol.Writer) error {
	w.WriteInt32(c.ThrottleTimeMs)
	w.WriteCompactArrayLen(len(c.Topics))
	for _, topic := range c.Topics {
		if err := topic.encode(w); err != nil {
			return err
		}
	}
	w.WriteEmptyTaggedFields()
	return nil
}

func (c *CreateTopicsResponse) Decode(r *protocol.Reader) error {
	var err error
	if c.ThrottleTimeMs, err = r.ReadInt32(); err != nil {
		return err
	}
	length, err := r.ReadCompactArrayLen()
	if err != nil {
		return err
	}
	var topics []CreateTopicResponseData
	for i := 0; i < length; i++ {
		topic := CreateTopicResponseData{}
		if err = topic.decode(r); err != nil {
			return err
		}
		topics = append(topics, topic)
	}
	c.Topics = topics

	return r.ReadTaggedFields()
}

type CreateTopicResponseData struct {
	Name              types.CompactString
	TopicId           uuid.UUID
	ErrorCode         int16
	ErrorMessage      types.CompactNullableString
	NumPartitions     int32
	ReplicationFactor int16
	Configs           []CreateTopicConfigResponse
}

func (c *CreateTopicResponseData) encode(w *protocol.Writer) error {
	w.WriteCompactString(c.Name)
	w.WriteUUID(c.TopicId)
	w.WriteInt16(c.ErrorCode)
	w.WriteCompactNullableString(c.ErrorMessage)
	w.WriteInt32(c.NumPartitions)
	w.WriteInt16(c.ReplicationFactor)

	w.WriteCompactArrayLen(len(c.Configs))
	for i := 0; i < len(c.Configs); i++ {
		if err := c.Configs[i].encode(w); err != nil {
			return err
		}
	}
	w.WriteEmptyTaggedFields() // tagged fields
	return nil
}

func (c *CreateTopicResponseData) decode(r *protocol.Reader) error {
	var err error
	if c.Name, err = r.ReadCompactString(); err != nil {
		return err
	}
	if c.TopicId, err = r.ReadUUID(); err != nil {
		return err
	}
	if c.ErrorCode, err = r.ReadInt16(); err != nil {
		return err
	}
	if c.ErrorMessage, err = r.ReadCompactNullableString(); err != nil {
		return err
	}
	if c.NumPartitions, err = r.ReadInt32(); err != nil {
		return err
	}
	if c.ReplicationFactor, err = r.ReadInt16(); err != nil {
		return err
	}

	configLen, err := r.ReadCompactArrayLen()
	if err != nil {
		return err
	}

	var configs []CreateTopicConfigResponse
	for i := 0; i < configLen; i++ {
		config := new(CreateTopicConfigResponse)
		if err = config.decode(r); err != nil {
			return err
		}
		configs = append(configs, *config)
	}
	c.Configs = configs

	return r.ReadTaggedFields()
}

type CreateTopicConfigResponse struct {
	Name                 types.CompactString
	Value                types.CompactNullableString
	ReadOnly             bool
	ConfigSource         int8
	IsSensitive          bool
	TopicConfigErrorCode int16
}

func (c *CreateTopicConfigResponse) encode(w *protocol.Writer) error {
	w.WriteCompactString(c.Name)
	w.WriteCompactNullableString(c.Value)
	w.WriteBool(c.ReadOnly)
	w.WriteInt8(c.ConfigSource)
	w.WriteBool(c.IsSensitive)
	w.WriteInt16(c.TopicConfigErrorCode)
	w.WriteEmptyTaggedFields()
	return nil
}

func (c *CreateTopicConfigResponse) decode(r *protocol.Reader) error {
	var err error
	if c.Name, err = r.ReadCompactString(); err != nil {
		return err
	}

	if c.Value, err = r.ReadCompactNullableString(); err != nil {
		return err
	}
	if c.ReadOnly, err = r.ReadBool(); err != nil {
		return err
	}
	if c.ConfigSource, err = r.ReadInt8(); err != nil {
		return err
	}
	if c.IsSensitive, err = r.ReadBool(); err != nil {
		return err
	}
	if c.TopicConfigErrorCode, err = r.ReadInt16(); err != nil {
		return err
	}

	return r.ReadTaggedFields()

}
