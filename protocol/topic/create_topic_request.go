package topic

import (
	"github.com/KhaiHust/kaf-go/constant"
	"github.com/KhaiHust/kaf-go/protocol"
	"github.com/KhaiHust/kaf-go/protocol/types"
)

type CreateTopicsRequest struct {
	Topics        []CreateTopicData
	TimeoutMs     int32
	ValidateOnly  bool
	version       int16
	correlationId int32
}

func (c *CreateTopicsRequest) Encode(w *protocol.Writer) error {
	w.WriteCompactArrayLen(len(c.Topics))
	for _, t := range c.Topics {
		if err := t.encode(w); err != nil {
			return err
		}
	}
	w.WriteInt32(c.TimeoutMs)
	w.WriteBool(c.ValidateOnly)
	w.WriteEmptyTaggedFields()
	return nil
}

func (c *CreateTopicsRequest) Decode(r *protocol.Reader) error {
	topicLen, err := r.ReadCompactArrayLen()
	if err != nil {
		return err
	}
	var topics []CreateTopicData
	for i := 0; i < topicLen; i++ {
		newTopic := new(CreateTopicData)
		if err := newTopic.decode(r); err != nil {
			return err
		}
		topics = append(topics, *newTopic)
	}
	c.Topics = topics

	timeoutMs, err := r.ReadInt32()
	if err != nil {
		return err
	}
	c.TimeoutMs = timeoutMs

	validateOnly, err := r.ReadBool()
	if err != nil {
		return err
	}
	c.ValidateOnly = validateOnly

	return r.ReadTaggedFields()
}

func (c *CreateTopicsRequest) ApiKey() int64 {
	return constant.ApiKeyCreateTopics
}

func (c *CreateTopicsRequest) ApiVersion() int16 {
	return c.version
}

func (c *CreateTopicsRequest) CorrelationID() int32 {
	return c.correlationId
}

type CreateTopicData struct {
	Name              types.CompactString
	NumPartitions     int32
	ReplicationFactor int16
	Assigments        []CreateTopicAssignment
	Configs           []CreateTopicConfig
}

func (c *CreateTopicData) encode(w *protocol.Writer) error {
	w.WriteCompactString(c.Name)
	w.WriteInt32(c.NumPartitions)
	w.WriteInt16(c.ReplicationFactor)

	assignmentLen := len(c.Assigments)
	w.WriteCompactArrayLen(assignmentLen)
	for i := 0; i < assignmentLen; i++ {
		if err := c.Assigments[i].encode(w); err != nil {
			return err
		}
	}

	configLen := len(c.Configs)
	w.WriteCompactArrayLen(configLen)
	for i := 0; i < configLen; i++ {
		if err := c.Configs[i].encode(w); err != nil {
			return err
		}
	}
	// Write empty tagged fields (required for flexible versions)
	w.WriteUVarInt(0)
	return nil
}

func (c *CreateTopicData) decode(r *protocol.Reader) error {
	var err error
	if c.Name, err = r.ReadCompactString(); err != nil {
		return err
	}
	if c.NumPartitions, err = r.ReadInt32(); err != nil {
		return err
	}
	if c.ReplicationFactor, err = r.ReadInt16(); err != nil {
		return err
	}
	assignmentLen, err := r.ReadCompactArrayLen()
	if err != nil {
		return err
	}
	var assignments []CreateTopicAssignment
	for i := 0; i < assignmentLen; i++ {
		var assignment = new(CreateTopicAssignment)
		if err = assignment.decode(r); err != nil {
			return err
		}
		assignments = append(assignments, *assignment)
	}
	configLen, err := r.ReadCompactArrayLen()
	if err != nil {
		return err
	}
	var configs []CreateTopicConfig
	for i := 0; i < configLen; i++ {
		var config = new(CreateTopicConfig)
		if err = config.decode(r); err != nil {
			return err
		}
		configs = append(configs, *config)
	}
	c.Assigments = assignments
	c.Configs = configs

	// Read tagged fields (required for flexible versions)
	if err = r.ReadTaggedFields(); err != nil {
		return err
	}
	return nil
}

type CreateTopicAssignment struct {
	PartitionIndex int32
	BrokerIds      []int32
}

func (c *CreateTopicAssignment) encode(w *protocol.Writer) error {
	w.WriteInt32(c.PartitionIndex)

	w.WriteCompactArrayLen(len(c.BrokerIds))
	for _, b := range c.BrokerIds {
		w.WriteInt32(b)
	}

	// Write empty tagged fields (required for flexible versions)
	w.WriteUVarInt(0)
	return nil
}

func (c *CreateTopicAssignment) decode(r *protocol.Reader) error {
	var err error
	if c.PartitionIndex, err = r.ReadInt32(); err != nil {
		return err
	}

	topicLen, err := r.ReadCompactArrayLen()
	if err != nil {
		return err
	}
	var brokerIds []int32
	for i := 0; i < topicLen; i++ {
		brokerId, err := r.ReadInt32()
		if err != nil {
			return err
		}
		brokerIds = append(brokerIds, brokerId)
	}
	c.BrokerIds = brokerIds

	return r.ReadTaggedFields()
}

type CreateTopicConfig struct {
	Name  types.CompactString
	Value types.CompactNullableString
}

func (c *CreateTopicConfig) encode(w *protocol.Writer) error {
	w.WriteCompactString(c.Name)
	w.WriteCompactNullableString(c.Value)
	// Write empty tagged fields (required for flexible versions)
	w.WriteUVarInt(0)
	return nil
}

func (c *CreateTopicConfig) decode(r *protocol.Reader) error {
	var err error
	if c.Name, err = r.ReadCompactString(); err != nil {
		return err
	}

	if c.Value, err = r.ReadCompactNullableString(); err != nil {
		return err
	}

	return r.ReadTaggedFields()
}
