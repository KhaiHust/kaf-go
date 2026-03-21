package producer

import (
	"github.com/KhaiHust/kaf-go/constant"
	"github.com/KhaiHust/kaf-go/protocol"
	"github.com/KhaiHust/kaf-go/protocol/types"
	"github.com/gofrs/uuid/v5"
)

type ProduceResponse struct {
	Responses      []TopicProduceResponse
	ThrottleTimeMs int32
	NodeEndpoints  []NodeEndpoint
}

func (p *ProduceResponse) Decode(r *protocol.Reader) error {
	//TODO implement me
	panic("implement me")
}

type TopicProduceResponse struct {
	TopicId            uuid.UUID
	PartitionResponses []PartitionProduceResponse
}

type PartitionProduceResponse struct {
	Index           int32
	ErrorCode       int16
	BaseOffset      int64
	LogAppendTimeMs int64
	LogStartOffset  int64
	RecordErrors    []RecordError               // empty for now
	ErrorMessage    types.CompactNullableString // null = no error
}

type RecordError struct {
	BatchIndex             int32
	BatchIndexErrorMessage types.CompactNullableString
}

type NodeEndpoint struct {
	NodeId int32
	Host   types.CompactString
	Port   int32
	Rack   types.CompactNullableString
}

func (node *NodeEndpoint) encode(w *protocol.Writer) error {
	// NodeEndpoints (v10+), send empty
	w.WriteInt32(node.NodeId)
	w.WriteCompactString(node.Host)
	w.WriteInt32(node.Port)
	w.WriteCompactNullableString(node.Rack)
	w.WriteEmptyTaggedFields()
	return nil
}

func (p *ProduceResponse) ApiKey() int16 {
	return constant.ApiKeyProduce
}

func (p *ProduceResponse) Encode(w *protocol.Writer) error {

	// Responses array
	w.WriteCompactArrayLen(len(p.Responses))
	for _, topic := range p.Responses {
		w.WriteUUID(topic.TopicId)

		// PartitionResponses array
		w.WriteCompactArrayLen(len(topic.PartitionResponses))
		for _, part := range topic.PartitionResponses {
			w.WriteInt32(part.Index)
			w.WriteInt16(part.ErrorCode)
			w.WriteInt64(part.BaseOffset)
			w.WriteInt64(part.LogAppendTimeMs)
			w.WriteInt64(part.LogStartOffset)

			// RecordErrors (empty)
			w.WriteCompactArrayLen(len(part.RecordErrors))
			for _, re := range part.RecordErrors {
				w.WriteInt32(re.BatchIndex)
				w.WriteCompactNullableString(re.BatchIndexErrorMessage)
				w.WriteEmptyTaggedFields()
			}

			w.WriteCompactNullableString(part.ErrorMessage)
			w.WriteEmptyTaggedFields() // partition tagged fields
		}
		w.WriteEmptyTaggedFields() // topic tagged fields
	}
	w.WriteInt32(p.ThrottleTimeMs)

	w.WriteEmptyTaggedFields() // response tagged fields
	//todo: write noteEndpoints
	return nil
}
