package admin

import (
	"github.com/KhaiHust/kaf-go/constant"
	"github.com/KhaiHust/kaf-go/protocol"
	"github.com/KhaiHust/kaf-go/protocol/types"
)

type ListOffsetsResponse struct {
	ThrottleTimeMs int32
	Topics         []ListOffsetsResponseTopic
}

type ListOffsetsResponseTopic struct {
	Name       types.CompactString
	Partitions []ListOffsetsResponsePartition
}

type ListOffsetsResponsePartition struct {
	PartitionIndex int32
	ErrorCode      int16
	Timestamp      int64 // -1 = no timestamp associated
	Offset         int64
	LeaderEpoch    int32
}

func (l *ListOffsetsResponse) ApiKey() int16 {
	return constant.ApiKeyListOffsets
}

func (l *ListOffsetsResponse) Decode(r *protocol.Reader) error {
	panic("ListOffsetsResponse.Decode not implemented — broker never decodes its own response")
}

func (l *ListOffsetsResponse) Encode(w *protocol.Writer) error {
	w.WriteInt32(l.ThrottleTimeMs)

	w.WriteCompactArrayLen(len(l.Topics))
	for i := range l.Topics {
		if err := l.Topics[i].encode(w); err != nil {
			return err
		}
	}

	w.WriteEmptyTaggedFields()
	return nil
}

func (t *ListOffsetsResponseTopic) encode(w *protocol.Writer) error {
	w.WriteCompactString(t.Name)

	w.WriteCompactArrayLen(len(t.Partitions))
	for i := range t.Partitions {
		t.Partitions[i].encode(w)
	}

	w.WriteEmptyTaggedFields()
	return nil
}

func (p *ListOffsetsResponsePartition) encode(w *protocol.Writer) {
	w.WriteInt32(p.PartitionIndex)
	w.WriteInt16(p.ErrorCode)
	w.WriteInt64(p.Timestamp)
	w.WriteInt64(p.Offset)
	w.WriteInt32(p.LeaderEpoch)
	w.WriteEmptyTaggedFields()
}
