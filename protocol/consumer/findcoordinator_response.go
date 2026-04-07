package consumer

import (
	"github.com/KhaiHust/kaf-go/constant"
	"github.com/KhaiHust/kaf-go/protocol"
	"github.com/KhaiHust/kaf-go/protocol/types"
)

type FindCoordinatorResponse struct {
	ThrottleTimeMs int32
	Coordinators   []Coordinator
}

type Coordinator struct {
	Key          types.CompactString
	NodeID       int32
	Host         types.CompactString
	Port         int32
	ErrorCode    int16
	ErrorMessage types.CompactNullableString
}

func (f *FindCoordinatorResponse) ApiKey() int16 {
	return constant.ApiFindCoordinator
}

func (f *FindCoordinatorResponse) Encode(w *protocol.Writer) error {
	w.WriteInt32(f.ThrottleTimeMs)
	w.WriteCompactArrayLen(len(f.Coordinators))
	for idx, _ := range f.Coordinators {
		if err := f.Coordinators[idx].Encode(w); err != nil {
			return err
		}
	}
	w.WriteEmptyTaggedFields()
	return nil
}

func (c *Coordinator) Encode(w *protocol.Writer) error {
	w.WriteCompactString(c.Key)
	w.WriteInt32(c.NodeID)
	w.WriteCompactString(c.Host)
	w.WriteInt32(c.Port)
	w.WriteInt16(c.ErrorCode)
	w.WriteCompactNullableString(c.ErrorMessage)
	w.WriteEmptyTaggedFields()
	return nil
}
