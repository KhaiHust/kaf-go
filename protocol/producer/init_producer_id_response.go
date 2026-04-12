package producer

import (
	"github.com/KhaiHust/kaf-go/constant"
	"github.com/KhaiHust/kaf-go/protocol"
)

type InitProducerIdResponse struct {
	ThrottleTimeMs int32
	ErrorCode      int16
	ProducerId     int64
	ProducerEpoch  int16
}

func (i *InitProducerIdResponse) ApiKey() int16 {
	return constant.ApiKeyInitProducerId
}

func (i *InitProducerIdResponse) Encode(w *protocol.Writer) error {
	w.WriteInt32(i.ThrottleTimeMs)
	w.WriteInt16(i.ErrorCode)
	w.WriteInt64(i.ProducerId)
	w.WriteInt16(i.ProducerEpoch)
	w.WriteEmptyTaggedFields()
	return nil
}
