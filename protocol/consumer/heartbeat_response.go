package consumer

import (
	"github.com/KhaiHust/kaf-go/constant"
	"github.com/KhaiHust/kaf-go/protocol"
)

type HeartbeatResponse struct {
	ThrottleTimeMs int32
	ErrorCode      int16
}

func (h *HeartbeatResponse) ApiKey() int16 {
	return constant.ApiKeyHeartbeat
}

func (h *HeartbeatResponse) Encode(w *protocol.Writer) error {
	w.WriteInt32(h.ThrottleTimeMs)
	w.WriteInt16(h.ErrorCode)
	w.WriteEmptyTaggedFields()
	return nil
}
