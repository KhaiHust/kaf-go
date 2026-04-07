package consumer

import (
	"github.com/KhaiHust/kaf-go/constant"
	"github.com/KhaiHust/kaf-go/protocol"
	"github.com/KhaiHust/kaf-go/protocol/types"
)

const HeartbeatRequestVersion = 4

type HeartbeatRequest struct {
	GroupID         types.CompactString
	GenerationID    int32
	MemberID        types.CompactString
	GroupInstanceID types.CompactNullableString
}

func (h *HeartbeatRequest) ApiKey() int64 {
	return constant.ApiKeyHeartbeat
}

func (h *HeartbeatRequest) ApiVersion() int16 {
	return HeartbeatRequestVersion
}

func (h *HeartbeatRequest) CorrelationID() int32 {
	panic("implement me")
}

func (h *HeartbeatRequest) Decode(r *protocol.Reader) error {
	var err error

	if h.GroupID, err = r.ReadCompactString(); err != nil {
		return err
	}

	if h.GenerationID, err = r.ReadInt32(); err != nil {
		return err
	}

	if h.MemberID, err = r.ReadCompactString(); err != nil {
		return err
	}

	if h.GroupInstanceID, err = r.ReadCompactNullableString(); err != nil {
		return err
	}

	return r.ReadTaggedFields()
}
