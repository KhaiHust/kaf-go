package consumer

import (
	"github.com/KhaiHust/kaf-go/constant"
	"github.com/KhaiHust/kaf-go/protocol"
	"github.com/KhaiHust/kaf-go/protocol/types"
)

type SyncGroupResponse struct {
	ThrottleTimeMs int32
	ErrorCode      int16
	ProtocolType   types.CompactNullableString
	ProtocolName   types.CompactNullableString
	Assignment     types.CompactBytes
}

func (s *SyncGroupResponse) ApiKey() int16 {
	return constant.ApiKeySyncGroup
}

func (s *SyncGroupResponse) Encode(w *protocol.Writer) error {
	w.WriteInt32(s.ThrottleTimeMs)
	w.WriteInt16(s.ErrorCode)
	w.WriteCompactNullableString(s.ProtocolType)
	w.WriteCompactNullableString(s.ProtocolName)
	w.WriteCompactBytes(s.Assignment)
	w.WriteEmptyTaggedFields()
	return nil
}
