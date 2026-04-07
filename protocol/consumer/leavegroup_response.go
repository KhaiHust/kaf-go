package consumer

import (
	"github.com/KhaiHust/kaf-go/constant"
	"github.com/KhaiHust/kaf-go/protocol"
	"github.com/KhaiHust/kaf-go/protocol/types"
)

type LeaveGroupMemberResponse struct {
	MemberID        types.CompactString
	GroupInstanceID types.CompactNullableString
	ErrorCode       int16
}

type LeaveGroupResponse struct {
	ThrottleTimeMs int32
	ErrorCode      int16
	Members        []LeaveGroupMemberResponse
}

func (l *LeaveGroupResponse) ApiKey() int16 {
	return constant.ApiKeyLeaveGroup
}

func (l *LeaveGroupResponse) Encode(w *protocol.Writer) error {
	w.WriteInt32(l.ThrottleTimeMs)
	w.WriteInt16(l.ErrorCode)
	w.WriteCompactArrayLen(len(l.Members))
	for _, m := range l.Members {
		if err := m.Encode(w); err != nil {
			return err
		}
	}
	w.WriteEmptyTaggedFields()
	return nil
}

func (m *LeaveGroupMemberResponse) Encode(w *protocol.Writer) error {
	w.WriteCompactString(m.MemberID)
	w.WriteCompactNullableString(m.GroupInstanceID)
	w.WriteInt16(m.ErrorCode)
	w.WriteEmptyTaggedFields()
	return nil
}
