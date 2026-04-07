package consumer

import (
	"github.com/KhaiHust/kaf-go/constant"
	"github.com/KhaiHust/kaf-go/protocol"
	"github.com/KhaiHust/kaf-go/protocol/types"
)

type JoinGroupMember struct {
	MemberID        types.CompactString
	GroupInstanceID types.CompactNullableString
	Metadata        types.CompactBytes
}

type JoinGroupResponse struct {
	ThrottleTimeMs int32
	ErrorCode      int16
	GenerationID   int32
	ProtocolType   types.CompactNullableString
	ProtocolName   types.CompactNullableString
	Leader         types.CompactString
	SkipAssignment bool
	MemberID       types.CompactString
	Members        []JoinGroupMember
}

func (j *JoinGroupResponse) ApiKey() int16 {
	return constant.ApiKeyJoinGroup
}

func (j *JoinGroupResponse) Encode(w *protocol.Writer) error {
	w.WriteInt32(j.ThrottleTimeMs)
	w.WriteInt16(j.ErrorCode)
	w.WriteInt32(j.GenerationID)
	w.WriteCompactNullableString(j.ProtocolType)
	w.WriteCompactNullableString(j.ProtocolName)
	w.WriteCompactString(j.Leader)
	w.WriteBool(j.SkipAssignment)
	w.WriteCompactString(j.MemberID)
	w.WriteCompactArrayLen(len(j.Members))
	for _, m := range j.Members {
		if err := m.Encode(w); err != nil {
			return err
		}
	}
	w.WriteEmptyTaggedFields()
	return nil
}

func (m *JoinGroupMember) Encode(w *protocol.Writer) error {
	w.WriteCompactString(m.MemberID)
	w.WriteCompactNullableString(m.GroupInstanceID)
	w.WriteCompactBytes(m.Metadata)
	w.WriteEmptyTaggedFields()
	return nil
}
