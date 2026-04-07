package consumer

import (
	"github.com/KhaiHust/kaf-go/constant"
	"github.com/KhaiHust/kaf-go/protocol"
	"github.com/KhaiHust/kaf-go/protocol/types"
)

const LeaveGroupRequestVersion = 5

type LeaveGroupMember struct {
	MemberID        types.CompactString
	GroupInstanceID types.CompactNullableString
	Reason          types.CompactNullableString
}

type LeaveGroupRequest struct {
	GroupID types.CompactString
	Members []LeaveGroupMember
}

func (l *LeaveGroupRequest) ApiKey() int64 {
	return constant.ApiKeyLeaveGroup
}

func (l *LeaveGroupRequest) ApiVersion() int16 {
	return LeaveGroupRequestVersion
}

func (l *LeaveGroupRequest) CorrelationID() int32 {
	panic("implement me")
}

func (l *LeaveGroupRequest) Decode(r *protocol.Reader) error {
	var err error

	if l.GroupID, err = r.ReadCompactString(); err != nil {
		return err
	}

	membersLen, err := r.ReadCompactArrayLen()
	if err != nil {
		return err
	}

	l.Members = make([]LeaveGroupMember, membersLen)
	for i := range l.Members {
		m := &l.Members[i]

		if m.MemberID, err = r.ReadCompactString(); err != nil {
			return err
		}
		if m.GroupInstanceID, err = r.ReadCompactNullableString(); err != nil {
			return err
		}
		if m.Reason, err = r.ReadCompactNullableString(); err != nil {
			return err
		}
		if err = r.ReadTaggedFields(); err != nil {
			return err
		}
	}

	return r.ReadTaggedFields()
}
