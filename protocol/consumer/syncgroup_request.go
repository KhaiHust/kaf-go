package consumer

import (
	"github.com/KhaiHust/kaf-go/constant"
	"github.com/KhaiHust/kaf-go/protocol"
	"github.com/KhaiHust/kaf-go/protocol/types"
)

const SyncGroupRequestVersion = 5

type SyncGroupAssignment struct {
	MemberID   types.CompactString
	Assignment types.CompactBytes
}

type SyncGroupRequest struct {
	GroupID         types.CompactString
	GenerationID    int32
	MemberID        types.CompactString
	GroupInstanceID types.CompactNullableString
	ProtocolType    types.CompactNullableString
	ProtocolName    types.CompactNullableString
	Assignments     []SyncGroupAssignment
}

func (s *SyncGroupRequest) ApiKey() int64 {
	return constant.ApiKeySyncGroup
}

func (s *SyncGroupRequest) ApiVersion() int16 {
	return SyncGroupRequestVersion
}

func (s *SyncGroupRequest) CorrelationID() int32 {
	panic("implement me")
}

func (s *SyncGroupRequest) Decode(r *protocol.Reader) error {
	var err error

	if s.GroupID, err = r.ReadCompactString(); err != nil {
		return err
	}

	if s.GenerationID, err = r.ReadInt32(); err != nil {
		return err
	}

	if s.MemberID, err = r.ReadCompactString(); err != nil {
		return err
	}

	if s.GroupInstanceID, err = r.ReadCompactNullableString(); err != nil {
		return err
	}

	if s.ProtocolType, err = r.ReadCompactNullableString(); err != nil {
		return err
	}

	if s.ProtocolName, err = r.ReadCompactNullableString(); err != nil {
		return err
	}

	assignmentsLen, err := r.ReadCompactArrayLen()
	if err != nil {
		return err
	}

	s.Assignments = make([]SyncGroupAssignment, assignmentsLen)
	for i := range s.Assignments {
		if s.Assignments[i].MemberID, err = r.ReadCompactString(); err != nil {
			return err
		}
		if s.Assignments[i].Assignment, err = r.ReadCompactBytes(); err != nil {
			return err
		}
		if err = r.ReadTaggedFields(); err != nil {
			return err
		}
	}

	return r.ReadTaggedFields()
}
