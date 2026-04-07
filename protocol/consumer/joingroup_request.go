package consumer

import (
	"github.com/KhaiHust/kaf-go/constant"
	"github.com/KhaiHust/kaf-go/protocol"
	"github.com/KhaiHust/kaf-go/protocol/types"
)

const JoinGroupRequestVersion = 9

type JoinGroupProtocol struct {
	Name     types.CompactString
	Metadata types.CompactBytes
}

type JoinGroupRequest struct {
	GroupID            types.CompactString
	SessionTimeoutMs   int32
	RebalanceTimeoutMs int32
	MemberID           types.CompactString
	GroupInstanceID    types.CompactNullableString
	ProtocolType       types.CompactString
	Protocols          []JoinGroupProtocol
	Reason             types.CompactNullableString
}

func (j *JoinGroupRequest) ApiKey() int64 {
	return constant.ApiKeyJoinGroup
}

func (j *JoinGroupRequest) ApiVersion() int16 {
	return JoinGroupRequestVersion
}

func (j *JoinGroupRequest) CorrelationID() int32 {
	panic("implement me")
}

func (j *JoinGroupRequest) Decode(r *protocol.Reader) error {
	var err error

	if j.GroupID, err = r.ReadCompactString(); err != nil {
		return err
	}

	if j.SessionTimeoutMs, err = r.ReadInt32(); err != nil {
		return err
	}

	if j.RebalanceTimeoutMs, err = r.ReadInt32(); err != nil {
		return err
	}

	if j.MemberID, err = r.ReadCompactString(); err != nil {
		return err
	}

	if j.GroupInstanceID, err = r.ReadCompactNullableString(); err != nil {
		return err
	}

	if j.ProtocolType, err = r.ReadCompactString(); err != nil {
		return err
	}

	protocolsLen, err := r.ReadCompactArrayLen()
	if err != nil {
		return err
	}

	j.Protocols = make([]JoinGroupProtocol, protocolsLen)
	for i := range j.Protocols {
		if j.Protocols[i].Name, err = r.ReadCompactString(); err != nil {
			return err
		}
		if j.Protocols[i].Metadata, err = r.ReadCompactBytes(); err != nil {
			return err
		}
		if err = r.ReadTaggedFields(); err != nil {
			return err
		}
	}

	if j.Reason, err = r.ReadCompactNullableString(); err != nil {
		return err
	}
	if err = r.ReadTaggedFields(); err != nil {
		return err
	}

	return nil
}
