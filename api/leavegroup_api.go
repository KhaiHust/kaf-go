package api

import (
	"net"

	"github.com/KhaiHust/kaf-go/constant"
	"github.com/KhaiHust/kaf-go/coordinator"
	"github.com/KhaiHust/kaf-go/protocol"
	"github.com/KhaiHust/kaf-go/protocol/consumer"
)

func HandleLeaveGroupApi(conn net.Conn, header *protocol.RequestHeader, r *protocol.Reader, groupStore *coordinator.GroupStore) error {
	var request consumer.LeaveGroupRequest
	if err := request.Decode(r); err != nil {
		return err
	}

	response := &consumer.LeaveGroupResponse{}
	for _, m := range request.Members {
		mr := consumer.LeaveGroupMemberResponse{
			MemberID:        m.MemberID,
			GroupInstanceID: m.GroupInstanceID,
		}
		if err := groupStore.LeaveGroup(request.GroupID.String(), m.MemberID.String()); err != nil {
			mr.ErrorCode = constant.GetErrorId(err)
		}
		response.Members = append(response.Members, mr)
	}

	responseHeader := &protocol.ResponseHeader{
		CorrelationId: header.CorrelationId,
		ApiVersion:    header.ApiVersion,
		ApiKey:        header.ApiKey,
	}
	return protocol.WriteFraming(conn, responseHeader, response)
}
