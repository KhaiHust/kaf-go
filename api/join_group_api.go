package api

import (
	"net"
	"time"

	"github.com/KhaiHust/kaf-go/constant"
	"github.com/KhaiHust/kaf-go/coordinator"
	"github.com/KhaiHust/kaf-go/protocol"
	"github.com/KhaiHust/kaf-go/protocol/consumer"
	"github.com/KhaiHust/kaf-go/protocol/types"
)

func HandleJoinGroupApi(conn net.Conn, requestHeader *protocol.RequestHeader, r *protocol.Reader, groupStore *coordinator.GroupStore) error {
	var request consumer.JoinGroupRequest

	if err := request.Decode(r); err != nil {
		return err
	}

	responseHeader := &protocol.ResponseHeader{
		CorrelationId: requestHeader.CorrelationId,
		ApiVersion:    requestHeader.ApiVersion,
		ApiKey:        requestHeader.ApiKey,
	}

	response := &consumer.JoinGroupResponse{}

	// Empty member_id = initial join: generate an ID and ask the client to retry.
	if request.MemberID == "" {
		generatedID := groupStore.GenerateMemberId(request.GroupID.String())
		response.MemberID = types.CompactString(generatedID)
		response.ErrorCode = constant.ErrMemberIdRequired
		return protocol.WriteFraming(conn, responseHeader, response)
	}

	var groupInstanceId string
	if request.GroupInstanceID != nil {
		groupInstanceId = *request.GroupInstanceID
	}

	resultCh, err := groupStore.JoinGroup(
		request.GroupID.String(),
		request.MemberID.String(),
		groupInstanceId,
		request.Protocols[0].Name.String(),
		request.Protocols[0].Metadata,
		time.Duration(request.SessionTimeoutMs)*time.Millisecond,
		time.Duration(request.RebalanceTimeoutMs)*time.Millisecond)

	if err != nil {
		response.ErrorCode = constant.GetErrorId(err)
		return protocol.WriteFraming(conn, responseHeader, response)
	}
	// go to rebalance

	result := <-resultCh

	if result.Err != nil {
		response.ErrorCode = constant.GetErrorId(result.Err)
		return protocol.WriteFraming(conn, responseHeader, response)
	}

	response.GenerationID = result.GenerationId
	response.ProtocolName = types.NewCompactNullableString(result.ProtocolName)
	response.Leader = types.CompactString(result.LeaderId)
	response.MemberID = request.MemberID

	response.Members = make([]consumer.JoinGroupMember, 0, len(result.Members))
	for _, member := range result.Members {
		response.Members = append(response.Members, consumer.JoinGroupMember{
			MemberID:        types.CompactString(member.MemberId),
			GroupInstanceID: types.NewCompactNullableString(member.GroupInstanceId),
			Metadata:        member.Metadata,
		})
	}

	return protocol.WriteFraming(conn, responseHeader, response)
}
