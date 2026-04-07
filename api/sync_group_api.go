package api

import (
	"net"

	"github.com/KhaiHust/kaf-go/constant"
	"github.com/KhaiHust/kaf-go/coordinator"
	"github.com/KhaiHust/kaf-go/protocol"
	"github.com/KhaiHust/kaf-go/protocol/consumer"
)

func HandleSyncGroupApi(conn net.Conn, requestHeader *protocol.RequestHeader, r *protocol.Reader, groupStore *coordinator.GroupStore) error {
	var request consumer.SyncGroupRequest
	if err := request.Decode(r); err != nil {
		return err
	}

	responseHeader := &protocol.ResponseHeader{
		CorrelationId: requestHeader.CorrelationId,
		ApiVersion:    requestHeader.ApiVersion,
		ApiKey:        requestHeader.ApiKey,
	}

	response := &consumer.SyncGroupResponse{
		ProtocolType: request.ProtocolType,
		ProtocolName: request.ProtocolName,
	}

	var assignments map[string][]byte
	if len(request.Assignments) > 0 {
		assignments = make(map[string][]byte, len(request.Assignments))
		for _, a := range request.Assignments {
			assignments[a.MemberID.String()] = a.Assignment
		}
	}

	resultCh, err := groupStore.SyncGroup(
		request.GroupID.String(),
		request.MemberID.String(),
		request.GenerationID,
		assignments,
	)
	if err != nil {
		response.ErrorCode = constant.GetErrorId(err)
		return protocol.WriteFraming(conn, responseHeader, response)
	}

	result := <-resultCh

	if result.Err != nil {
		response.ErrorCode = constant.GetErrorId(result.Err)
		return protocol.WriteFraming(conn, responseHeader, response)
	}

	response.Assignment = result.Assignment
	return protocol.WriteFraming(conn, responseHeader, response)
}
