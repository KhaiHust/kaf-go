package api

import (
	"net"

	"github.com/KhaiHust/kaf-go/coordinator"
	"github.com/KhaiHust/kaf-go/protocol"
	"github.com/KhaiHust/kaf-go/protocol/consumer"
)

func HandleHeartbeatApi(conn net.Conn, requestHeader *protocol.RequestHeader, r *protocol.Reader, groupStore *coordinator.GroupStore) error {
	var heartbeatRequest consumer.HeartbeatRequest
	if err := heartbeatRequest.Decode(r); err != nil {
		return err
	}

	result := groupStore.Heartbeat(
		heartbeatRequest.GroupID.String(),
		heartbeatRequest.MemberID.String(),
		heartbeatRequest.GenerationID,
	)

	responseHeader := &protocol.ResponseHeader{
		CorrelationId: requestHeader.CorrelationId,
		ApiVersion:    requestHeader.ApiVersion,
		ApiKey:        requestHeader.ApiKey,
	}

	response := &consumer.HeartbeatResponse{
		ThrottleTimeMs: 0,
		ErrorCode:      int16(result),
	}

	return protocol.WriteFraming(conn, responseHeader, response)
}
