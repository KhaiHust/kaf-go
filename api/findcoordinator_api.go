package api

import (
	"net"

	"github.com/KhaiHust/kaf-go/protocol"
	"github.com/KhaiHust/kaf-go/protocol/consumer"
)

func HandleFindCoordinatorApi(conn net.Conn, requestHeader *protocol.RequestHeader, r *protocol.Reader) error {
	var request consumer.FindCoordinatorRequest
	if err := request.Decode(r); err != nil {
		return err
	}

	responseHeader := &protocol.ResponseHeader{
		CorrelationId: requestHeader.CorrelationId,
		ApiKey:        requestHeader.ApiKey,
		ApiVersion:    requestHeader.ApiVersion,
	}

	response := &consumer.FindCoordinatorResponse{
		ThrottleTimeMs: 0,
	}

	response.Coordinators = []consumer.Coordinator{
		{
			Key:       request.CoordinatorKeys[0],
			NodeID:    1, // broker ID — must match the NodeID in the Metadata response
			Host:      "localhost",
			Port:      9092,
			ErrorCode: 0,
		},
	}

	return protocol.WriteFraming(conn, responseHeader, response)
}
