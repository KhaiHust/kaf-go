package api

import (
	"net"

	"github.com/KhaiHust/kaf-go/protocol"
	"github.com/KhaiHust/kaf-go/protocol/consumer"
	"github.com/KhaiHust/kaf-go/protocol/types"
)

func HandleFindCoordinatorApi(conn net.Conn, requestHeader *protocol.RequestHeader, r *protocol.Reader, ctx IBrokerContext) error {
	var request consumer.FindCoordinatorRequest
	if err := request.Decode(r); err != nil {
		return err
	}

	responseHeader := &protocol.ResponseHeader{
		CorrelationId: requestHeader.CorrelationId,
		ApiKey:        requestHeader.ApiKey,
		ApiVersion:    requestHeader.ApiVersion,
	}

	// Coordinator = self for now (single-broker group coordinator). Resolve own
	// host/port from the BrokerRegistry so the response uses the advertised
	// hostname (e.g. "kaf1" inside Docker), not a hardcoded "localhost".
	selfID := ctx.GetBrokerID()
	host, port := ctx.GetBrokerRegistry().GetBrokerAddr(selfID)
	if host == "" {
		host = "localhost"
	}
	if port == 0 {
		port = 9092
	}

	response := &consumer.FindCoordinatorResponse{
		ThrottleTimeMs: 0,
	}
	response.Coordinators = []consumer.Coordinator{
		{
			Key:       request.CoordinatorKeys[0],
			NodeID:    selfID,
			Host:      types.CompactString(host),
			Port:      port,
			ErrorCode: 0,
		},
	}

	return protocol.WriteFraming(conn, responseHeader, response)
}
