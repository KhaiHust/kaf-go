package api

import (
	"net"

	"github.com/KhaiHust/kaf-go/constant"
	"github.com/KhaiHust/kaf-go/protocol"
	brokerproto "github.com/KhaiHust/kaf-go/protocol/broker"
	metadatapkg "github.com/KhaiHust/kaf-go/protocol/metadata"
)

func HandleBrokerRegistrationApi(
	conn net.Conn,
	brokerContext IBrokerContext,
	header *protocol.RequestHeader,
	r *protocol.Reader,
) error {
	var req brokerproto.BrokerRegistrationRequest
	if err := req.Decode(r); err != nil {
		return err
	}

	responseHeader := &protocol.ResponseHeader{
		CorrelationId: header.CorrelationId,
	}
	resp := &brokerproto.BrokerRegistrationResponse{}

	if !brokerContext.IsController() {
		resp.ErrorCode = constant.ErrNotController
		return protocol.WriteFraming(conn, responseHeader, resp)
	}

	if req.BrokerId <= 0 {
		resp.ErrorCode = constant.ErrInvalidRequest
		return protocol.WriteFraming(conn, responseHeader, resp)
	}

	var host string
	var port int32
	for _, l := range req.Listeners {
		if l.Name.String() == "PLAINTEXT" {
			host = l.Host.String()
			port = l.Port
			break
		}
	}
	if host == "" && len(req.Listeners) > 0 {
		host = req.Listeners[0].Host.String()
		port = req.Listeners[0].Port
	}

	epoch := brokerContext.GetBrokerRegistry().RegisterBroker(req.BrokerId, host, port)

	rec := &metadatapkg.BrokerRecord{
		BrokerId: req.BrokerId,
		Host:     host,
		Port:     port,
		Epoch:    epoch,
	}
	_ = brokerContext.GetMetadataLog().Append(rec.Encode())

	resp.BrokerEpoch = epoch
	return protocol.WriteFraming(conn, responseHeader, resp)
}
