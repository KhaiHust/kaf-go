package api

import (
	"net"

	"github.com/KhaiHust/kaf-go/constant"
	"github.com/KhaiHust/kaf-go/protocol"
	brokerproto "github.com/KhaiHust/kaf-go/protocol/broker"
)

const fenceThreshold = 10 // broker is fenced when it falls this many records behind

func HandleBrokerHeartbeatApi(
	conn net.Conn,
	brokerContext IBrokerContext,
	header *protocol.RequestHeader,
	r *protocol.Reader,
) error {
	var req brokerproto.BrokerHeartbeatRequest
	if err := req.Decode(r); err != nil {
		return err
	}

	responseHeader := &protocol.ResponseHeader{
		CorrelationId: header.CorrelationId,
	}
	resp := &brokerproto.BrokerHeartbeatResponse{}

	if !brokerContext.IsController() {
		resp.ErrorCode = constant.ErrNotController
		return protocol.WriteFraming(conn, responseHeader, resp)
	}

	err := brokerContext.GetBrokerRegistry().ValidateAndUpdate(
		req.BrokerId, req.BrokerEpoch, req.CurrentMetadataOffset,
	)
	if err != nil {
		resp.ErrorCode = constant.ErrStaleBrokerEpoch
		return protocol.WriteFraming(conn, responseHeader, resp)
	}

	newestOffset := brokerContext.GetMetadataLog().NewestOffset()
	resp.IsCaughtUp = req.CurrentMetadataOffset >= newestOffset
	resp.IsFenced = newestOffset-req.CurrentMetadataOffset > fenceThreshold
	resp.ShouldShutDown = req.WantShutDown

	return protocol.WriteFraming(conn, responseHeader, resp)
}
