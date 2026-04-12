package api

import (
	"net"

	"github.com/KhaiHust/kaf-go/constant"
	"github.com/KhaiHust/kaf-go/protocol"
	"github.com/KhaiHust/kaf-go/protocol/producer"
)

func HandleInitProducerIdApi(conn net.Conn, brokerContext IBrokerContext, header *protocol.RequestHeader, r *protocol.Reader) error {
	req := &producer.InitProducerIdRequest{}
	if err := req.Decode(r); err != nil {
		return err
	}

	resp := &producer.InitProducerIdResponse{}

	switch {
	case req.TransactionalId != nil:
		// TODO: forward to TxnCoordinator for the partition that hashes
		resp.ErrorCode = constant.ErrInvalidRequest
		resp.ProducerId = -1
		resp.ProducerEpoch = -1

	case !brokerContext.IsController():
		resp.ErrorCode = constant.ErrNotController
		resp.ProducerId = -1
		resp.ProducerEpoch = -1

	default:
		pid, err := brokerContext.GetPidManager().AllocateOne()
		if err != nil {
			resp.ErrorCode = constant.ErrKafkaStorageError
			resp.ProducerId = -1
			resp.ProducerEpoch = -1
			break
		}
		resp.ProducerId = pid
		resp.ProducerEpoch = 0
	}

	responseHeader := &protocol.ResponseHeader{
		CorrelationId: header.CorrelationId,
		ApiKey:        header.ApiKey,
		ApiVersion:    header.ApiVersion,
	}
	return protocol.WriteFraming(conn, responseHeader, resp)
}
