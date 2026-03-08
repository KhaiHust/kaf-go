package api

import (
	"net"

	"github.com/KhaiHust/kaf-go/common"
	"github.com/KhaiHust/kaf-go/protocol"
	"github.com/KhaiHust/kaf-go/protocol/admin"
)

func HandleMetaData(conn net.Conn, header *protocol.RequestHeader, r *protocol.Reader) error {
	responseHeader := &protocol.ResponseHeader{
		CorrelationId: header.CorrelationId,
		ApiVersion:    header.ApiVersion,
		ApiKey:        header.ApiKey,
	}

	requestBody := admin.MetaDataRequest{}
	if err := requestBody.Decode(r); err != nil {
		return err
	}

	responseBody := &admin.MetadataResponse{
		ThrottleTimeMs: 0,
		Brokers: []admin.MetadataResponseBroker{
			{
				NodeID: 1,
				Host:   "localhost",
				Port:   9092,
				Rack:   nil,
			},
		},
		ClusterID:    common.StringPtr("my-cluster"),
		ControllerID: 1,
		Topics:       nil,
		ErrorCode:    0,
	}

	if err := protocol.WriteFraming(conn, responseHeader, responseBody); err != nil {
		return err
	}
	return nil
}
