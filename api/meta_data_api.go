package api

import (
	"fmt"
	"log/slog"
	"net"

	"github.com/KhaiHust/kaf-go/common"
	"github.com/KhaiHust/kaf-go/protocol"
	"github.com/KhaiHust/kaf-go/protocol/admin"
	"github.com/KhaiHust/kaf-go/storage"
)

func HandleMetaData(conn net.Conn, header *protocol.RequestHeader, r *protocol.Reader, store *storage.TopicStore) error {
	responseHeader := &protocol.ResponseHeader{
		CorrelationId: header.CorrelationId,
		ApiVersion:    header.ApiVersion,
		ApiKey:        header.ApiKey,
	}

	requestBody := admin.MetaDataRequest{}
	if err := requestBody.Decode(r); err != nil {
		return err
	}

	slog.Info("Decoded MetaDataRequest ", "requestBody", requestBody,
		"numTopics", len(requestBody.Topics))

	for i, t := range requestBody.Topics {
		slog.Info(fmt.Sprintf("Topic[%d]", i),
			"name", *t.Name,
			"topicId", t.TopicId)
	}
	topicNames := make([]string, len(requestBody.Topics))
	for i, t := range requestBody.Topics {
		topicNames[i] = *t.Name
	}
	topicsMetaData, _ := store.GetTopicMetadataByNames(topicNames)

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
		Topics:       topicsMetaData,
		ErrorCode:    0,
	}

	if err := protocol.WriteFraming(conn, responseHeader, responseBody); err != nil {
		return err
	}
	return nil
}
