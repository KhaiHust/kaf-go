package api

import (
	"fmt"
	"log/slog"
	"net"
	"sort"

	"github.com/KhaiHust/kaf-go/common"
	"github.com/KhaiHust/kaf-go/protocol"
	"github.com/KhaiHust/kaf-go/protocol/admin"
	"github.com/KhaiHust/kaf-go/protocol/types"
)

func HandleMetaData(conn net.Conn, brokerContext IBrokerContext, header *protocol.RequestHeader, r *protocol.Reader) error {
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
	pss := brokerContext.GetPartitionStateStore()
	topicsMetaData, _ := brokerContext.GetTopicStore().GetTopicMetadataByNames(topicNames, pss)

	// Build broker list from registry (sorted by ID for determinism).
	allIDs := brokerContext.GetAllBrokerIDs()
	sort.Slice(allIDs, func(i, j int) bool { return allIDs[i] < allIDs[j] })

	reg := brokerContext.GetBrokerRegistry()
	brokers := make([]admin.MetadataResponseBroker, 0, len(allIDs))
	for _, id := range allIDs {
		host, port := reg.GetBrokerAddr(id)
		brokers = append(brokers, admin.MetadataResponseBroker{
			NodeID: id,
			Host:   types.CompactString(host),
			Port:   port,
			Rack:   nil,
		})
	}

	// Controller is the broker with the lowest ID.
	controllerID := int32(1)
	if len(allIDs) > 0 {
		controllerID = allIDs[0]
	}

	responseBody := &admin.MetadataResponse{
		ThrottleTimeMs: 0,
		Brokers:        brokers,
		ClusterID:      common.StringPtr("my-cluster"),
		ControllerID:   controllerID,
		Topics:         topicsMetaData,
		ErrorCode:      0,
	}

	if err := protocol.WriteFraming(conn, responseHeader, responseBody); err != nil {
		return err
	}
	return nil
}
