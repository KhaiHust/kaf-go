package api

import (
	"net"

	"github.com/KhaiHust/kaf-go/coordinator"
	"github.com/KhaiHust/kaf-go/protocol"
	"github.com/KhaiHust/kaf-go/protocol/consumer"
	"github.com/KhaiHust/kaf-go/protocol/types"
)

func HandleOffsetFetchApi(conn net.Conn, requestHeader *protocol.RequestHeader, r *protocol.Reader, groupStore *coordinator.GroupStore) error {
	var request consumer.OffsetFetchRequest
	if err := request.Decode(r); err != nil {
		return err
	}

	response := &consumer.OffsetFetchResponse{}

	for _, g := range request.Groups {
		var queries []coordinator.FetchOffsetQuery
		for _, t := range g.Topics {
			for _, partIdx := range t.PartitionIndexes {
				queries = append(queries, coordinator.FetchOffsetQuery{
					TopicName:    t.Topic.String(),
					PartitionIdx: partIdx,
				})
			}
		}

		results := groupStore.FetchOffsets(g.GroupID.String(), queries)

		topicMap := make(map[string]*consumer.OffsetFetchResponseTopic)
		for _, res := range results {
			key := res.TopicName
			rt, ok := topicMap[key]
			if !ok {
				rt = &consumer.OffsetFetchResponseTopic{Topic: types.CompactString(res.TopicName)}
				topicMap[key] = rt
			}
			rt.Partitions = append(rt.Partitions, consumer.OffsetFetchResponsePartition{
				PartitionIndex:       res.PartitionIdx,
				CommittedOffset:      res.Offset,
				CommittedLeaderEpoch: res.LeaderEpoch,
				Metadata:             res.Metadata,
				ErrorCode:            0,
			})
		}

		rg := consumer.OffsetFetchResponseGroup{
			GroupID: g.GroupID,
		}
		for _, rt := range topicMap {
			rg.Topics = append(rg.Topics, *rt)
		}
		response.Groups = append(response.Groups, rg)
	}

	responseHeader := &protocol.ResponseHeader{
		CorrelationId: requestHeader.CorrelationId,
		ApiVersion:    requestHeader.ApiVersion,
		ApiKey:        requestHeader.ApiKey,
	}
	return protocol.WriteFraming(conn, responseHeader, response)
}
