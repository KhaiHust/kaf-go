package api

import (
	"net"

	"github.com/KhaiHust/kaf-go/coordinator"
	"github.com/KhaiHust/kaf-go/protocol"
	"github.com/KhaiHust/kaf-go/protocol/consumer"
)

func HandleOffsetCommitApi(conn net.Conn, requestHeader *protocol.RequestHeader, r *protocol.Reader, groupStore *coordinator.GroupStore) error {
	var request consumer.OffsetCommitRequest
	if err := request.Decode(r); err != nil {
		return err
	}

	var commits []coordinator.CommitOffset
	for _, t := range request.Topics {
		for _, p := range t.Partitions {
			commits = append(commits, coordinator.CommitOffset{
				TopicName:    t.Topic.String(),
				PartitionIdx: p.PartitionIndex,
				Offset:       p.CommittedOffset,
				LeaderEpoch:  p.CommittedLeaderEpoch,
				Metadata:     p.CommittedMetadata,
			})
		}
	}
	groupStore.CommitOffsets(request.GroupID.String(), commits)

	response := &consumer.OffsetCommitResponse{}
	for _, t := range request.Topics {
		rt := consumer.OffsetCommitResponseTopic{Topic: t.Topic}
		for _, p := range t.Partitions {
			rt.Partitions = append(rt.Partitions, consumer.OffsetCommitResponsePartition{
				PartitionIndex: p.PartitionIndex,
				ErrorCode:      0,
			})
		}
		response.Topics = append(response.Topics, rt)
	}

	responseHeader := &protocol.ResponseHeader{
		CorrelationId: requestHeader.CorrelationId,
		ApiVersion:    requestHeader.ApiVersion,
		ApiKey:        requestHeader.ApiKey,
	}
	return protocol.WriteFraming(conn, responseHeader, response)
}
