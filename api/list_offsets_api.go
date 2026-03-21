package api

import (
	"net"

	"github.com/KhaiHust/kaf-go/constant"
	"github.com/KhaiHust/kaf-go/protocol"
	"github.com/KhaiHust/kaf-go/protocol/admin"
	"github.com/KhaiHust/kaf-go/storage"
	"github.com/gofrs/uuid/v5"
)

func HandleListOffsetsApi(conn net.Conn, header *protocol.RequestHeader, r *protocol.Reader, store *storage.TopicStore) error {
	req := admin.ListOffsetsRequest{}
	if err := req.Decode(r); err != nil {
		return err
	}

	resp := &admin.ListOffsetsResponse{}

	for _, topic := range req.Topics {
		topicResp := admin.ListOffsetsResponseTopic{Name: topic.Name}
		topicMeta, err := store.GetTopicByName(string(topic.Name))
		var topicId uuid.UUID
		if err != nil {
			topicId = uuid.Nil
		} else {
			topicId = topicMeta.TopicId
		}

		for _, partition := range topic.Partitions {
			partResp := admin.ListOffsetsResponsePartition{
				PartitionIndex: partition.PartitionIndex,
				Timestamp:      -1,
				LeaderEpoch:    0,
			}
			partCommitLog := store.GetCommitLog(topicId, partition.PartitionIndex)
			if partCommitLog == nil {
				partResp.ErrorCode = constant.ErrUnknownTopicOrPartition
				partResp.Offset = -1
				topicResp.Partitions = append(topicResp.Partitions, partResp)
				continue
			}

			switch partition.Timestamp {
			case admin.ListOffsetsEarliest:
				partResp.Offset = partCommitLog.OldestOffset()
			case admin.ListOffsetsLatest:
				partResp.Offset = partCommitLog.NewestOffset()
			default:
				partResp.Offset = partCommitLog.NewestOffset() + 1
			}
			topicResp.Partitions = append(topicResp.Partitions, partResp)
		}
		resp.Topics = append(resp.Topics, topicResp)
	}

	responseHeader := &protocol.ResponseHeader{
		CorrelationId: header.CorrelationId,
		ApiVersion:    header.ApiVersion,
		ApiKey:        header.ApiKey,
	}
	return protocol.WriteFraming(conn, responseHeader, resp)
}
