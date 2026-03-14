package api

import (
	"log/slog"
	"net"
	"time"

	"github.com/KhaiHust/kaf-go/common"
	"github.com/KhaiHust/kaf-go/constant"
	"github.com/KhaiHust/kaf-go/protocol"
	"github.com/KhaiHust/kaf-go/protocol/producer"
	"github.com/KhaiHust/kaf-go/protocol/types"
	"github.com/KhaiHust/kaf-go/storage"
	"github.com/KhaiHust/kaf-go/storage/commitlog"
)

type partitionResult struct {
	index      int32
	baseOffset int64
	errorCode  int16
}
type topicResult struct {
	name       string
	partitions []partitionResult
}

func HandleProduceApiKeys(conn net.Conn, header *protocol.RequestHeader, r *protocol.Reader, topicStore *storage.TopicStore) error {
	produceRequest := &producer.ProduceRequest{}
	if err := produceRequest.Decode(r); err != nil {
		slog.Error("HandleProduceApiKeys: Decode produceRequest error:", "error", err)
		return err
	}

	var topicResults []topicResult
	for _, topic := range produceRequest.TopicData {
		toResult := &topicResult{
			name: topic.Name.String(),
		}

		for _, partition := range topic.PartitionData {
			pr := &partitionResult{
				index: partition.Index,
			}
			if partition.Records == nil {
				toResult.partitions = append(toResult.partitions, *pr)
				continue
			}

			batch, err := producer.DecodeRecordBatch(partition.Records)
			if err != nil {
				pr.errorCode = constant.ErrCorruptMessage
				toResult.partitions = append(toResult.partitions, *pr)
				continue
			}
			messages := make([]commitlog.Message, len(batch.Records))
			for i, record := range batch.Records {
				messages[i] = commitlog.Message{
					Key:   record.Key,
					Value: record.Value,
					Timestamp: time.Unix(0,
						batch.FirstTimestamp+record.TimestampDelta*int64(time.Millisecond)),
					Headers: record.Headers,
				}
			}

			//write to commit log
			partitionCommitLog := topicStore.GetCommitLog(string(topic.Name), partition.Index)
			if partitionCommitLog == nil {
				pr.errorCode = constant.ErrUnknownTopicOrPartition
				toResult.partitions = append(toResult.partitions, *pr)
				continue
			}
			pr.baseOffset, err = partitionCommitLog.Append(messages)
			if err != nil {
				pr.errorCode = constant.ErrKafkaStorageError
			}
			toResult.partitions = append(toResult.partitions, *pr)
		}
		topicResults = append(topicResults, *toResult)
	}
	return writeProduceApiResponse(conn, header, topicResults, produceRequest.Acks)
}

func writeProduceApiResponse(conn net.Conn, header *protocol.RequestHeader, results []topicResult, acks int16) error {
	if acks == 0 {
		return nil
	}
	resp := &producer.ProduceResponse{
		ThrottleTimeMs: 0,
		NodeEndpoints:  []producer.NodeEndpoint{}, // empty, single broker
	}

	for _, tr := range results {
		topicResp := producer.TopicProduceResponse{
			Name: types.NewCompactString(tr.name),
		}
		for _, pr := range tr.partitions {
			var errMsg *string
			if pr.errorCode != 0 {
				msg := common.ErrorMessage(pr.errorCode)
				errMsg = &msg
			}
			topicResp.PartitionResponses = append(
				topicResp.PartitionResponses,
				producer.PartitionProduceResponse{
					Index:           pr.index,
					ErrorCode:       pr.errorCode,
					BaseOffset:      pr.baseOffset,
					LogAppendTimeMs: -1, // -1 = CreateTime, not LogAppendTime
					LogStartOffset:  pr.baseOffset,
					RecordErrors:    []producer.RecordError{},
					ErrorMessage:    errMsg,
				},
			)
		}
		resp.Responses = append(resp.Responses, topicResp)
	}

	responseHeader := &protocol.ResponseHeader{
		CorrelationId: header.CorrelationId,
		ApiVersion:    header.ApiVersion,
		ApiKey:        header.ApiKey,
	}

	return protocol.WriteFraming(conn, responseHeader, resp)
}
