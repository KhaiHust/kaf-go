package api

import (
	"log/slog"
	"net"
	"time"

	"github.com/KhaiHust/kaf-go/common"
	"github.com/KhaiHust/kaf-go/constant"
	"github.com/KhaiHust/kaf-go/coordinator"
	"github.com/KhaiHust/kaf-go/protocol"
	"github.com/KhaiHust/kaf-go/protocol/producer"
	"github.com/gofrs/uuid/v5"
)

type partitionResult struct {
	index      int32
	baseOffset int64
	errorCode  int16
	suppressed bool
}
type topicResult struct {
	name       string
	topicId    uuid.UUID
	partitions []partitionResult
}

func HandleProduceApiKeys(conn net.Conn, brokerContext IBrokerContext, header *protocol.RequestHeader, r *protocol.Reader) error {
	produceRequest := &producer.ProduceRequest{}
	if err := produceRequest.Decode(r); err != nil {
		slog.Error("HandleProduceApiKeys: Decode produceRequest error:", "error", err)
		return err
	}

	validAck := validateValidAcks(produceRequest.Acks)

	var topicResults []topicResult
	pss := brokerContext.GetPartitionStateStore()
	for _, topic := range produceRequest.TopicData {
		topicMeta, err := brokerContext.GetTopicStore().GetTopic(topic.TopicId)
		if err != nil {
			continue
		}
		toResult := &topicResult{
			name:    topic.Name.String(),
			topicId: topic.TopicId,
		}

		for _, partition := range topic.PartitionData {
			pr := &partitionResult{
				index: partition.Index,
			}
			if !validAck {
				pr.errorCode = constant.ErrInvalidRequiredAcks
				toResult.partitions = append(toResult.partitions, *pr)
				continue
			}

			ps := pss.GetPartitionState(topicMeta.Name.String(), partition.Index)
			if ps == nil || ps.LeaderBrokerID != brokerContext.GetBrokerID() {
				pr.errorCode = constant.ErrErrNotLeaderForPartition
				toResult.partitions = append(toResult.partitions, *pr)
				continue
			}

			if produceRequest.Acks == constant.AckExactlyOnce {
				minISR := brokerContext.GetTopicStore().GetMinInsyncReplicas(topic.TopicId)
				if int16(len(ps.ISR)) < minISR {
					pr.errorCode = constant.ErrNotEnoughReplicas
					toResult.partitions = append(toResult.partitions, *pr)
					continue
				}
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

			// Idempotence dedup gate. PID < 0 means non-idempotent producer; skip the path.
			pid := batch.ProducerId
			epoch := batch.ProducerEpoch
			firstSeq := batch.BaseSequence
			lastSeq := firstSeq + int32(len(batch.Records)) - 1
			if pid >= 0 && ps.Idempotence != nil {
				cachedBase, dup, vErr := ps.Idempotence.Validate(pid, epoch, firstSeq, lastSeq)
				if vErr != nil {
					pr.errorCode = constant.GetErrorId(vErr)
					toResult.partitions = append(toResult.partitions, *pr)
					continue
				}
				if dup {
					pr.baseOffset = cachedBase
					toResult.partitions = append(toResult.partitions, *pr)
					continue
				}
			}

			//write to commit log
			partitionCommitLog := brokerContext.GetTopicStore().GetCommitLog(topic.TopicId, partition.Index)
			if partitionCommitLog == nil {
				pr.errorCode = constant.ErrUnknownTopicOrPartition
				toResult.partitions = append(toResult.partitions, *pr)
				continue
			}
			baseOffset, err := partitionCommitLog.AppendRaw(partition.Records, int32(len(batch.Records)))
			if err != nil {
				pr.errorCode = constant.ErrKafkaStorageError
				toResult.partitions = append(toResult.partitions, *pr)
				continue
			}

			if pid >= 0 && ps.Idempotence != nil {
				ps.Idempotence.Record(pid, epoch, firstSeq, lastSeq, baseOffset)
			}

			lastOffset := baseOffset + int64(len(batch.Records)) - 1
			ps.OnLeaderAppend(lastOffset)

			switch produceRequest.Acks {
			case constant.AckAtMostOnce:
				pr.suppressed = true
			case constant.AckAtLeastOnce:
				pr.baseOffset = baseOffset
			case constant.AckExactlyOnce:
				w := &coordinator.PartitionAppendWaiter{
					RequiredOffset: lastOffset,
					ISRSnapshot:    append([]int32(nil), ps.ISR...),
					Deadline:       time.Now().Add(time.Duration(produceRequest.TimeoutMs) * time.Millisecond),
					Done:           make(chan error, 1),
				}
				ps.Purgatory.Add(w)
				ps.Purgatory.CompleteUpTo(ps.GetHWM())
				select {
				case err := <-w.Done:
					if err != nil {
						pr.errorCode = constant.GetErrorId(err)
					}
					pr.baseOffset = baseOffset
				case <-time.After(time.Until(w.Deadline)):
					pr.errorCode = constant.ErrRequestTimedOut
					pr.baseOffset = baseOffset
				}

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
			TopicId: tr.topicId,
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

func validateValidAcks(ack int16) bool {
	return ack == constant.AckAtMostOnce || ack == constant.AckAtLeastOnce || ack == constant.AckExactlyOnce
}
