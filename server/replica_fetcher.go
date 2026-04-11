package server

import (
	"encoding/binary"
	"io"
	"time"

	"github.com/KhaiHust/kaf-go/protocol/fetch"
	"github.com/KhaiHust/kaf-go/storage/commitlog"
	"github.com/gofrs/uuid/v5"
)

type ReplicaFetcher struct {
	client        *BrokerClient
	topicID       uuid.UUID
	topicName     string
	partition     int32
	fetchOffset   int64 // start from local LEO
	commitLog     commitlog.ICommitLog
	highWatermark int64
	stopCh        chan struct{}
}

func NewReplicaFetcher(client *BrokerClient, topicName string, topicId uuid.UUID, partition int32, commitLog commitlog.ICommitLog) *ReplicaFetcher {
	return &ReplicaFetcher{
		client:      client,
		topicName:   topicName,
		topicID:     topicId,
		partition:   partition,
		fetchOffset: 0,
		commitLog:   commitLog,
		stopCh:      make(chan struct{}),
	}
}

func (rf *ReplicaFetcher) Run() {
	for {
		select {
		case <-rf.stopCh:
			return
		default:

		}

		localLogStart := int64(0)
		if rf.commitLog != nil {
			localLogStart = rf.commitLog.OldestOffset()
		}

		req := &fetch.FetchRequest{
			MaxWaitMs:      500,
			MinBytes:       1,
			MaxBytes:       50 << 20, //50 MB
			IsolationLevel: 0,
			SessionId:      0,
			SessionEpoch:   0,
			Topics: []fetch.FetchTopic{
				{
					TopicId: rf.topicID,
					Partitions: []fetch.FetchPartition{
						{
							Partition:          rf.partition,
							CurrentLeaderEpoch: -1,
							FetchOffset:        rf.fetchOffset,
							LastFetchedEpoch:   -1,
							LogStartOffset:     localLogStart,
							PartitionMaxBytes:  10 << 20,
							ReplicaDirectoryId: nil,
							HighWatermark:      nil,
						},
					},
				},
			},
			ForgottenTopics: []fetch.ForgottenTopic{},
			RackId:          "",
			ClusterId:       nil,
			ReplicaState:    nil,
		}

		resp, err := rf.client.Fetch(req)
		if err != nil {
			time.Sleep(500 * time.Millisecond)
			continue
		}

		for _, topicResponse := range resp.Responses {
			for _, partResponse := range topicResponse.Partitions {
				if partResponse.ErrorCode != 0 {
					continue
				}
				if partResponse.Records == nil || len(partResponse.Records) == 0 {
					rf.highWatermark = partResponse.HighWatermark
					continue
				}
				records := partResponse.Records
				recordCount, err := countRecordsInRecordBatches(records)
				if err != nil || recordCount <= 0 {
					continue
				}

				baseOffset, err := rf.commitLog.AppendRaw(records, recordCount)
				if err != nil {
					continue
				}

				rf.fetchOffset = baseOffset + int64(recordCount)
				rf.highWatermark = partResponse.HighWatermark
			}
		}

	}

}

func (rf *ReplicaFetcher) Stop() {
	rf.stopCh <- struct{}{}
}

func countRecordsInRecordBatches(raw []byte) (int32, error) {
	// Batch layout:
	// [0..7]   BaseOffset (int64)
	// [8..11]  BatchLength (int32)
	// ...
	// [57..60] RecordCount (int32)
	const (
		logOverhead        = 12
		recordCountOffset  = 57
		recordCountEnd     = 61
		minBatchHeaderSize = recordCountEnd
	)

	var total int32
	pos := 0

	for pos < len(raw) {
		if len(raw)-pos < minBatchHeaderSize {
			return 0, io.ErrUnexpectedEOF
		}

		batchLen := int(binary.BigEndian.Uint32(raw[pos+8 : pos+12]))
		if batchLen < logOverhead {
			return 0, io.ErrUnexpectedEOF
		}

		recordCount := int32(raw[pos+recordCountOffset]) | int32(raw[pos+recordCountOffset+1])<<8 | int32(raw[pos+recordCountOffset+2])<<16 | int32(raw[pos+recordCountOffset+3])<<24
		total += recordCount

		pos += logOverhead + batchLen
	}
	return total, nil
}
