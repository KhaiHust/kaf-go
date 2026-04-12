package server

import (
	"log/slog"
	"net"
	"sync/atomic"
	"time"

	"github.com/KhaiHust/kaf-go/protocol/fetch"
	metadatapkg "github.com/KhaiHust/kaf-go/protocol/metadata"
)

// MetadataFetcher runs on every non-controller broker. It continuously polls
// __cluster_metadata from the controller and calls ApplyMetadataRecord for each record.
type MetadataFetcher struct {
	controllerAddr string
	server         *Server
	fetchOffset    int64
	currentOffset  atomic.Int64
	stopCh         chan struct{}
}

func NewMetadataFetcher(server *Server, controllerAddr string) *MetadataFetcher {
	return &MetadataFetcher{
		controllerAddr: controllerAddr,
		server:         server,
		stopCh:         make(chan struct{}),
	}
}

// CurrentOffset returns the fetch offset this broker has caught up to.
func (mf *MetadataFetcher) CurrentOffset() int64 {
	return mf.currentOffset.Load()
}

// Run polls the controller indefinitely, reconnecting on errors.
func (mf *MetadataFetcher) Run() {
	for {
		select {
		case <-mf.stopCh:
			return
		default:
		}

		conn, err := net.Dial("tcp", mf.controllerAddr)
		if err != nil {
			slog.Warn("metadata fetcher: dial controller failed", "addr", mf.controllerAddr, "err", err)
			select {
			case <-mf.stopCh:
				return
			case <-time.After(2 * time.Second):
			}
			continue
		}

		client := NewBrokerClient(conn, 0, mf.controllerAddr)
		slog.Info("metadata fetcher: connected to controller", "addr", mf.controllerAddr)
		mf.fetchLoop(client)
		client.Close()
	}
}

func (mf *MetadataFetcher) fetchLoop(client *BrokerClient) {
	for {
		select {
		case <-mf.stopCh:
			return
		default:
		}

		req := &fetch.FetchRequest{
			MaxWaitMs:      500,
			MinBytes:       1,
			MaxBytes:       10 << 20,
			IsolationLevel: 0,
			SessionId:      0,
			SessionEpoch:   -1,
			Topics: []fetch.FetchTopic{
				{
					TopicId: metadatapkg.ClusterMetadataTopicID,
					Partitions: []fetch.FetchPartition{
						{
							Partition:          0,
							CurrentLeaderEpoch: -1,
							FetchOffset:        mf.fetchOffset,
							LastFetchedEpoch:   -1,
							LogStartOffset:     0,
							PartitionMaxBytes:  10 << 20,
						},
					},
				},
			},
			ForgottenTopics: []fetch.ForgottenTopic{},
			RackId:          "",
			ReplicaState:    &fetch.ReplicaState{ReplicaId: mf.server.brokerID, ReplicaEpoch: -1},
		}

		resp, err := client.Fetch(req)
		if err != nil {
			slog.Warn("metadata fetcher: fetch failed, reconnecting", "err", err)
			return
		}

		for _, topicResp := range resp.Responses {
			for _, partResp := range topicResp.Partitions {
				if partResp.ErrorCode != 0 {
					continue
				}
				if len(partResp.Records) == 0 {
					continue
				}

				values := metadatapkg.DecodeRecordValues(partResp.Records)
				for _, value := range values {
					mf.server.ApplyMetadataRecord(value)
				}

				recordCount, err := countRecordsInRecordBatches(partResp.Records)
				if err == nil && recordCount > 0 {
					mf.fetchOffset += int64(recordCount)
					mf.currentOffset.Store(mf.fetchOffset)
					slog.Info("metadata fetcher: applied records",
						"count", recordCount, "fetchOffset", mf.fetchOffset)
				}
			}
		}
	}
}

func (mf *MetadataFetcher) Stop() {
	close(mf.stopCh)
}
