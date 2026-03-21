package api

import (
	"encoding/binary"
	"io"
	"net"

	"github.com/KhaiHust/kaf-go/constant"
	"github.com/KhaiHust/kaf-go/protocol"
	"github.com/KhaiHust/kaf-go/protocol/fetch"
	"github.com/KhaiHust/kaf-go/storage"
	"github.com/KhaiHust/kaf-go/storage/commitlog"
)

type partitionRegion struct {
	partResp *fetch.FetchPartitionResponse
	region   *commitlog.RecordsRegion
}

func HandleFetchApiKeys(conn net.Conn, header *protocol.RequestHeader, r *protocol.Reader, topicStore *storage.TopicStore) error {
	fetchRequest := fetch.FetchRequest{}
	if err := fetchRequest.Decode(r); err != nil {
		return err
	}

	var topicResps []fetch.FetchTopicResponse
	var allRegions []*commitlog.RecordsRegion
	totalRecordsBytes := int64(0)

	for _, topic := range fetchRequest.Topics {
		topicResp := fetch.FetchTopicResponse{
			TopicId: topic.TopicId,
		}

		for _, part := range topic.Partitions {
			pr := fetch.FetchPartitionResponse{
				PartitionIndex:       part.Partition,
				PreferredReadReplica: -1,
			}
			partCommitLog := topicStore.GetCommitLog(topic.TopicId, part.Partition)
			if partCommitLog == nil {
				pr.ErrorCode = constant.ErrUnknownTopicOrPartition // unknown topic or partition
				allRegions = append(allRegions, nil)
				topicResp.Partitions = append(topicResp.Partitions, pr)
				continue
			}

			region, err := partCommitLog.FindRecords(part.FetchOffset, int64(part.PartitionMaxBytes))
			if err != nil || region == nil {
				allRegions = append(allRegions, nil)
			} else {
				pr.HighWatermark = region.LEO
				pr.LastStableOffset = region.LEO
				pr.LogStartOffset = region.LogStart
				allRegions = append(allRegions, region)
				totalRecordsBytes += region.Size
			}
			topicResp.Partitions = append(topicResp.Partitions, pr)
		}
		topicResps = append(topicResps, topicResp)
	}

	resp := &fetch.FetchResponse{
		SessionId: fetchRequest.SessionId,
		Responses: topicResps,
	}

	w := protocol.NewWriter(512)
	if err := resp.Encode(w); err != nil {
		return err
	}

	// Frame: [4-byte total payload size][encoded header]
	// Total = encoded header bytes + all record regions
	headerBytes := w.Bytes()
	//totalPayload := uint32(4 + len(headerBytes) + int(totalRecordsBytes))
	// response header (CorrelationId + optional tagged fields)
	responseHdr := protocol.NewWriter(8)
	rhdr := &protocol.ResponseHeader{
		CorrelationId: header.CorrelationId,
		ApiKey:        header.ApiKey,
		ApiVersion:    header.ApiVersion,
	}
	_ = rhdr.Encode(responseHdr)

	frameSize := make([]byte, 4)
	binary.BigEndian.PutUint32(frameSize, uint32(len(responseHdr.Bytes())+len(headerBytes))+uint32(totalRecordsBytes))

	// Write frame size + response header + fetch response metadata
	if _, err := conn.Write(frameSize); err != nil {
		return err
	}
	if _, err := conn.Write(responseHdr.Bytes()); err != nil {
		return err
	}
	if _, err := conn.Write(headerBytes); err != nil {
		return err
	}

	tc, ok := conn.(*net.TCPConn)
	if !ok {
		// fallback: read into memory and write
		for _, region := range allRegions {
			if region == nil {
				continue
			}
			buf := make([]byte, region.Size)
			if _, err := region.File.ReadAt(buf, region.FileOffset); err != nil {
				return err
			}
			if _, err := conn.Write(buf); err != nil {
				return err
			}
		}
		return nil
	}

	for _, region := range allRegions {
		if region == nil {
			continue
		}
		sr := io.NewSectionReader(region.File, region.FileOffset, region.Size)
		if _, err := io.Copy(tc, sr); err != nil {
			return err
		}
	}
	return nil

}
