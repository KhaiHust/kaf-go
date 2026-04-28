package api

import (
	"encoding/binary"
	"io"
	"net"
	"os"
	"time"

	"github.com/KhaiHust/kaf-go/constant"
	"github.com/KhaiHust/kaf-go/metrics"
	"github.com/KhaiHust/kaf-go/protocol"
	"github.com/KhaiHust/kaf-go/protocol/fetch"
	"github.com/KhaiHust/kaf-go/storage/commitlog"
)

// ioSegment is either an in-memory buffer or a file region (for zero-copy).
type ioSegment struct {
	data       []byte
	file       *os.File
	fileOffset int64
	fileSize   int64
}

func (s *ioSegment) size() int64 {
	if s.file != nil {
		return s.fileSize
	}
	return int64(len(s.data))
}

func HandleFetchApiKeys(conn net.Conn, brokerContext IBrokerContext, header *protocol.RequestHeader, r *protocol.Reader) error {
	start := time.Now()
	fetchRequest := fetch.FetchRequest{}
	if err := fetchRequest.Decode(r); err != nil {
		return err
	}

	role := "consumer"
	if fetchRequest.ReplicaState != nil && fetchRequest.ReplicaState.ReplicaId > 0 {
		role = "replica"
	}
	defer func() {
		metrics.FetchLatency.WithLabelValues(role).Observe(time.Since(start).Seconds())
	}()

	type partResult struct {
		pr     fetch.FetchPartitionResponse
		region *commitlog.RecordsRegion
	}
	type topicResult struct {
		topicId  [16]byte
		topicTag string
		parts    []partResult
	}

	var topicResults []topicResult

	pss := brokerContext.GetPartitionStateStore()

	for _, t := range fetchRequest.Topics {
		tr := topicResult{topicId: t.TopicId}
		topicMeta, _ := brokerContext.GetTopicStore().GetTopic(t.TopicId)
		if topicMeta != nil {
			tr.topicTag = topicMeta.Name.String()
		} else {
			tr.topicTag = "unknown"
		}
		for _, part := range t.Partitions {
			metrics.FetchRequests.WithLabelValues(tr.topicTag, role).Inc()
			pr := fetch.FetchPartitionResponse{
				PartitionIndex:       part.Partition,
				PreferredReadReplica: -1,
			}
			if topicMeta == nil {
				pr.ErrorCode = constant.ErrUnknownTopicOrPartition
				tr.parts = append(tr.parts, partResult{pr, nil})
				continue
			}
			ps := pss.GetPartitionState(topicMeta.Name.String(), part.Partition)
			if ps == nil {
				pr.ErrorCode = constant.ErrUnknownTopicOrPartition
				tr.parts = append(tr.parts, partResult{pr, nil})
				continue
			}

			if ps.LeaderBrokerID != brokerContext.GetBrokerID() {
				pr.ErrorCode = constant.ErrErrNotLeaderForPartition
				tr.parts = append(tr.parts, partResult{pr, nil})
				continue
			}

			var region *commitlog.RecordsRegion
			cl := brokerContext.GetTopicStore().GetCommitLog(t.TopicId, part.Partition)
			if cl == nil {
				pr.ErrorCode = constant.ErrUnknownTopicOrPartition
				tr.parts = append(tr.parts, partResult{pr, nil})
				continue
			}

			isReplicated := fetchRequest.ReplicaState != nil && fetchRequest.ReplicaState.ReplicaId > 0
			if isReplicated {
				ps.UpdateReplicaLEO(fetchRequest.ReplicaState.ReplicaId, part.FetchOffset)
			}

			maxVisible := ps.LEO
			if !isReplicated {
				maxVisible = ps.HWM
			}

			reg, err := cl.FindRecords(part.FetchOffset, int64(part.PartitionMaxBytes))
			if err == nil && reg != nil {
				clipped, err := commitlog.ClipRegionToOffset(reg, maxVisible)
				if err == nil && clipped != nil && clipped.Size > 0 {
					region = clipped
				}
				pr.LogStartOffset = reg.LogStart
			}
			pr.HighWatermark = ps.HWM
			pr.LastStableOffset = ps.HWM

			if region != nil {
				metrics.FetchBytes.WithLabelValues(tr.topicTag, role).Add(float64(region.Size))
			}

			tr.parts = append(tr.parts, partResult{pr, region})
		}
		topicResults = append(topicResults, tr)
	}

	// Build alternating in-memory / file segments.
	var segs []ioSegment
	w := protocol.NewWriter(512)

	// snapshot flushes w into a bytes segment and resets w.
	snapshot := func() {
		b := w.Bytes()
		if len(b) == 0 {
			return
		}
		cp := make([]byte, len(b))
		copy(cp, b)
		segs = append(segs, ioSegment{data: cp})
		w.Reset()
	}

	resp := fetch.FetchResponse{SessionId: fetchRequest.SessionId}
	resp.EncodeHeader(w, len(topicResults))

	for _, tr := range topicResults {
		w.WriteUUID(tr.topicId)
		w.WriteCompactArrayLen(len(tr.parts))

		for _, p := range tr.parts {
			if p.region != nil {
				p.pr.EncodePartitionPreRecords(w, p.region.Size)
				snapshot()
				segs = append(segs, ioSegment{
					file:       p.region.File,
					fileOffset: p.region.FileOffset,
					fileSize:   p.region.Size,
				})
			} else {
				p.pr.EncodePartitionPreRecords(w, -1)
			}
			if err := p.pr.EncodePartitionPostRecords(w); err != nil {
				return err
			}
		}
		w.WriteEmptyTaggedFields() // topic-level tagged fields
	}

	if err := resp.EncodeFooter(w); err != nil {
		return err
	}
	snapshot()

	// Compute total payload size.
	totalPayload := int64(0)
	for i := range segs {
		totalPayload += segs[i].size()
	}

	// Encode Kafka response header.
	rhdrW := protocol.NewWriter(8)
	rhdr := &protocol.ResponseHeader{
		CorrelationId: header.CorrelationId,
		ApiKey:        header.ApiKey,
		ApiVersion:    header.ApiVersion,
	}
	if err := rhdr.Encode(rhdrW); err != nil {
		return err
	}
	rhdrBytes := rhdrW.Bytes()

	// Write [4-byte frame size][response header][payload segments].
	var frameSz [4]byte
	binary.BigEndian.PutUint32(frameSz[:], uint32(int64(len(rhdrBytes))+totalPayload))
	if _, err := conn.Write(frameSz[:]); err != nil {
		return err
	}
	if _, err := conn.Write(rhdrBytes); err != nil {
		return err
	}

	tc, isTCP := conn.(*net.TCPConn)
	for _, s := range segs {
		if s.file != nil {
			if isTCP {
				sr := io.NewSectionReader(s.file, s.fileOffset, s.fileSize)
				if _, err := io.Copy(tc, sr); err != nil {
					return err
				}
			} else {
				buf := make([]byte, s.fileSize)
				if _, err := s.file.ReadAt(buf, s.fileOffset); err != nil {
					return err
				}
				if _, err := conn.Write(buf); err != nil {
					return err
				}
			}
		} else {
			if _, err := conn.Write(s.data); err != nil {
				return err
			}
		}
	}
	return nil
}
