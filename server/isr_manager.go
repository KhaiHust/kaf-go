package server

import (
	"log/slog"
	"time"

	"github.com/KhaiHust/kaf-go/constant"
	"github.com/KhaiHust/kaf-go/coordinator"
	"github.com/KhaiHust/kaf-go/metrics"
	metadatapkg "github.com/KhaiHust/kaf-go/protocol/metadata"
)

const (
	defaultISRLagMax   = 10 * time.Second
	defaultISRInterval = 500 * time.Millisecond
)

// ISRManager runs on the leader of each partition and shrinks ISR when a
// follower lags beyond lagMax, expands ISR when a follower catches up, and
// fails parked produce waiters when ISR drops below min.insync.replicas.
type ISRManager struct {
	s        *Server
	lagMax   time.Duration
	interval time.Duration
	stopCh   chan struct{}
}

func NewISRManager(s *Server) *ISRManager {
	return &ISRManager{
		s:        s,
		lagMax:   defaultISRLagMax,
		interval: defaultISRInterval,
		stopCh:   make(chan struct{}),
	}
}

func (isr *ISRManager) Run() {
	t := time.NewTicker(isr.interval)
	defer t.Stop()
	for {
		select {
		case <-isr.stopCh:
			return
		case <-t.C:
			isr.tick()
		}
	}
}

func (isr *ISRManager) Stop() {
	select {
	case <-isr.stopCh:
		// already closed
	default:
		close(isr.stopCh)
	}
}

func (isr *ISRManager) tick() {
	for _, ps := range isr.s.partitionStateStore.PartitionsLedBy(isr.s.brokerID) {
		snap := ps.Snapshot()
		changed := false
		now := time.Now()

		partLabel := metrics.FormatPartition(ps.PartitionIndex)
		for _, f := range snap.ISR {
			if f == snap.LeaderBrokerID {
				continue
			}
			last, ok := snap.LastCaughtUp[f]
			if !ok || now.Sub(last) > isr.lagMax {
				ps.RemoveFromISR(f)
				changed = true
				metrics.ISRShrink.WithLabelValues(ps.TopicName, partLabel).Inc()
				slog.Info("ISR shrink", "topic", ps.TopicName, "partition", ps.PartitionIndex, "removed", f)
			}
		}

		for _, r := range snap.Replicas {
			if r == snap.LeaderBrokerID || int32SliceContains(snap.ISR, r) {
				continue
			}
			if leo, ok := snap.ReplicaLEO[r]; ok && leo >= snap.HWM {
				ps.AddToISR(r)
				changed = true
				metrics.ISRExpand.WithLabelValues(ps.TopicName, partLabel).Inc()
				slog.Info("ISR expand", "topic", ps.TopicName, "partition", ps.PartitionIndex, "added", r)
			}
		}

		if !changed {
			continue
		}

		isr.publishPartitionRecord(ps)
		ps.AdvanceHWM()
		isr.checkMinISR(ps)
	}
}

func (isr *ISRManager) publishPartitionRecord(ps *coordinator.PartitionState) {
	topicData, err := isr.s.topicStore.GetTopicByName(ps.TopicName)
	if err != nil {
		slog.Warn("ISR: cannot resolve topicId", "topic", ps.TopicName, "err", err)
		return
	}
	snap := ps.Snapshot()
	rec := &metadatapkg.PartitionRecord{
		TopicId:     topicData.TopicId,
		PartitionId: ps.PartitionIndex,
		Leader:      snap.LeaderBrokerID,
		LeaderEpoch: snap.LeaderEpoch,
		Replicas:    snap.Replicas,
		ISR:         snap.ISR,
	}
	if err := isr.s.metadataLog.Append(rec.Encode()); err != nil {
		slog.Error("ISR: append PartitionRecord failed", "err", err)
	}
}

func (isr *ISRManager) checkMinISR(ps *coordinator.PartitionState) {
	topicData, err := isr.s.topicStore.GetTopicByName(ps.TopicName)
	if err != nil {
		return
	}
	minISR := isr.s.topicStore.GetMinInsyncReplicas(topicData.TopicId)
	snap := ps.Snapshot()
	if int16(len(snap.ISR)) < minISR && ps.Purgatory != nil {
		ps.Purgatory.FailAll(constant.CreateError(constant.ErrNotEnoughReplicasAfterAppend))
	}
}

func int32SliceContains(s []int32, v int32) bool {
	for _, x := range s {
		if x == v {
			return true
		}
	}
	return false
}
