package coordinator

import (
	"log/slog"

	"github.com/KhaiHust/kaf-go/metrics"
)

const defaultRetainSnapshots = 2

type SnapshotBatchHeader struct {
	BaseOffset      int64
	LastOffsetDelta int32
	ProducerId      int64
	ProducerEpoch   int16
	BaseSequence    int32
}

type CommitLogReader interface {
	Dir() string
	ActiveSegmentBaseOffset() int64
	WalkBatchHeadersFrom(fromOffset int64, fn func(SnapshotBatchHeader) error) error
}

type ProducerStateManager struct {
	dir             string
	topic           string
	partition       int32
	state           *IdempotenceState
	retainSnapshots int
}

func NewProducerStateManager(dir string, state *IdempotenceState) *ProducerStateManager {
	return &ProducerStateManager{
		dir:             dir,
		state:           state,
		retainSnapshots: defaultRetainSnapshots,
	}
}

// SetLabels attaches topic+partition for metric labelling. Optional; callers that
// don't set this still produce snapshots, just without per-partition gauges.
func (m *ProducerStateManager) SetLabels(topic string, partition int32) {
	if m == nil {
		return
	}
	m.topic = topic
	m.partition = partition
}

func (m *ProducerStateManager) OnSegmentRoll(newBase int64) {
	if m == nil || m.state == nil {
		return
	}
	entries := m.state.Snapshot()
	if err := WriteProducerSnapshot(m.dir, newBase, entries); err != nil {
		slog.Warn("producer snapshot write failed", "dir", m.dir, "base", newBase, "err", err)
		return
	}
	if m.topic != "" {
		metrics.SnapshotWrites.WithLabelValues(m.topic, metrics.FormatPartition(m.partition)).Inc()
	}
	m.pruneOldSnapshots(newBase)
}

func (m *ProducerStateManager) Recover(cl CommitLogReader) error {
	if m == nil || m.state == nil || cl == nil {
		return nil
	}
	activeBase := cl.ActiveSegmentBaseOffset()
	snapBase, entries, err := LoadLatestProducerSnapshot(m.dir, activeBase)
	if err != nil {
		slog.Warn("producer snapshot load failed", "dir", m.dir, "err", err)
	}
	if entries != nil {
		m.state.Restore(entries)
	}

	scanFrom := snapBase
	if activeBase > scanFrom {
		scanFrom = activeBase
	}

	return cl.WalkBatchHeadersFrom(scanFrom, func(h SnapshotBatchHeader) error {
		if h.ProducerId < 0 {
			return nil
		}
		firstSeq := h.BaseSequence
		lastSeq := firstSeq + h.LastOffsetDelta
		m.state.Record(h.ProducerId, h.ProducerEpoch, firstSeq, lastSeq, h.BaseOffset)
		return nil
	})
}

func (m *ProducerStateManager) pruneOldSnapshots(currentBase int64) {
	bases, err := ListProducerSnapshotOffsets(m.dir)
	if err != nil || len(bases) <= m.retainSnapshots {
		return
	}
	keep := m.retainSnapshots
	if keep < 1 {
		keep = 1
	}
	cutoffIdx := len(bases) - keep
	for i := 0; i < cutoffIdx; i++ {
		if bases[i] >= currentBase {
			continue
		}
		if derr := DeleteProducerSnapshot(m.dir, bases[i]); derr != nil {
			slog.Warn("producer snapshot prune failed", "dir", m.dir, "base", bases[i], "err", derr)
		}
	}
}
