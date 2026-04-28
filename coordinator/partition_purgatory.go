package coordinator

import (
	"sync"

	"github.com/KhaiHust/kaf-go/metrics"
)

type PartitionPurgatory struct {
	waiters   []*PartitionAppendWaiter
	mu        sync.Mutex
	topic     string
	partition string
}

func NewPartitionPurgatory() *PartitionPurgatory {
	return &PartitionPurgatory{
		waiters: make([]*PartitionAppendWaiter, 0),
		mu:      sync.Mutex{},
	}
}

// SetLabels attaches topic/partition labels for the purgatory_depth gauge.
// Optional — unset purgatories simply skip publishing.
func (p *PartitionPurgatory) SetLabels(topic string, partition int32) {
	if p == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.topic = topic
	p.partition = metrics.FormatPartition(partition)
}

func (p *PartitionPurgatory) publishDepthLocked() {
	if p.topic == "" {
		return
	}
	metrics.PurgatoryDepth.WithLabelValues(p.topic, p.partition).Set(float64(len(p.waiters)))
}

func (p *PartitionPurgatory) Add(w *PartitionAppendWaiter) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.waiters = append(p.waiters, w)
	p.publishDepthLocked()
}

func (p *PartitionPurgatory) CompleteUpTo(hwm int64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	remaining := p.waiters[:0]
	for _, w := range p.waiters {
		if w.RequiredOffset < hwm {
			select {
			case w.Done <- nil:
			default:
			}
			close(w.Done)
		} else {
			remaining = append(remaining, w)
		}
	}
	p.waiters = remaining
	p.publishDepthLocked()
}

func (p *PartitionPurgatory) FailAll(err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, w := range p.waiters {
		select {
		case w.Done <- err:
		default:
		}
		close(w.Done)
	}
	p.waiters = p.waiters[:0]
	metrics.PurgatoryFailAll.Inc()
	p.publishDepthLocked()
}
