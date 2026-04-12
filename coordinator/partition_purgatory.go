package coordinator

import (
	"sync"
)

type PartitionPurgatory struct {
	waiters []*PartitionAppendWaiter
	mu      sync.Mutex
}

func NewPartitionPurgatory() *PartitionPurgatory {
	return &PartitionPurgatory{
		waiters: make([]*PartitionAppendWaiter, 0),
		mu:      sync.Mutex{},
	}
}

func (p *PartitionPurgatory) Add(w *PartitionAppendWaiter) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.waiters = append(p.waiters, w)
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
}
