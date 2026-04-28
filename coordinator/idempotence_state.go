package coordinator

import (
	"math"
	"sync"
	"time"

	"github.com/KhaiHust/kaf-go/constant"
	"github.com/KhaiHust/kaf-go/metrics"
)

const (
	IdempotenceRingSize          = 5
	producerIdExpirationMs int64 = 60 * 60 * 1000
)

type ProducerRingSlot struct {
	FirstSeq    int32
	LastSeq     int32
	BaseOffset  int64
	LeaderEpoch int32
}

type ProducerBatchLog struct {
	LastSeq         int32
	LastOffset      int64
	LastEpoch       int16
	LastLeaderEpoch int32
	LastTimestamp   int64
	Ring            [IdempotenceRingSize]ProducerRingSlot
	RingHead        int
}

type IdempotenceState struct {
	mu        sync.Mutex
	Producers map[int64]*ProducerBatchLog
}

func NewIdempotenceState() *IdempotenceState {
	return &IdempotenceState{
		Producers: make(map[int64]*ProducerBatchLog),
	}
}

func (s *IdempotenceState) Validate(pid int64, epoch int16, firstSeq, lastSeq int32) (baseOff int64, alreadyCommitted bool, err error) {
	defer func() {
		if err != nil {
			metrics.IdempotenceValidateErrors.WithLabelValues(
				metrics.FormatErrorCode(constant.GetErrorId(err)),
			).Inc()
		}
	}()

	s.mu.Lock()
	defer s.mu.Unlock()

	prod, exists := s.Producers[pid]

	if exists && time.Now().UnixMilli()-prod.LastTimestamp > producerIdExpirationMs {
		delete(s.Producers, pid)
		exists = false
		prod = nil
	}

	if !exists {
		if firstSeq != 0 {
			return 0, false, constant.CreateError(constant.ErrUnknownProducerId)
		}
		return 0, false, nil
	}

	if epoch < prod.LastEpoch {
		return 0, false, constant.CreateError(constant.ErrInvalidProducerEpoch)
	}

	if epoch > prod.LastEpoch {
		if firstSeq != 0 {
			return 0, false, constant.CreateError(constant.ErrOutOfOrderSequenceNumber)
		}
		delete(s.Producers, pid)
		return 0, false, nil
	}

	switch {
	case firstSeq == prod.LastSeq+1:
		return 0, false, nil
	case firstSeq == 0 && prod.LastSeq == math.MaxInt32:
		return 0, false, nil
	case firstSeq > prod.LastSeq+1:
		return 0, false, constant.CreateError(constant.ErrOutOfOrderSequenceNumber)
	default:
		if cached, found := lookupRing(prod, firstSeq, lastSeq); found {
			return cached, true, nil
		}
		return 0, false, constant.CreateError(constant.ErrDuplicateSequenceNumber)
	}
}

func (s *IdempotenceState) Record(pid int64, epoch int16, firstSeq, lastSeq int32, baseOff int64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	prod, exists := s.Producers[pid]
	if !exists {
		prod = newProducerBatchLog(epoch)
		s.Producers[pid] = prod
	}

	prod.LastEpoch = epoch
	prod.LastSeq = lastSeq
	prod.LastOffset = baseOff + int64(lastSeq-firstSeq)
	prod.LastTimestamp = time.Now().UnixMilli()
	prod.Ring[prod.RingHead] = ProducerRingSlot{
		FirstSeq:    firstSeq,
		LastSeq:     lastSeq,
		BaseOffset:  baseOff,
		LeaderEpoch: -1,
	}
	prod.RingHead = (prod.RingHead + 1) % IdempotenceRingSize
}

func newProducerBatchLog(epoch int16) *ProducerBatchLog {
	p := &ProducerBatchLog{
		LastSeq:         -1,
		LastEpoch:       epoch,
		LastLeaderEpoch: -1,
	}
	for i := range p.Ring {
		p.Ring[i].FirstSeq = -1
	}
	return p
}

type ProducerSnapshotEntry struct {
	ProducerId int64
	Log        ProducerBatchLog
}

func (s *IdempotenceState) Snapshot() []ProducerSnapshotEntry {
	s.mu.Lock()
	defer s.mu.Unlock()
	entries := make([]ProducerSnapshotEntry, 0, len(s.Producers))
	for pid, prod := range s.Producers {
		cp := *prod
		entries = append(entries, ProducerSnapshotEntry{ProducerId: pid, Log: cp})
	}
	return entries
}

func (s *IdempotenceState) Restore(entries []ProducerSnapshotEntry) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.Producers = make(map[int64]*ProducerBatchLog, len(entries))
	for _, e := range entries {
		cp := e.Log
		s.Producers[e.ProducerId] = &cp
	}
}

func lookupRing(prod *ProducerBatchLog, firstSeq, lastSeq int32) (int64, bool) {
	for _, e := range prod.Ring {
		if e.FirstSeq < 0 {
			continue
		}
		if e.FirstSeq == firstSeq && e.LastSeq == lastSeq {
			return e.BaseOffset, true
		}
	}
	return 0, false
}
