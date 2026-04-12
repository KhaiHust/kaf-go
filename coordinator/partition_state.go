package coordinator

import (
	"fmt"
	"sync"
	"time"
)

type PartitionState struct {
	TopicName      string
	PartitionIndex int32
	LeaderBrokerID int32
	LeaderEpoch    int32
	Replicas       []int32
	ISR            []int32

	LEO          int64
	HWM          int64
	ReplicaLEO   map[int32]int64
	LastCaughtUp map[int32]time.Time
	Purgatory    *PartitionPurgatory
	Idempotence  *IdempotenceState
	mu           *sync.RWMutex
}

type PartitionStateStore struct {
	mu         sync.RWMutex
	partitions map[string]*PartitionState // topicName-partitionIndex -> PartitionState
}

func NewPartitionStateStore() *PartitionStateStore {
	return &PartitionStateStore{
		mu:         sync.RWMutex{},
		partitions: make(map[string]*PartitionState),
	}
}
func (p *PartitionStateStore) GetPartitionState(topicName string, partition int32) *PartitionState {
	p.mu.RLock()
	defer p.mu.RUnlock()

	key := fmt.Sprintf("%s-%d", topicName, partition)
	if ps, ok := p.partitions[key]; ok {
		return ps
	}

	return nil
}

func (p *PartitionStateStore) SetLeader(topicName string, partition int32, brokerID int32) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	key := fmt.Sprintf("%s-%d", topicName, partition)
	if ps, ok := p.partitions[key]; ok {
		ps.LeaderBrokerID = brokerID
		if ps.Idempotence == nil {
			ps.Idempotence = NewIdempotenceState()
		}
	} else {
		p.partitions[key] = &PartitionState{
			TopicName:      topicName,
			PartitionIndex: partition,
			LeaderBrokerID: brokerID,
			Replicas:       []int32{brokerID},
			ISR:            []int32{brokerID},
			ReplicaLEO:     make(map[int32]int64),
			LastCaughtUp:   make(map[int32]time.Time),
			Purgatory:      NewPartitionPurgatory(),
			Idempotence:    NewIdempotenceState(),
			mu:             &sync.RWMutex{},
		}
	}

	return nil
}

// SetPartitionState upserts the full partition state (used when applying metadata records).
func (p *PartitionStateStore) SetPartitionState(topicName string, partitionId, leader, leaderEpoch int32, replicas, isr []int32) {
	p.mu.Lock()
	defer p.mu.Unlock()

	key := fmt.Sprintf("%s-%d", topicName, partitionId)
	p.partitions[key] = &PartitionState{
		TopicName:      topicName,
		PartitionIndex: partitionId,
		LeaderBrokerID: leader,
		LeaderEpoch:    leaderEpoch,
		Replicas:       replicas,
		ISR:            isr,
		ReplicaLEO:     make(map[int32]int64, len(replicas)),
		LastCaughtUp:   make(map[int32]time.Time, len(replicas)),
		Purgatory:      NewPartitionPurgatory(),
		Idempotence:    NewIdempotenceState(),
		mu:             &sync.RWMutex{},
	}
}

func (p *PartitionStateStore) UpdateISR(topicName string, partition int32, newISR int32) {
	p.mu.Lock()
	defer p.mu.Unlock()

	key := fmt.Sprintf("%s-%d", topicName, partition)
	if ps, ok := p.partitions[key]; ok {
		for _, brokerID := range ps.ISR {
			if brokerID == newISR {
				return
			}
		}
		ps.ISR = append(ps.ISR, newISR)
	}
}

func (p *PartitionStateStore) AssignReplicas(topicName string, numPartitions int32, replicationFactor int32, brokerIDs []int32) {
	p.mu.Lock()
	defer p.mu.Unlock()

	//partition index => list of brokerIds
	// round-robin with shift:
	for partition := int32(0); partition < numPartitions; partition++ {
		replicas := make([]int32, 0, replicationFactor)
		for i := int32(0); i < replicationFactor; i++ {
			brokerID := brokerIDs[(int(partition)+int(i))%len(brokerIDs)]
			replicas = append(replicas, brokerID)
		}

		key := fmt.Sprintf("%s-%d", topicName, partition)
		p.partitions[key] = &PartitionState{
			TopicName:      topicName,
			PartitionIndex: partition,
			LeaderBrokerID: replicas[0],
			Replicas:       replicas,
			ISR:            []int32{replicas[0]},
			ReplicaLEO:     make(map[int32]int64, len(replicas)),
			LastCaughtUp:   make(map[int32]time.Time, len(replicas)),
			Purgatory:      NewPartitionPurgatory(),
			Idempotence:    NewIdempotenceState(),
			mu:             &sync.RWMutex{},
		}
	}

}

func (p *PartitionStateStore) PartitionsLedBy(brokerID int32) []*PartitionState {
	p.mu.Lock()
	defer p.mu.Unlock()
	states := make([]*PartitionState, 0, len(p.partitions))
	for _, ps := range p.partitions {
		if ps.LeaderBrokerID == brokerID {
			states = append(states, ps)
		}
	}
	return states
}

func (ps *PartitionState) RemoveFromISR(followerId int32) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	isrs := make([]int32, 0, len(ps.ISR))
	for _, isr := range ps.ISR {
		if isr == followerId {
			continue
		}
		isrs = append(isrs, isr)
	}
	ps.ISR = isrs
}

func (ps *PartitionState) GetHWM() int64 {
	ps.mu.RLock()
	defer ps.mu.RUnlock()
	return ps.HWM
}

type PartitionSnapshot struct {
	ISR            []int32
	Replicas       []int32
	LeaderBrokerID int32
	LeaderEpoch    int32
	HWM            int64
	LEO            int64
	ReplicaLEO     map[int32]int64
	LastCaughtUp   map[int32]time.Time
}

func (ps *PartitionState) Snapshot() PartitionSnapshot {
	ps.mu.RLock()
	defer ps.mu.RUnlock()
	rl := make(map[int32]int64, len(ps.ReplicaLEO))
	for k, v := range ps.ReplicaLEO {
		rl[k] = v
	}
	lc := make(map[int32]time.Time, len(ps.LastCaughtUp))
	for k, v := range ps.LastCaughtUp {
		lc[k] = v
	}
	return PartitionSnapshot{
		ISR:            append([]int32(nil), ps.ISR...),
		Replicas:       append([]int32(nil), ps.Replicas...),
		LeaderBrokerID: ps.LeaderBrokerID,
		LeaderEpoch:    ps.LeaderEpoch,
		HWM:            ps.HWM,
		LEO:            ps.LEO,
		ReplicaLEO:     rl,
		LastCaughtUp:   lc,
	}
}

func (ps *PartitionState) AddToISR(followerId int32) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	isrs := make([]int32, 0, len(ps.ISR))
	for _, isr := range ps.ISR {
		if isr == followerId {
			continue
		}
		isrs = append(isrs, isr)
	}
	isrs = append(isrs, followerId)
	ps.ISR = isrs
}
func (ps *PartitionState) UpdateReplicaLEO(follower int32, upto int64) {
	ps.mu.Lock()
	ps.ReplicaLEO[follower] = upto
	if upto >= ps.LEO {
		ps.LastCaughtUp[follower] = time.Now()
	}
	ps.mu.Unlock()
	ps.AdvanceHWM()
}

func (ps *PartitionState) AdvanceHWM() {
	ps.mu.Lock()
	newHWM := ps.LEO
	for _, id := range ps.ISR {
		if id == ps.LeaderBrokerID {
			continue
		}
		leo, existed := ps.ReplicaLEO[id]
		if !existed {
			leo = 0
		}
		if leo < newHWM {
			newHWM = leo
		}
	}
	advanced := newHWM > ps.HWM
	if advanced {
		ps.HWM = newHWM
	}
	hwm := ps.HWM
	purgatory := ps.Purgatory
	ps.mu.Unlock()

	if advanced && purgatory != nil {
		purgatory.CompleteUpTo(hwm)
	}
}

func (ps *PartitionState) OnLeaderAppend(lastOffset int64) {
	ps.mu.Lock()
	if lastOffset+1 > ps.LEO {
		ps.LEO = lastOffset + 1
	}
	ps.mu.Unlock()
	ps.AdvanceHWM()
}
