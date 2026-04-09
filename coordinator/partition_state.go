package coordinator

import (
	"fmt"
	"sync"
)

type PartitionState struct {
	TopicName      string
	PartitionIndex int32
	LeaderBrokerID int32
	LeaderEpoch    int32
	Replicas       []int32
	ISR            []int32
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
func (p *PartitionStateStore) GetPartitionState(topicName string, partition int32) (*PartitionState, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	key := fmt.Sprintf("%s-%d", topicName, partition)
	if ps, ok := p.partitions[key]; ok {
		return ps, nil
	}

	return nil, nil
}

func (p *PartitionStateStore) SetLeader(topicName string, partition int32, brokerID int32) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	key := fmt.Sprintf("%s-%d", topicName, partition)
	if ps, ok := p.partitions[key]; ok {
		ps.LeaderBrokerID = brokerID
	} else {
		p.partitions[key] = &PartitionState{
			TopicName:      topicName,
			PartitionIndex: partition,
			LeaderBrokerID: brokerID,
			Replicas:       []int32{brokerID},
			ISR:            []int32{brokerID},
		}
	}

	return nil
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
