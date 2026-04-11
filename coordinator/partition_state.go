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
		}
	}

}
