package server

import (
	"fmt"
	"math"
	"sync"
	"time"
)

type BrokerInfo struct {
	BrokerId       int32
	Host           string
	Port           int32
	Epoch          int64
	MetadataOffset int64
	LastSeen       time.Time
}

type BrokerRegistry struct {
	mu      sync.RWMutex
	brokers map[int32]*BrokerInfo
}

func NewBrokerRegistry() *BrokerRegistry {
	return &BrokerRegistry{
		brokers: make(map[int32]*BrokerInfo),
	}
}

func (r *BrokerRegistry) RegisterBroker(id int32, host string, port int32) int64 {
	r.mu.Lock()
	defer r.mu.Unlock()

	existing, ok := r.brokers[id]
	var newEpoch int64 = 1
	if ok {
		newEpoch = existing.Epoch + 1
	}
	r.brokers[id] = &BrokerInfo{
		BrokerId: id,
		Host:     host,
		Port:     port,
		Epoch:    newEpoch,
		LastSeen: time.Now(),
	}
	return newEpoch
}

func (r *BrokerRegistry) ValidateAndUpdate(id int32, epoch int64, metadataOffset int64) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	info, ok := r.brokers[id]
	if !ok {
		return fmt.Errorf("broker %d not found", id)
	}
	if info.Epoch != epoch {
		return fmt.Errorf("stale epoch: expected %d got %d", info.Epoch, epoch)
	}
	info.MetadataOffset = metadataOffset
	info.LastSeen = time.Now()
	return nil
}

func (r *BrokerRegistry) GetMetadataOffset(id int32) int64 {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if info, ok := r.brokers[id]; ok {
		return info.MetadataOffset
	}
	return 0
}

func (r *BrokerRegistry) LowestBrokerID() int32 {
	r.mu.RLock()
	defer r.mu.RUnlock()

	lowest := int32(math.MaxInt32)
	for id := range r.brokers {
		if id < lowest {
			lowest = id
		}
	}
	if lowest == math.MaxInt32 {
		return -1
	}
	return lowest
}

func (r *BrokerRegistry) All() []*BrokerInfo {
	r.mu.RLock()
	defer r.mu.RUnlock()

	result := make([]*BrokerInfo, 0, len(r.brokers))
	for _, info := range r.brokers {
		result = append(result, info)
	}
	return result
}

func (r *BrokerRegistry) Addr(id int32) string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if info, ok := r.brokers[id]; ok {
		return fmt.Sprintf("%s:%d", info.Host, info.Port)
	}
	return ""
}

func (r *BrokerRegistry) GetBrokerAddr(id int32) (host string, port int32) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if info, ok := r.brokers[id]; ok {
		return info.Host, info.Port
	}
	return "", 0
}
