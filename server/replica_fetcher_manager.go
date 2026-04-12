package server

import (
	"fmt"
	"log/slog"
	"net"
	"path/filepath"
	"sync"

	"github.com/KhaiHust/kaf-go/storage/commitlog"
)

type ReplicaFetcherManager struct {
	fetchers      map[string]*ReplicaFetcher // "topicName - partitionIndex" -> ReplicaFetcher
	server        *Server
	brokerClients map[int32]*BrokerClient // "topicName - partitionIndex" -> BrokerClient is its broker leader
	mu            sync.RWMutex
}

func NewReplicaFetcherManager(server *Server) *ReplicaFetcherManager {
	return &ReplicaFetcherManager{
		fetchers:      make(map[string]*ReplicaFetcher),
		server:        server,
		mu:            sync.RWMutex{},
		brokerClients: make(map[int32]*BrokerClient),
	}
}

func (m *ReplicaFetcherManager) AddFetcher(topicName string, partition, leaderBrokerId int32) {
	m.mu.Lock()
	defer m.mu.Unlock()

	key := fmt.Sprintf("%s-%d", topicName, partition)
	if _, ok := m.fetchers[key]; ok {
		return
	}

	brokerClient, ok := m.brokerClients[leaderBrokerId]
	if !ok {
		bc, err := m.dialBrokerClientLocked(leaderBrokerId)
		if err != nil {
			slog.Warn("ISR: cannot start replica fetcher, peer unreachable",
				"topic", topicName, "partition", partition, "leader", leaderBrokerId, "err", err)
			return
		}
		brokerClient = bc
	}

	commitLogConfig := defaultCommitLogConfig()
	dir := filepath.Join(m.server.logDir, topicName, fmt.Sprintf("%s-%d", topicName, partition))
	commitLog, err := commitlog.NewCommitLog(dir, commitLogConfig)
	if err != nil {
		slog.Warn("ISR: open replica commitlog failed", "topic", topicName, "partition", partition, "err", err)
		return
	}

	topicData, err := m.server.topicStore.GetTopicByName(topicName)
	if err != nil {
		return
	}
	fetcher := NewReplicaFetcher(brokerClient, topicName, topicData.TopicId, partition, m.server.brokerID, commitLog)
	m.fetchers[key] = fetcher
	slog.Info("replica fetcher started", "topic", topicName, "partition", partition, "leader", leaderBrokerId)
	go fetcher.Run()
}

func (m *ReplicaFetcherManager) dialBrokerClientLocked(brokerId int32) (*BrokerClient, error) {
	addr := m.server.brokerRegistry.Addr(brokerId)
	if addr == "" {
		return nil, fmt.Errorf("broker %d not in registry", brokerId)
	}
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("dial broker %d at %s: %w", brokerId, addr, err)
	}
	bc := NewBrokerClient(conn, brokerId, addr)
	m.brokerClients[brokerId] = bc
	return bc, nil
}

func (m *ReplicaFetcherManager) StopFetcher(topicName string, partition int32) {
	m.mu.Lock()
	defer m.mu.Unlock()

	key := fmt.Sprintf("%s-%d", topicName, partition)
	fetcher, ok := m.fetchers[key]
	if !ok {
		return
	}
	fetcher.Stop()
	delete(m.fetchers, key)
}

func (m *ReplicaFetcherManager) RegisterBrokerClient(brokerId int32, addr string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, ok := m.brokerClients[brokerId]; ok {
		return fmt.Errorf("broker client with id %d already exists", brokerId)
	}
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return fmt.Errorf("failed to connect to broker at %s", addr)
	}

	m.brokerClients[brokerId] = NewBrokerClient(conn, brokerId, addr)
	return nil
}
