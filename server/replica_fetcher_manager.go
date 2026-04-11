package server

import (
	"fmt"
	"net"
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

//func (m *ReplicaFetcherManager) StartFetchers() {
//	m.mu.RLock()
//	defer m.mu.RUnlock()
//
//	for _, fetcher := range m.fetchers {
//		go fetcher.Run()
//	}
//}

func (m *ReplicaFetcherManager) AddFetcher(topicName string, partition, leaderBrokerId int32) {
	m.mu.Lock()
	defer m.mu.Unlock()

	key := fmt.Sprintf("%s-%d", topicName, partition)
	if _, ok := m.fetchers[key]; ok {
		return
	}

	brokerClient, ok := m.brokerClients[leaderBrokerId]
	if !ok {
		return
	}

	commitLogConfig := defaultCommitLogConfig()
	commitLog, err := commitlog.NewCommitLog(m.server.logDir, commitLogConfig)
	if err != nil {
		return
	}

	topicData, err := m.server.topicStore.GetTopicByName(topicName)
	if err != nil {
		return
	}
	fetcher := NewReplicaFetcher(brokerClient, topicName, topicData.TopicId, partition, commitLog)
	m.fetchers[key] = fetcher
	go fetcher.Run()
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
