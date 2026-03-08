package api

import "sync"

// TopicStore stores topic information in memory
type TopicStore struct {
	mu     sync.RWMutex
	topics map[string]*TopicInfo
}

// TopicInfo contains information about a topic
type TopicInfo struct {
	Name              string
	NumPartitions     int32
	ReplicationFactor int16
}

// NewTopicStore creates a new TopicStore
func NewTopicStore() *TopicStore {
	return &TopicStore{
		topics: make(map[string]*TopicInfo),
	}
}

// AddTopic adds a topic to the store
func (ts *TopicStore) AddTopic(name string, numPartitions int32, replicationFactor int16) {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	ts.topics[name] = &TopicInfo{
		Name:              name,
		NumPartitions:     numPartitions,
		ReplicationFactor: replicationFactor,
	}
}

// GetTopic retrieves a topic from the store
func (ts *TopicStore) GetTopic(name string) (*TopicInfo, bool) {
	ts.mu.RLock()
	defer ts.mu.RUnlock()
	topic, exists := ts.topics[name]
	return topic, exists
}

// ListTopics returns all topics
func (ts *TopicStore) ListTopics() []string {
	ts.mu.RLock()
	defer ts.mu.RUnlock()
	topics := make([]string, 0, len(ts.topics))
	for name := range ts.topics {
		topics = append(topics, name)
	}
	return topics
}
