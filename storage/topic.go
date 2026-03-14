package storage

import (
	"fmt"
	"sync"

	"github.com/KhaiHust/kaf-go/common"
	"github.com/KhaiHust/kaf-go/constant"
	"github.com/KhaiHust/kaf-go/protocol/admin"
	"github.com/KhaiHust/kaf-go/protocol/topic"
	"github.com/KhaiHust/kaf-go/storage/commitlog"
)

type TopicStore struct {
	topics     map[string]topic.CreateTopicResponseData
	commitLogs map[string]commitlog.ICommitLog
	mu         sync.RWMutex
}

func NewTopicStore() *TopicStore {
	return &TopicStore{
		topics:     make(map[string]topic.CreateTopicResponseData),
		commitLogs: make(map[string]commitlog.ICommitLog),
		mu:         sync.RWMutex{},
	}
}

func (t *TopicStore) AddCommitLog(topic string, partition int32, log commitlog.ICommitLog) {
	t.mu.Lock()
	defer t.mu.Unlock()
	key := fmt.Sprintf("%s-%d", topic, partition)
	t.commitLogs[key] = log
}

func (t *TopicStore) GetCommitLog(topic string, partition int32) commitlog.ICommitLog {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.commitLogs[fmt.Sprintf("%s-%d", topic, partition)]
}
func (t *TopicStore) AddTopic(topicData topic.CreateTopicResponseData) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if _, ok := t.topics[string(topicData.Name)]; ok {
		return constant.CreateError(constant.ErrTopicAlreadyExists)
	}
	t.topics[string(topicData.Name)] = topicData
	return nil
}

func (t *TopicStore) GetTopic(name string) (*topic.CreateTopicResponseData, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	topicData, ok := t.topics[name]
	if !ok {
		return nil, constant.CreateError(constant.ErrInvalidTopicException)
	}
	return &topicData, nil
}

func (t *TopicStore) GetTopicMetadata(names []string) ([]admin.MetadataResponseTopic, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	var topics []admin.MetadataResponseTopic
	for _, name := range names {
		topicData, ok := t.topics[name]
		if !ok {
			return nil, constant.CreateError(constant.ErrInvalidTopicException)
		}
		topics = append(topics, admin.MetadataResponseTopic{
			ErrorCode:  0,
			Name:       common.StringPtr(string(topicData.Name)),
			TopicID:    topicData.TopicId,
			IsInternal: false,
			Partitions: []admin.MetadataResponsePartition{
				{
					ErrorCode:       0,
					PartitionIndex:  0,
					LeaderID:        1,
					LeaderEpoch:     1,
					ReplicaNodes:    []int32{1},
					IsrNodes:        []int32{1},
					OfflineReplicas: []int32{1},
				},
			},
			TopicAuthorizedOperations: 0,
		})
	}
	return topics, nil
}
