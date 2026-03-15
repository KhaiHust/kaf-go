package storage

import (
	"fmt"
	"sync"

	"github.com/KhaiHust/kaf-go/common"
	"github.com/KhaiHust/kaf-go/constant"
	"github.com/KhaiHust/kaf-go/protocol/admin"
	"github.com/KhaiHust/kaf-go/protocol/topic"
	"github.com/KhaiHust/kaf-go/protocol/types"
	"github.com/KhaiHust/kaf-go/storage/commitlog"
	"github.com/gofrs/uuid/v5"
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

		var metadataResponsePartitions []admin.MetadataResponsePartition
		for idx := int32(0); idx < topicData.NumPartitions; idx++ {
			metadataResponsePartitions = append(metadataResponsePartitions, admin.MetadataResponsePartition{

				ErrorCode:       0,
				PartitionIndex:  idx,
				LeaderID:        1,
				LeaderEpoch:     1,
				ReplicaNodes:    []int32{1},
				IsrNodes:        []int32{1},
				OfflineReplicas: []int32{1},
			},
			)
		}

		topics = append(topics, admin.MetadataResponseTopic{
			ErrorCode:                 0,
			Name:                      common.StringPtr(string(topicData.Name)),
			TopicID:                   topicData.TopicId,
			IsInternal:                false,
			Partitions:                metadataResponsePartitions,
			TopicAuthorizedOperations: 0,
		})
	}
	return topics, nil
}

func (t *TopicStore) AddTopicMetadata(metaData *TopicMeta) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if _, ok := t.topics[(metaData.Name)]; ok {
		return constant.CreateError(constant.ErrTopicAlreadyExists)
	}

	topicId, err := uuid.FromString(metaData.TopicId)
	if err != nil {
		return fmt.Errorf("topic id %s is not a valid uuid", metaData.TopicId)
	}
	t.topics[(metaData.Name)] = topic.CreateTopicResponseData{
		Name:              types.CompactString(metaData.Name),
		TopicId:           topicId,
		NumPartitions:     metaData.NumPartitions,
		ReplicationFactor: int16(metaData.ReplicationFactor),
		ErrorCode:         0,
	}
	return nil
}
