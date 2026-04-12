package storage

import (
	"fmt"
	"sync"

	"github.com/KhaiHust/kaf-go/common"
	"github.com/KhaiHust/kaf-go/constant"
	"github.com/KhaiHust/kaf-go/coordinator"
	"github.com/KhaiHust/kaf-go/protocol/admin"
	"github.com/KhaiHust/kaf-go/protocol/topic"
	"github.com/KhaiHust/kaf-go/protocol/types"
	"github.com/KhaiHust/kaf-go/storage/commitlog"
	"github.com/gofrs/uuid/v5"
)

type TopicStore struct {
	topics     map[uuid.UUID]topic.CreateTopicResponseData
	topicNames map[string]uuid.UUID
	commitLogs map[string]commitlog.ICommitLog
	metas      map[uuid.UUID]*TopicMeta // sidecar for fields not in CreateTopicResponseData (min.insync.replicas, etc)
	mu         sync.RWMutex
}

func NewTopicStore() *TopicStore {
	return &TopicStore{
		topics:     make(map[uuid.UUID]topic.CreateTopicResponseData),
		topicNames: make(map[string]uuid.UUID),
		commitLogs: make(map[string]commitlog.ICommitLog),
		metas:      make(map[uuid.UUID]*TopicMeta),
		mu:         sync.RWMutex{},
	}
}

// GetMinInsyncReplicas returns the configured min.insync.replicas for topicId,
// defaulting to 1 (Kafka default) when no meta has been registered yet.
func (t *TopicStore) GetMinInsyncReplicas(topicId uuid.UUID) int16 {
	t.mu.RLock()
	defer t.mu.RUnlock()
	if m, ok := t.metas[topicId]; ok && m.MinInsyncReplicas > 0 {
		return m.MinInsyncReplicas
	}
	return 1
}

// SetTopicMeta upserts the in-memory TopicMeta sidecar. Called from CreateTopics
// on the controller and from ApplyMetadataRecord on followers, so both ends of
// the cluster honor min.insync.replicas without re-reading meta.json.
func (t *TopicStore) SetTopicMeta(topicId uuid.UUID, meta *TopicMeta) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.metas[topicId] = meta
}

func (t *TopicStore) AddCommitLog(topicId uuid.UUID, partition int32, log commitlog.ICommitLog) {
	t.mu.Lock()
	defer t.mu.Unlock()
	key := fmt.Sprintf("%s-%d", topicId, partition)
	t.commitLogs[key] = log
}

func (t *TopicStore) GetCommitLog(topicId uuid.UUID, partition int32) commitlog.ICommitLog {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.commitLogs[fmt.Sprintf("%s-%d", topicId.String(), partition)]
}

// RegisterTopic registers a topic by ID and name (used when applying metadata records).
// Skips silently if the topic is already registered.
func (t *TopicStore) RegisterTopic(topicId uuid.UUID, name string, numPartitions int32) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if _, ok := t.topics[topicId]; ok {
		return nil
	}
	t.topics[topicId] = topic.CreateTopicResponseData{
		Name:              types.CompactString(name),
		TopicId:           topicId,
		NumPartitions:     numPartitions,
		ReplicationFactor: 1,
		ErrorCode:         0,
	}
	t.topicNames[name] = topicId
	return nil
}

func (t *TopicStore) AddTopic(topicData topic.CreateTopicResponseData) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if _, ok := t.topics[topicData.TopicId]; ok {
		return constant.CreateError(constant.ErrTopicAlreadyExists)
	}
	t.topics[topicData.TopicId] = topicData
	t.topicNames[string(topicData.Name)] = topicData.TopicId
	return nil
}

func (t *TopicStore) GetTopic(topicId uuid.UUID) (*topic.CreateTopicResponseData, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	topicData, ok := t.topics[topicId]
	if !ok {
		return nil, constant.CreateError(constant.ErrInvalidTopicException)
	}
	return &topicData, nil
}

func (t *TopicStore) GetTopicByName(topicName string) (*topic.CreateTopicResponseData, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	topicId, ok := t.topicNames[topicName]
	if !ok {
		return nil, fmt.Errorf("topic name %s not found", topicName)
	}
	topicData, ok := t.topics[topicId]
	if !ok {
		return nil, constant.CreateError(constant.ErrInvalidTopicException)
	}
	return &topicData, nil
}

func (t *TopicStore) GetTopicMetadataByNames(topicNames []string, pss *coordinator.PartitionStateStore) ([]admin.MetadataResponseTopic, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	topicIds := make([]uuid.UUID, 0)
	for _, topicName := range topicNames {
		topicId, ok := t.topicNames[topicName]
		if !ok {
			continue
		}
		topicIds = append(topicIds, topicId)
	}
	var topics []admin.MetadataResponseTopic
	for _, topicId := range topicIds {
		topicData, ok := t.topics[topicId]
		if !ok {
			return nil, constant.CreateError(constant.ErrInvalidTopicException)
		}

		var metadataResponsePartitions []admin.MetadataResponsePartition
		for idx := int32(0); idx < topicData.NumPartitions; idx++ {
			ps := pss.GetPartitionState(string(topicData.Name), idx)
			if ps == nil {
				metadataResponsePartitions = append(metadataResponsePartitions, admin.MetadataResponsePartition{
					ErrorCode:      constant.ErrLeaderNotAvailable,
					PartitionIndex: idx,
				})
				continue
			}
			metadataResponsePartitions = append(metadataResponsePartitions, admin.MetadataResponsePartition{
				ErrorCode:       0,
				PartitionIndex:  idx,
				LeaderID:        ps.LeaderBrokerID,
				LeaderEpoch:     ps.LeaderEpoch,
				ReplicaNodes:    ps.Replicas,
				IsrNodes:        ps.ISR,
				OfflineReplicas: []int32{0},
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

func (t *TopicStore) GetTopicMetadata(topicIds []uuid.UUID) ([]admin.MetadataResponseTopic, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	var topics []admin.MetadataResponseTopic
	for _, topicId := range topicIds {
		topicData, ok := t.topics[topicId]
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

	var topicId uuid.UUID
	var err error
	if topicId, err = uuid.FromString(metaData.TopicId); err != nil {
		return fmt.Errorf("topic id %s is not a valid uuid", metaData.TopicId)
	}
	if _, ok := t.topics[topicId]; ok {
		return constant.CreateError(constant.ErrTopicAlreadyExists)
	}

	t.topics[topicId] = topic.CreateTopicResponseData{
		Name:              types.CompactString(metaData.Name),
		TopicId:           topicId,
		NumPartitions:     metaData.NumPartitions,
		ReplicationFactor: int16(metaData.ReplicationFactor),
		ErrorCode:         0,
	}
	t.topicNames[metaData.Name] = topicId
	t.metas[topicId] = metaData
	return nil
}
