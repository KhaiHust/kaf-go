package api

import (
	"github.com/KhaiHust/kaf-go/coordinator"
	"github.com/KhaiHust/kaf-go/storage"
)

type IBrokerContext interface {
	GetTopicStore() *storage.TopicStore
	GetGroupStore() *coordinator.GroupStore
	GetPartitionStateStore() *coordinator.PartitionStateStore
	GetBrokerID() int32
	StartFollowerFetch(topicName string, partition, leaderID int32)
}
