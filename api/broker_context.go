package api

import (
	"github.com/KhaiHust/kaf-go/coordinator"
	"github.com/KhaiHust/kaf-go/storage"
)

type IBrokerRegistry interface {
	RegisterBroker(id int32, host string, port int32) int64
	ValidateAndUpdate(id int32, epoch int64, metadataOffset int64) error
	GetMetadataOffset(id int32) int64
	GetBrokerAddr(id int32) (host string, port int32)
}

type IMetadataLog interface {
	Append(value []byte) error
	NewestOffset() int64
}

type IBrokerContext interface {
	GetTopicStore() *storage.TopicStore
	GetGroupStore() *coordinator.GroupStore
	GetPartitionStateStore() *coordinator.PartitionStateStore
	GetBrokerID() int32
	StartFollowerFetch(topicName string, partition, leaderID int32)
	GetBrokerRegistry() IBrokerRegistry
	GetMetadataLog() IMetadataLog
	IsController() bool
	GetAllBrokerIDs() []int32
	GetPidManager() *coordinator.PidManager
	GetLogDir() string
}
