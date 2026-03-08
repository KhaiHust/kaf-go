package api

import (
	"fmt"
	"log/slog"
	"net"
	"regexp"
	"time"

	"github.com/KhaiHust/kaf-go/config"
	"github.com/KhaiHust/kaf-go/constant"
	"github.com/KhaiHust/kaf-go/protocol"
	"github.com/KhaiHust/kaf-go/protocol/topic"
	"github.com/KhaiHust/kaf-go/storage/commitlog"
	"github.com/gofrs/uuid/v5"
)

var (
	LogStorageDataFolder = "./var/log/"
)

func HandleCreateTopics(conn net.Conn, header *protocol.RequestHeader, reader *protocol.Reader) error {

	createTopicRequest := new(topic.CreateTopicsRequest)
	if err := createTopicRequest.Decode(reader); err != nil {
		return err
	}

	slog.Info("Decoded CreateTopicsRequest",
		"numTopics", len(createTopicRequest.Topics),
		"timeoutMs", createTopicRequest.TimeoutMs,
		"validateOnly", createTopicRequest.ValidateOnly)

	for i, t := range createTopicRequest.Topics {
		slog.Info(fmt.Sprintf("Topic[%d]", i),
			"name", t.Name,
			"numPartitions", t.NumPartitions,
			"replicationFactor", t.ReplicationFactor,
			"numConfigs", len(t.Configs))
	}

	responseHeader := &protocol.ResponseHeader{
		CorrelationId: header.CorrelationId,
		ApiVersion:    header.ApiVersion,
		ApiKey:        header.ApiKey,
	}

	response := &topic.CreateTopicsResponse{}
	response.Topics = validateTopics(createTopicRequest.Topics)
	response.ThrottleTimeMs = 0
	return protocol.WriteFraming(conn, responseHeader, response)

}

func validateTopics(topics []topic.CreateTopicData) []topic.CreateTopicResponseData {
	topicsResponses := make([]topic.CreateTopicResponseData, len(topics))
	for idx := 0; idx < len(topics); idx++ {
		newTopicResponse := topic.CreateTopicResponseData{}
		isValidTopic := validateTopicData(topics[idx])
		newTopicResponse.ErrorCode = isValidTopic
		var topicConfis []topic.CreateTopicConfigResponse
		for _, conf := range topics[idx].Configs {
			topicConfis = append(topicConfis, topic.CreateTopicConfigResponse{
				Name:  conf.Name,
				Value: conf.Value})
		}
		if isValidTopic == 0 {
			newTopicResponse.TopicId = uuid.Must(uuid.NewV7())
			newTopicResponse.Name = topics[idx].Name
			newTopicResponse.NumPartitions = topics[idx].NumPartitions
			newTopicResponse.ReplicationFactor = topics[idx].ReplicationFactor
			newTopicResponse.Configs = topicConfis
			//todo: create storage for topics
			createTopicStorage(topics[idx])
		}

		topicsResponses[idx] = newTopicResponse
	}
	return topicsResponses
}

func validateTopicData(topicData topic.CreateTopicData) int16 {
	if len(topicData.Name) == 0 {
		return constant.ErrInvalidTopicException
	}
	// check regex topic name
	if match, err := regexp.MatchString(`^[a-zA-Z0-9._-]+$`, string(topicData.Name)); !match || err != nil {
		return constant.ErrInvalidTopicException
	}
	if topicData.NumPartitions <= 0 {
		return constant.ErrInvalidTopicException
	}

	if topicData.ReplicationFactor <= 0 {
		return constant.ErrInvalidTopicException
	}

	//Todo: Validate topic name is existed
	return 0
}

func createTopicStorage(topicRequestData topic.CreateTopicData) {
	slog.Info("Create topic storage")
	commitLogConfig := &config.CommitLogConfig{
		SegmentMaxBytes: 1024 * 1024 * 100,
		IndexInterval:   4 * 1024,
		RetentionBytes:  1024 * 1024 * 100,
		RetentionTime:   7 * 24 * time.Hour,
	}
	for numPar := int32(0); numPar < topicRequestData.NumPartitions; numPar++ {
		dir := LogStorageDataFolder + fmt.Sprintf("%s-%d", topicRequestData.Name, numPar)
		newCommitLog, err := commitlog.NewCommitLog(dir, commitLogConfig)
		if err != nil {
			slog.Error(err.Error())
			return
		}
		_ = newCommitLog
	}
	slog.Info("Create topic storage successfully")
}
