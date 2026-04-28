package api

import (
	"fmt"
	"log/slog"
	"net"
	"path/filepath"
	"regexp"
	"strconv"
	"time"

	"github.com/KhaiHust/kaf-go/config"
	"github.com/KhaiHust/kaf-go/constant"
	"github.com/KhaiHust/kaf-go/coordinator"
	"github.com/KhaiHust/kaf-go/metrics"
	"github.com/KhaiHust/kaf-go/protocol"
	metadatapkg "github.com/KhaiHust/kaf-go/protocol/metadata"
	"github.com/KhaiHust/kaf-go/protocol/topic"
	"github.com/KhaiHust/kaf-go/storage"
	"github.com/KhaiHust/kaf-go/storage/commitlog"
	"github.com/gofrs/uuid/v5"
)

var (
	LogStorageDataFolder = "./var/log/"
)

type TopicConfig struct {
	MinInsyncReplicas int16
}

func HandleCreateTopics(conn net.Conn, brokerContext IBrokerContext, header *protocol.RequestHeader, reader *protocol.Reader) error {

	createTopicRequest := new(topic.CreateTopicsRequest)
	if err := createTopicRequest.Decode(reader); err != nil {
		return err
	}

	responseHeader := &protocol.ResponseHeader{
		CorrelationId: header.CorrelationId,
		ApiVersion:    header.ApiVersion,
		ApiKey:        header.ApiKey,
	}

	response := &topic.CreateTopicsResponse{}
	response.Topics = validateTopics(createTopicRequest.Topics)
	response.ThrottleTimeMs = 0

	topicStore := brokerContext.GetTopicStore()
	pss := brokerContext.GetPartitionStateStore()
	knownBrokers := brokerContext.GetAllBrokerIDs()
	if len(knownBrokers) == 0 {
		knownBrokers = []int32{brokerContext.GetBrokerID()}
	}

	for _, topicData := range response.Topics {
		if topicData.ErrorCode != 0 {
			continue
		}

		pss.AssignReplicas(topicData.Name.String(), topicData.NumPartitions, int32(topicData.ReplicationFactor), knownBrokers)

		_ = topicStore.AddTopic(topicData)

		topicConfig, cfgErr := parseTopicConfig(topicData.Configs)
		if cfgErr != nil {
			slog.Error("invalid topic config", "topic", topicData.Name, "err", cfgErr)
			topicConfig = &TopicConfig{MinInsyncReplicas: 1}
		}

		_ = createTopicStorage(topicStore, pss, brokerContext.GetLogDir(), topicData, topicConfig)

		// StartFollowerFetch must run AFTER AddTopic + createTopicStorage so the
		// fetcher can resolve the topicId and dial the leader's address.
		for partition := int32(0); partition < topicData.NumPartitions; partition++ {
			ps := pss.GetPartitionState(topicData.Name.String(), partition)
			if ps != nil && ps.LeaderBrokerID != brokerContext.GetBrokerID() {
				brokerContext.StartFollowerFetch(topicData.Name.String(), partition, ps.LeaderBrokerID)
			}
		}

		// Append TopicRecord + PartitionRecords to metadata log so followers learn about this topic.
		topicRec := &metadatapkg.TopicRecord{
			TopicId:           topicData.TopicId,
			Name:              topicData.Name.String(),
			NumPartitions:     topicData.NumPartitions,
			MinInsyncReplicas: topicConfig.MinInsyncReplicas,
		}
		_ = brokerContext.GetMetadataLog().Append(topicRec.Encode())

		for partition := int32(0); partition < topicData.NumPartitions; partition++ {
			ps := pss.GetPartitionState(topicData.Name.String(), partition)
			if ps == nil {
				continue
			}
			partRec := &metadatapkg.PartitionRecord{
				TopicId:     topicData.TopicId,
				PartitionId: partition,
				Leader:      ps.LeaderBrokerID,
				LeaderEpoch: 0,
				Replicas:    ps.Replicas,
				ISR:         ps.ISR,
			}
			_ = brokerContext.GetMetadataLog().Append(partRec.Encode())
		}
	}
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

func createTopicStorage(topicStore *storage.TopicStore, pss *coordinator.PartitionStateStore, logDir string, topicResponseData topic.CreateTopicResponseData, topicConfig *TopicConfig) error {
	commitLogConfig := &config.CommitLogConfig{
		SegmentMaxBytes: 1024 * 1024 * 100,
		IndexInterval:   4 * 1024,
		RetentionBytes:  1024 * 1024 * 100,
		RetentionTime:   7 * 24 * time.Hour,
	}

	minISR := int16(1)
	if topicConfig != nil && topicConfig.MinInsyncReplicas > 0 {
		minISR = topicConfig.MinInsyncReplicas
	}

	if logDir == "" {
		logDir = LogStorageDataFolder
	}
	topicName := topicResponseData.Name.String()
	topicDir := filepath.Join(logDir, topicName)
	meta := storage.TopicMeta{
		Name:              topicName,
		TopicId:           topicResponseData.TopicId.String(),
		NumPartitions:     topicResponseData.NumPartitions,
		ReplicationFactor: topicResponseData.ReplicationFactor,
		MinInsyncReplicas: minISR,
	}
	if err := storage.SaveTopicMeta(topicDir, meta); err != nil {
		return fmt.Errorf("failed to save topic meta: %w", err)
	}
	topicStore.SetTopicMeta(topicResponseData.TopicId, &meta)

	for numPar := int32(0); numPar < topicResponseData.NumPartitions; numPar++ {
		dir := filepath.Join(topicDir, fmt.Sprintf("%s-%d", topicResponseData.Name, numPar))

		newCommitLog, err := commitlog.NewCommitLog(dir, commitLogConfig)
		if err != nil {
			slog.Error("Error creating new commit log", "error", err)
			return err
		}

		topicStore.AddCommitLog(topicResponseData.TopicId, numPar, newCommitLog)

		if pss != nil {
			if ps := pss.GetPartitionState(topicName, numPar); ps != nil && ps.Idempotence != nil {
				psm := coordinator.NewProducerStateManager(newCommitLog.Dir(), ps.Idempotence)
				psm.SetLabels(topicName, numPar)
				ps.ProducerStateManager = psm
				cl := newCommitLog
				topic := topicName
				part := numPar
				cl.SetSegmentRollHook(func(newBase int64) {
					psm.OnSegmentRoll(newBase)
					metrics.ActiveSegments.WithLabelValues(topic, metrics.FormatPartition(part)).Set(float64(cl.NumSegments()))
				})
				metrics.ActiveSegments.WithLabelValues(topic, metrics.FormatPartition(part)).Set(float64(cl.NumSegments()))
			}
		}
	}
	return nil
}

func parseTopicConfig(configs []topic.CreateTopicConfigResponse) (*TopicConfig, error) {
	topicConfig := &TopicConfig{MinInsyncReplicas: 1}
	for _, createTopicConfig := range configs {
		value := createTopicConfig.Value
		if value == nil {
			continue
		}
		switch createTopicConfig.Name.String() {
		case "min.insync.replicas":
			n, err := strconv.ParseInt(*value, 10, 16)
			if err != nil {
				return nil, fmt.Errorf("invalid min.insync.replicas value: %w", err)
			}
			topicConfig.MinInsyncReplicas = int16(n)
		default:
		}
	}
	return topicConfig, nil
}
