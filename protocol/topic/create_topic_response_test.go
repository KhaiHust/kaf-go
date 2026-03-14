package topic

import (
	"testing"

	"github.com/KhaiHust/kaf-go/constant"
	"github.com/KhaiHust/kaf-go/protocol"
	"github.com/KhaiHust/kaf-go/protocol/types"
	"github.com/gofrs/uuid/v5"
)

func TestCreateTopicsResponse_ApiKey(t *testing.T) {
	resp := &CreateTopicsResponse{}
	if resp.ApiKey() != constant.ApiKeyCreateTopics {
		t.Errorf("ApiKey() = %d, want %d", resp.ApiKey(), constant.ApiKeyCreateTopics)
	}
}

func TestCreateTopicsResponse_EncodeDecodeRoundtrip_Empty(t *testing.T) {
	original := &CreateTopicsResponse{
		ThrottleTimeMs: 100,
		Topics:         []CreateTopicResponseData{},
	}

	w := protocol.NewWriter(100)
	if err := original.Encode(w); err != nil {
		t.Fatalf("encode() error = %v", err)
	}

	r := protocol.NewReader(w.Bytes())
	decoded := &CreateTopicsResponse{}
	if err := decoded.Decode(r); err != nil {
		t.Fatalf("Decode() error = %v", err)
	}

	if len(decoded.Topics) != 0 {
		t.Errorf("topics length = %d, want 0", len(decoded.Topics))
	}
	if decoded.ThrottleTimeMs != original.ThrottleTimeMs {
		t.Errorf("ThrottleTimeMs = %d, want %d", decoded.ThrottleTimeMs, original.ThrottleTimeMs)
	}
}

func TestCreateTopicsResponse_EncodeDecodeRoundtrip_SingleTopic(t *testing.T) {
	topicId, err := uuid.NewV4()
	if err != nil {
		t.Fatalf("Failed to generate UUID: %v", err)
	}

	original := &CreateTopicsResponse{
		ThrottleTimeMs: 50,
		Topics: []CreateTopicResponseData{
			{
				Name:              types.CompactString("test-topic"),
				TopicId:           topicId,
				ErrorCode:         0,
				ErrorMessage:      nil,
				NumPartitions:     3,
				ReplicationFactor: 2,
				Configs:           []CreateTopicConfigResponse{},
			},
		},
	}

	w := protocol.NewWriter(200)
	if err := original.Encode(w); err != nil {
		t.Fatalf("encode() error = %v", err)
	}

	r := protocol.NewReader(w.Bytes())
	decoded := &CreateTopicsResponse{}
	if err := decoded.Decode(r); err != nil {
		t.Fatalf("Decode() error = %v", err)
	}

	if len(decoded.Topics) != 1 {
		t.Fatalf("topics length = %d, want 1", len(decoded.Topics))
	}
	if string(decoded.Topics[0].Name) != "test-topic" {
		t.Errorf("Topic name = %s, want test-topic", decoded.Topics[0].Name)
	}
	if decoded.Topics[0].TopicId != topicId {
		t.Errorf("TopicId = %v, want %v", decoded.Topics[0].TopicId, topicId)
	}
	if decoded.Topics[0].ErrorCode != 0 {
		t.Errorf("ErrorCode = %d, want 0", decoded.Topics[0].ErrorCode)
	}
	if decoded.Topics[0].NumPartitions != 3 {
		t.Errorf("NumPartitions = %d, want 3", decoded.Topics[0].NumPartitions)
	}
	if decoded.Topics[0].ReplicationFactor != 2 {
		t.Errorf("ReplicationFactor = %d, want 2", decoded.Topics[0].ReplicationFactor)
	}
	if decoded.ThrottleTimeMs != 50 {
		t.Errorf("ThrottleTimeMs = %d, want 50", decoded.ThrottleTimeMs)
	}
}

func TestCreateTopicsResponse_EncodeDecodeRoundtrip_WithErrorMessage(t *testing.T) {
	topicId, err := uuid.NewV4()
	if err != nil {
		t.Fatalf("Failed to generate UUID: %v", err)
	}

	errorMsg := "Topic already exists"
	original := &CreateTopicsResponse{
		ThrottleTimeMs: 75,
		Topics: []CreateTopicResponseData{
			{
				Name:              types.CompactString("existing-topic"),
				TopicId:           topicId,
				ErrorCode:         36, // TOPIC_ALREADY_EXISTS error code
				ErrorMessage:      &errorMsg,
				NumPartitions:     1,
				ReplicationFactor: 1,
				Configs:           []CreateTopicConfigResponse{},
			},
		},
	}

	w := protocol.NewWriter(300)
	if err := original.Encode(w); err != nil {
		t.Fatalf("encode() error = %v", err)
	}

	r := protocol.NewReader(w.Bytes())
	decoded := &CreateTopicsResponse{}
	if err := decoded.Decode(r); err != nil {
		t.Fatalf("Decode() error = %v", err)
	}

	if len(decoded.Topics) != 1 {
		t.Fatalf("topics length = %d, want 1", len(decoded.Topics))
	}
	if decoded.Topics[0].ErrorCode != 36 {
		t.Errorf("ErrorCode = %d, want 36", decoded.Topics[0].ErrorCode)
	}
	decodedMsg := decoded.Topics[0].ErrorMessage
	if decodedMsg == nil || *decodedMsg != errorMsg {
		t.Errorf("ErrorMessage = %v, want %s", decodedMsg, errorMsg)
	}
}

func TestCreateTopicsResponse_EncodeDecodeRoundtrip_WithConfigs(t *testing.T) {
	topicId, err := uuid.NewV4()
	if err != nil {
		t.Fatalf("Failed to generate UUID: %v", err)
	}

	configValue := "1000"
	original := &CreateTopicsResponse{
		ThrottleTimeMs: 25,
		Topics: []CreateTopicResponseData{
			{
				Name:              types.CompactString("configured-topic"),
				TopicId:           topicId,
				ErrorCode:         0,
				ErrorMessage:      nil,
				NumPartitions:     5,
				ReplicationFactor: 3,
				Configs: []CreateTopicConfigResponse{
					{
						Name:                 types.CompactString("retention.ms"),
						Value:                &configValue,
						ReadOnly:             false,
						ConfigSource:         5,
						IsSensitive:          false,
						TopicConfigErrorCode: 0,
					},
					{
						Name:                 types.CompactString("segment.bytes"),
						Value:                nil,
						ReadOnly:             true,
						ConfigSource:         1,
						IsSensitive:          false,
						TopicConfigErrorCode: 0,
					},
				},
			},
		},
	}

	w := protocol.NewWriter(400)
	if err := original.Encode(w); err != nil {
		t.Fatalf("encode() error = %v", err)
	}

	r := protocol.NewReader(w.Bytes())
	decoded := &CreateTopicsResponse{}
	if err := decoded.Decode(r); err != nil {
		t.Fatalf("Decode() error = %v", err)
	}

	if len(decoded.Topics) != 1 {
		t.Fatalf("topics length = %d, want 1", len(decoded.Topics))
	}
	if len(decoded.Topics[0].Configs) != 2 {
		t.Fatalf("Configs length = %d, want 2", len(decoded.Topics[0].Configs))
	}

	// Check first config
	config1 := decoded.Topics[0].Configs[0]
	if string(config1.Name) != "retention.ms" {
		t.Errorf("Config[0] name = %s, want retention.ms", config1.Name)
	}
	val1 := config1.Value
	if val1 == nil || *val1 != configValue {
		t.Errorf("Config[0] value = %v, want %s", val1, configValue)
	}
	if config1.ReadOnly != false {
		t.Errorf("Config[0] ReadOnly = %v, want false", config1.ReadOnly)
	}
	if config1.ConfigSource != 5 {
		t.Errorf("Config[0] ConfigSource = %d, want 5", config1.ConfigSource)
	}
	if config1.IsSensitive != false {
		t.Errorf("Config[0] IsSensitive = %v, want false", config1.IsSensitive)
	}

	// Check second config
	config2 := decoded.Topics[0].Configs[1]
	if string(config2.Name) != "segment.bytes" {
		t.Errorf("Config[1] name = %s, want segment.bytes", config2.Name)
	}
	val2 := config2.Value
	if val2 != nil {
		t.Errorf("Config[1] value = %v, want nil", val2)
	}
	if config2.ReadOnly != true {
		t.Errorf("Config[1] ReadOnly = %v, want true", config2.ReadOnly)
	}
	if config2.ConfigSource != 1 {
		t.Errorf("Config[1] ConfigSource = %d, want 1", config2.ConfigSource)
	}
}

func TestCreateTopicsResponse_EncodeDecodeRoundtrip_MultipleTopics(t *testing.T) {
	topicId1, _ := uuid.NewV4()
	topicId2, _ := uuid.NewV4()
	topicId3, _ := uuid.NewV4()

	original := &CreateTopicsResponse{
		ThrottleTimeMs: 150,
		Topics: []CreateTopicResponseData{
			{
				Name:              types.CompactString("topic-1"),
				TopicId:           topicId1,
				ErrorCode:         0,
				ErrorMessage:      nil,
				NumPartitions:     1,
				ReplicationFactor: 1,
				Configs:           []CreateTopicConfigResponse{},
			},
			{
				Name:              types.CompactString("topic-2"),
				TopicId:           topicId2,
				ErrorCode:         0,
				ErrorMessage:      nil,
				NumPartitions:     5,
				ReplicationFactor: 3,
				Configs:           []CreateTopicConfigResponse{},
			},
			{
				Name:              types.CompactString("topic-3"),
				TopicId:           topicId3,
				ErrorCode:         0,
				ErrorMessage:      nil,
				NumPartitions:     10,
				ReplicationFactor: 2,
				Configs:           []CreateTopicConfigResponse{},
			},
		},
	}

	w := protocol.NewWriter(600)
	if err := original.Encode(w); err != nil {
		t.Fatalf("encode() error = %v", err)
	}

	r := protocol.NewReader(w.Bytes())
	decoded := &CreateTopicsResponse{}
	if err := decoded.Decode(r); err != nil {
		t.Fatalf("Decode() error = %v", err)
	}

	if len(decoded.Topics) != 3 {
		t.Fatalf("topics length = %d, want 3", len(decoded.Topics))
	}

	expectedNames := []string{"topic-1", "topic-2", "topic-3"}
	expectedPartitions := []int32{1, 5, 10}
	expectedReplFactors := []int16{1, 3, 2}
	expectedIds := []uuid.UUID{topicId1, topicId2, topicId3}

	for i, topic := range decoded.Topics {
		if string(topic.Name) != expectedNames[i] {
			t.Errorf("Topic[%d] name = %s, want %s", i, topic.Name, expectedNames[i])
		}
		if topic.NumPartitions != expectedPartitions[i] {
			t.Errorf("Topic[%d] NumPartitions = %d, want %d", i, topic.NumPartitions, expectedPartitions[i])
		}
		if topic.ReplicationFactor != expectedReplFactors[i] {
			t.Errorf("Topic[%d] ReplicationFactor = %d, want %d", i, topic.ReplicationFactor, expectedReplFactors[i])
		}
		if topic.TopicId != expectedIds[i] {
			t.Errorf("Topic[%d] TopicId = %v, want %v", i, topic.TopicId, expectedIds[i])
		}
		if topic.ErrorCode != 0 {
			t.Errorf("Topic[%d] ErrorCode = %d, want 0", i, topic.ErrorCode)
		}
	}

	if decoded.ThrottleTimeMs != 150 {
		t.Errorf("ThrottleTimeMs = %d, want 150", decoded.ThrottleTimeMs)
	}
}

func TestCreateTopicConfigResponse_EncodeDecodeRoundtrip(t *testing.T) {
	configValue := "test-value"
	original := &CreateTopicConfigResponse{
		Name:                 types.CompactString("test.config"),
		Value:                &configValue,
		ReadOnly:             true,
		ConfigSource:         2,
		IsSensitive:          true,
		TopicConfigErrorCode: 0,
	}

	w := protocol.NewWriter(100)
	if err := original.encode(w); err != nil {
		t.Fatalf("encode() error = %v", err)
	}

	r := protocol.NewReader(w.Bytes())
	decoded := &CreateTopicConfigResponse{}
	if err := decoded.decode(r); err != nil {
		t.Fatalf("decode() error = %v", err)
	}

	if string(decoded.Name) != "test.config" {
		t.Errorf("Name = %s, want test.config", decoded.Name)
	}
	val := decoded.Value
	if val == nil || *val != configValue {
		t.Errorf("Value = %v, want %s", val, configValue)
	}
	if decoded.ReadOnly != true {
		t.Errorf("ReadOnly = %v, want true", decoded.ReadOnly)
	}
	if decoded.ConfigSource != 2 {
		t.Errorf("ConfigSource = %d, want 2", decoded.ConfigSource)
	}
	if decoded.IsSensitive != true {
		t.Errorf("IsSensitive = %v, want true", decoded.IsSensitive)
	}
	if decoded.TopicConfigErrorCode != 0 {
		t.Errorf("TopicConfigErrorCode = %d, want 0", decoded.TopicConfigErrorCode)
	}
}

func TestCreateTopicResponseData_EncodeDecodeRoundtrip(t *testing.T) {
	topicId, _ := uuid.NewV4()
	errorMsg := "Test error"

	original := &CreateTopicResponseData{
		Name:              types.CompactString("test-topic-data"),
		TopicId:           topicId,
		ErrorCode:         10,
		ErrorMessage:      &errorMsg,
		NumPartitions:     7,
		ReplicationFactor: 4,
		Configs:           []CreateTopicConfigResponse{},
	}

	w := protocol.NewWriter(200)
	if err := original.encode(w); err != nil {
		t.Fatalf("encode() error = %v", err)
	}

	r := protocol.NewReader(w.Bytes())
	decoded := &CreateTopicResponseData{}
	if err := decoded.decode(r); err != nil {
		t.Fatalf("decode() error = %v", err)
	}

	if string(decoded.Name) != "test-topic-data" {
		t.Errorf("Name = %s, want test-topic-data", decoded.Name)
	}
	if decoded.TopicId != topicId {
		t.Errorf("TopicId = %v, want %v", decoded.TopicId, topicId)
	}
	if decoded.ErrorCode != 10 {
		t.Errorf("ErrorCode = %d, want 10", decoded.ErrorCode)
	}
	decodedMsg := decoded.ErrorMessage
	if decodedMsg == nil || *decodedMsg != errorMsg {
		t.Errorf("ErrorMessage = %v, want %s", decodedMsg, errorMsg)
	}
	if decoded.NumPartitions != 7 {
		t.Errorf("NumPartitions = %d, want 7", decoded.NumPartitions)
	}
	if decoded.ReplicationFactor != 4 {
		t.Errorf("ReplicationFactor = %d, want 4", decoded.ReplicationFactor)
	}
}

func TestCreateTopicConfigResponse_WithSensitiveConfig(t *testing.T) {
	sensitiveValue := "secret-password"
	original := &CreateTopicConfigResponse{
		Name:                 types.CompactString("password"),
		Value:                &sensitiveValue,
		ReadOnly:             false,
		ConfigSource:         4,
		IsSensitive:          true,
		TopicConfigErrorCode: 0,
	}

	w := protocol.NewWriter(150)
	if err := original.encode(w); err != nil {
		t.Fatalf("encode() error = %v", err)
	}

	r := protocol.NewReader(w.Bytes())
	decoded := &CreateTopicConfigResponse{}
	if err := decoded.decode(r); err != nil {
		t.Fatalf("decode() error = %v", err)
	}

	if decoded.IsSensitive != true {
		t.Errorf("IsSensitive = %v, want true", decoded.IsSensitive)
	}
	val := decoded.Value
	if val == nil || *val != sensitiveValue {
		t.Errorf("Value = %v, want %s", val, sensitiveValue)
	}
}
