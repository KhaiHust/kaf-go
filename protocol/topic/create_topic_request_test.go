package topic

import (
	"testing"

	"github.com/KhaiHust/kaf-go/constant"
	"github.com/KhaiHust/kaf-go/protocol"
	"github.com/KhaiHust/kaf-go/protocol/types"
)

func TestCreateTopicsRequest_ApiKey(t *testing.T) {
	req := &CreateTopicsRequest{}
	if req.ApiKey() != constant.ApiKeyCreateTopics {
		t.Errorf("ApiKey() = %d, want %d", req.ApiKey(), constant.ApiKeyCreateTopics)
	}
}

func TestCreateTopicsRequest_ApiVersion(t *testing.T) {
	req := &CreateTopicsRequest{version: 5}
	if req.ApiVersion() != 5 {
		t.Errorf("ApiVersion() = %d, want 5", req.ApiVersion())
	}
}

func TestCreateTopicsRequest_CorrelationID(t *testing.T) {
	req := &CreateTopicsRequest{correlationId: 12345}
	if req.CorrelationID() != 12345 {
		t.Errorf("CorrelationID() = %d, want 12345", req.CorrelationID())
	}
}

func TestCreateTopicsRequest_EncodeDecodeRoundtrip_Empty(t *testing.T) {
	original := &CreateTopicsRequest{
		Topics:       []CreateTopicData{},
		TimeoutMs:    30000,
		ValidateOnly: false,
	}

	w := protocol.NewWriter(100)
	if err := original.Encode(w); err != nil {
		t.Fatalf("encode() error = %v", err)
	}

	r := protocol.NewReader(w.Bytes())
	decoded := &CreateTopicsRequest{}
	if err := decoded.Decode(r); err != nil {
		t.Fatalf("Decode() error = %v", err)
	}

	if len(decoded.Topics) != 0 {
		t.Errorf("Topics length = %d, want 0", len(decoded.Topics))
	}
	if decoded.TimeoutMs != original.TimeoutMs {
		t.Errorf("TimeoutMs = %d, want %d", decoded.TimeoutMs, original.TimeoutMs)
	}
	if decoded.ValidateOnly != original.ValidateOnly {
		t.Errorf("ValidateOnly = %v, want %v", decoded.ValidateOnly, original.ValidateOnly)
	}
}

func TestCreateTopicsRequest_EncodeDecodeRoundtrip_SingleTopic(t *testing.T) {
	original := &CreateTopicsRequest{
		Topics: []CreateTopicData{
			{
				Name:              "test-topic",
				NumPartitions:     3,
				ReplicationFactor: 2,
				Assigments:        []CreateTopicAssignment{},
				Configs:           []CreateTopicConfig{},
			},
		},
		TimeoutMs:    60000,
		ValidateOnly: true,
	}

	w := protocol.NewWriter(200)
	if err := original.Encode(w); err != nil {
		t.Fatalf("encode() error = %v", err)
	}

	r := protocol.NewReader(w.Bytes())
	decoded := &CreateTopicsRequest{}
	if err := decoded.Decode(r); err != nil {
		t.Fatalf("Decode() error = %v", err)
	}

	if len(decoded.Topics) != 1 {
		t.Fatalf("Topics length = %d, want 1", len(decoded.Topics))
	}
	if string(decoded.Topics[0].Name) != "test-topic" {
		t.Errorf("Topic name = %s, want test-topic", decoded.Topics[0].Name)
	}
	if decoded.Topics[0].NumPartitions != 3 {
		t.Errorf("NumPartitions = %d, want 3", decoded.Topics[0].NumPartitions)
	}
	if decoded.Topics[0].ReplicationFactor != 2 {
		t.Errorf("ReplicationFactor = %d, want 2", decoded.Topics[0].ReplicationFactor)
	}
	if decoded.TimeoutMs != 60000 {
		t.Errorf("TimeoutMs = %d, want 60000", decoded.TimeoutMs)
	}
	if decoded.ValidateOnly != true {
		t.Errorf("ValidateOnly = %v, want true", decoded.ValidateOnly)
	}
}

func TestCreateTopicsRequest_EncodeDecodeRoundtrip_MultipleTopics(t *testing.T) {
	original := &CreateTopicsRequest{
		Topics: []CreateTopicData{
			{
				Name:              "topic-1",
				NumPartitions:     1,
				ReplicationFactor: 1,
				Assigments:        []CreateTopicAssignment{},
				Configs:           []CreateTopicConfig{},
			},
			{
				Name:              "topic-2",
				NumPartitions:     5,
				ReplicationFactor: 3,
				Assigments:        []CreateTopicAssignment{},
				Configs:           []CreateTopicConfig{},
			},
			{
				Name:              "topic-3",
				NumPartitions:     10,
				ReplicationFactor: 2,
				Assigments:        []CreateTopicAssignment{},
				Configs:           []CreateTopicConfig{},
			},
		},
		TimeoutMs:    45000,
		ValidateOnly: false,
	}

	w := protocol.NewWriter(500)
	if err := original.Encode(w); err != nil {
		t.Fatalf("encode() error = %v", err)
	}

	r := protocol.NewReader(w.Bytes())
	decoded := &CreateTopicsRequest{}
	if err := decoded.Decode(r); err != nil {
		t.Fatalf("Decode() error = %v", err)
	}

	if len(decoded.Topics) != 3 {
		t.Fatalf("Topics length = %d, want 3", len(decoded.Topics))
	}

	expectedNames := []string{"topic-1", "topic-2", "topic-3"}
	expectedPartitions := []int32{1, 5, 10}
	expectedReplication := []int16{1, 3, 2}

	for i, topic := range decoded.Topics {
		if string(topic.Name) != expectedNames[i] {
			t.Errorf("Topic[%d] name = %s, want %s", i, topic.Name, expectedNames[i])
		}
		if topic.NumPartitions != expectedPartitions[i] {
			t.Errorf("Topic[%d] NumPartitions = %d, want %d", i, topic.NumPartitions, expectedPartitions[i])
		}
		if topic.ReplicationFactor != expectedReplication[i] {
			t.Errorf("Topic[%d] ReplicationFactor = %d, want %d", i, topic.ReplicationFactor, expectedReplication[i])
		}
	}
}

func TestCreateTopicsRequest_EncodeDecodeRoundtrip_WithAssignments(t *testing.T) {
	original := &CreateTopicsRequest{
		Topics: []CreateTopicData{
			{
				Name:              "assigned-topic",
				NumPartitions:     -1, // Use assignments instead
				ReplicationFactor: -1,
				Assigments: []CreateTopicAssignment{
					{PartitionIndex: 0, BrokerIds: []int32{1, 2, 3}},
					{PartitionIndex: 1, BrokerIds: []int32{2, 3, 1}},
					{PartitionIndex: 2, BrokerIds: []int32{3, 1, 2}},
				},
				Configs: []CreateTopicConfig{},
			},
		},
		TimeoutMs:    30000,
		ValidateOnly: false,
	}

	w := protocol.NewWriter(500)
	if err := original.Encode(w); err != nil {
		t.Fatalf("encode() error = %v", err)
	}

	r := protocol.NewReader(w.Bytes())
	decoded := &CreateTopicsRequest{}
	if err := decoded.Decode(r); err != nil {
		t.Fatalf("Decode() error = %v", err)
	}

	if len(decoded.Topics) != 1 {
		t.Fatalf("Topics length = %d, want 1", len(decoded.Topics))
	}

	topic := decoded.Topics[0]
	if len(topic.Assigments) != 3 {
		t.Fatalf("Assignments length = %d, want 3", len(topic.Assigments))
	}

	// Verify first assignment
	if topic.Assigments[0].PartitionIndex != 0 {
		t.Errorf("Assignment[0] PartitionIndex = %d, want 0", topic.Assigments[0].PartitionIndex)
	}
	if len(topic.Assigments[0].BrokerIds) != 3 {
		t.Errorf("Assignment[0] BrokerIds length = %d, want 3", len(topic.Assigments[0].BrokerIds))
	}
	expectedBrokers := []int32{1, 2, 3}
	for i, brokerId := range topic.Assigments[0].BrokerIds {
		if brokerId != expectedBrokers[i] {
			t.Errorf("Assignment[0] BrokerId[%d] = %d, want %d", i, brokerId, expectedBrokers[i])
		}
	}
}

func TestCreateTopicsRequest_EncodeDecodeRoundtrip_WithConfigs(t *testing.T) {
	retentionMs := "86400000"
	cleanupPolicy := "delete"

	original := &CreateTopicsRequest{
		Topics: []CreateTopicData{
			{
				Name:              "configured-topic",
				NumPartitions:     3,
				ReplicationFactor: 2,
				Assigments:        []CreateTopicAssignment{},
				Configs: []CreateTopicConfig{
					{Name: "retention.ms", Value: &retentionMs},
					{Name: "cleanup.policy", Value: &cleanupPolicy},
				},
			},
		},
		TimeoutMs:    30000,
		ValidateOnly: false,
	}

	w := protocol.NewWriter(500)
	if err := original.Encode(w); err != nil {
		t.Fatalf("encode() error = %v", err)
	}

	r := protocol.NewReader(w.Bytes())
	decoded := &CreateTopicsRequest{}
	if err := decoded.Decode(r); err != nil {
		t.Fatalf("Decode() error = %v", err)
	}

	if len(decoded.Topics) != 1 {
		t.Fatalf("Topics length = %d, want 1", len(decoded.Topics))
	}

	topic := decoded.Topics[0]
	if len(topic.Configs) != 2 {
		t.Fatalf("Configs length = %d, want 2", len(topic.Configs))
	}

	// Verify configs
	if string(topic.Configs[0].Name) != "retention.ms" {
		t.Errorf("Config[0] Name = %s, want retention.ms", topic.Configs[0].Name)
	}
	if topic.Configs[0].Value == nil || *topic.Configs[0].Value != "86400000" {
		t.Errorf("Config[0] Value = %v, want 86400000", topic.Configs[0].Value)
	}

	if string(topic.Configs[1].Name) != "cleanup.policy" {
		t.Errorf("Config[1] Name = %s, want cleanup.policy", topic.Configs[1].Name)
	}
	if topic.Configs[1].Value == nil || *topic.Configs[1].Value != "delete" {
		t.Errorf("Config[1] Value = %v, want delete", topic.Configs[1].Value)
	}
}

func TestCreateTopicsRequest_EncodeDecodeRoundtrip_WithNullConfigValue(t *testing.T) {
	original := &CreateTopicsRequest{
		Topics: []CreateTopicData{
			{
				Name:              "topic-with-null-config",
				NumPartitions:     1,
				ReplicationFactor: 1,
				Assigments:        []CreateTopicAssignment{},
				Configs: []CreateTopicConfig{
					{Name: "some.config", Value: nil}, // null value
				},
			},
		},
		TimeoutMs:    30000,
		ValidateOnly: false,
	}

	w := protocol.NewWriter(200)
	if err := original.Encode(w); err != nil {
		t.Fatalf("encode() error = %v", err)
	}

	r := protocol.NewReader(w.Bytes())
	decoded := &CreateTopicsRequest{}
	if err := decoded.Decode(r); err != nil {
		t.Fatalf("Decode() error = %v", err)
	}

	if len(decoded.Topics[0].Configs) != 1 {
		t.Fatalf("Configs length = %d, want 1", len(decoded.Topics[0].Configs))
	}

	if decoded.Topics[0].Configs[0].Value != nil {
		t.Errorf("Config Value should be nil, got %v", decoded.Topics[0].Configs[0].Value)
	}
}

func TestCreateTopicsRequest_EncodeDecodeRoundtrip_FullExample(t *testing.T) {
	retentionMs := "604800000"
	compressionType := "lz4"

	original := &CreateTopicsRequest{
		Topics: []CreateTopicData{
			{
				Name:              "full-example-topic",
				NumPartitions:     -1,
				ReplicationFactor: -1,
				Assigments: []CreateTopicAssignment{
					{PartitionIndex: 0, BrokerIds: []int32{1, 2}},
					{PartitionIndex: 1, BrokerIds: []int32{2, 3}},
				},
				Configs: []CreateTopicConfig{
					{Name: "retention.ms", Value: &retentionMs},
					{Name: "compression.type", Value: &compressionType},
				},
			},
		},
		TimeoutMs:    120000,
		ValidateOnly: true,
	}

	w := protocol.NewWriter(1000)
	if err := original.Encode(w); err != nil {
		t.Fatalf("encode() error = %v", err)
	}

	r := protocol.NewReader(w.Bytes())
	decoded := &CreateTopicsRequest{}
	if err := decoded.Decode(r); err != nil {
		t.Fatalf("Decode() error = %v", err)
	}

	// Verify all fields
	if len(decoded.Topics) != 1 {
		t.Fatalf("Topics length = %d, want 1", len(decoded.Topics))
	}

	topic := decoded.Topics[0]
	if string(topic.Name) != "full-example-topic" {
		t.Errorf("Topic name = %s, want full-example-topic", topic.Name)
	}
	if topic.NumPartitions != -1 {
		t.Errorf("NumPartitions = %d, want -1", topic.NumPartitions)
	}
	if topic.ReplicationFactor != -1 {
		t.Errorf("ReplicationFactor = %d, want -1", topic.ReplicationFactor)
	}
	if len(topic.Assigments) != 2 {
		t.Errorf("Assignments length = %d, want 2", len(topic.Assigments))
	}
	if len(topic.Configs) != 2 {
		t.Errorf("Configs length = %d, want 2", len(topic.Configs))
	}
	if decoded.TimeoutMs != 120000 {
		t.Errorf("TimeoutMs = %d, want 120000", decoded.TimeoutMs)
	}
	if decoded.ValidateOnly != true {
		t.Errorf("ValidateOnly = %v, want true", decoded.ValidateOnly)
	}
}

// Test CreateTopicData encode/decode directly

func TestCreateTopicData_EncodeDecodeRoundtrip(t *testing.T) {
	original := &CreateTopicData{
		Name:              "direct-test",
		NumPartitions:     5,
		ReplicationFactor: 3,
		Assigments:        []CreateTopicAssignment{},
		Configs:           []CreateTopicConfig{},
	}

	w := protocol.NewWriter(100)
	if err := original.encode(w); err != nil {
		t.Fatalf("encode() error = %v", err)
	}

	r := protocol.NewReader(w.Bytes())
	decoded := &CreateTopicData{}
	if err := decoded.decode(r); err != nil {
		t.Fatalf("decode() error = %v", err)
	}

	if string(decoded.Name) != "direct-test" {
		t.Errorf("Name = %s, want direct-test", decoded.Name)
	}
	if decoded.NumPartitions != 5 {
		t.Errorf("NumPartitions = %d, want 5", decoded.NumPartitions)
	}
	if decoded.ReplicationFactor != 3 {
		t.Errorf("ReplicationFactor = %d, want 3", decoded.ReplicationFactor)
	}
}

// Test CreateTopicAssignment encode/decode directly

func TestCreateTopicAssignment_EncodeDecodeRoundtrip(t *testing.T) {
	original := &CreateTopicAssignment{
		PartitionIndex: 5,
		BrokerIds:      []int32{1, 2, 3, 4, 5},
	}

	w := protocol.NewWriter(50)
	if err := original.encode(w); err != nil {
		t.Fatalf("encode() error = %v", err)
	}

	r := protocol.NewReader(w.Bytes())
	decoded := &CreateTopicAssignment{}
	if err := decoded.decode(r); err != nil {
		t.Fatalf("decode() error = %v", err)
	}

	if decoded.PartitionIndex != 5 {
		t.Errorf("PartitionIndex = %d, want 5", decoded.PartitionIndex)
	}
	if len(decoded.BrokerIds) != 5 {
		t.Fatalf("BrokerIds length = %d, want 5", len(decoded.BrokerIds))
	}
	for i, id := range decoded.BrokerIds {
		if id != int32(i+1) {
			t.Errorf("BrokerId[%d] = %d, want %d", i, id, i+1)
		}
	}
}

func TestCreateTopicAssignment_EncodeDecodeRoundtrip_EmptyBrokers(t *testing.T) {
	original := &CreateTopicAssignment{
		PartitionIndex: 0,
		BrokerIds:      []int32{},
	}

	w := protocol.NewWriter(20)
	if err := original.encode(w); err != nil {
		t.Fatalf("encode() error = %v", err)
	}

	r := protocol.NewReader(w.Bytes())
	decoded := &CreateTopicAssignment{}
	if err := decoded.decode(r); err != nil {
		t.Fatalf("decode() error = %v", err)
	}

	if decoded.PartitionIndex != 0 {
		t.Errorf("PartitionIndex = %d, want 0", decoded.PartitionIndex)
	}
	if len(decoded.BrokerIds) != 0 {
		t.Errorf("BrokerIds length = %d, want 0", len(decoded.BrokerIds))
	}
}

// Test CreateTopicConfig encode/decode directly

func TestCreateTopicConfig_EncodeDecodeRoundtrip(t *testing.T) {
	value := "test-value"
	original := &CreateTopicConfig{
		Name:  "test-config",
		Value: &value,
	}

	w := protocol.NewWriter(50)
	if err := original.encode(w); err != nil {
		t.Fatalf("encode() error = %v", err)
	}

	r := protocol.NewReader(w.Bytes())
	decoded := &CreateTopicConfig{}
	if err := decoded.decode(r); err != nil {
		t.Fatalf("decode() error = %v", err)
	}

	if string(decoded.Name) != "test-config" {
		t.Errorf("Name = %s, want test-config", decoded.Name)
	}
	if decoded.Value == nil {
		t.Fatal("Value should not be nil")
	}
	if *decoded.Value != "test-value" {
		t.Errorf("Value = %s, want test-value", *decoded.Value)
	}
}

func TestCreateTopicConfig_EncodeDecodeRoundtrip_NullValue(t *testing.T) {
	original := &CreateTopicConfig{
		Name:  "null-config",
		Value: nil,
	}

	w := protocol.NewWriter(50)
	if err := original.encode(w); err != nil {
		t.Fatalf("encode() error = %v", err)
	}

	r := protocol.NewReader(w.Bytes())
	decoded := &CreateTopicConfig{}
	if err := decoded.decode(r); err != nil {
		t.Fatalf("decode() error = %v", err)
	}

	if string(decoded.Name) != "null-config" {
		t.Errorf("Name = %s, want null-config", decoded.Name)
	}
	if decoded.Value != nil {
		t.Errorf("Value should be nil, got %v", *decoded.Value)
	}
}

// Test edge cases

func TestCreateTopicsRequest_Decode_EmptyBuffer(t *testing.T) {
	r := protocol.NewReader([]byte{})
	decoded := &CreateTopicsRequest{}
	err := decoded.Decode(r)

	if err == nil {
		t.Error("Decode() should return error for empty buffer")
	}
}

func TestCreateTopicData_Decode_EmptyBuffer(t *testing.T) {
	r := protocol.NewReader([]byte{})
	decoded := &CreateTopicData{}
	err := decoded.decode(r)

	if err == nil {
		t.Error("decode() should return error for empty buffer")
	}
}

func TestCreateTopicAssignment_Decode_EmptyBuffer(t *testing.T) {
	r := protocol.NewReader([]byte{})
	decoded := &CreateTopicAssignment{}
	err := decoded.decode(r)

	if err == nil {
		t.Error("decode() should return error for empty buffer")
	}
}

func TestCreateTopicConfig_Decode_EmptyBuffer(t *testing.T) {
	r := protocol.NewReader([]byte{})
	decoded := &CreateTopicConfig{}
	err := decoded.decode(r)

	if err == nil {
		t.Error("decode() should return error for empty buffer")
	}
}

// Test with special characters in topic names

func TestCreateTopicsRequest_SpecialCharacterTopicName(t *testing.T) {
	original := &CreateTopicsRequest{
		Topics: []CreateTopicData{
			{
				Name:              types.CompactString("topic-with-dashes_and_underscores.and.dots"),
				NumPartitions:     1,
				ReplicationFactor: 1,
				Assigments:        []CreateTopicAssignment{},
				Configs:           []CreateTopicConfig{},
			},
		},
		TimeoutMs:    30000,
		ValidateOnly: false,
	}

	w := protocol.NewWriter(200)
	if err := original.Encode(w); err != nil {
		t.Fatalf("encode() error = %v", err)
	}

	r := protocol.NewReader(w.Bytes())
	decoded := &CreateTopicsRequest{}
	if err := decoded.Decode(r); err != nil {
		t.Fatalf("Decode() error = %v", err)
	}

	if string(decoded.Topics[0].Name) != "topic-with-dashes_and_underscores.and.dots" {
		t.Errorf("Topic name = %s, want topic-with-dashes_and_underscores.and.dots", decoded.Topics[0].Name)
	}
}

// Benchmark tests

func BenchmarkCreateTopicsRequest_Encode(b *testing.B) {
	req := &CreateTopicsRequest{
		Topics: []CreateTopicData{
			{
				Name:              "benchmark-topic",
				NumPartitions:     10,
				ReplicationFactor: 3,
				Assigments:        []CreateTopicAssignment{},
				Configs:           []CreateTopicConfig{},
			},
		},
		TimeoutMs:    30000,
		ValidateOnly: false,
	}

	w := protocol.NewWriter(200)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		w.Reset()
		_ = req.Encode(w)
	}
}

func BenchmarkCreateTopicsRequest_Decode(b *testing.B) {
	req := &CreateTopicsRequest{
		Topics: []CreateTopicData{
			{
				Name:              "benchmark-topic",
				NumPartitions:     10,
				ReplicationFactor: 3,
				Assigments:        []CreateTopicAssignment{},
				Configs:           []CreateTopicConfig{},
			},
		},
		TimeoutMs:    30000,
		ValidateOnly: false,
	}

	w := protocol.NewWriter(200)
	_ = req.Encode(w)
	data := w.Bytes()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		r := protocol.NewReader(data)
		decoded := &CreateTopicsRequest{}
		_ = decoded.Decode(r)
	}
}
