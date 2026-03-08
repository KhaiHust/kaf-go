package api

import (
	"bytes"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/KhaiHust/kaf-go/constant"
	"github.com/KhaiHust/kaf-go/protocol"
	"github.com/KhaiHust/kaf-go/protocol/topic"
	"github.com/KhaiHust/kaf-go/protocol/types"
)

// mockConn is a mock implementation of net.Conn for testing
type mockConn struct {
	writeBuffer *bytes.Buffer
	readBuffer  *bytes.Buffer
}

func newMockConn() *mockConn {
	return &mockConn{
		writeBuffer: new(bytes.Buffer),
		readBuffer:  new(bytes.Buffer),
	}
}

func (m *mockConn) Read(b []byte) (int, error) {
	return m.readBuffer.Read(b)
}

func (m *mockConn) Write(b []byte) (int, error) {
	return m.writeBuffer.Write(b)
}

func (m *mockConn) Close() error {
	return nil
}

func (m *mockConn) LocalAddr() net.Addr {
	return nil
}

func (m *mockConn) RemoteAddr() net.Addr {
	return nil
}

func (m *mockConn) SetDeadline(_ time.Time) error {
	return nil
}

func (m *mockConn) SetReadDeadline(_ time.Time) error {
	return nil
}

func (m *mockConn) SetWriteDeadline(_ time.Time) error {
	return nil
}

func TestValidateTopicData(t *testing.T) {
	tests := []struct {
		name      string
		topicData topic.CreateTopicData
		want      int16
	}{
		{
			name: "valid topic",
			topicData: topic.CreateTopicData{
				Name:              types.CompactString("valid-topic"),
				NumPartitions:     3,
				ReplicationFactor: 1,
			},
			want: 0,
		},
		{
			name: "empty topic name",
			topicData: topic.CreateTopicData{
				Name:              types.CompactString(""),
				NumPartitions:     3,
				ReplicationFactor: 1,
			},
			want: constant.ErrInvalidTopicException,
		},
		{
			name: "invalid topic name with special characters",
			topicData: topic.CreateTopicData{
				Name:              types.CompactString("topic@#$%"),
				NumPartitions:     3,
				ReplicationFactor: 1,
			},
			want: constant.ErrInvalidTopicException,
		},
		{
			name: "zero partitions",
			topicData: topic.CreateTopicData{
				Name:              types.CompactString("valid-topic"),
				NumPartitions:     0,
				ReplicationFactor: 1,
			},
			want: constant.ErrInvalidTopicException,
		},
		{
			name: "negative partitions",
			topicData: topic.CreateTopicData{
				Name:              types.CompactString("valid-topic"),
				NumPartitions:     -1,
				ReplicationFactor: 1,
			},
			want: constant.ErrInvalidTopicException,
		},
		{
			name: "zero replication factor",
			topicData: topic.CreateTopicData{
				Name:              types.CompactString("valid-topic"),
				NumPartitions:     3,
				ReplicationFactor: 0,
			},
			want: constant.ErrInvalidTopicException,
		},
		{
			name: "negative replication factor",
			topicData: topic.CreateTopicData{
				Name:              types.CompactString("valid-topic"),
				NumPartitions:     3,
				ReplicationFactor: -1,
			},
			want: constant.ErrInvalidTopicException,
		},
		{
			name: "valid topic with underscores",
			topicData: topic.CreateTopicData{
				Name:              types.CompactString("valid_topic_name"),
				NumPartitions:     5,
				ReplicationFactor: 2,
			},
			want: 0,
		},
		{
			name: "valid topic with dots",
			topicData: topic.CreateTopicData{
				Name:              types.CompactString("valid.topic.name"),
				NumPartitions:     1,
				ReplicationFactor: 1,
			},
			want: 0,
		},
		{
			name: "valid topic with hyphens",
			topicData: topic.CreateTopicData{
				Name:              types.CompactString("valid-topic-name"),
				NumPartitions:     10,
				ReplicationFactor: 3,
			},
			want: 0,
		},
		{
			name: "valid topic with numbers",
			topicData: topic.CreateTopicData{
				Name:              types.CompactString("topic123"),
				NumPartitions:     2,
				ReplicationFactor: 1,
			},
			want: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := validateTopicData(tt.topicData)
			if got != tt.want {
				t.Errorf("validateTopicData() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestValidateTopics(t *testing.T) {
	// Setup temp directory for storage
	tempDir := "./var/test_logs/"
	defer os.RemoveAll(tempDir)

	oldLogStorageDataFolder := LogStorageDataFolder
	LogStorageDataFolder = tempDir
	defer func() {
		LogStorageDataFolder = oldLogStorageDataFolder
	}()

	tests := []struct {
		name   string
		topics []topic.CreateTopicData
		checks func(t *testing.T, responses []topic.CreateTopicResponseData)
	}{
		{
			name: "single valid topic",
			topics: []topic.CreateTopicData{
				{
					Name:              types.CompactString("test-topic"),
					NumPartitions:     3,
					ReplicationFactor: 1,
					Configs:           []topic.CreateTopicConfig{},
				},
			},
			checks: func(t *testing.T, responses []topic.CreateTopicResponseData) {
				if len(responses) != 1 {
					t.Errorf("expected 1 response, got %d", len(responses))
					return
				}
				if responses[0].ErrorCode != 0 {
					t.Errorf("expected error code 0, got %d", responses[0].ErrorCode)
				}
				if responses[0].Name != types.CompactString("test-topic") {
					t.Errorf("expected name 'test-topic', got '%s'", responses[0].Name)
				}
				if responses[0].NumPartitions != 3 {
					t.Errorf("expected 3 partitions, got %d", responses[0].NumPartitions)
				}
				if responses[0].ReplicationFactor != 1 {
					t.Errorf("expected replication factor 1, got %d", responses[0].ReplicationFactor)
				}
			},
		},
		{
			name: "single invalid topic - empty name",
			topics: []topic.CreateTopicData{
				{
					Name:              types.CompactString(""),
					NumPartitions:     3,
					ReplicationFactor: 1,
				},
			},
			checks: func(t *testing.T, responses []topic.CreateTopicResponseData) {
				if len(responses) != 1 {
					t.Errorf("expected 1 response, got %d", len(responses))
					return
				}
				if responses[0].ErrorCode != constant.ErrInvalidTopicException {
					t.Errorf("expected error code %d, got %d", constant.ErrInvalidTopicException, responses[0].ErrorCode)
				}
			},
		},
		{
			name: "multiple topics - mixed valid and invalid",
			topics: []topic.CreateTopicData{
				{
					Name:              types.CompactString("valid-topic-1"),
					NumPartitions:     2,
					ReplicationFactor: 1,
				},
				{
					Name:              types.CompactString(""),
					NumPartitions:     3,
					ReplicationFactor: 1,
				},
				{
					Name:              types.CompactString("valid-topic-2"),
					NumPartitions:     5,
					ReplicationFactor: 2,
				},
			},
			checks: func(t *testing.T, responses []topic.CreateTopicResponseData) {
				if len(responses) != 3 {
					t.Errorf("expected 3 responses, got %d", len(responses))
					return
				}
				// First topic should be valid
				if responses[0].ErrorCode != 0 {
					t.Errorf("expected first topic to be valid (error code 0), got %d", responses[0].ErrorCode)
				}
				// Second topic should be invalid
				if responses[1].ErrorCode != constant.ErrInvalidTopicException {
					t.Errorf("expected second topic to be invalid (error code %d), got %d", constant.ErrInvalidTopicException, responses[1].ErrorCode)
				}
				// Third topic should be valid
				if responses[2].ErrorCode != 0 {
					t.Errorf("expected third topic to be valid (error code 0), got %d", responses[2].ErrorCode)
				}
			},
		},
		{
			name: "topic with configs",
			topics: []topic.CreateTopicData{
				{
					Name:              types.CompactString("topic-with-configs"),
					NumPartitions:     1,
					ReplicationFactor: 1,
					Configs: []topic.CreateTopicConfig{
						{
							Name:  types.CompactString("retention.ms"),
							Value: types.CompactNullableString(stringPtr("86400000")),
						},
						{
							Name:  types.CompactString("compression.type"),
							Value: types.CompactNullableString(stringPtr("gzip")),
						},
					},
				},
			},
			checks: func(t *testing.T, responses []topic.CreateTopicResponseData) {
				if len(responses) != 1 {
					t.Errorf("expected 1 response, got %d", len(responses))
					return
				}
				if responses[0].ErrorCode != 0 {
					t.Errorf("expected error code 0, got %d", responses[0].ErrorCode)
				}
				if len(responses[0].Configs) != 2 {
					t.Errorf("expected 2 configs, got %d", len(responses[0].Configs))
				}
			},
		},
		{
			name:   "empty topics list",
			topics: []topic.CreateTopicData{},
			checks: func(t *testing.T, responses []topic.CreateTopicResponseData) {
				if len(responses) != 0 {
					t.Errorf("expected 0 responses, got %d", len(responses))
				}
			},
		},
		{
			name: "topic with zero partitions",
			topics: []topic.CreateTopicData{
				{
					Name:              types.CompactString("invalid-partitions"),
					NumPartitions:     0,
					ReplicationFactor: 1,
				},
			},
			checks: func(t *testing.T, responses []topic.CreateTopicResponseData) {
				if len(responses) != 1 {
					t.Errorf("expected 1 response, got %d", len(responses))
					return
				}
				if responses[0].ErrorCode != constant.ErrInvalidTopicException {
					t.Errorf("expected error code %d, got %d", constant.ErrInvalidTopicException, responses[0].ErrorCode)
				}
			},
		},
		{
			name: "topic with zero replication factor",
			topics: []topic.CreateTopicData{
				{
					Name:              types.CompactString("invalid-replication"),
					NumPartitions:     3,
					ReplicationFactor: 0,
				},
			},
			checks: func(t *testing.T, responses []topic.CreateTopicResponseData) {
				if len(responses) != 1 {
					t.Errorf("expected 1 response, got %d", len(responses))
					return
				}
				if responses[0].ErrorCode != constant.ErrInvalidTopicException {
					t.Errorf("expected error code %d, got %d", constant.ErrInvalidTopicException, responses[0].ErrorCode)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			responses := validateTopics(tt.topics)
			tt.checks(t, responses)
		})
	}
}

func TestHandleCreateTopics_DecodeError(t *testing.T) {
	// Create a mock connection
	conn := newMockConn()

	// Create a header
	header := &protocol.RequestHeader{
		ApiKey:        constant.ApiKeyCreateTopics,
		ApiVersion:    7,
		CorrelationId: 1,
		ClientId:      types.NullableString(stringPtr("test-client")),
	}

	// Create invalid request data (empty buffer should cause decode error)
	reader := protocol.NewReader([]byte{})

	// Call HandleCreateTopics
	err := HandleCreateTopics(conn, header, reader)

	// Should return an error
	if err == nil {
		t.Error("expected error when decoding invalid request, got nil")
	}
}

func TestHandleCreateTopics_Success(t *testing.T) {
	// Setup temp directory for storage
	tempDir := "./var/test_logs_handle/"
	defer os.RemoveAll(tempDir)

	oldLogStorageDataFolder := LogStorageDataFolder
	LogStorageDataFolder = tempDir
	defer func() {
		LogStorageDataFolder = oldLogStorageDataFolder
	}()

	// Create a mock connection
	conn := newMockConn()

	// Create a header
	header := &protocol.RequestHeader{
		ApiKey:        constant.ApiKeyCreateTopics,
		ApiVersion:    7,
		CorrelationId: 1,
		ClientId:      types.NullableString(stringPtr("test-client")),
	}

	// Create a valid request
	request := &topic.CreateTopicsRequest{
		Topics: []topic.CreateTopicData{
			{
				Name:              types.CompactString("test-handle-topic"),
				NumPartitions:     2,
				ReplicationFactor: 1,
				Configs:           []topic.CreateTopicConfig{},
			},
		},
		TimeoutMs:    5000,
		ValidateOnly: false,
	}

	// Encode the request
	writer := protocol.NewWriter(256)
	if err := request.Encode(writer); err != nil {
		t.Fatalf("failed to encode request: %v", err)
	}

	reader := protocol.NewReader(writer.Bytes())

	// Call HandleCreateTopics
	err := HandleCreateTopics(conn, header, reader)

	// Should succeed
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}

	// Check that data was written to connection
	if conn.writeBuffer.Len() == 0 {
		t.Error("expected response to be written to connection, but buffer is empty")
	}
}

func TestCreateTopicStorage(t *testing.T) {
	// Setup temp directory
	tempDir := "./var/test_storage/"
	defer os.RemoveAll(tempDir)

	oldLogStorageDataFolder := LogStorageDataFolder
	LogStorageDataFolder = tempDir
	defer func() {
		LogStorageDataFolder = oldLogStorageDataFolder
	}()

	tests := []struct {
		name      string
		topicData topic.CreateTopicData
		checkDirs func(t *testing.T, topicData topic.CreateTopicData)
	}{
		{
			name: "single partition",
			topicData: topic.CreateTopicData{
				Name:              types.CompactString("single-partition-topic"),
				NumPartitions:     1,
				ReplicationFactor: 1,
			},
			checkDirs: func(t *testing.T, topicData topic.CreateTopicData) {
				dir := filepath.Join(tempDir, "single-partition-topic-0")
				if _, err := os.Stat(dir); os.IsNotExist(err) {
					t.Errorf("expected directory %s to exist, but it doesn't", dir)
				}
			},
		},
		{
			name: "multiple partitions",
			topicData: topic.CreateTopicData{
				Name:              types.CompactString("multi-partition-topic"),
				NumPartitions:     3,
				ReplicationFactor: 1,
			},
			checkDirs: func(t *testing.T, topicData topic.CreateTopicData) {
				for i := int32(0); i < 3; i++ {
					dir := filepath.Join(tempDir, "multi-partition-topic-"+string(rune('0'+i)))
					if _, err := os.Stat(dir); os.IsNotExist(err) {
						t.Errorf("expected directory %s to exist, but it doesn't", dir)
					}
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			createTopicStorage(tt.topicData)
			tt.checkDirs(t, tt.topicData)
		})
	}
}

func TestValidateTopicData_EdgeCases(t *testing.T) {
	tests := []struct {
		name      string
		topicData topic.CreateTopicData
		want      int16
	}{
		{
			name: "max int32 partitions",
			topicData: topic.CreateTopicData{
				Name:              types.CompactString("max-partitions"),
				NumPartitions:     2147483647, // max int32
				ReplicationFactor: 1,
			},
			want: 0,
		},
		{
			name: "max int16 replication factor",
			topicData: topic.CreateTopicData{
				Name:              types.CompactString("max-replication"),
				NumPartitions:     1,
				ReplicationFactor: 32767, // max int16
			},
			want: 0,
		},
		{
			name: "very long valid topic name",
			topicData: topic.CreateTopicData{
				Name:              types.CompactString("a-very-long-topic-name-with-many-characters-that-is-still-valid"),
				NumPartitions:     1,
				ReplicationFactor: 1,
			},
			want: 0,
		},
		{
			name: "topic name with all valid characters",
			topicData: topic.CreateTopicData{
				Name:              types.CompactString("aA0._-"),
				NumPartitions:     1,
				ReplicationFactor: 1,
			},
			want: 0,
		},
		{
			name: "topic name with only special characters",
			topicData: topic.CreateTopicData{
				Name:              types.CompactString("@#$%^&*"),
				NumPartitions:     1,
				ReplicationFactor: 1,
			},
			want: constant.ErrInvalidTopicException,
		},
		{
			name: "topic name with mixed valid and invalid characters",
			topicData: topic.CreateTopicData{
				Name:              types.CompactString("valid@@@"),
				NumPartitions:     1,
				ReplicationFactor: 1,
			},
			want: constant.ErrInvalidTopicException,
		},
		{
			name: "topic name with spaces",
			topicData: topic.CreateTopicData{
				Name:              types.CompactString("topic with spaces"),
				NumPartitions:     1,
				ReplicationFactor: 1,
			},
			want: constant.ErrInvalidTopicException,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := validateTopicData(tt.topicData)
			if got != tt.want {
				t.Errorf("validateTopicData() = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestValidateTopicData_ComplexCases tests complex validation scenarios
func TestValidateTopicData_ComplexCases(t *testing.T) {
	tests := []struct {
		name      string
		topicData topic.CreateTopicData
		want      int16
	}{
		{
			name: "topic name ending with hyphen",
			topicData: topic.CreateTopicData{
				Name:              types.CompactString("topic-"),
				NumPartitions:     1,
				ReplicationFactor: 1,
			},
			want: 0,
		},
		{
			name: "topic name starting with number",
			topicData: topic.CreateTopicData{
				Name:              types.CompactString("123topic"),
				NumPartitions:     1,
				ReplicationFactor: 1,
			},
			want: 0,
		},
		{
			name: "topic name with consecutive dots",
			topicData: topic.CreateTopicData{
				Name:              types.CompactString("topic..name"),
				NumPartitions:     1,
				ReplicationFactor: 1,
			},
			want: 0,
		},
		{
			name: "topic name with consecutive underscores",
			topicData: topic.CreateTopicData{
				Name:              types.CompactString("topic__name"),
				NumPartitions:     1,
				ReplicationFactor: 1,
			},
			want: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := validateTopicData(tt.topicData)
			if got != tt.want {
				t.Errorf("validateTopicData() = %v, want %v", got, tt.want)
			}
		})
	}
}

// Helper function to create string pointers
func stringPtr(s string) *string {
	return &s
}
