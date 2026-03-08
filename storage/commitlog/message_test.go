package commitlog

import (
	"bytes"
	"testing"
	"time"
)

func TestSerializeDeserializeMessage(t *testing.T) {
	tests := []struct {
		name    string
		message Message
	}{
		{
			name: "simple message without headers",
			message: Message{
				Offset:    1,
				Timestamp: time.Unix(0, 1234567890),
				Key:       []byte("key1"),
				Value:     []byte("value1"),
				Headers:   map[string][]byte{},
			},
		},
		{
			name: "message with headers",
			message: Message{
				Offset:    42,
				Timestamp: time.Unix(0, 9876543210),
				Key:       []byte("test-key"),
				Value:     []byte("test-value"),
				Headers: map[string][]byte{
					"header1": []byte("value1"),
					"header2": []byte("value2"),
				},
			},
		},
		{
			name: "message with empty key and value",
			message: Message{
				Offset:    0,
				Timestamp: time.Unix(0, 0),
				Key:       []byte{},
				Value:     []byte{},
				Headers:   map[string][]byte{},
			},
		},
		{
			name: "message with large offset",
			message: Message{
				Offset:    int64(^uint32(0)), // max uint32
				Timestamp: time.Unix(0, 1000000000),
				Key:       []byte("large-offset-key"),
				Value:     []byte("large-offset-value"),
				Headers:   map[string][]byte{},
			},
		},
		{
			name: "message with binary data",
			message: Message{
				Offset:    100,
				Timestamp: time.Unix(0, 5555555555),
				Key:       []byte{0x00, 0x01, 0x02, 0xFF, 0xFE},
				Value:     []byte{0xDE, 0xAD, 0xBE, 0xEF},
				Headers: map[string][]byte{
					"binary-header": {0x01, 0x02, 0x03},
				},
			},
		},
		{
			name: "message with multiple headers",
			message: Message{
				Offset:    999,
				Timestamp: time.Unix(0, 1111111111),
				Key:       []byte("multi-header-key"),
				Value:     []byte("multi-header-value"),
				Headers: map[string][]byte{
					"alpha":   []byte("first"),
					"beta":    []byte("second"),
					"gamma":   []byte("third"),
					"delta":   []byte("fourth"),
					"epsilon": []byte("fifth"),
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Serialize
			serialized, err := SerializeMessage(tt.message)
			if err != nil {
				t.Fatalf("SerializeMessage() error = %v", err)
			}

			// Deserialize
			reader := bytes.NewReader(serialized)
			deserialized, bytesRead, err := DeserializeMessage(reader)
			if err != nil {
				t.Fatalf("DeserializeMessage() error = %v", err)
			}

			// Verify bytes read matches serialized length
			if bytesRead != len(serialized) {
				t.Errorf("bytesRead = %d, want %d", bytesRead, len(serialized))
			}

			// Verify offset
			if deserialized.Offset != tt.message.Offset {
				t.Errorf("Offset = %d, want %d", deserialized.Offset, tt.message.Offset)
			}

			// Verify timestamp
			if !deserialized.Timestamp.Equal(tt.message.Timestamp) {
				t.Errorf("Timestamp = %v, want %v", deserialized.Timestamp, tt.message.Timestamp)
			}

			// Verify key
			if !bytes.Equal(deserialized.Key, tt.message.Key) {
				t.Errorf("Key = %v, want %v", deserialized.Key, tt.message.Key)
			}

			// Verify value
			if !bytes.Equal(deserialized.Value, tt.message.Value) {
				t.Errorf("Value = %v, want %v", deserialized.Value, tt.message.Value)
			}

			// Verify headers count
			if len(deserialized.Headers) != len(tt.message.Headers) {
				t.Errorf("Headers count = %d, want %d", len(deserialized.Headers), len(tt.message.Headers))
			}

			// Verify each header
			for k, v := range tt.message.Headers {
				if got, ok := deserialized.Headers[k]; !ok {
					t.Errorf("Header %q not found", k)
				} else if !bytes.Equal(got, v) {
					t.Errorf("Header[%q] = %v, want %v", k, got, v)
				}
			}
		})
	}
}

func TestDeserializeMessage_InvalidCRC(t *testing.T) {
	msg := Message{
		Offset:    1,
		Timestamp: time.Unix(0, 1234567890),
		Key:       []byte("key"),
		Value:     []byte("value"),
		Headers:   map[string][]byte{},
	}

	serialized, err := SerializeMessage(msg)
	if err != nil {
		t.Fatalf("SerializeMessage() error = %v", err)
	}

	// Corrupt the data (modify a byte after the CRC field)
	if len(serialized) > 10 {
		serialized[10] ^= 0xFF // Flip bits in the payload
	}

	reader := bytes.NewReader(serialized)
	_, _, err = DeserializeMessage(reader)
	if err == nil {
		t.Error("DeserializeMessage() expected CRC mismatch error, got nil")
	}
}

func TestDeserializeMessage_EmptyReader(t *testing.T) {
	reader := bytes.NewReader([]byte{})
	_, _, err := DeserializeMessage(reader)
	if err == nil {
		t.Error("DeserializeMessage() expected error for empty reader, got nil")
	}
}

func TestDeserializeMessage_TruncatedData(t *testing.T) {
	msg := Message{
		Offset:    1,
		Timestamp: time.Unix(0, 1234567890),
		Key:       []byte("key"),
		Value:     []byte("value"),
		Headers:   map[string][]byte{},
	}

	serialized, err := SerializeMessage(msg)
	if err != nil {
		t.Fatalf("SerializeMessage() error = %v", err)
	}

	// Truncate the data
	truncated := serialized[:len(serialized)/2]
	reader := bytes.NewReader(truncated)
	_, _, err = DeserializeMessage(reader)
	if err == nil {
		t.Error("DeserializeMessage() expected error for truncated data, got nil")
	}
}

func TestSerializeDeserializeMessage_MultipleMessages(t *testing.T) {
	messages := []Message{
		{
			Offset:    1,
			Timestamp: time.Unix(0, 1000000000),
			Key:       []byte("key1"),
			Value:     []byte("value1"),
			Headers:   map[string][]byte{},
		},
		{
			Offset:    2,
			Timestamp: time.Unix(0, 2000000000),
			Key:       []byte("key2"),
			Value:     []byte("value2"),
			Headers:   map[string][]byte{"h": []byte("v")},
		},
		{
			Offset:    3,
			Timestamp: time.Unix(0, 3000000000),
			Key:       []byte("key3"),
			Value:     []byte("value3"),
			Headers:   map[string][]byte{},
		},
	}

	// Serialize all messages into one buffer
	var buf bytes.Buffer
	for _, msg := range messages {
		serialized, err := SerializeMessage(msg)
		if err != nil {
			t.Fatalf("SerializeMessage() error = %v", err)
		}
		buf.Write(serialized)
	}

	// Deserialize all messages from the buffer
	reader := bytes.NewReader(buf.Bytes())
	for i, expected := range messages {
		deserialized, _, err := DeserializeMessage(reader)
		if err != nil {
			t.Fatalf("DeserializeMessage() message %d error = %v", i, err)
		}

		if deserialized.Offset != expected.Offset {
			t.Errorf("Message %d: Offset = %d, want %d", i, deserialized.Offset, expected.Offset)
		}
		if !bytes.Equal(deserialized.Key, expected.Key) {
			t.Errorf("Message %d: Key = %v, want %v", i, deserialized.Key, expected.Key)
		}
		if !bytes.Equal(deserialized.Value, expected.Value) {
			t.Errorf("Message %d: Value = %v, want %v", i, deserialized.Value, expected.Value)
		}
	}
}

func BenchmarkSerializeMessage(b *testing.B) {
	msg := Message{
		Offset:    12345,
		Timestamp: time.Now(),
		Key:       []byte("benchmark-key"),
		Value:     []byte("benchmark-value-with-some-content"),
		Headers: map[string][]byte{
			"header1": []byte("value1"),
			"header2": []byte("value2"),
		},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = SerializeMessage(msg)
	}
}

func BenchmarkDeserializeMessage(b *testing.B) {
	msg := Message{
		Offset:    12345,
		Timestamp: time.Now(),
		Key:       []byte("benchmark-key"),
		Value:     []byte("benchmark-value-with-some-content"),
		Headers: map[string][]byte{
			"header1": []byte("value1"),
			"header2": []byte("value2"),
		},
	}

	serialized, _ := SerializeMessage(msg)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		reader := bytes.NewReader(serialized)
		_, _, _ = DeserializeMessage(reader)
	}
}
