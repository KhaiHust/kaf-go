package commitlog

import (
	"bytes"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/KhaiHust/kaf-go/config"
)

// createTestCommitLogConfig creates a test configuration for commit log tests
func createTestCommitLogConfig() *config.CommitLogConfig {
	return &config.CommitLogConfig{
		SegmentMaxBytes: 1024 * 1024, // 1MB
		IndexInterval:   100,         // Index every 100 bytes
		RetentionBytes:  10 * 1024 * 1024,
		RetentionTime:   24 * time.Hour,
	}
}

// createSmallSegmentConfig creates a configuration with small segment size for testing segment rollover
func createSmallSegmentConfig() *config.CommitLogConfig {
	return &config.CommitLogConfig{
		SegmentMaxBytes: 500, // Very small segment for testing rollover
		IndexInterval:   50,
		RetentionBytes:  10 * 1024 * 1024,
		RetentionTime:   24 * time.Hour,
	}
}

func TestNewCommitLog(t *testing.T) {
	dir := t.TempDir()
	cfg := createTestCommitLogConfig()

	commitLog, err := NewCommitLog(dir, cfg)
	if err != nil {
		t.Fatalf("NewCommitLog() error = %v", err)
	}
	defer commitLog.Close()

	// Verify that a new commit log starts with expected offsets
	if commitLog.OldestOffset() != 0 {
		t.Errorf("OldestOffset() = %d, want 0", commitLog.OldestOffset())
	}

	if commitLog.NewestOffset() != -1 {
		t.Errorf("NewestOffset() = %d, want -1", commitLog.NewestOffset())
	}
}

func TestNewCommitLog_CreatesDirectory(t *testing.T) {
	dir := t.TempDir() + "/nested/dir"
	cfg := createTestCommitLogConfig()

	commitLog, err := NewCommitLog(dir, cfg)
	if err != nil {
		t.Fatalf("NewCommitLog() error = %v", err)
	}
	defer commitLog.Close()

	// The commit log should have created the directory
	if commitLog.OldestOffset() != 0 {
		t.Errorf("OldestOffset() = %d, want 0", commitLog.OldestOffset())
	}
}

func TestCommitLog_AppendSingleMessage(t *testing.T) {
	dir := t.TempDir()
	cfg := createTestCommitLogConfig()

	commitLog, err := NewCommitLog(dir, cfg)
	if err != nil {
		t.Fatalf("NewCommitLog() error = %v", err)
	}
	defer commitLog.Close()

	messages := []Message{
		{
			Key:     []byte("key1"),
			Value:   []byte("value1"),
			Headers: map[string][]byte{},
		},
	}

	startOffset, err := commitLog.Append(messages)
	if err != nil {
		t.Fatalf("Append() error = %v", err)
	}

	if startOffset != 0 {
		t.Errorf("Append() startOffset = %d, want 0", startOffset)
	}

	if commitLog.NewestOffset() != 0 {
		t.Errorf("NewestOffset() = %d, want 0", commitLog.NewestOffset())
	}
}

func TestCommitLog_AppendMultipleMessages(t *testing.T) {
	dir := t.TempDir()
	cfg := createTestCommitLogConfig()

	commitLog, err := NewCommitLog(dir, cfg)
	if err != nil {
		t.Fatalf("NewCommitLog() error = %v", err)
	}
	defer commitLog.Close()

	messages := []Message{
		{
			Key:     []byte("key1"),
			Value:   []byte("value1"),
			Headers: map[string][]byte{},
		},
		{
			Key:     []byte("key2"),
			Value:   []byte("value2"),
			Headers: map[string][]byte{"header": []byte("value")},
		},
		{
			Key:     []byte("key3"),
			Value:   []byte("value3"),
			Headers: map[string][]byte{},
		},
	}

	startOffset, err := commitLog.Append(messages)
	if err != nil {
		t.Fatalf("Append() error = %v", err)
	}

	if startOffset != 0 {
		t.Errorf("Append() startOffset = %d, want 0", startOffset)
	}

	if commitLog.NewestOffset() != 2 {
		t.Errorf("NewestOffset() = %d, want 2", commitLog.NewestOffset())
	}
}

func TestCommitLog_AppendMultipleBatches(t *testing.T) {
	dir := t.TempDir()
	cfg := createTestCommitLogConfig()

	commitLog, err := NewCommitLog(dir, cfg)
	if err != nil {
		t.Fatalf("NewCommitLog() error = %v", err)
	}
	defer commitLog.Close()

	// First batch
	messages1 := []Message{
		{Key: []byte("key1"), Value: []byte("value1"), Headers: map[string][]byte{}},
		{Key: []byte("key2"), Value: []byte("value2"), Headers: map[string][]byte{}},
	}

	startOffset1, err := commitLog.Append(messages1)
	if err != nil {
		t.Fatalf("Append() batch 1 error = %v", err)
	}

	if startOffset1 != 0 {
		t.Errorf("Append() batch 1 startOffset = %d, want 0", startOffset1)
	}

	// Second batch
	messages2 := []Message{
		{Key: []byte("key3"), Value: []byte("value3"), Headers: map[string][]byte{}},
		{Key: []byte("key4"), Value: []byte("value4"), Headers: map[string][]byte{}},
	}

	startOffset2, err := commitLog.Append(messages2)
	if err != nil {
		t.Fatalf("Append() batch 2 error = %v", err)
	}

	if startOffset2 != 2 {
		t.Errorf("Append() batch 2 startOffset = %d, want 2", startOffset2)
	}

	if commitLog.NewestOffset() != 3 {
		t.Errorf("NewestOffset() = %d, want 3", commitLog.NewestOffset())
	}
}

func TestCommitLog_AppendWithTimestamp(t *testing.T) {
	dir := t.TempDir()
	cfg := createTestCommitLogConfig()

	commitLog, err := NewCommitLog(dir, cfg)
	if err != nil {
		t.Fatalf("NewCommitLog() error = %v", err)
	}
	defer commitLog.Close()

	now := time.Now()
	messages := []Message{
		{
			Key:       []byte("key1"),
			Value:     []byte("value1"),
			Timestamp: now,
			Headers:   map[string][]byte{},
		},
	}

	_, err = commitLog.Append(messages)
	if err != nil {
		t.Fatalf("Append() error = %v", err)
	}

	// Read the message back and verify timestamp was set
	readMessages, err := commitLog.Read(0, 1024)
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}

	if len(readMessages) != 1 {
		t.Fatalf("Read() returned %d messages, want 1", len(readMessages))
	}

	// Note: The timestamp might be slightly different due to serialization
	if readMessages[0].Timestamp.IsZero() {
		t.Errorf("Read() message timestamp is zero, expected non-zero")
	}
}

func TestCommitLog_AppendClosedLog(t *testing.T) {
	dir := t.TempDir()
	cfg := createTestCommitLogConfig()

	commitLog, err := NewCommitLog(dir, cfg)
	if err != nil {
		t.Fatalf("NewCommitLog() error = %v", err)
	}

	if err := commitLog.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	messages := []Message{
		{Key: []byte("key1"), Value: []byte("value1"), Headers: map[string][]byte{}},
	}

	_, err = commitLog.Append(messages)
	if err == nil {
		t.Error("Append() on closed log should return error")
	}
}

func TestCommitLog_ReadFromBeginning(t *testing.T) {
	dir := t.TempDir()
	cfg := createTestCommitLogConfig()

	commitLog, err := NewCommitLog(dir, cfg)
	if err != nil {
		t.Fatalf("NewCommitLog() error = %v", err)
	}
	defer commitLog.Close()

	messages := []Message{
		{Key: []byte("key1"), Value: []byte("value1"), Headers: map[string][]byte{}},
		{Key: []byte("key2"), Value: []byte("value2"), Headers: map[string][]byte{}},
		{Key: []byte("key3"), Value: []byte("value3"), Headers: map[string][]byte{}},
	}

	_, err = commitLog.Append(messages)
	if err != nil {
		t.Fatalf("Append() error = %v", err)
	}

	readMessages, err := commitLog.Read(0, 4096)
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}

	if len(readMessages) != 3 {
		t.Fatalf("Read() returned %d messages, want 3", len(readMessages))
	}

	// Verify all message offsets are correct
	for i, msg := range readMessages {
		if msg.Offset != int64(i) {
			t.Errorf("Message %d offset = %d, want %d", i, msg.Offset, i)
		}
	}

	// Verify message content
	if !bytes.Equal(readMessages[0].Key, []byte("key1")) {
		t.Errorf("Message 0 key = %s, want key1", readMessages[0].Key)
	}
	if !bytes.Equal(readMessages[1].Key, []byte("key2")) {
		t.Errorf("Message 1 key = %s, want key2", readMessages[1].Key)
	}
	if !bytes.Equal(readMessages[2].Key, []byte("key3")) {
		t.Errorf("Message 2 key = %s, want key3", readMessages[2].Key)
	}
}

func TestCommitLog_ReadFromMiddle(t *testing.T) {
	dir := t.TempDir()
	cfg := createTestCommitLogConfig()

	commitLog, err := NewCommitLog(dir, cfg)
	if err != nil {
		t.Fatalf("NewCommitLog() error = %v", err)
	}
	defer commitLog.Close()

	messages := []Message{
		{Key: []byte("key1"), Value: []byte("value1"), Headers: map[string][]byte{}},
		{Key: []byte("key2"), Value: []byte("value2"), Headers: map[string][]byte{}},
		{Key: []byte("key3"), Value: []byte("value3"), Headers: map[string][]byte{}},
		{Key: []byte("key4"), Value: []byte("value4"), Headers: map[string][]byte{}},
	}

	_, err = commitLog.Append(messages)
	if err != nil {
		t.Fatalf("Append() error = %v", err)
	}

	// Read starting from offset 2
	readMessages, err := commitLog.Read(2, 4096)
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}

	if len(readMessages) != 2 {
		t.Fatalf("Read() returned %d messages, want 2", len(readMessages))
	}

	// Verify offsets
	if readMessages[0].Offset != 2 {
		t.Errorf("First message offset = %d, want 2", readMessages[0].Offset)
	}
	if readMessages[1].Offset != 3 {
		t.Errorf("Second message offset = %d, want 3", readMessages[1].Offset)
	}

	// Verify content
	if !bytes.Equal(readMessages[0].Key, []byte("key3")) {
		t.Errorf("First message key = %s, want key3", readMessages[0].Key)
	}
	if !bytes.Equal(readMessages[1].Key, []byte("key4")) {
		t.Errorf("Second message key = %s, want key4", readMessages[1].Key)
	}
}

func TestCommitLog_ReadWithMaxBytes(t *testing.T) {
	dir := t.TempDir()
	cfg := createTestCommitLogConfig()

	commitLog, err := NewCommitLog(dir, cfg)
	if err != nil {
		t.Fatalf("NewCommitLog() error = %v", err)
	}
	defer commitLog.Close()

	// Create messages with known sizes
	messages := []Message{
		{Key: []byte("key1"), Value: []byte("value1"), Headers: map[string][]byte{}},
		{Key: []byte("key2"), Value: []byte("value2"), Headers: map[string][]byte{}},
		{Key: []byte("key3"), Value: []byte("value3"), Headers: map[string][]byte{}},
	}

	_, err = commitLog.Append(messages)
	if err != nil {
		t.Fatalf("Append() error = %v", err)
	}

	// Read with a very small maxBytes - should get at least one message
	readMessages, err := commitLog.Read(0, 100)
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}

	if len(readMessages) < 1 {
		t.Errorf("Read() should return at least 1 message")
	}
}

func TestCommitLog_ReadClosedLog(t *testing.T) {
	dir := t.TempDir()
	cfg := createTestCommitLogConfig()

	commitLog, err := NewCommitLog(dir, cfg)
	if err != nil {
		t.Fatalf("NewCommitLog() error = %v", err)
	}

	messages := []Message{
		{Key: []byte("key1"), Value: []byte("value1"), Headers: map[string][]byte{}},
	}

	_, err = commitLog.Append(messages)
	if err != nil {
		t.Fatalf("Append() error = %v", err)
	}

	if err := commitLog.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	_, err = commitLog.Read(0, 4096)
	if err == nil {
		t.Error("Read() on closed log should return error")
	}
}

func TestCommitLog_ReadEmptyLog(t *testing.T) {
	dir := t.TempDir()
	cfg := createTestCommitLogConfig()

	commitLog, err := NewCommitLog(dir, cfg)
	if err != nil {
		t.Fatalf("NewCommitLog() error = %v", err)
	}
	defer commitLog.Close()

	readMessages, err := commitLog.Read(0, 4096)
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}

	if len(readMessages) != 0 {
		t.Errorf("Read() on empty log returned %d messages, want 0", len(readMessages))
	}
}

func TestCommitLog_OldestOffset(t *testing.T) {
	dir := t.TempDir()
	cfg := createTestCommitLogConfig()

	commitLog, err := NewCommitLog(dir, cfg)
	if err != nil {
		t.Fatalf("NewCommitLog() error = %v", err)
	}
	defer commitLog.Close()

	// Empty log should return 0
	if commitLog.OldestOffset() != 0 {
		t.Errorf("OldestOffset() on new log = %d, want 0", commitLog.OldestOffset())
	}

	// After appending messages, oldest offset should still be 0
	messages := []Message{
		{Key: []byte("key1"), Value: []byte("value1"), Headers: map[string][]byte{}},
	}

	_, err = commitLog.Append(messages)
	if err != nil {
		t.Fatalf("Append() error = %v", err)
	}

	if commitLog.OldestOffset() != 0 {
		t.Errorf("OldestOffset() after append = %d, want 0", commitLog.OldestOffset())
	}
}

func TestCommitLog_NewestOffset(t *testing.T) {
	dir := t.TempDir()
	cfg := createTestCommitLogConfig()

	commitLog, err := NewCommitLog(dir, cfg)
	if err != nil {
		t.Fatalf("NewCommitLog() error = %v", err)
	}
	defer commitLog.Close()

	// Empty log should return -1
	if commitLog.NewestOffset() != -1 {
		t.Errorf("NewestOffset() on new log = %d, want -1", commitLog.NewestOffset())
	}

	// After appending one message
	messages := []Message{
		{Key: []byte("key1"), Value: []byte("value1"), Headers: map[string][]byte{}},
	}

	_, err = commitLog.Append(messages)
	if err != nil {
		t.Fatalf("Append() error = %v", err)
	}

	if commitLog.NewestOffset() != 0 {
		t.Errorf("NewestOffset() after 1 message = %d, want 0", commitLog.NewestOffset())
	}

	// After appending two more messages
	messages2 := []Message{
		{Key: []byte("key2"), Value: []byte("value2"), Headers: map[string][]byte{}},
		{Key: []byte("key3"), Value: []byte("value3"), Headers: map[string][]byte{}},
	}

	_, err = commitLog.Append(messages2)
	if err != nil {
		t.Fatalf("Append() error = %v", err)
	}

	if commitLog.NewestOffset() != 2 {
		t.Errorf("NewestOffset() after 3 messages = %d, want 2", commitLog.NewestOffset())
	}
}

func TestCommitLog_Close(t *testing.T) {
	dir := t.TempDir()
	cfg := createTestCommitLogConfig()

	commitLog, err := NewCommitLog(dir, cfg)
	if err != nil {
		t.Fatalf("NewCommitLog() error = %v", err)
	}

	messages := []Message{
		{Key: []byte("key1"), Value: []byte("value1"), Headers: map[string][]byte{}},
	}

	_, err = commitLog.Append(messages)
	if err != nil {
		t.Fatalf("Append() error = %v", err)
	}

	// First close should succeed
	if err := commitLog.Close(); err != nil {
		t.Errorf("Close() error = %v", err)
	}

	// Second close should be a no-op and not return error
	if err := commitLog.Close(); err != nil {
		t.Errorf("Close() second call error = %v", err)
	}
}

func TestCommitLog_Persistence(t *testing.T) {
	dir := t.TempDir()
	cfg := createTestCommitLogConfig()

	// Create commit log and write messages
	commitLog1, err := NewCommitLog(dir, cfg)
	if err != nil {
		t.Fatalf("NewCommitLog() error = %v", err)
	}

	messages := []Message{
		{Key: []byte("key1"), Value: []byte("value1"), Headers: map[string][]byte{}},
		{Key: []byte("key2"), Value: []byte("value2"), Headers: map[string][]byte{}},
	}

	_, err = commitLog1.Append(messages)
	if err != nil {
		t.Fatalf("Append() error = %v", err)
	}

	if err := commitLog1.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	// Reopen commit log and verify messages are still there
	commitLog2, err := NewCommitLog(dir, cfg)
	if err != nil {
		t.Fatalf("NewCommitLog() reopen error = %v", err)
	}
	defer commitLog2.Close()

	// After reopening, the newest offset should be 1 (2 messages: offset 0 and 1)
	if commitLog2.NewestOffset() != 1 {
		t.Errorf("NewestOffset() after reopen = %d, want 1", commitLog2.NewestOffset())
	}

	readMessages, err := commitLog2.Read(0, 4096)
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}

	if len(readMessages) != 2 {
		t.Fatalf("Read() returned %d messages, want 2", len(readMessages))
	}

	// Verify message content
	if !bytes.Equal(readMessages[0].Key, []byte("key1")) {
		t.Errorf("Message 0 key = %s, want key1", readMessages[0].Key)
	}
	if !bytes.Equal(readMessages[1].Key, []byte("key2")) {
		t.Errorf("Message 1 key = %s, want key2", readMessages[1].Key)
	}
}

func TestCommitLog_PersistenceAndContinueAppending(t *testing.T) {
	dir := t.TempDir()
	cfg := createTestCommitLogConfig()

	// Create commit log and write initial messages
	commitLog1, err := NewCommitLog(dir, cfg)
	if err != nil {
		t.Fatalf("NewCommitLog() error = %v", err)
	}

	messages1 := []Message{
		{Key: []byte("key1"), Value: []byte("value1"), Headers: map[string][]byte{}},
		{Key: []byte("key2"), Value: []byte("value2"), Headers: map[string][]byte{}},
	}

	startOffset1, err := commitLog1.Append(messages1)
	if err != nil {
		t.Fatalf("Append() error = %v", err)
	}

	if startOffset1 != 0 {
		t.Errorf("First append startOffset = %d, want 0", startOffset1)
	}

	if err := commitLog1.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	// Reopen and append more messages
	commitLog2, err := NewCommitLog(dir, cfg)
	if err != nil {
		t.Fatalf("NewCommitLog() reopen error = %v", err)
	}
	defer commitLog2.Close()

	messages2 := []Message{
		{Key: []byte("key3"), Value: []byte("value3"), Headers: map[string][]byte{}},
	}

	// After reopening, the commit log should continue from offset 2
	startOffset2, err := commitLog2.Append(messages2)
	if err != nil {
		t.Fatalf("Append() after reopen error = %v", err)
	}

	if startOffset2 != 2 {
		t.Errorf("Append() startOffset after reopen = %d, want 2", startOffset2)
	}

	if commitLog2.NewestOffset() != 2 {
		t.Errorf("NewestOffset() after reopen and append = %d, want 2", commitLog2.NewestOffset())
	}

	// Read all messages
	readMessages, err := commitLog2.Read(0, 8192)
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}

	if len(readMessages) != 3 {
		t.Fatalf("Read() returned %d messages, want 3", len(readMessages))
	}

	// Verify all message keys
	expectedKeys := []string{"key1", "key2", "key3"}
	for i, msg := range readMessages {
		if !bytes.Equal(msg.Key, []byte(expectedKeys[i])) {
			t.Errorf("Message %d key = %s, want %s", i, msg.Key, expectedKeys[i])
		}
	}
}

func TestCommitLog_SegmentRollover(t *testing.T) {
	dir := t.TempDir()
	cfg := createSmallSegmentConfig() // Small segment size to trigger rollover

	commitLog, err := NewCommitLog(dir, cfg)
	if err != nil {
		t.Fatalf("NewCommitLog() error = %v", err)
	}
	defer commitLog.Close()

	// Write enough messages to fill multiple segments
	totalMessages := 20
	for i := 0; i < totalMessages; i++ {
		messages := []Message{
			{
				Key:     []byte(fmt.Sprintf("key%d", i)),
				Value:   []byte("value-with-some-padding-to-make-it-bigger-and-trigger-segment-rollover"),
				Headers: map[string][]byte{},
			},
		}

		_, err := commitLog.Append(messages)
		if err != nil {
			t.Fatalf("Append() iteration %d error = %v", i, err)
		}
	}

	// Verify the newest offset is correct
	expectedNewestOffset := int64(totalMessages - 1)
	if commitLog.NewestOffset() != expectedNewestOffset {
		t.Errorf("NewestOffset() = %d, want %d", commitLog.NewestOffset(), expectedNewestOffset)
	}

	// Read all messages from the beginning
	readMessages, err := commitLog.Read(0, 100000)
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}

	if len(readMessages) != totalMessages {
		t.Errorf("Read() returned %d messages, want %d", len(readMessages), totalMessages)
	}

	// Verify message offsets are sequential
	for i, msg := range readMessages {
		if msg.Offset != int64(i) {
			t.Errorf("Message %d offset = %d, want %d", i, msg.Offset, i)
		}
	}
}

func TestCommitLog_ConcurrentWrites(t *testing.T) {
	dir := t.TempDir()
	cfg := createTestCommitLogConfig()

	commitLog, err := NewCommitLog(dir, cfg)
	if err != nil {
		t.Fatalf("NewCommitLog() error = %v", err)
	}
	defer commitLog.Close()

	numGoroutines := 10
	messagesPerGoroutine := 10

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func(goroutineID int) {
			defer wg.Done()

			for j := 0; j < messagesPerGoroutine; j++ {
				messages := []Message{
					{
						Key:     []byte("key"),
						Value:   []byte("value"),
						Headers: map[string][]byte{},
					},
				}

				_, err := commitLog.Append(messages)
				if err != nil {
					t.Errorf("Append() goroutine %d iteration %d error = %v", goroutineID, j, err)
				}
			}
		}(i)
	}

	wg.Wait()

	expectedMessages := numGoroutines * messagesPerGoroutine
	if commitLog.NewestOffset() != int64(expectedMessages-1) {
		t.Errorf("NewestOffset() = %d, want %d", commitLog.NewestOffset(), expectedMessages-1)
	}
}

func TestCommitLog_ConcurrentReadsAndWrites(t *testing.T) {
	dir := t.TempDir()
	cfg := createTestCommitLogConfig()

	commitLog, err := NewCommitLog(dir, cfg)
	if err != nil {
		t.Fatalf("NewCommitLog() error = %v", err)
	}
	defer commitLog.Close()

	// Start by adding some messages
	initialMessages := []Message{
		{Key: []byte("initial"), Value: []byte("value"), Headers: map[string][]byte{}},
	}
	_, err = commitLog.Append(initialMessages)
	if err != nil {
		t.Fatalf("Initial Append() error = %v", err)
	}

	var wg sync.WaitGroup
	numWriters := 5
	numReaders := 5
	iterations := 20

	// Start writers
	wg.Add(numWriters)
	for i := 0; i < numWriters; i++ {
		go func(writerID int) {
			defer wg.Done()

			for j := 0; j < iterations; j++ {
				messages := []Message{
					{
						Key:     []byte("key"),
						Value:   []byte("value"),
						Headers: map[string][]byte{},
					},
				}

				_, err := commitLog.Append(messages)
				if err != nil {
					t.Errorf("Append() writer %d iteration %d error = %v", writerID, j, err)
				}
			}
		}(i)
	}

	// Start readers
	wg.Add(numReaders)
	for i := 0; i < numReaders; i++ {
		go func(readerID int) {
			defer wg.Done()

			for j := 0; j < iterations; j++ {
				_, err := commitLog.Read(0, 4096)
				if err != nil {
					t.Errorf("Read() reader %d iteration %d error = %v", readerID, j, err)
				}
			}
		}(i)
	}

	wg.Wait()
}

func TestCommitLog_MessageWithHeaders(t *testing.T) {
	dir := t.TempDir()
	cfg := createTestCommitLogConfig()

	commitLog, err := NewCommitLog(dir, cfg)
	if err != nil {
		t.Fatalf("NewCommitLog() error = %v", err)
	}
	defer commitLog.Close()

	headers := map[string][]byte{
		"header1": []byte("value1"),
		"header2": []byte("value2"),
		"header3": []byte("value3"),
	}

	messages := []Message{
		{
			Key:     []byte("key"),
			Value:   []byte("value"),
			Headers: headers,
		},
	}

	_, err = commitLog.Append(messages)
	if err != nil {
		t.Fatalf("Append() error = %v", err)
	}

	readMessages, err := commitLog.Read(0, 4096)
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}

	if len(readMessages) != 1 {
		t.Fatalf("Read() returned %d messages, want 1", len(readMessages))
	}

	// Verify headers
	if len(readMessages[0].Headers) != 3 {
		t.Errorf("Message has %d headers, want 3", len(readMessages[0].Headers))
	}

	for key, expectedValue := range headers {
		actualValue, ok := readMessages[0].Headers[key]
		if !ok {
			t.Errorf("Header %s not found", key)
			continue
		}
		if !bytes.Equal(actualValue, expectedValue) {
			t.Errorf("Header %s value = %s, want %s", key, actualValue, expectedValue)
		}
	}
}

func TestCommitLog_EmptyMessages(t *testing.T) {
	dir := t.TempDir()
	cfg := createTestCommitLogConfig()

	commitLog, err := NewCommitLog(dir, cfg)
	if err != nil {
		t.Fatalf("NewCommitLog() error = %v", err)
	}
	defer commitLog.Close()

	// Append empty batch
	messages := []Message{}
	_, err = commitLog.Append(messages)
	if err != nil {
		t.Fatalf("Append() empty batch error = %v", err)
	}

	// NewestOffset should still be -1 (no real messages appended)
	if commitLog.NewestOffset() != -1 {
		t.Errorf("NewestOffset() after empty append = %d, want -1", commitLog.NewestOffset())
	}
}

func TestCommitLog_LargeMessage(t *testing.T) {
	dir := t.TempDir()
	cfg := createTestCommitLogConfig()

	commitLog, err := NewCommitLog(dir, cfg)
	if err != nil {
		t.Fatalf("NewCommitLog() error = %v", err)
	}
	defer commitLog.Close()

	// Create a large message (100KB value)
	largeValue := make([]byte, 100*1024)
	for i := range largeValue {
		largeValue[i] = byte(i % 256)
	}

	messages := []Message{
		{
			Key:     []byte("large-key"),
			Value:   largeValue,
			Headers: map[string][]byte{},
		},
	}

	_, err = commitLog.Append(messages)
	if err != nil {
		t.Fatalf("Append() large message error = %v", err)
	}

	// Read it back
	readMessages, err := commitLog.Read(0, 200*1024)
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}

	if len(readMessages) != 1 {
		t.Fatalf("Read() returned %d messages, want 1", len(readMessages))
	}

	if !bytes.Equal(readMessages[0].Value, largeValue) {
		t.Errorf("Large message value mismatch")
	}
}

func TestCommitLog_ReadOutOfRange(t *testing.T) {
	dir := t.TempDir()
	cfg := createTestCommitLogConfig()

	commitLog, err := NewCommitLog(dir, cfg)
	if err != nil {
		t.Fatalf("NewCommitLog() error = %v", err)
	}
	defer commitLog.Close()

	messages := []Message{
		{Key: []byte("key1"), Value: []byte("value1"), Headers: map[string][]byte{}},
	}

	_, err = commitLog.Append(messages)
	if err != nil {
		t.Fatalf("Append() error = %v", err)
	}

	// Read with offset beyond available messages
	readMessages, err := commitLog.Read(100, 4096)
	if err != nil {
		t.Fatalf("Read() out of range error = %v", err)
	}

	if len(readMessages) != 0 {
		t.Errorf("Read() out of range returned %d messages, want 0", len(readMessages))
	}
}

// Benchmark tests

func BenchmarkCommitLog_Append(b *testing.B) {
	dir := b.TempDir()
	cfg := createTestCommitLogConfig()

	commitLog, err := NewCommitLog(dir, cfg)
	if err != nil {
		b.Fatalf("NewCommitLog() error = %v", err)
	}
	defer commitLog.Close()

	messages := []Message{
		{
			Key:     []byte("benchmark-key"),
			Value:   []byte("benchmark-value-with-some-content"),
			Headers: map[string][]byte{"header": []byte("value")},
		},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := commitLog.Append(messages)
		if err != nil {
			b.Fatalf("Append() error = %v", err)
		}
	}
}

func BenchmarkCommitLog_Read(b *testing.B) {
	dir := b.TempDir()
	cfg := createTestCommitLogConfig()

	commitLog, err := NewCommitLog(dir, cfg)
	if err != nil {
		b.Fatalf("NewCommitLog() error = %v", err)
	}
	defer commitLog.Close()

	// Pre-populate with messages
	for i := 0; i < 1000; i++ {
		messages := []Message{
			{
				Key:     []byte("benchmark-key"),
				Value:   []byte("benchmark-value-with-some-content"),
				Headers: map[string][]byte{},
			},
		}
		_, err := commitLog.Append(messages)
		if err != nil {
			b.Fatalf("Append() error = %v", err)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := commitLog.Read(0, 4096)
		if err != nil {
			b.Fatalf("Read() error = %v", err)
		}
	}
}

func BenchmarkCommitLog_AppendBatch(b *testing.B) {
	dir := b.TempDir()
	cfg := createTestCommitLogConfig()

	commitLog, err := NewCommitLog(dir, cfg)
	if err != nil {
		b.Fatalf("NewCommitLog() error = %v", err)
	}
	defer commitLog.Close()

	// Create a batch of 100 messages
	messages := make([]Message, 100)
	for i := range messages {
		messages[i] = Message{
			Key:     []byte("benchmark-key"),
			Value:   []byte("benchmark-value-with-some-content"),
			Headers: map[string][]byte{},
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := commitLog.Append(messages)
		if err != nil {
			b.Fatalf("Append() error = %v", err)
		}
	}
}
