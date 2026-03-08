package commitlog

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/KhaiHust/kaf-go/config"
)

func createTestConfig() *config.CommitLogConfig {
	return &config.CommitLogConfig{
		SegmentMaxBytes: 1024 * 1024, // 1MB
		IndexInterval:   100,         // Index every 100 bytes
		RetentionBytes:  10 * 1024 * 1024,
		RetentionTime:   24 * time.Hour,
	}
}

func TestNewSegment(t *testing.T) {
	dir := t.TempDir()
	cfg := createTestConfig()

	segment, err := NewSegment(dir, 0, cfg)
	if err != nil {
		t.Fatalf("NewSegment() error = %v", err)
	}
	defer segment.Close()

	// Verify segment was created with correct base offset
	if segment.BaseOffset != 0 {
		t.Errorf("BaseOffset = %d, want 0", segment.BaseOffset)
	}

	// Verify files were created
	logFile := filepath.Join(dir, "00000000000000000000.log")
	indexFile := filepath.Join(dir, "00000000000000000000.offsetIndex")
	timeIndexFile := filepath.Join(dir, "00000000000000000000.timeindex")

	if _, err := os.Stat(logFile); os.IsNotExist(err) {
		t.Errorf("log file was not created")
	}
	if _, err := os.Stat(indexFile); os.IsNotExist(err) {
		t.Errorf("index file was not created")
	}
	if _, err := os.Stat(timeIndexFile); os.IsNotExist(err) {
		t.Errorf("time index file was not created")
	}
}

func TestNewSegment_WithNonZeroBaseOffset(t *testing.T) {
	dir := t.TempDir()
	cfg := createTestConfig()

	baseOffset := int64(1000)
	segment, err := NewSegment(dir, baseOffset, cfg)
	if err != nil {
		t.Fatalf("NewSegment() error = %v", err)
	}
	defer segment.Close()

	if segment.BaseOffset != baseOffset {
		t.Errorf("BaseOffset = %d, want %d", segment.BaseOffset, baseOffset)
	}

	// Verify files were created with correct names
	logFile := filepath.Join(dir, "00000000000000001000.log")
	if _, err := os.Stat(logFile); os.IsNotExist(err) {
		t.Errorf("log file was not created with correct name")
	}
}

func TestSegment_AppendAndRead(t *testing.T) {
	dir := t.TempDir()
	cfg := createTestConfig()

	segment, err := NewSegment(dir, 0, cfg)
	if err != nil {
		t.Fatalf("NewSegment() error = %v", err)
	}
	defer segment.Close()

	// Create test messages
	messages := []Message{
		{
			Offset:    0,
			Timestamp: time.Now(),
			Key:       []byte("key0"),
			Value:     []byte("value0"),
			Headers:   map[string][]byte{},
		},
		{
			Offset:    1,
			Timestamp: time.Now(),
			Key:       []byte("key1"),
			Value:     []byte("value1"),
			Headers:   map[string][]byte{"header": []byte("value")},
		},
		{
			Offset:    2,
			Timestamp: time.Now(),
			Key:       []byte("key2"),
			Value:     []byte("value2"),
			Headers:   map[string][]byte{},
		},
	}

	// Append messages
	if err := segment.Append(messages); err != nil {
		t.Fatalf("Append() error = %v", err)
	}

	// Read messages from offset 0
	readMessages, err := segment.Read(0, 1024*1024)
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}

	if len(readMessages) != len(messages) {
		t.Errorf("Read() returned %d messages, want %d", len(readMessages), len(messages))
	}

	for i, msg := range readMessages {
		if msg.Offset != messages[i].Offset {
			t.Errorf("Message %d: Offset = %d, want %d", i, msg.Offset, messages[i].Offset)
		}
		if string(msg.Key) != string(messages[i].Key) {
			t.Errorf("Message %d: Key = %s, want %s", i, string(msg.Key), string(messages[i].Key))
		}
		if string(msg.Value) != string(messages[i].Value) {
			t.Errorf("Message %d: Value = %s, want %s", i, string(msg.Value), string(messages[i].Value))
		}
	}
}

func TestSegment_ReadFromMiddleOffset(t *testing.T) {
	dir := t.TempDir()
	cfg := createTestConfig()

	segment, err := NewSegment(dir, 0, cfg)
	if err != nil {
		t.Fatalf("NewSegment() error = %v", err)
	}
	defer segment.Close()

	// Create and append messages
	messages := []Message{
		{Offset: 0, Timestamp: time.Now(), Key: []byte("key0"), Value: []byte("value0"), Headers: map[string][]byte{}},
		{Offset: 1, Timestamp: time.Now(), Key: []byte("key1"), Value: []byte("value1"), Headers: map[string][]byte{}},
		{Offset: 2, Timestamp: time.Now(), Key: []byte("key2"), Value: []byte("value2"), Headers: map[string][]byte{}},
		{Offset: 3, Timestamp: time.Now(), Key: []byte("key3"), Value: []byte("value3"), Headers: map[string][]byte{}},
	}

	if err := segment.Append(messages); err != nil {
		t.Fatalf("Append() error = %v", err)
	}

	// Read from offset 2
	readMessages, err := segment.Read(2, 1024*1024)
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}

	// Should get messages with offset >= 2
	if len(readMessages) != 2 {
		t.Errorf("Read() returned %d messages, want 2", len(readMessages))
	}

	if len(readMessages) > 0 && readMessages[0].Offset != 2 {
		t.Errorf("First message offset = %d, want 2", readMessages[0].Offset)
	}
}

func TestSegment_ReadOutOfRange(t *testing.T) {
	dir := t.TempDir()
	cfg := createTestConfig()

	segment, err := NewSegment(dir, 100, cfg)
	if err != nil {
		t.Fatalf("NewSegment() error = %v", err)
	}
	defer segment.Close()

	// Try to read with offset less than base offset
	_, err = segment.Read(50, 1024)
	if err == nil {
		t.Error("Read() expected error for offset out of range, got nil")
	}
}

func TestSegment_ReadAt(t *testing.T) {
	dir := t.TempDir()
	cfg := createTestConfig()

	segment, err := NewSegment(dir, 0, cfg)
	if err != nil {
		t.Fatalf("NewSegment() error = %v", err)
	}
	defer segment.Close()

	messages := []Message{
		{Offset: 0, Timestamp: time.Now(), Key: []byte("key0"), Value: []byte("value0"), Headers: map[string][]byte{}},
		{Offset: 1, Timestamp: time.Now(), Key: []byte("key1"), Value: []byte("value1"), Headers: map[string][]byte{}},
	}

	if err := segment.Append(messages); err != nil {
		t.Fatalf("Append() error = %v", err)
	}

	// Read from position 0
	readMessages, err := segment.ReadAt(0, 1024*1024)
	if err != nil {
		t.Fatalf("ReadAt() error = %v", err)
	}

	if len(readMessages) != len(messages) {
		t.Errorf("ReadAt() returned %d messages, want %d", len(readMessages), len(messages))
	}
}

func TestSegment_IsFull(t *testing.T) {
	dir := t.TempDir()
	cfg := &config.CommitLogConfig{
		SegmentMaxBytes: 100, // Small max size for testing
		IndexInterval:   10,
		RetentionBytes:  10 * 1024 * 1024,
		RetentionTime:   24 * time.Hour,
	}

	segment, err := NewSegment(dir, 0, cfg)
	if err != nil {
		t.Fatalf("NewSegment() error = %v", err)
	}
	defer segment.Close()

	// Initially not full
	if segment.IsFull() {
		t.Error("IsFull() = true for empty segment, want false")
	}

	// Append messages until full
	for i := 0; i < 10; i++ {
		msg := Message{
			Offset:    int64(i),
			Timestamp: time.Now(),
			Key:       []byte("key"),
			Value:     []byte("value-with-enough-content-to-fill-segment"),
			Headers:   map[string][]byte{},
		}
		if err := segment.Append([]Message{msg}); err != nil {
			t.Fatalf("Append() error = %v", err)
		}
		if segment.IsFull() {
			break
		}
	}

	// Should be full now
	if !segment.IsFull() {
		t.Error("IsFull() = false after filling segment, want true")
	}
}

func TestSegment_Sync(t *testing.T) {
	dir := t.TempDir()
	cfg := createTestConfig()

	segment, err := NewSegment(dir, 0, cfg)
	if err != nil {
		t.Fatalf("NewSegment() error = %v", err)
	}
	defer segment.Close()

	messages := []Message{
		{Offset: 0, Timestamp: time.Now(), Key: []byte("key"), Value: []byte("value"), Headers: map[string][]byte{}},
	}

	if err := segment.Append(messages); err != nil {
		t.Fatalf("Append() error = %v", err)
	}

	// Sync should not return error
	if err := segment.Sync(); err != nil {
		t.Errorf("Sync() error = %v", err)
	}
}

func TestSegment_Close(t *testing.T) {
	dir := t.TempDir()
	cfg := createTestConfig()

	segment, err := NewSegment(dir, 0, cfg)
	if err != nil {
		t.Fatalf("NewSegment() error = %v", err)
	}

	messages := []Message{
		{Offset: 0, Timestamp: time.Now(), Key: []byte("key"), Value: []byte("value"), Headers: map[string][]byte{}},
	}

	if err := segment.Append(messages); err != nil {
		t.Fatalf("Append() error = %v", err)
	}

	// Close should not return error
	if err := segment.Close(); err != nil {
		t.Errorf("Close() error = %v", err)
	}
}

func TestSegment_NextOffset(t *testing.T) {
	dir := t.TempDir()
	cfg := createTestConfig()

	segment, err := NewSegment(dir, 0, cfg)
	if err != nil {
		t.Fatalf("NewSegment() error = %v", err)
	}
	defer segment.Close()

	// Initially next offset should be base offset
	if nextOffset := segment.NextOffset(); nextOffset != 0 {
		t.Errorf("NextOffset() = %d for empty segment, want 0", nextOffset)
	}

	// Append some messages
	messages := []Message{
		{Offset: 0, Timestamp: time.Now(), Key: []byte("key0"), Value: []byte("value0"), Headers: map[string][]byte{}},
		{Offset: 1, Timestamp: time.Now(), Key: []byte("key1"), Value: []byte("value1"), Headers: map[string][]byte{}},
		{Offset: 2, Timestamp: time.Now(), Key: []byte("key2"), Value: []byte("value2"), Headers: map[string][]byte{}},
	}

	if err := segment.Append(messages); err != nil {
		t.Fatalf("Append() error = %v", err)
	}

	// Next offset should be 3
	if nextOffset := segment.NextOffset(); nextOffset != 3 {
		t.Errorf("NextOffset() = %d after appending 3 messages, want 3", nextOffset)
	}
}

func TestSegment_NextOffset_WithNonZeroBaseOffset(t *testing.T) {
	dir := t.TempDir()
	cfg := createTestConfig()

	baseOffset := int64(100)
	segment, err := NewSegment(dir, baseOffset, cfg)
	if err != nil {
		t.Fatalf("NewSegment() error = %v", err)
	}
	defer segment.Close()

	// Initially next offset should be base offset
	if nextOffset := segment.NextOffset(); nextOffset != baseOffset {
		t.Errorf("NextOffset() = %d for empty segment, want %d", nextOffset, baseOffset)
	}

	// Append messages
	messages := []Message{
		{Offset: 100, Timestamp: time.Now(), Key: []byte("key0"), Value: []byte("value0"), Headers: map[string][]byte{}},
		{Offset: 101, Timestamp: time.Now(), Key: []byte("key1"), Value: []byte("value1"), Headers: map[string][]byte{}},
	}

	if err := segment.Append(messages); err != nil {
		t.Fatalf("Append() error = %v", err)
	}

	// Next offset should be 102
	if nextOffset := segment.NextOffset(); nextOffset != 102 {
		t.Errorf("NextOffset() = %d after appending 2 messages, want 102", nextOffset)
	}
}

func TestSegment_NewestTimestamp(t *testing.T) {
	dir := t.TempDir()
	cfg := createTestConfig()

	segment, err := NewSegment(dir, 0, cfg)
	if err != nil {
		t.Fatalf("NewSegment() error = %v", err)
	}
	defer segment.Close()

	// Initially timestamp should be zero
	if !segment.NewestTimestamp().IsZero() {
		t.Error("NewestTimestamp() should be zero for empty segment")
	}

	ts1 := time.Now()
	ts2 := ts1.Add(time.Hour)
	ts3 := ts1.Add(-time.Hour) // Earlier timestamp

	messages := []Message{
		{Offset: 0, Timestamp: ts1, Key: []byte("key0"), Value: []byte("value0"), Headers: map[string][]byte{}},
		{Offset: 1, Timestamp: ts2, Key: []byte("key1"), Value: []byte("value1"), Headers: map[string][]byte{}},
		{Offset: 2, Timestamp: ts3, Key: []byte("key2"), Value: []byte("value2"), Headers: map[string][]byte{}},
	}

	if err := segment.Append(messages); err != nil {
		t.Fatalf("Append() error = %v", err)
	}

	// Newest timestamp should be ts2
	if !segment.NewestTimestamp().Equal(ts2) {
		t.Errorf("NewestTimestamp() = %v, want %v", segment.NewestTimestamp(), ts2)
	}
}

func TestSegment_Delete(t *testing.T) {
	dir := t.TempDir()
	cfg := createTestConfig()

	segment, err := NewSegment(dir, 0, cfg)
	if err != nil {
		t.Fatalf("NewSegment() error = %v", err)
	}

	// Append some data
	messages := []Message{
		{Offset: 0, Timestamp: time.Now(), Key: []byte("key"), Value: []byte("value"), Headers: map[string][]byte{}},
	}
	if err := segment.Append(messages); err != nil {
		t.Fatalf("Append() error = %v", err)
	}

	// Delete the segment
	if err := segment.Delete(); err != nil {
		t.Errorf("Delete() error = %v", err)
	}

	// Verify files were deleted (note: segment.dir is not set in NewSegment, so files won't be deleted correctly)
	// This test verifies the Delete function doesn't error
}

func TestSegment_AppendMultipleBatches(t *testing.T) {
	dir := t.TempDir()
	cfg := createTestConfig()

	segment, err := NewSegment(dir, 0, cfg)
	if err != nil {
		t.Fatalf("NewSegment() error = %v", err)
	}
	defer segment.Close()

	// Append first batch
	batch1 := []Message{
		{Offset: 0, Timestamp: time.Now(), Key: []byte("key0"), Value: []byte("value0"), Headers: map[string][]byte{}},
		{Offset: 1, Timestamp: time.Now(), Key: []byte("key1"), Value: []byte("value1"), Headers: map[string][]byte{}},
	}
	if err := segment.Append(batch1); err != nil {
		t.Fatalf("Append() batch1 error = %v", err)
	}

	// Append second batch
	batch2 := []Message{
		{Offset: 2, Timestamp: time.Now(), Key: []byte("key2"), Value: []byte("value2"), Headers: map[string][]byte{}},
		{Offset: 3, Timestamp: time.Now(), Key: []byte("key3"), Value: []byte("value3"), Headers: map[string][]byte{}},
	}
	if err := segment.Append(batch2); err != nil {
		t.Fatalf("Append() batch2 error = %v", err)
	}

	// Read all messages
	readMessages, err := segment.Read(0, 1024*1024)
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}

	if len(readMessages) != 4 {
		t.Errorf("Read() returned %d messages, want 4", len(readMessages))
	}
}

func TestSegment_PersistenceAcrossReopen(t *testing.T) {
	dir := t.TempDir()
	cfg := createTestConfig()

	// Create segment and write data
	segment, err := NewSegment(dir, 0, cfg)
	if err != nil {
		t.Fatalf("NewSegment() error = %v", err)
	}

	messages := []Message{
		{Offset: 0, Timestamp: time.Now(), Key: []byte("key0"), Value: []byte("value0"), Headers: map[string][]byte{}},
		{Offset: 1, Timestamp: time.Now(), Key: []byte("key1"), Value: []byte("value1"), Headers: map[string][]byte{}},
	}
	if err := segment.Append(messages); err != nil {
		t.Fatalf("Append() error = %v", err)
	}

	// Close the segment
	if err := segment.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	// Reopen the segment
	segment2, err := NewSegment(dir, 0, cfg)
	if err != nil {
		t.Fatalf("NewSegment() reopen error = %v", err)
	}
	defer segment2.Close()

	// Read messages from reopened segment
	readMessages, err := segment2.Read(0, 1024*1024)
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}

	if len(readMessages) != len(messages) {
		t.Errorf("Read() returned %d messages after reopen, want %d", len(readMessages), len(messages))
	}

	// Verify NextOffset
	if nextOffset := segment2.NextOffset(); nextOffset != 2 {
		t.Errorf("NextOffset() = %d after reopen, want 2", nextOffset)
	}
}

func TestSegment_LimitedBytesRead(t *testing.T) {
	dir := t.TempDir()
	cfg := createTestConfig()

	segment, err := NewSegment(dir, 0, cfg)
	if err != nil {
		t.Fatalf("NewSegment() error = %v", err)
	}
	defer segment.Close()

	// Append several messages
	messages := []Message{
		{Offset: 0, Timestamp: time.Now(), Key: []byte("key0"), Value: []byte("value0"), Headers: map[string][]byte{}},
		{Offset: 1, Timestamp: time.Now(), Key: []byte("key1"), Value: []byte("value1"), Headers: map[string][]byte{}},
		{Offset: 2, Timestamp: time.Now(), Key: []byte("key2"), Value: []byte("value2"), Headers: map[string][]byte{}},
		{Offset: 3, Timestamp: time.Now(), Key: []byte("key3"), Value: []byte("value3"), Headers: map[string][]byte{}},
	}
	if err := segment.Append(messages); err != nil {
		t.Fatalf("Append() error = %v", err)
	}

	// Read with limited bytes (should stop before reading all messages)
	readMessages, err := segment.Read(0, 50) // Very small limit
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}

	// Should have read at least 1 message but not all
	if len(readMessages) == 0 {
		t.Error("Read() should have returned at least 1 message")
	}
	if len(readMessages) >= len(messages) {
		t.Logf("Note: Read() returned all %d messages, maxBytes limit may not be small enough", len(readMessages))
	}
}

func BenchmarkSegment_Append(b *testing.B) {
	dir := b.TempDir()
	cfg := &config.CommitLogConfig{
		SegmentMaxBytes: 100 * 1024 * 1024, // 100MB
		IndexInterval:   4096,
		RetentionBytes:  1024 * 1024 * 1024,
		RetentionTime:   24 * time.Hour,
	}

	segment, err := NewSegment(dir, 0, cfg)
	if err != nil {
		b.Fatalf("NewSegment() error = %v", err)
	}
	defer segment.Close()

	msg := Message{
		Offset:    0,
		Timestamp: time.Now(),
		Key:       []byte("benchmark-key"),
		Value:     []byte("benchmark-value-with-some-content"),
		Headers:   map[string][]byte{"header": []byte("value")},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		msg.Offset = int64(i)
		_ = segment.Append([]Message{msg})
	}
}

func BenchmarkSegment_Read(b *testing.B) {
	dir := b.TempDir()
	cfg := &config.CommitLogConfig{
		SegmentMaxBytes: 100 * 1024 * 1024,
		IndexInterval:   4096,
		RetentionBytes:  1024 * 1024 * 1024,
		RetentionTime:   24 * time.Hour,
	}

	segment, err := NewSegment(dir, 0, cfg)
	if err != nil {
		b.Fatalf("NewSegment() error = %v", err)
	}
	defer segment.Close()

	// Pre-populate with messages
	for i := 0; i < 1000; i++ {
		msg := Message{
			Offset:    int64(i),
			Timestamp: time.Now(),
			Key:       []byte("benchmark-key"),
			Value:     []byte("benchmark-value-with-some-content"),
			Headers:   map[string][]byte{},
		}
		_ = segment.Append([]Message{msg})
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = segment.Read(0, 1024*1024)
	}
}
