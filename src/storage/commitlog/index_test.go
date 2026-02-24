//go:build linux || darwin
// +build linux darwin

package commitlog

import (
	"os"
	"path/filepath"
	"sync"
	"testing"
)

// TestNewOffsetIndex tests creating a new offset index
func TestNewOffsetIndex(t *testing.T) {
	tmpDir := t.TempDir()
	indexPath := filepath.Join(tmpDir, "test.index")

	file, err := os.Create(indexPath)
	if err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}
	defer file.Close()

	baseOffset := int64(100)
	index, err := NewOffsetIndex(file, baseOffset)
	if err != nil {
		t.Fatalf("NewOffsetIndex failed: %v", err)
	}
	defer index.Close()

	if index.baseOffset != baseOffset {
		t.Errorf("expected baseOffset %d, got %d", baseOffset, index.baseOffset)
	}
	if index.entryCount != 0 {
		t.Errorf("expected entryCount 0, got %d", index.entryCount)
	}
	if index.size != 1024*1024 {
		t.Errorf("expected size 1MB, got %d", index.size)
	}
	if index.mu == nil {
		t.Error("expected mutex to be initialized")
	}
}

// TestOffsetIndexAppend tests appending entries to the offset index
func TestOffsetIndexAppend(t *testing.T) {
	tmpDir := t.TempDir()
	indexPath := filepath.Join(tmpDir, "test.index")

	file, err := os.Create(indexPath)
	if err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}
	defer file.Close()

	baseOffset := int64(100)
	index, err := NewOffsetIndex(file, baseOffset)
	if err != nil {
		t.Fatalf("NewOffsetIndex failed: %v", err)
	}
	defer index.Close()

	// Append entries
	testCases := []struct {
		offset   int64
		position int
	}{
		{100, 0},
		{101, 1024},
		{102, 2048},
		{103, 3072},
		{104, 4096},
	}

	for _, tc := range testCases {
		if err := index.Append(tc.offset, tc.position); err != nil {
			t.Fatalf("Append(%d, %d) failed: %v", tc.offset, tc.position, err)
		}
	}

	if index.EntryCount() != len(testCases) {
		t.Errorf("expected entryCount %d, got %d", len(testCases), index.EntryCount())
	}
}

// TestOffsetIndexLookup tests looking up entries in the offset index
func TestOffsetIndexLookup(t *testing.T) {
	tmpDir := t.TempDir()
	indexPath := filepath.Join(tmpDir, "test.index")

	file, err := os.Create(indexPath)
	if err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}
	defer file.Close()

	baseOffset := int64(100)
	index, err := NewOffsetIndex(file, baseOffset)
	if err != nil {
		t.Fatalf("NewOffsetIndex failed: %v", err)
	}
	defer index.Close()

	// Append entries
	testCases := []struct {
		offset   int64
		position int
	}{
		{100, 0},
		{101, 1024},
		{102, 2048},
		{103, 3072},
		{104, 4096},
	}

	for _, tc := range testCases {
		if err := index.Append(tc.offset, tc.position); err != nil {
			t.Fatalf("Append failed: %v", err)
		}
	}

	// Lookup entries
	for _, tc := range testCases {
		position, err := index.Lookup(tc.offset)
		if err != nil {
			t.Errorf("Lookup(%d) failed: %v", tc.offset, err)
			continue
		}
		if position != tc.position {
			t.Errorf("Lookup(%d): expected position %d, got %d", tc.offset, tc.position, position)
		}
	}
}

// TestOffsetIndexGrow tests index growth when capacity is exceeded
func TestOffsetIndexGrow(t *testing.T) {
	tmpDir := t.TempDir()
	indexPath := filepath.Join(tmpDir, "test.index")

	file, err := os.Create(indexPath)
	if err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}
	defer file.Close()

	// Create small initial size to force growth
	if err := file.Truncate(64); err != nil {
		t.Fatalf("failed to truncate: %v", err)
	}

	baseOffset := int64(0)
	index, err := NewOffsetIndex(file, baseOffset)
	if err != nil {
		t.Fatalf("NewOffsetIndex failed: %v", err)
	}
	defer index.Close()

	initialSize := index.size

	// Append enough entries to trigger growth (64 bytes / 8 bytes per entry = 8 entries max)
	for i := 0; i < 10; i++ {
		if err := index.Append(int64(i), i*1024); err != nil {
			t.Fatalf("Append(%d) failed: %v", i, err)
		}
	}

	if index.size <= initialSize {
		t.Errorf("expected index to grow, initial size: %d, current size: %d", initialSize, index.size)
	}

	// Verify all entries are still accessible after growth
	for i := 0; i < 10; i++ {
		position, err := index.Lookup(int64(i))
		if err != nil {
			t.Errorf("Lookup(%d) failed after growth: %v", i, err)
			continue
		}
		expectedPosition := i * 1024
		if position != expectedPosition {
			t.Errorf("Lookup(%d) after growth: expected position %d, got %d", i, expectedPosition, position)
		}
	}
}

// TestOffsetIndexConcurrent tests concurrent access to the offset index
func TestOffsetIndexConcurrent(t *testing.T) {
	tmpDir := t.TempDir()
	indexPath := filepath.Join(tmpDir, "test.index")

	file, err := os.Create(indexPath)
	if err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}
	defer file.Close()

	baseOffset := int64(0)
	index, err := NewOffsetIndex(file, baseOffset)
	if err != nil {
		t.Fatalf("NewOffsetIndex failed: %v", err)
	}
	defer index.Close()

	// First, append some entries sequentially
	for i := 0; i < 10; i++ {
		if err := index.Append(int64(i), i*100); err != nil {
			t.Fatalf("Append failed: %v", err)
		}
	}

	// Concurrent reads
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(offset int64) {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				_, err := index.Lookup(offset)
				if err != nil {
					t.Errorf("Lookup(%d) failed: %v", offset, err)
				}
			}
		}(int64(i))
	}
	wg.Wait()
}

// TestNewTimeIndex tests creating a new time index
func TestNewTimeIndex(t *testing.T) {
	tmpDir := t.TempDir()
	indexPath := filepath.Join(tmpDir, "test.timeindex")

	file, err := os.Create(indexPath)
	if err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}
	defer file.Close()

	baseOffset := int64(100)
	index, err := NewTimeIndex(file, baseOffset)
	if err != nil {
		t.Fatalf("NewTimeIndex failed: %v", err)
	}
	defer index.Close()

	if index.baseOffset != baseOffset {
		t.Errorf("expected baseOffset %d, got %d", baseOffset, index.baseOffset)
	}
	if index.entryCount != 0 {
		t.Errorf("expected entryCount 0, got %d", index.entryCount)
	}
	if index.size != 1024*1024 {
		t.Errorf("expected size 1MB, got %d", index.size)
	}
	if index.mu == nil {
		t.Error("expected mutex to be initialized")
	}
}

// TestTimeIndexAppend tests appending entries to the time index
func TestTimeIndexAppend(t *testing.T) {
	tmpDir := t.TempDir()
	indexPath := filepath.Join(tmpDir, "test.timeindex")

	file, err := os.Create(indexPath)
	if err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}
	defer file.Close()

	baseOffset := int64(100)
	index, err := NewTimeIndex(file, baseOffset)
	if err != nil {
		t.Fatalf("NewTimeIndex failed: %v", err)
	}
	defer index.Close()

	testCases := []struct {
		offset    int64
		timestamp int64
	}{
		{100, 1000000},
		{101, 2000000},
		{102, 3000000},
		{103, 4000000},
		{104, 5000000},
	}

	for _, tc := range testCases {
		if err := index.Append(tc.offset, tc.timestamp); err != nil {
			t.Fatalf("Append(%d, %d) failed: %v", tc.offset, tc.timestamp, err)
		}
	}

	if index.entryCount != len(testCases) {
		t.Errorf("expected entryCount %d, got %d", len(testCases), index.entryCount)
	}
}

// TestTimeIndexLookup tests looking up entries in the time index
func TestTimeIndexLookup(t *testing.T) {
	tmpDir := t.TempDir()
	indexPath := filepath.Join(tmpDir, "test.timeindex")

	file, err := os.Create(indexPath)
	if err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}
	defer file.Close()

	baseOffset := int64(100)
	index, err := NewTimeIndex(file, baseOffset)
	if err != nil {
		t.Fatalf("NewTimeIndex failed: %v", err)
	}
	defer index.Close()

	testCases := []struct {
		offset    int64
		timestamp int64
	}{
		{100, 1000000},
		{101, 2000000},
		{102, 3000000},
		{103, 4000000},
		{104, 5000000},
	}

	for _, tc := range testCases {
		if err := index.Append(tc.offset, tc.timestamp); err != nil {
			t.Fatalf("Append failed: %v", err)
		}
	}

	// Lookup entries
	for _, tc := range testCases {
		offset, err := index.Lookup(tc.timestamp)
		if err != nil {
			t.Errorf("Lookup(%d) failed: %v", tc.timestamp, err)
			continue
		}
		if offset != tc.offset {
			t.Errorf("Lookup(%d): expected offset %d, got %d", tc.timestamp, tc.offset, offset)
		}
	}
}

// TestTimeIndexLookupNotFound tests looking up non-existent timestamp
func TestTimeIndexLookupNotFound(t *testing.T) {
	tmpDir := t.TempDir()
	indexPath := filepath.Join(tmpDir, "test.timeindex")

	file, err := os.Create(indexPath)
	if err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}
	defer file.Close()

	index, err := NewTimeIndex(file, 100)
	if err != nil {
		t.Fatalf("NewTimeIndex failed: %v", err)
	}
	defer index.Close()

	// Add a few entries
	_ = index.Append(100, 1000000)
	_ = index.Append(101, 2000000)
	_ = index.Append(102, 3000000)

	// Try to lookup non-existent timestamp
	_, err = index.Lookup(999999999)
	if err == nil {
		t.Error("expected error for non-existent timestamp, got nil")
	}

	// Try to lookup timestamp that's before all entries
	_, err = index.Lookup(500000)
	if err == nil {
		t.Error("expected error for timestamp before all entries, got nil")
	}
}

// TestTimeIndexGrow tests time index growth when capacity is exceeded
func TestTimeIndexGrow(t *testing.T) {
	tmpDir := t.TempDir()
	indexPath := filepath.Join(tmpDir, "test.timeindex")

	file, err := os.Create(indexPath)
	if err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}
	defer file.Close()

	// Create small initial size to force growth
	if err := file.Truncate(96); err != nil {
		t.Fatalf("failed to truncate: %v", err)
	}

	baseOffset := int64(0)
	index, err := NewTimeIndex(file, baseOffset)
	if err != nil {
		t.Fatalf("NewTimeIndex failed: %v", err)
	}
	defer index.Close()

	initialSize := index.size

	// Append enough entries to trigger growth (96 bytes / 12 bytes per entry = 8 entries max)
	for i := 0; i < 10; i++ {
		if err := index.Append(int64(i), int64(1000000+i*1000)); err != nil {
			t.Fatalf("Append(%d) failed: %v", i, err)
		}
	}

	if index.size <= initialSize {
		t.Errorf("expected time index to grow, initial size: %d, current size: %d", initialSize, index.size)
	}

	// Verify all entries are still accessible after growth
	for i := 0; i < 10; i++ {
		offset, err := index.Lookup(int64(1000000 + i*1000))
		if err != nil {
			t.Errorf("Lookup(%d) failed after growth: %v", 1000000+i*1000, err)
			continue
		}
		expectedOffset := int64(i)
		if offset != expectedOffset {
			t.Errorf("Lookup(%d) after growth: expected offset %d, got %d", 1000000+i*1000, expectedOffset, offset)
		}
	}
}

// TestTimeIndexConcurrent tests concurrent access to the time index
func TestTimeIndexConcurrent(t *testing.T) {
	tmpDir := t.TempDir()
	indexPath := filepath.Join(tmpDir, "test.timeindex")

	file, err := os.Create(indexPath)
	if err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}
	defer file.Close()

	baseOffset := int64(0)
	index, err := NewTimeIndex(file, baseOffset)
	if err != nil {
		t.Fatalf("NewTimeIndex failed: %v", err)
	}
	defer index.Close()

	// First, append some entries sequentially
	for i := 0; i < 10; i++ {
		if err := index.Append(int64(i), int64(1000000+i*1000)); err != nil {
			t.Fatalf("Append failed: %v", err)
		}
	}

	// Concurrent reads
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(timestamp int64) {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				_, err := index.Lookup(timestamp)
				if err != nil {
					t.Errorf("Lookup(%d) failed: %v", timestamp, err)
				}
			}
		}(int64(1000000 + i*1000))
	}
	wg.Wait()
}

// TestTimeIndexEmptyLookup tests lookup on empty time index
func TestTimeIndexEmptyLookup(t *testing.T) {
	tmpDir := t.TempDir()
	indexPath := filepath.Join(tmpDir, "test.timeindex")

	file, err := os.Create(indexPath)
	if err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}
	defer file.Close()

	index, err := NewTimeIndex(file, 0)
	if err != nil {
		t.Fatalf("NewTimeIndex failed: %v", err)
	}
	defer index.Close()

	_, err = index.Lookup(1000000)
	if err == nil {
		t.Error("expected error for lookup on empty time index, got nil")
	}
}

// TestOffsetIndexBoundaries tests boundary conditions
func TestOffsetIndexBoundaries(t *testing.T) {
	tmpDir := t.TempDir()
	indexPath := filepath.Join(tmpDir, "test.index")

	file, err := os.Create(indexPath)
	if err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}
	defer file.Close()

	baseOffset := int64(0)
	index, err := NewOffsetIndex(file, baseOffset)
	if err != nil {
		t.Fatalf("NewOffsetIndex failed: %v", err)
	}
	defer index.Close()

	// Test with large offset values
	largeOffset := int64(1000000)
	largePosition := 999999
	if err := index.Append(largeOffset, largePosition); err != nil {
		t.Fatalf("Append with large values failed: %v", err)
	}

	position, err := index.Lookup(largeOffset)
	if err != nil {
		t.Fatalf("Lookup large offset failed: %v", err)
	}
	if position != largePosition {
		t.Errorf("expected position %d, got %d", largePosition, position)
	}
}

// TestTimeIndexBoundaries tests boundary conditions for time index
func TestTimeIndexBoundaries(t *testing.T) {
	tmpDir := t.TempDir()
	indexPath := filepath.Join(tmpDir, "test.timeindex")

	file, err := os.Create(indexPath)
	if err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}
	defer file.Close()

	baseOffset := int64(0)
	index, err := NewTimeIndex(file, baseOffset)
	if err != nil {
		t.Fatalf("NewTimeIndex failed: %v", err)
	}
	defer index.Close()

	// Test with large timestamp values
	largeTimestamp := int64(9999999999999)
	largeOffset := int64(1000000)
	if err := index.Append(largeOffset, largeTimestamp); err != nil {
		t.Fatalf("Append with large values failed: %v", err)
	}

	offset, err := index.Lookup(largeTimestamp)
	if err != nil {
		t.Fatalf("Lookup large timestamp failed: %v", err)
	}
	if offset != largeOffset {
		t.Errorf("expected offset %d, got %d", largeOffset, offset)
	}
}

// TestOffsetIndexMultipleGrowth tests multiple growth cycles
func TestOffsetIndexMultipleGrowth(t *testing.T) {
	tmpDir := t.TempDir()
	indexPath := filepath.Join(tmpDir, "test.index")

	file, err := os.Create(indexPath)
	if err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}
	defer file.Close()

	// Start with very small size
	if err := file.Truncate(32); err != nil {
		t.Fatalf("failed to truncate: %v", err)
	}

	baseOffset := int64(0)
	index, err := NewOffsetIndex(file, baseOffset)
	if err != nil {
		t.Fatalf("NewOffsetIndex failed: %v", err)
	}
	defer index.Close()

	// Append many entries to trigger multiple growth cycles
	numEntries := 50
	for i := 0; i < numEntries; i++ {
		if err := index.Append(int64(i), i*100); err != nil {
			t.Fatalf("Append(%d) failed: %v", i, err)
		}
	}

	// Verify all entries
	for i := 0; i < numEntries; i++ {
		position, err := index.Lookup(int64(i))
		if err != nil {
			t.Errorf("Lookup(%d) failed: %v", i, err)
			continue
		}
		expectedPosition := i * 100
		if position != expectedPosition {
			t.Errorf("Lookup(%d): expected position %d, got %d", i, expectedPosition, position)
		}
	}
}

// TestTimeIndexMultipleGrowth tests multiple growth cycles for time index
func TestTimeIndexMultipleGrowth(t *testing.T) {
	tmpDir := t.TempDir()
	indexPath := filepath.Join(tmpDir, "test.timeindex")

	file, err := os.Create(indexPath)
	if err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}
	defer file.Close()

	// Start with very small size
	if err := file.Truncate(48); err != nil {
		t.Fatalf("failed to truncate: %v", err)
	}

	baseOffset := int64(0)
	index, err := NewTimeIndex(file, baseOffset)
	if err != nil {
		t.Fatalf("NewTimeIndex failed: %v", err)
	}
	defer index.Close()

	// Append many entries to trigger multiple growth cycles
	numEntries := 50
	for i := 0; i < numEntries; i++ {
		if err := index.Append(int64(i), int64(1000000+i*1000)); err != nil {
			t.Fatalf("Append(%d) failed: %v", i, err)
		}
	}

	// Verify all entries
	for i := 0; i < numEntries; i++ {
		timestamp := int64(1000000 + i*1000)
		offset, err := index.Lookup(timestamp)
		if err != nil {
			t.Errorf("Lookup(%d) failed: %v", timestamp, err)
			continue
		}
		expectedOffset := int64(i)
		if offset != expectedOffset {
			t.Errorf("Lookup(%d): expected offset %d, got %d", timestamp, expectedOffset, offset)
		}
	}
}

// TestOffsetIndexOrderedInserts tests that appending maintains order
func TestOffsetIndexOrderedInserts(t *testing.T) {
	tmpDir := t.TempDir()
	indexPath := filepath.Join(tmpDir, "test.index")

	file, err := os.Create(indexPath)
	if err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}
	defer file.Close()

	baseOffset := int64(1000)
	index, err := NewOffsetIndex(file, baseOffset)
	if err != nil {
		t.Fatalf("NewOffsetIndex failed: %v", err)
	}
	defer index.Close()

	// Insert in sequential order
	for i := 0; i < 20; i++ {
		if err := index.Append(baseOffset+int64(i), i*512); err != nil {
			t.Fatalf("Append failed: %v", err)
		}
	}

	// Verify all can be looked up
	for i := 0; i < 20; i++ {
		position, err := index.Lookup(baseOffset + int64(i))
		if err != nil {
			t.Errorf("Lookup failed for offset %d: %v", baseOffset+int64(i), err)
		}
		if position != i*512 {
			t.Errorf("expected position %d, got %d", i*512, position)
		}
	}
}

// TestTimeIndexOrderedInserts tests that time index appending maintains order
func TestTimeIndexOrderedInserts(t *testing.T) {
	tmpDir := t.TempDir()
	indexPath := filepath.Join(tmpDir, "test.timeindex")

	file, err := os.Create(indexPath)
	if err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}
	defer file.Close()

	baseOffset := int64(1000)
	index, err := NewTimeIndex(file, baseOffset)
	if err != nil {
		t.Fatalf("NewTimeIndex failed: %v", err)
	}
	defer index.Close()

	// Insert in sequential order
	for i := 0; i < 20; i++ {
		timestamp := int64(1609459200 + i*3600) // Hour increments from Unix timestamp
		offset := baseOffset + int64(i)
		if err := index.Append(offset, timestamp); err != nil {
			t.Fatalf("Append failed: %v", err)
		}
	}

	// Verify all can be looked up
	for i := 0; i < 20; i++ {
		timestamp := int64(1609459200 + i*3600)
		offset, err := index.Lookup(timestamp)
		if err != nil {
			t.Errorf("Lookup failed for timestamp %d: %v", timestamp, err)
		}
		expectedOffset := baseOffset + int64(i)
		if offset != expectedOffset {
			t.Errorf("expected offset %d, got %d", expectedOffset, offset)
		}
	}
}

// TestOffsetIndexWithNonZeroBase tests index with non-zero base offset
func TestOffsetIndexWithNonZeroBase(t *testing.T) {
	tmpDir := t.TempDir()
	indexPath := filepath.Join(tmpDir, "test.index")

	file, err := os.Create(indexPath)
	if err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}
	defer file.Close()

	baseOffset := int64(5000)
	index, err := NewOffsetIndex(file, baseOffset)
	if err != nil {
		t.Fatalf("NewOffsetIndex failed: %v", err)
	}
	defer index.Close()

	// Append entries with offsets relative to base
	for i := 0; i < 5; i++ {
		offset := baseOffset + int64(i)
		if err := index.Append(offset, i*256); err != nil {
			t.Fatalf("Append failed: %v", err)
		}
	}

	// Lookup entries
	for i := 0; i < 5; i++ {
		offset := baseOffset + int64(i)
		position, err := index.Lookup(offset)
		if err != nil {
			t.Errorf("Lookup(%d) failed: %v", offset, err)
			continue
		}
		expectedPosition := i * 256
		if position != expectedPosition {
			t.Errorf("expected position %d, got %d", expectedPosition, position)
		}
	}
}

// TestTimeIndexWithNonZeroBase tests time index with non-zero base offset
func TestTimeIndexWithNonZeroBase(t *testing.T) {
	tmpDir := t.TempDir()
	indexPath := filepath.Join(tmpDir, "test.timeindex")

	file, err := os.Create(indexPath)
	if err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}
	defer file.Close()

	baseOffset := int64(5000)
	index, err := NewTimeIndex(file, baseOffset)
	if err != nil {
		t.Fatalf("NewTimeIndex failed: %v", err)
	}
	defer index.Close()

	// Append entries with offsets relative to base
	for i := 0; i < 5; i++ {
		offset := baseOffset + int64(i)
		timestamp := int64(1609459200 + i*3600)
		if err := index.Append(offset, timestamp); err != nil {
			t.Fatalf("Append failed: %v", err)
		}
	}

	// Lookup entries
	for i := 0; i < 5; i++ {
		timestamp := int64(1609459200 + i*3600)
		offset, err := index.Lookup(timestamp)
		if err != nil {
			t.Errorf("Lookup(%d) failed: %v", timestamp, err)
			continue
		}
		expectedOffset := baseOffset + int64(i)
		if offset != expectedOffset {
			t.Errorf("expected offset %d, got %d", expectedOffset, offset)
		}
	}
}

// TestOffsetIndexFindLastOffset tests finding the last offset
func TestOffsetIndexFindLastOffset(t *testing.T) {
	tmpDir := t.TempDir()
	indexPath := filepath.Join(tmpDir, "test.index")

	file, err := os.Create(indexPath)
	if err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}
	defer file.Close()

	baseOffset := int64(100)
	index, err := NewOffsetIndex(file, baseOffset)
	if err != nil {
		t.Fatalf("NewOffsetIndex failed: %v", err)
	}
	defer index.Close()

	// Empty index should return error
	_, err = index.FindLastOffset()
	if err == nil {
		t.Error("expected error for FindLastOffset on empty index, got nil")
	}

	// Add entries
	for i := 0; i < 5; i++ {
		if err := index.Append(baseOffset+int64(i), i*100); err != nil {
			t.Fatalf("Append failed: %v", err)
		}
	}

	lastOffset, err := index.FindLastOffset()
	if err != nil {
		t.Fatalf("FindLastOffset failed: %v", err)
	}

	expectedLastOffset := baseOffset + 4
	if lastOffset != expectedLastOffset {
		t.Errorf("expected last offset %d, got %d", expectedLastOffset, lastOffset)
	}
}

// TestOffsetIndexEntryCount tests the EntryCount method
func TestOffsetIndexEntryCount(t *testing.T) {
	tmpDir := t.TempDir()
	indexPath := filepath.Join(tmpDir, "test.index")

	file, err := os.Create(indexPath)
	if err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}
	defer file.Close()

	index, err := NewOffsetIndex(file, 0)
	if err != nil {
		t.Fatalf("NewOffsetIndex failed: %v", err)
	}
	defer index.Close()

	if index.EntryCount() != 0 {
		t.Errorf("expected entry count 0, got %d", index.EntryCount())
	}

	for i := 0; i < 3; i++ {
		_ = index.Append(int64(i), i*100)
	}

	if index.EntryCount() != 3 {
		t.Errorf("expected entry count 3, got %d", index.EntryCount())
	}
}

// BenchmarkOffsetIndexAppend benchmarks appending to offset index
func BenchmarkOffsetIndexAppend(b *testing.B) {
	tmpDir := b.TempDir()
	indexPath := filepath.Join(tmpDir, "bench.index")

	file, err := os.Create(indexPath)
	if err != nil {
		b.Fatalf("failed to create test file: %v", err)
	}
	defer file.Close()

	index, err := NewOffsetIndex(file, 0)
	if err != nil {
		b.Fatalf("NewOffsetIndex failed: %v", err)
	}
	defer index.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = index.Append(int64(i), i*100)
	}
}

// BenchmarkOffsetIndexLookup benchmarks looking up entries in offset index
func BenchmarkOffsetIndexLookup(b *testing.B) {
	tmpDir := b.TempDir()
	indexPath := filepath.Join(tmpDir, "bench.index")

	file, err := os.Create(indexPath)
	if err != nil {
		b.Fatalf("failed to create test file: %v", err)
	}
	defer file.Close()

	index, err := NewOffsetIndex(file, 0)
	if err != nil {
		b.Fatalf("NewOffsetIndex failed: %v", err)
	}
	defer index.Close()

	// Pre-populate with 1000 entries
	for i := 0; i < 1000; i++ {
		_ = index.Append(int64(i), i*100)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = index.Lookup(int64(i % 1000))
	}
}

// BenchmarkTimeIndexAppend benchmarks appending to time index
func BenchmarkTimeIndexAppend(b *testing.B) {
	tmpDir := b.TempDir()
	indexPath := filepath.Join(tmpDir, "bench.timeindex")

	file, err := os.Create(indexPath)
	if err != nil {
		b.Fatalf("failed to create test file: %v", err)
	}
	defer file.Close()

	index, err := NewTimeIndex(file, 0)
	if err != nil {
		b.Fatalf("NewTimeIndex failed: %v", err)
	}
	defer index.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = index.Append(int64(i), int64(1000000+i*1000))
	}
}

// BenchmarkTimeIndexLookup benchmarks looking up entries in time index
func BenchmarkTimeIndexLookup(b *testing.B) {
	tmpDir := b.TempDir()
	indexPath := filepath.Join(tmpDir, "bench.timeindex")

	file, err := os.Create(indexPath)
	if err != nil {
		b.Fatalf("failed to create test file: %v", err)
	}
	defer file.Close()

	index, err := NewTimeIndex(file, 0)
	if err != nil {
		b.Fatalf("NewTimeIndex failed: %v", err)
	}
	defer index.Close()

	// Pre-populate with 1000 entries
	for i := 0; i < 1000; i++ {
		_ = index.Append(int64(i), int64(1000000+i*1000))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = index.Lookup(int64(1000000 + (i%1000)*1000))
	}
}
