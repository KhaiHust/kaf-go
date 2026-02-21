//go:build linux || darwin
// +build linux darwin

package commitlog

import (
	"encoding/binary"
	"fmt"
	"os"
	"sync"
	"syscall"

	"golang.org/x/sys/unix"
)

const entrySize = 8      // 4 bytes for "relative" offset + 4 bytes for position
const timeEntrySize = 12 // 8 bytes for timestamp + 4 bytes for offset

type OffsetIndex struct {
	file       *os.File
	mmap       []byte
	baseOffset int64
	size       int64
	entryCount int
	mu         *sync.RWMutex
}

type IndexEntry struct {
	RelativeOffset int // Offset relative to the base offset of the segment
	Position       int // Byte position of the message in the log file
}

func NewOffsetIndex(file *os.File, baseOffset int64) (*OffsetIndex, error) {
	fileInfo, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("failed to get index file info: %v", err)
	}
	fileSize := fileInfo.Size()

	// If the file is empty, we can initialize it with a default size (e.g., 1MB) to avoid issues with mmap.
	if fileSize == 0 {
		fileSize = int64(1024 * 1024) // 1MB
		if err := file.Truncate(fileSize); err != nil {
			return nil, fmt.Errorf("failed to initialize index file: %v", err)
		}
	}

	mmap, err := syscall.Mmap(int(file.Fd()), 0, int(fileSize), syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		return nil, fmt.Errorf("failed to mmap index file: %v", err)
	}

	return &OffsetIndex{
		file:       file,
		baseOffset: baseOffset,
		size:       fileSize,
		entryCount: 0,
		mmap:       mmap,
		mu:         new(sync.RWMutex),
	}, nil
}

func (idx *OffsetIndex) Append(offset int64, position int) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	requiredSize := int64(idx.entryCount+1) * entrySize
	if requiredSize > idx.size {
		err := idx.grow()
		if err != nil {
			return fmt.Errorf("failed to grow index file: %v", err)
		}
	}

	relativeOffset := int32(offset - idx.baseOffset)
	entryPosition := int64(idx.entryCount * entrySize)
	binary.BigEndian.PutUint32(idx.mmap[entryPosition:entryPosition+4], uint32(relativeOffset))
	binary.BigEndian.PutUint32(idx.mmap[entryPosition+4:entryPosition+8], uint32(position))
	idx.entryCount++

	return nil
}

func (idx *OffsetIndex) Lookup(offset int64) (int, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	relativeOffset := int(offset - idx.baseOffset)
	left, right := 0, idx.entryCount-1
	result := 0
	for left <= right {
		mid := left + (right-left)/2

		entryPosition := int64(mid * entrySize)
		entryRelativeOffset := int(binary.BigEndian.Uint32(idx.mmap[entryPosition : entryPosition+4]))
		if entryRelativeOffset == relativeOffset {
			result = int(binary.BigEndian.Uint32(idx.mmap[entryPosition+4 : entryPosition+8]))
			return result, nil
		}
		if entryRelativeOffset < relativeOffset {
			left = mid + 1
		} else {
			right = mid - 1
		}
	}

	return 0, fmt.Errorf("relative offset %d not found in index", offset)
}

func (idx *OffsetIndex) EntryCount() int {
	return idx.entryCount
}

func (idx *OffsetIndex) grow() error {
	newSize := idx.size * 2
	if err := idx.file.Truncate(newSize); err != nil {
		return fmt.Errorf("failed to grow index file: %v", err)
	}

	// Unmap the old mmap before creating a new one
	if err := syscall.Munmap(idx.mmap); err != nil {
		return fmt.Errorf("failed to unmap old index file: %v", err)
	}

	// important: handle global error
	idx.mmap = nil
	// Create a new mmap with the updated size
	newMmap, err := syscall.Mmap(int(idx.file.Fd()), 0, int(newSize), syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		return fmt.Errorf("failed to mmap new index file: %v", err)
	}

	idx.mmap = newMmap
	idx.size = newSize
	return nil
}

// FindLastOffset returns the last offset in the index
func (idx *OffsetIndex) FindLastOffset() (int64, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	if idx.entryCount == 0 {
		return 0, fmt.Errorf("index is empty")
	}

	lastEntryPosition := int64((idx.entryCount - 1) * entrySize)
	relativeOffset := int64(binary.BigEndian.Uint32(idx.mmap[lastEntryPosition : lastEntryPosition+4]))
	return idx.baseOffset + relativeOffset, nil
}

func (idx *OffsetIndex) Close() error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	if idx.mmap == nil {
		_ = idx.file.Close()
		return fmt.Errorf("index is in invalid state (mmap is nil)")
	}

	//sync to disk before unmapping
	if err := unix.Msync(idx.mmap, unix.MS_SYNC); err != nil {
		return fmt.Errorf("failed to sync index file: %v", err)
	}

	// Unmap the memory-mapped file
	if err := syscall.Munmap(idx.mmap); err != nil {
		return fmt.Errorf("failed to unmap index file: %v", err)
	}

	// Truncate to actual size
	if err := idx.file.Truncate(int64(idx.entryCount * entrySize)); err != nil {
		return fmt.Errorf("failed to truncate index file: %v", err)
	}

	return idx.file.Close()
}

// TimeIndex is mapping timestamp to offset
type TimeIndex struct {
	file       *os.File
	mmap       []byte
	baseOffset int64
	size       int64
	entryCount int
	mu         *sync.RWMutex
}

type TimeIndexEntry struct {
	Timestamp      int64 // Timestamp of the message
	RelativeOffset int   // Offset relative to the base offset of the segment
}

func NewTimeIndex(file *os.File, baseOffset int64) (*TimeIndex, error) {
	fileInfo, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("failed to get time index file info: %v", err)
	}

	fileSize := fileInfo.Size()
	// If the file is empty, we can initialize it with a default size (e.g., 1MB) to avoid issues with mmap.
	if fileSize == 0 {
		fileSize = int64(1024 * 1024) // 1MB
		if err = file.Truncate(fileSize); err != nil {
			return nil, fmt.Errorf("failed to initialize time index file: %v", err)
		}
	}

	newMmap, err := syscall.Mmap(int(file.Fd()), 0, int(fileSize), syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		return nil, fmt.Errorf("failed to mmap time index file: %v", err)
	}

	return &TimeIndex{
		file:       file,
		baseOffset: baseOffset,
		size:       fileSize,
		entryCount: 0,
		mmap:       newMmap,
		mu:         new(sync.RWMutex),
	}, nil
}

func (idx *TimeIndex) Append(offset int64, timestamp int64) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	requiredSize := int64(idx.entryCount+1) * timeEntrySize
	if requiredSize > idx.size {
		if err := idx.grow(); err != nil {
			return fmt.Errorf("failed to grow time index file: %v", err)
		}
	}

	entryPosition := int64(idx.entryCount * timeEntrySize)
	binary.BigEndian.PutUint64(idx.mmap[entryPosition:], uint64(timestamp))
	binary.BigEndian.PutUint32(idx.mmap[entryPosition+8:], uint32(offset-idx.baseOffset))
	idx.entryCount++

	return nil
}

func (idx *TimeIndex) Lookup(timestamp int64) (int64, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	left, right := 0, idx.entryCount-1
	result := 0
	for left <= right {
		mid := left + (right-left)/2

		entryPosition := int64(mid * timeEntrySize)
		entryTimestamp := int64(binary.BigEndian.Uint64(idx.mmap[entryPosition : entryPosition+8]))
		if entryTimestamp == timestamp {
			result = int(binary.BigEndian.Uint32(idx.mmap[entryPosition+8 : entryPosition+12]))
			return idx.baseOffset + int64(result), nil
		}
		if entryTimestamp < timestamp {
			left = mid + 1
		} else {
			right = mid - 1
		}
	}
	return 0, fmt.Errorf("timestamp %d not found in index", timestamp)
}

func (idx *TimeIndex) grow() error {
	newSize := idx.size * 2
	if err := idx.file.Truncate(newSize); err != nil {
		return fmt.Errorf("failed to grow time index file: %v", err)
	}

	// Unmap the old mmap before creating a new one
	if err := syscall.Munmap(idx.mmap); err != nil {
		return fmt.Errorf("failed to unmap old time index file: %v", err)
	}

	// important: handle global error
	idx.mmap = nil
	// Create a new mmap with the updated size
	newMmap, err := syscall.Mmap(int(idx.file.Fd()), 0, int(newSize), syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		return fmt.Errorf("failed to mmap new time index file: %v", err)
	}

	idx.mmap = newMmap
	idx.size = newSize
	return nil
}

func (idx *TimeIndex) Close() error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	if idx.mmap == nil {
		_ = idx.file.Close()
		return fmt.Errorf("index is in invalid state (mmap is nil)")
	}

	//sync to disk before unmapping
	if err := unix.Msync(idx.mmap, unix.MS_SYNC); err != nil {
		return fmt.Errorf("failed to sync time index file: %v", err)
	}
	// Unmap the memory-mapped file
	if err := syscall.Munmap(idx.mmap); err != nil {
		return fmt.Errorf("failed to unmap time index file: %v", err)
	}

	// Truncate to actual size
	if err := idx.file.Truncate(int64(idx.entryCount * timeEntrySize)); err != nil {
		return fmt.Errorf("failed to truncate time index file: %v", err)
	}

	return idx.file.Close()
}
