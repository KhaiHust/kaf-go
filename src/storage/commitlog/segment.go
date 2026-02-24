package commitlog

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/KhaiHust/kaf-go/config"
)

type Segment struct {
	BaseOffset int64

	dir           string
	logFile       *os.File
	indexFile     *os.File
	timeIndexFile *os.File

	offsetIndex *OffsetIndex
	timeIndex   *TimeIndex

	maxBytes      int64
	currentSize   int64
	indexInterval int64 // Number of bytes between index entries

	lastIndexedPosition int64
	newestTimestamp     time.Time

	mu *sync.RWMutex
}

// NewSegment creates a new segment with the given base offset and configuration.
func NewSegment(dir string, baseOffset int64, segmentConfig *config.CommitLogConfig) (*Segment, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}
	// Create file names: {baseOffset}.log, {baseOffset}.offsetIndex, {baseOffset}.timeindex
	logFilePath := filepath.Join(dir, fmt.Sprintf("%020d.log", baseOffset))
	indexFilePath := filepath.Join(dir, fmt.Sprintf("%020d.offsetIndex", baseOffset))
	timeIndexFilePath := filepath.Join(dir, fmt.Sprintf("%020d.timeindex", baseOffset))

	logFile, err := os.OpenFile(logFilePath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}

	offsetIndexFile, err := os.OpenFile(indexFilePath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		_ = logFile.Close()
		return nil, err
	}

	timeIndexFile, err := os.OpenFile(timeIndexFilePath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		_ = logFile.Close()
		_ = offsetIndexFile.Close()
		return nil, err
	}

	offsetIndex, err := NewOffsetIndex(offsetIndexFile, baseOffset)
	if err != nil {
		_ = logFile.Close()
		_ = offsetIndexFile.Close()
		_ = timeIndexFile.Close()
		return nil, err
	}

	timeIndex, err := NewTimeIndex(timeIndexFile, baseOffset)
	if err != nil {
		_ = logFile.Close()
		_ = offsetIndexFile.Close()
		_ = timeIndexFile.Close()
		_ = offsetIndex.Close()
		return nil, err
	}

	fileInfo, err := os.Stat(logFilePath)
	if err != nil {
		_ = logFile.Close()
		_ = offsetIndexFile.Close()
		_ = timeIndexFile.Close()
		_ = offsetIndex.Close()
		_ = timeIndex.Close()
		return nil, err
	}

	return &Segment{
		BaseOffset:    baseOffset,
		dir:           dir,
		logFile:       logFile,
		indexFile:     offsetIndexFile,
		timeIndexFile: timeIndexFile,
		offsetIndex:   offsetIndex,
		timeIndex:     timeIndex,
		maxBytes:      segmentConfig.SegmentMaxBytes,
		indexInterval: int64(segmentConfig.IndexInterval),
		currentSize:   fileInfo.Size(),
		mu:            new(sync.RWMutex),
	}, nil

}

// Append writes a batch of messages to the segment.
// It updates the offset and time indexes as needed.
// It returns an error if any message fails to append.
// The caller is responsible for ensuring that the messages have correct offsets and timestamps before calling Append.
func (s *Segment) Append(messages []Message) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	position, err := s.logFile.Seek(0, io.SeekEnd)
	if err != nil {
		return fmt.Errorf("failed to seek to end of log file: %v", err)
	}

	for _, message := range messages {

		data, err := SerializeMessage(message)
		if err != nil {
			return fmt.Errorf("failed to append message: %v", err)
		}

		dataSize, err := s.logFile.Write(data)
		if err != nil {
			return fmt.Errorf("failed to write message to log file: %v", err)
		}

		s.currentSize += int64(dataSize)

		// If the message is the first one in the segment
		//Or we've written enough bytes since the last index entry, add a new index entry
		if s.offsetIndex.EntryCount() == 0 || position-s.lastIndexedPosition >= s.indexInterval {
			if err = s.offsetIndex.Append(message.Offset, int(position)); err != nil {
				return fmt.Errorf("failed to append to offset index: %v", err)
			}

			if err = s.timeIndex.Append(message.Offset, message.Timestamp.UnixNano()); err != nil {
				return fmt.Errorf("failed to append to time index: %v", err)
			}
			s.lastIndexedPosition = position
		}

		if message.Timestamp.After(s.newestTimestamp) {
			s.newestTimestamp = message.Timestamp
		}

		position += int64(dataSize)
	}
	return nil
}

// Read returns messages starting from the given offset up to maxBytes.
func (s *Segment) Read(offset int64, maxBytes int) ([]Message, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if offset < s.BaseOffset {
		return nil, fmt.Errorf("offset %d is out of range for segment with base offset %d", offset, s.BaseOffset)
	}

	position, err := s.offsetIndex.Lookup(offset)
	if err != nil {
		return nil, fmt.Errorf("failed to lookup offset in index: %v", err)
	}

	if _, err = s.logFile.Seek(int64(position), io.SeekStart); err != nil {
		return nil, fmt.Errorf("failed to seek to position in log file: %v", err)
	}

	reader := bufio.NewReader(s.logFile)
	messages := make([]Message, 0)
	bytesRead := 0
	for bytesRead < maxBytes {
		message, byteRead, err := DeserializeMessage(reader)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break // Reached end of file, stop reading
			}
			return nil, fmt.Errorf("failed to deserialize message: %v", err)
		}

		if message.Offset < offset {
			continue
		}
		messages = append(messages, *message)
		bytesRead += byteRead
	}

	return messages, nil
}

// ReadAt returns messages starting from the given file position up to maxBytes.
func (s *Segment) ReadAt(position int64, maxBytes int) ([]Message, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if _, err := s.logFile.Seek(position, io.SeekStart); err != nil {
		return nil, fmt.Errorf("failed to seek to position in log file: %v", err)
	}

	reader := bufio.NewReader(s.logFile)
	messages := make([]Message, 0)
	bytesRead := 0
	for bytesRead < maxBytes {
		message, byteRead, err := DeserializeMessage(reader)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break // Reached end of file, stop reading
			}
			return nil, fmt.Errorf("failed to deserialize message: %v", err)
		}
		messages = append(messages, *message)
		bytesRead += byteRead
	}

	return messages, nil
}

// IsFull checks if the segment has reached its maximum size.
func (s *Segment) IsFull() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.currentSize >= s.maxBytes
}

// Sync flushes any pending writes to the log file and ensures that the data is persisted to disk.
func (s *Segment) Sync() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.logFile.Sync()
}

// Close flushes any pending writes and closes the log file and index files.
func (s *Segment) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.logFile.Sync(); err != nil {
		return fmt.Errorf("failed to sync log file: %v", err)
	}

	if err := s.offsetIndex.Close(); err != nil {
		return fmt.Errorf("failed to close offset index: %v", err)
	}

	if err := s.timeIndex.Close(); err != nil {
		return fmt.Errorf("failed to close time index: %v", err)
	}

	return s.logFile.Close()
}

// NextOffset returns the next offset to be assigned to a new message in this segment.
func (s *Segment) NextOffset() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if _, err := s.logFile.Seek(0, io.SeekStart); err != nil {
		return s.BaseOffset
	}
	reader := bufio.NewReader(s.logFile)

	lastOffset := s.BaseOffset - 1

	for {
		msg, _, err := DeserializeMessage(reader)
		if err == io.EOF {
			break
		}
		if err != nil {
			break
		}
		lastOffset = msg.Offset
	}

	return lastOffset + 1
}

// NewestTimestamp returns the timestamp of the most recently appended message in the segment.
func (s *Segment) NewestTimestamp() time.Time {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.newestTimestamp
}

// Delete removes the segment files from disk.
func (s *Segment) Delete() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	_ = s.logFile.Close()
	_ = s.indexFile.Close()
	_ = s.timeIndexFile.Close()

	logFilePath := filepath.Join(s.dir, fmt.Sprintf("%020d.log", s.BaseOffset))
	indexFilePath := filepath.Join(s.dir, fmt.Sprintf("%020d.offsetIndex", s.BaseOffset))
	timeIndexFilePath := filepath.Join(s.dir, fmt.Sprintf("%020d.timeindex", s.BaseOffset))

	_ = os.Remove(logFilePath)
	_ = os.Remove(indexFilePath)
	_ = os.Remove(timeIndexFilePath)

	return nil
}
