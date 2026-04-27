package commitlog

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/KhaiHust/kaf-go/config"
)

// RecordBatch wire format offsets — all reads relative to batch start
const (
	offsetBaseOffset      = 0  // int64, 8 bytes
	offsetBatchLength     = 8  // int32, 4 bytes
	offsetLastOffsetDelta = 23 // int32, 4 bytes — only read in slow path

	// 17 bytes: enough for baseOffset + batchLength (fast path)
	headerSizeUpToMagic = 17
	// 27 bytes: adds CRC(4) + Attributes(2) + LastOffsetDelta(4)
	headerSizeWithDelta = 27
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

// batchMeta holds the minimum info needed for the search algorithm.
// Mirrors Kafka's lazy FileChannelRecordBatch — lastOffset is only
// populated when the slow path is triggered.
type batchMeta struct {
	filePos    int64 // byte position of this batch in the log file
	baseOffset int64 // always populated (17-byte read)
	totalLen   int64 // 12 + BatchLength (12 = 8 BaseOffset + 4 BatchLength)
	lastOffset int64 // populated lazily on slow path (-1 = not loaded)
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

// AppendRaw patches the broker-assigned BaseOffset into the RecordBatch,
// then writes the bytes directly to disk.
//
// The RecordBatch CRC covers from Attributes onward (byte 21+), so
// patching BaseOffset (bytes 0-7) does NOT invalidate the CRC.
//
// On-disk layout — exactly what the consumer wire format expects:
//
//	[BaseOffset: 8B][BatchLength: 4B][PartLeaderEpoch: 4B][Magic: 1B]
//	[CRC: 4B][Attributes: 2B]...[Records]
//
// No envelope, no wrapper — the file IS the Kafka log segment.
func (s *Segment) AppendRaw(baseOffset int64, data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(data) < 8 {
		return fmt.Errorf("Record Batch data too short to contain BaseOffset")
	}

	binary.BigEndian.PutUint64(data[0:8], uint64(baseOffset))

	position, err := s.logFile.Seek(0, io.SeekEnd)
	if err != nil {
		return err
	}

	if _, err = s.logFile.Write(data); err != nil {
		return err
	}

	s.currentSize += int64(len(data))

	//Index: maps baseOffset => file position of this RecordBatch
	if s.offsetIndex.EntryCount() == 0 || position-s.lastIndexedPosition >= s.indexInterval {
		if err = s.offsetIndex.Append(baseOffset, int(position)); err != nil {
			return fmt.Errorf("failed to append to offset index: %v", err)
		}
		if err = s.timeIndex.Append(baseOffset, time.Now().UnixNano()); err != nil {
			return fmt.Errorf("failed to append to time index: %v", err)
		}
		s.lastIndexedPosition = position
	}

	return nil

}

// FindRawRegion implements the KAFKA-18989 algorithm:
//
//	Fast path (17-byte read per batch):
//	  if nextBatch.baseOffset >= fetchOffset
//	    → fetchOffset lives in current batch — return it immediately
//
//	Slow path (27-byte read, only when fast path is inconclusive):
//	  read LastOffsetDelta to compute lastOffset
//	  if lastOffset >= fetchOffset → return current batch
//
//	After the start batch is found, accumulate subsequent batches up to maxBytes.
//
// Record data never enters userspace — only headers are scanned.
func (s *Segment) FindRawRegion(fetchOffset int64, maxBytes int64) (filePos int64, size int64, err error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	indexPos, err := s.offsetIndex.Lookup(fetchOffset)
	if err != nil {
		return 0, 0, err
	}

	fileInfo, err := s.logFile.Stat()
	if err != nil {
		return 0, 0, err
	}

	fileSize := fileInfo.Size()

	pos := int64(indexPos)
	startPos := int64(-1)

	var preMeta *batchMeta

	for pos+headerSizeUpToMagic <= fileSize {
		curr, err := s.readBatchMeta17(pos)
		if err != nil {
			return 0, 0, err
		}

		if curr.baseOffset >= fetchOffset {
			if preMeta != nil {
				startPos = preMeta.filePos
			} else {
				startPos = curr.filePos
			}
			break
		}

		lastOffset, err := s.readLastOffset(curr)
		if err != nil {
			return 0, 0, err
		}

		curr.lastOffset = lastOffset
		if lastOffset >= fetchOffset {
			startPos = curr.filePos
			break
		}

		preMeta = curr
		pos = curr.filePos + curr.totalLen
	}

	if startPos < 0 {
		if preMeta != nil {
			startPos = preMeta.filePos
		} else {
			return 0, 0, nil
		}
	}

	pos = startPos
	accumulated := int64(0)
	for pos+headerSizeUpToMagic <= fileSize {
		curr, err := s.readBatchMeta17(pos)
		if err != nil {
			return 0, 0, err
		}

		if maxBytes > 0 && accumulated > 0 && accumulated+curr.totalLen > maxBytes {
			break
		}

		accumulated += curr.totalLen
		pos += curr.totalLen
		if maxBytes > 0 && accumulated > maxBytes {
			break
		}
	}
	if accumulated == 0 {
		return 0, 0, nil
	}

	return startPos, accumulated, nil
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

	fileInfo, err := s.logFile.Stat()
	if err != nil {
		return s.BaseOffset
	}
	fileSize := fileInfo.Size()
	pos := int64(0)
	lastBaseOffset := int64(-1)
	lastTotalLen := int64(0)

	for pos+headerSizeUpToMagic <= fileSize {
		var buf [headerSizeWithDelta]byte
		n, err := s.logFile.ReadAt(buf[:], pos)
		if err != nil || n < headerSizeWithDelta {
			break
		}

		baseOffset := int64(binary.BigEndian.Uint64(buf[0:8]))
		batchLength := int64(binary.BigEndian.Uint32(buf[8:12]))

		totalLen := int64(12) + batchLength
		if totalLen <= 0 || pos+totalLen > fileSize {
			break
		}

		lastOffsetDelta := int32(binary.BigEndian.Uint32(buf[23:27]))
		lastBaseOffset = baseOffset
		lastTotalLen = int64(lastOffsetDelta)
		_ = lastTotalLen

		pos += totalLen
	}

	if lastBaseOffset < 0 {
		return s.BaseOffset
	}
	return s.scanLastOffset(fileSize)
}

func (s *Segment) scanLastOffset(fileSize int64) int64 {
	pos := int64(0)
	nextOffset := s.BaseOffset

	for pos+headerSizeWithDelta <= fileSize {
		var buf [headerSizeWithDelta]byte
		n, err := s.logFile.ReadAt(buf[:], pos)
		if err != nil || n < headerSizeWithDelta {
			break
		}

		baseOffset := int64(binary.BigEndian.Uint64(buf[0:8]))
		batchLength := int32(binary.BigEndian.Uint32(buf[8:12]))
		lastOffsetDelta := int32(binary.BigEndian.Uint32(buf[23:27]))
		totalLen := int64(12) + int64(batchLength)

		if totalLen <= 0 || pos+totalLen > fileSize {
			break
		}

		nextOffset = baseOffset + int64(lastOffsetDelta) + 1
		pos += totalLen
	}

	return nextOffset
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

const headerSizeWithProducerInfo = 57

func (s *Segment) walkBatchHeaders(fromOffset int64, fn func(BatchHeader) error) error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	fileInfo, err := s.logFile.Stat()
	if err != nil {
		return err
	}
	fileSize := fileInfo.Size()

	pos := int64(0)
	for pos+headerSizeWithProducerInfo <= fileSize {
		var buf [headerSizeWithProducerInfo]byte
		n, rerr := s.logFile.ReadAt(buf[:], pos)
		if rerr != nil || n < headerSizeWithProducerInfo {
			break
		}

		baseOffset := int64(binary.BigEndian.Uint64(buf[0:8]))
		batchLength := int32(binary.BigEndian.Uint32(buf[8:12]))
		totalLen := int64(12) + int64(batchLength)
		if totalLen <= 0 || pos+totalLen > fileSize {
			break
		}

		lastOffsetDelta := int32(binary.BigEndian.Uint32(buf[23:27]))
		producerId := int64(binary.BigEndian.Uint64(buf[43:51]))
		producerEpoch := int16(binary.BigEndian.Uint16(buf[51:53]))
		baseSequence := int32(binary.BigEndian.Uint32(buf[53:57]))

		if baseOffset >= fromOffset {
			if cberr := fn(BatchHeader{
				BaseOffset:      baseOffset,
				LastOffsetDelta: lastOffsetDelta,
				ProducerId:      producerId,
				ProducerEpoch:   producerEpoch,
				BaseSequence:    baseSequence,
			}); cberr != nil {
				return cberr
			}
		}

		pos += totalLen
	}
	return nil
}

// readBatchMeta17 reads 17 bytes and returns the batch's position,
// baseOffset, and totalLen. Equivalent to Kafka's nextBatch() which
// reads HEADER_SIZE_UP_TO_MAGIC bytes into a lazy wrapper.
func (s *Segment) readBatchMeta17(pos int64) (*batchMeta, error) {
	var buf [headerSizeUpToMagic]byte
	if _, err := s.logFile.ReadAt(buf[:], pos); err != nil {
		return nil, fmt.Errorf("read batch header at %d: %w", pos, err)
	}

	baseOffset := int64(binary.BigEndian.Uint64(buf[offsetBaseOffset : offsetBaseOffset+8]))
	batchLength := int32(binary.BigEndian.Uint32(buf[offsetBatchLength : offsetBatchLength+4]))

	// totalLen = 8 (BaseOffset) + 4 (BatchLength field) + batchLength value
	// This matches Kafka: sizeInBytes() = LOG_OVERHEAD + batchLength
	//   where LOG_OVERHEAD = OFFSET_OFFSET(8) + SIZE_OFFSET(4) = 12
	totalLen := int64(12) + int64(batchLength)

	return &batchMeta{
		filePos:    pos,
		baseOffset: baseOffset,
		totalLen:   totalLen,
		lastOffset: -1, // not loaded yet
	}, nil
}

// readLastOffset reads the additional 10 bytes needed to compute lastOffset.
// Equivalent to Kafka's FileChannelRecordBatch.loadBatchHeader() which
// reads headerSize() bytes — triggered only in the slow path.
//
//	bytes 17-20: CRC        (4B, not needed for offset but in the read range)
//	bytes 21-22: Attributes (2B, not needed)
//	bytes 23-26: LastOffsetDelta (4B) ← this is what we need
func (s *Segment) readLastOffset(m *batchMeta) (int64, error) {
	// Read bytes 17-26 (10 bytes) to reach LastOffsetDelta at byte 23
	var buf [10]byte
	if _, err := s.logFile.ReadAt(buf[:], m.filePos+headerSizeUpToMagic); err != nil {
		return 0, fmt.Errorf("read lastOffsetDelta at %d: %w", m.filePos, err)
	}

	// LastOffsetDelta is at absolute byte 23 = relative byte 23-17 = 6
	lastOffsetDelta := int32(binary.BigEndian.Uint32(buf[6:10]))
	return m.baseOffset + int64(lastOffsetDelta), nil
}
