package commitlog

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/KhaiHust/kaf-go/config"
)

const (
	suffixCommitLogFile = ".log"
)

type ICommitLog interface {
	// Append writes a batch of messages to the commit log.
	// It returns the starting offset of the first message in the batch.
	// Messages are assigned sequential offsets starting from the returned offset.
	// If the active segment is full, a new segment is created automatically.
	Append(messages []Message) (offset int64, err error)

	// Read retrieves messages starting from the given offset.
	// It reads up to maxBytes of message data.
	// Returns an empty slice if the offset is out of range.
	Read(offset int64, maxBytes int) ([]Message, error)

	// TruncateTo removes all messages with offsets greater than or equal to the given offset.
	// This is useful for log compaction and recovery scenarios.
	TruncateTo(offset int64) error

	// OldestOffset returns the offset of the oldest available message in the log.
	// Returns 0 if there are no segments.
	OldestOffset() int64

	// NewestOffset returns the offset of the newest message in the log.
	// Returns -1 if no messages have been written yet.
	NewestOffset() int64

	// Close flushes all pending writes and closes all segments.
	// After Close is called, any further operations will return an error.
	Close() error
}

type commitLog struct {
	dir             string
	segments        []*Segment
	activeSegment   *Segment
	commitLogConfig *config.CommitLogConfig
	isClosed        bool
	nextOffset      int64
	mu              *sync.RWMutex
}

func (c *commitLog) Append(messages []Message) (offset int64, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.isClosed {
		return 0, errors.New("commitLog is already closed")
	}

	if len(c.segments) == 0 {
		return 0, nil
	}

	if c.activeSegment.IsFull() {
		newSegment, err := NewSegment(c.dir, c.activeSegment.NextOffset(), c.commitLogConfig)
		if err != nil {
			return 0, err
		}
		c.segments = append(c.segments, newSegment)
		c.activeSegment = newSegment

		//todo: clean up Old segments
	}

	startOffset := c.nextOffset
	for i := range messages {
		messages[i].Offset = c.nextOffset
		if messages[i].Timestamp.IsZero() {
			messages[i].Timestamp = time.Now()
		}
		c.nextOffset++
	}

	if err = c.activeSegment.Append(messages); err != nil {
		return 0, err
	}

	return startOffset, nil
}

func (c *commitLog) Read(offset int64, maxBytes int) ([]Message, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.isClosed {
		return nil, errors.New("commitLog is already closed")
	}

	segmentIndex := c.findSegmentByOffset(offset)

	if segmentIndex < 0 {
		return []Message{}, nil
	}

	messages := make([]Message, 0, maxBytes)
	byteReads := 0

	for i := segmentIndex; i < len(c.segments) && byteReads < maxBytes; i++ {
		readOffset := offset
		if i > segmentIndex {
			readOffset = c.segments[i].BaseOffset
		}

		msgs, err := c.segments[i].Read(readOffset, maxBytes-byteReads)
		if err != nil {
			slog.Error(fmt.Sprintf("Failed to read segment %d: %v", i, err))
			return nil, err
		}

		for _, msg := range msgs {
			messages = append(messages, msg)
			data, _ := SerializeMessage(msg)
			byteReads += len(data)
		}
	}

	return messages, nil

}

func (c *commitLog) TruncateTo(offset int64) error {
	//TODO implement me
	panic("implement me")
}

func (c *commitLog) OldestOffset() int64 {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if len(c.segments) == 0 {
		return 0
	}

	return c.segments[0].BaseOffset
}

func (c *commitLog) NewestOffset() int64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.nextOffset - 1
}

func (c *commitLog) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.isClosed {
		return nil
	}

	c.isClosed = true
	for _, segment := range c.segments {
		if err := segment.Close(); err != nil {
			return err
		}
	}
	return nil
}

func NewCommitLog(dir string, commitLogConfig *config.CommitLogConfig) (ICommitLog, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}
	commitlog := &commitLog{
		dir:             dir,
		commitLogConfig: commitLogConfig,
		mu:              new(sync.RWMutex),
	}

	segments, err := commitlog.loadSegments(dir)
	if err != nil {
		return nil, err
	}

	commitlog.segments = segments
	commitlog.activeSegment = segments[len(segments)-1]

	commitlog.nextOffset = commitlog.activeSegment.NextOffset()

	return commitlog, nil
}

func (c *commitLog) loadSegments(dir string) ([]*Segment, error) {
	files, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	segments := make([]*Segment, 0)
	for _, file := range files {
		if strings.HasSuffix(file.Name(), suffixCommitLogFile) {
			offsetStr := strings.TrimSuffix(file.Name(), suffixCommitLogFile)
			baseOffset, err := strconv.ParseInt(offsetStr, 10, 64)
			if err != nil {
				return nil, err
			}
			segment, err := NewSegment(dir, baseOffset, c.commitLogConfig)
			if err != nil {
				return nil, err
			}
			segments = append(segments, segment)
		}
	}

	if len(segments) == 0 {
		newSegment, err := NewSegment(dir, 0, c.commitLogConfig)
		if err != nil {
			return nil, err
		}
		segments = append(segments, newSegment)
	}

	sort.Slice(segments, func(i, j int) bool {
		return segments[i].BaseOffset < segments[j].BaseOffset
	})

	return segments, nil
}

func (c *commitLog) findSegmentByOffset(offset int64) int {
	left, right := 0, len(c.segments)-1
	result := -1
	for left <= right {
		mid := (left + right) / 2

		if c.segments[mid].BaseOffset <= offset {
			result = mid
			left = mid + 1
		} else {
			right = mid - 1
		}
	}
	return result
}
