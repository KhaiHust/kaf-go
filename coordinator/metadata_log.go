package coordinator

import (
	"math"
	"sync"

	metadatapkg "github.com/KhaiHust/kaf-go/protocol/metadata"
	"github.com/KhaiHust/kaf-go/storage/commitlog"
)

type MetadataLog struct {
	mu        sync.Mutex
	commitLog commitlog.ICommitLog
	ps        *PartitionState
}

func NewMetadataLog() *MetadataLog {
	return &MetadataLog{}
}

func (m *MetadataLog) Init(cl commitlog.ICommitLog) {
	m.mu.Lock()
	m.commitLog = cl
	m.mu.Unlock()
}

func (m *MetadataLog) SetPartitionState(ps *PartitionState) {
	m.mu.Lock()
	m.ps = ps
	m.mu.Unlock()
}

func (m *MetadataLog) Append(value []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.commitLog == nil {
		return nil // not yet initialized — silently drop
	}

	batch := metadatapkg.EncodeAsRecordBatch(value)
	baseOffset, err := m.commitLog.AppendRaw(batch, 1)
	if err != nil {
		return err
	}
	if m.ps != nil {
		m.ps.OnLeaderAppend(baseOffset)
	}
	return nil
}

func (m *MetadataLog) Replay(apply func(value []byte)) error {
	m.mu.Lock()
	cl := m.commitLog
	m.mu.Unlock()

	if cl == nil {
		return nil
	}

	region, err := cl.FindRecords(0, math.MaxInt64)
	if err != nil || region == nil {
		return err
	}
	raw := make([]byte, region.Size)
	if _, err := region.File.ReadAt(raw, region.FileOffset); err != nil {
		return err
	}
	for _, value := range metadatapkg.DecodeRecordValues(raw) {
		apply(value)
	}
	return nil
}

func (m *MetadataLog) NewestOffset() int64 {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.commitLog == nil {
		return 0
	}
	neo := m.commitLog.NewestOffset()
	if neo < 0 {
		return 0
	}
	return neo + 1
}
