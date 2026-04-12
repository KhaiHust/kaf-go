package coordinator

import (
	"sync"

	metadatapkg "github.com/KhaiHust/kaf-go/protocol/metadata"
)

const DefaultPidBlockSize = 1000

type IMetadataAppender interface {
	Append(value []byte) error
}

type PidManager struct {
	mu          sync.Mutex
	nextBase    int64 // next PID to issue
	blockEnd    int64 // exclusive end of current lease
	step        int64
	brokerID    int32
	brokerEpoch int64
	metaLog     IMetadataAppender
}

func NewPidManager() *PidManager {
	return &PidManager{step: DefaultPidBlockSize}
}

func (m *PidManager) Configure(brokerID int32, brokerEpoch int64, metaLog IMetadataAppender) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.brokerID = brokerID
	m.brokerEpoch = brokerEpoch
	m.metaLog = metaLog
}

func (m *PidManager) SeedFromLog(maxSeen int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if maxSeen > m.nextBase {
		m.nextBase = maxSeen
	}
	if maxSeen > m.blockEnd {
		m.blockEnd = maxSeen
	}
}

func (m *PidManager) AllocateOne() (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.nextBase >= m.blockEnd {
		if err := m.leaseNextBlockLocked(); err != nil {
			return 0, err
		}
	}
	pid := m.nextBase
	m.nextBase++
	return pid, nil
}

func (m *PidManager) leaseNextBlockLocked() error {
	newEnd := m.blockEnd + m.step
	if m.metaLog != nil {
		rec := &metadatapkg.ProducerIdsRecord{
			BrokerId:       m.brokerID,
			BrokerEpoch:    m.brokerEpoch,
			NextProducerId: newEnd,
		}
		if err := m.metaLog.Append(rec.Encode()); err != nil {
			return err
		}
	}
	if m.nextBase < m.blockEnd {
		m.nextBase = m.blockEnd
	}
	m.blockEnd = newEnd
	return nil
}
