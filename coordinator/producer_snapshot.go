package coordinator

import (
	"encoding/binary"
	"errors"
	"io"
	"math"
	"os"
	"path/filepath"
)

const (
	producerStateSnapshotFile  = "producer_state.snapshot"
	producerStateHeaderSize    = 35
	producerStateRingSlotBytes = 20
)

func SaveProducerStateSnapshot(dir string, entries []ProducerSnapshotEntry) error {
	if dir == "" {
		return nil
	}
	path := filepath.Join(dir, producerStateSnapshotFile)
	tmp := path + ".tmp"

	cap0 := 4 + len(entries)*(producerStateHeaderSize+IdempotenceRingSize*producerStateRingSlotBytes)
	buf := make([]byte, 0, cap0)
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(entries)))
	for _, e := range entries {
		buf = encodeProducerStateEntry(buf, &e)
	}

	if err := os.WriteFile(tmp, buf, 0644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func LoadProducerStateSnapshot(dir string) ([]ProducerSnapshotEntry, error) {
	if dir == "" {
		return nil, nil
	}
	path := filepath.Join(dir, producerStateSnapshotFile)
	raw, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	if len(raw) < 4 {
		return nil, io.ErrUnexpectedEOF
	}
	count := int(binary.BigEndian.Uint32(raw[0:]))
	off := 4

	entries := make([]ProducerSnapshotEntry, 0, count)
	for i := 0; i < count; i++ {
		e, consumed, derr := decodeProducerStateEntry(raw[off:])
		if derr != nil {
			return nil, derr
		}
		entries = append(entries, e)
		off += consumed
	}
	return entries, nil
}

func encodeProducerStateEntry(buf []byte, e *ProducerSnapshotEntry) []byte {
	ringLen := 0
	for _, slot := range e.Log.Ring {
		if slot.FirstSeq >= 0 {
			ringLen++
		}
	}
	if ringLen > math.MaxUint8 {
		ringLen = math.MaxUint8
	}

	buf = binary.BigEndian.AppendUint64(buf, uint64(e.ProducerId))
	buf = binary.BigEndian.AppendUint16(buf, uint16(e.Log.LastEpoch))
	buf = binary.BigEndian.AppendUint32(buf, uint32(e.Log.LastSeq))
	buf = binary.BigEndian.AppendUint64(buf, uint64(e.Log.LastOffset))
	buf = binary.BigEndian.AppendUint32(buf, uint32(e.Log.LastLeaderEpoch))
	buf = binary.BigEndian.AppendUint64(buf, uint64(e.Log.LastTimestamp))
	buf = append(buf, byte(ringLen))
	for _, slot := range e.Log.Ring {
		if slot.FirstSeq < 0 {
			continue
		}
		buf = binary.BigEndian.AppendUint32(buf, uint32(slot.FirstSeq))
		buf = binary.BigEndian.AppendUint32(buf, uint32(slot.LastSeq))
		buf = binary.BigEndian.AppendUint64(buf, uint64(slot.BaseOffset))
		buf = binary.BigEndian.AppendUint32(buf, uint32(slot.LeaderEpoch))
	}
	return buf
}

func decodeProducerStateEntry(raw []byte) (ProducerSnapshotEntry, int, error) {
	if len(raw) < producerStateHeaderSize {
		return ProducerSnapshotEntry{}, 0, io.ErrUnexpectedEOF
	}
	off := 0
	pid := int64(binary.BigEndian.Uint64(raw[off:]))
	off += 8
	lastEpoch := int16(binary.BigEndian.Uint16(raw[off:]))
	off += 2
	lastSeq := int32(binary.BigEndian.Uint32(raw[off:]))
	off += 4
	lastOffset := int64(binary.BigEndian.Uint64(raw[off:]))
	off += 8
	lastLeaderEpoch := int32(binary.BigEndian.Uint32(raw[off:]))
	off += 4
	lastTimestamp := int64(binary.BigEndian.Uint64(raw[off:]))
	off += 8
	ringLen := int(raw[off])
	off++

	if ringLen > IdempotenceRingSize {
		return ProducerSnapshotEntry{}, 0, errors.New("snapshot: ringLen exceeds capacity")
	}
	if len(raw)-off < ringLen*producerStateRingSlotBytes {
		return ProducerSnapshotEntry{}, 0, io.ErrUnexpectedEOF
	}

	log := ProducerBatchLog{
		LastSeq:         lastSeq,
		LastOffset:      lastOffset,
		LastEpoch:       lastEpoch,
		LastLeaderEpoch: lastLeaderEpoch,
		LastTimestamp:   lastTimestamp,
	}
	for i := range log.Ring {
		log.Ring[i].FirstSeq = -1
	}
	for i := 0; i < ringLen; i++ {
		first := int32(binary.BigEndian.Uint32(raw[off:]))
		off += 4
		last := int32(binary.BigEndian.Uint32(raw[off:]))
		off += 4
		base := int64(binary.BigEndian.Uint64(raw[off:]))
		off += 8
		le := int32(binary.BigEndian.Uint32(raw[off:]))
		off += 4
		log.Ring[i] = ProducerRingSlot{
			FirstSeq: first, LastSeq: last, BaseOffset: base, LeaderEpoch: le,
		}
	}
	log.RingHead = ringLen % IdempotenceRingSize

	return ProducerSnapshotEntry{ProducerId: pid, Log: log}, off, nil
}
