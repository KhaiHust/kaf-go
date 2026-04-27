package coordinator

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

const (
	producerSnapshotMagic      uint32 = 0x4B41464B
	producerSnapshotVersion    uint16 = 1
	producerSnapshotSuffix            = ".producer.snapshot"
	producerStateHeaderSize           = 35
	producerStateRingSlotBytes        = 20
)

var crcCastagnoli = crc32.MakeTable(crc32.Castagnoli)

func snapshotFileName(snapshotOffset int64) string {
	return fmt.Sprintf("%020d%s", snapshotOffset, producerSnapshotSuffix)
}

func WriteProducerSnapshot(dir string, snapshotOffset int64, entries []ProducerSnapshotEntry) error {
	if dir == "" {
		return nil
	}
	path := filepath.Join(dir, snapshotFileName(snapshotOffset))
	tmp := path + ".tmp"

	cap0 := 4 + 2 + 8 + 4 + 4 + len(entries)*(producerStateHeaderSize+IdempotenceRingSize*producerStateRingSlotBytes)
	buf := make([]byte, 0, cap0)
	buf = binary.BigEndian.AppendUint32(buf, producerSnapshotMagic)
	buf = binary.BigEndian.AppendUint16(buf, producerSnapshotVersion)
	buf = binary.BigEndian.AppendUint64(buf, uint64(snapshotOffset))
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(entries)))
	for _, e := range entries {
		buf = encodeProducerStateEntry(buf, &e)
	}
	checksum := crc32.Checksum(buf, crcCastagnoli)
	buf = binary.BigEndian.AppendUint32(buf, checksum)

	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	if _, werr := f.Write(buf); werr != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return werr
	}
	if serr := f.Sync(); serr != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return serr
	}
	if cerr := f.Close(); cerr != nil {
		_ = os.Remove(tmp)
		return cerr
	}
	return os.Rename(tmp, path)
}

func LoadLatestProducerSnapshot(dir string, maxBase int64) (int64, []ProducerSnapshotEntry, error) {
	if dir == "" {
		return 0, nil, nil
	}
	files, err := listSnapshotFiles(dir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return 0, nil, nil
		}
		return 0, nil, err
	}

	for i := len(files) - 1; i >= 0; i-- {
		base := files[i]
		if base > maxBase {
			continue
		}
		entries, derr := decodeSnapshotFile(filepath.Join(dir, snapshotFileName(base)), base)
		if derr != nil {
			continue
		}
		return base, entries, nil
	}
	return 0, nil, nil
}

func ListProducerSnapshotOffsets(dir string) ([]int64, error) {
	return listSnapshotFiles(dir)
}

func DeleteProducerSnapshot(dir string, base int64) error {
	return os.Remove(filepath.Join(dir, snapshotFileName(base)))
}

func listSnapshotFiles(dir string) ([]int64, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	bases := make([]int64, 0, len(entries))
	for _, ent := range entries {
		if ent.IsDir() {
			continue
		}
		name := ent.Name()
		if !strings.HasSuffix(name, producerSnapshotSuffix) {
			continue
		}
		prefix := strings.TrimSuffix(name, producerSnapshotSuffix)
		base, perr := strconv.ParseInt(prefix, 10, 64)
		if perr != nil {
			continue
		}
		bases = append(bases, base)
	}
	sort.Slice(bases, func(i, j int) bool { return bases[i] < bases[j] })
	return bases, nil
}

func decodeSnapshotFile(path string, expectedBase int64) ([]ProducerSnapshotEntry, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	if len(raw) < 4+2+8+4+4 {
		return nil, io.ErrUnexpectedEOF
	}

	body := raw[:len(raw)-4]
	want := binary.BigEndian.Uint32(raw[len(raw)-4:])
	if got := crc32.Checksum(body, crcCastagnoli); got != want {
		return nil, errors.New("snapshot: crc mismatch")
	}

	off := 0
	if magic := binary.BigEndian.Uint32(body[off:]); magic != producerSnapshotMagic {
		return nil, errors.New("snapshot: bad magic")
	}
	off += 4
	if ver := binary.BigEndian.Uint16(body[off:]); ver != producerSnapshotVersion {
		return nil, fmt.Errorf("snapshot: unsupported version %d", ver)
	}
	off += 2
	storedBase := int64(binary.BigEndian.Uint64(body[off:]))
	off += 8
	if storedBase != expectedBase {
		return nil, fmt.Errorf("snapshot: base offset mismatch (file=%d, header=%d)", expectedBase, storedBase)
	}
	count := int(binary.BigEndian.Uint32(body[off:]))
	off += 4

	entries := make([]ProducerSnapshotEntry, 0, count)
	for i := 0; i < count; i++ {
		e, consumed, derr := decodeProducerStateEntry(body[off:])
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
