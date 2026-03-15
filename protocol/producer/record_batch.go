package producer

import (
	"encoding/binary"
	"fmt"

	"github.com/KhaiHust/kaf-go/protocol"
	"github.com/KhaiHust/kaf-go/protocol/types"
)

type RecordBatch struct {
	BaseOffset           int64
	PartitionLeaderEpoch int32
	Magic                int8
	CRC                  uint32
	Attributes           int16
	LastOffsetDelta      int32
	FirstTimestamp       int64
	MaxTimestamp         int64
	ProducerId           int64
	ProducerEpoch        int16
	BaseSequence         int32
	Records              []BatchRecord
}

type BatchRecord struct {
	TimestampDelta types.Varlong
	OffsetDelta    types.Varint
	Key            []byte
	Value          []byte
	Headers        map[string][]byte
}

func DecodeRecordBatch(raw []byte) (*RecordBatch, error) {
	r := protocol.NewReader(raw)
	batch := &RecordBatch{}
	var err error

	if batch.BaseOffset, err = r.ReadInt64(); err != nil {
		return nil, err
	}
	// BatchLength (skip, we have the full bytes already)
	if _, err = r.ReadInt32(); err != nil {
		return nil, err
	}
	if batch.PartitionLeaderEpoch, err = r.ReadInt32(); err != nil {
		return nil, err
	}
	if batch.Magic, err = r.ReadInt8(); err != nil {
		return nil, err
	}
	// CRC covers everything after this field
	crcBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(crcBytes, 0)
	if batch.CRC, err = r.ReadUint32(); err != nil {
		return nil, err
	}
	if batch.Attributes, err = r.ReadInt16(); err != nil {
		return nil, err
	}
	if batch.LastOffsetDelta, err = r.ReadInt32(); err != nil {
		return nil, err
	}
	if batch.FirstTimestamp, err = r.ReadInt64(); err != nil {
		return nil, err
	}
	if batch.MaxTimestamp, err = r.ReadInt64(); err != nil {
		return nil, err
	}
	if batch.ProducerId, err = r.ReadInt64(); err != nil {
		return nil, err
	}
	if batch.ProducerEpoch, err = r.ReadInt16(); err != nil {
		return nil, err
	}
	if batch.BaseSequence, err = r.ReadInt32(); err != nil {
		return nil, err
	}

	// Records count is plain INT32 (not compact) inside RecordBatch
	recordCount, err := r.ReadInt32()
	if err != nil {
		return nil, err
	}

	batch.Records = make([]BatchRecord, recordCount)
	for i := range batch.Records {
		batch.Records[i], err = decodeRecord(r, batch.FirstTimestamp)
		if err != nil {
			return nil, fmt.Errorf("record %d: %w", i, err)
		}
	}
	return batch, nil
}

func decodeRecord(r *protocol.Reader, baseTimestamp int64) (BatchRecord, error) {
	var rec BatchRecord
	var err error

	// Length (VarInt, skip)
	if _, err = r.ReadVarInt(); err != nil {
		return rec, err
	}
	// Attributes (INT8, always 0 for now)
	if _, err = r.ReadInt8(); err != nil {
		return rec, err
	}
	// TimestampDelta (VarInt zigzag)
	tsDelta, err := r.ReadVarlong()
	if err != nil {
		return rec, err
	}
	rec.TimestampDelta = tsDelta

	// OffsetDelta
	offDelta, err := r.ReadVarint()
	if err != nil {
		return rec, err
	}
	rec.OffsetDelta = offDelta

	// Key: VarInt length (-1 = null)
	keyLen, err := r.ReadVarInt()
	if err != nil {
		return rec, err
	}
	if keyLen > 0 {
		rec.Key = make([]byte, keyLen)
		for i := range rec.Key {
			b, err := r.ReadInt8()
			if err != nil {
				return rec, err
			}
			rec.Key[i] = byte(b)
		}
	}

	// Value
	valLen, err := r.ReadVarInt()
	if err != nil {
		return rec, err
	}
	if valLen > 0 {
		rec.Value = make([]byte, valLen)
		for i := range rec.Value {
			b, err := r.ReadInt8()
			if err != nil {
				return rec, err
			}
			rec.Value[i] = byte(b)
		}
	}

	// Headers
	headerCount, err := r.ReadVarInt()
	if err != nil {
		return rec, err
	}
	rec.Headers = make(map[string][]byte, headerCount)
	for i := int64(0); i < headerCount; i++ {
		kLen, _ := r.ReadVarInt()
		k := make([]byte, kLen)
		for j := range k {
			b, _ := r.ReadInt8()
			k[j] = byte(b)
		}
		vLen, _ := r.ReadVarInt()
		v := make([]byte, vLen)
		for j := range v {
			b, _ := r.ReadInt8()
			v[j] = byte(b)
		}
		rec.Headers[string(k)] = v
	}
	return rec, nil
}
