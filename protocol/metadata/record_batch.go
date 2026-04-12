package metadata

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"io"
	"time"
)

var crc32cTable = crc32.MakeTable(crc32.Castagnoli)

// EncodeAsRecordBatch wraps a single record value in a Kafka RecordBatch (magic=2).
// The baseOffset field is set to 0 and will be patched by commitlog.AppendRaw.
func EncodeAsRecordBatch(value []byte) []byte {
	rec := encodeRecord(value)

	now := time.Now().UnixMilli()
	var afterCRC bytes.Buffer
	writeInt16BE(&afterCRC, 0)   // attributes
	writeInt32BE(&afterCRC, 0)   // lastOffsetDelta
	writeInt64BE(&afterCRC, now) // baseTimestamp
	writeInt64BE(&afterCRC, now) // maxTimestamp
	writeInt64BE(&afterCRC, -1)  // producerId
	writeInt16BE(&afterCRC, -1)  // producerEpoch
	writeInt32BE(&afterCRC, -1)  // baseSequence
	writeInt32BE(&afterCRC, 1)   // numRecords
	afterCRC.Write(rec)

	afterCRCBytes := afterCRC.Bytes()
	crc := crc32.Checksum(afterCRCBytes, crc32cTable)

	// batchLength = 4 (partitionLeaderEpoch) + 1 (magic) + 4 (crc) + len(afterCRC)
	batchLength := int32(4 + 1 + 4 + len(afterCRCBytes))

	var batch bytes.Buffer
	writeInt64BE(&batch, 0)           // baseOffset (patched by AppendRaw)
	writeInt32BE(&batch, batchLength) // batchLength
	writeInt32BE(&batch, -1)          // partitionLeaderEpoch
	batch.WriteByte(2)                // magic = 2
	writeUint32BE(&batch, crc)
	batch.Write(afterCRCBytes)

	return batch.Bytes()
}

// DecodeRecordValues parses raw RecordBatch bytes (as served by the Fetch API)
// and returns the value bytes of each record.
func DecodeRecordValues(raw []byte) [][]byte {
	var result [][]byte
	pos := 0
	for pos < len(raw) {
		if len(raw)-pos < 61 {
			break
		}
		// batchLength at offset 8 from batch start
		batchLen := int(binary.BigEndian.Uint32(raw[pos+8:]))
		totalSize := 12 + batchLen // 8 (baseOffset) + 4 (batchLength) + rest
		if pos+totalSize > len(raw) {
			break
		}
		batchBytes := raw[pos : pos+totalSize]
		pos += totalSize

		// numRecords at byte offset 57 within the batch
		numRecords := int(binary.BigEndian.Uint32(batchBytes[57:]))
		if numRecords <= 0 {
			continue
		}

		// Records start at byte offset 61
		bs := &byteScanner{data: batchBytes[61:], pos: 0}
		for i := 0; i < numRecords; i++ {
			// length (zigzag varint)
			if _, err := bs.readZigzagVarInt(); err != nil {
				break
			}
			// attributes (int8)
			if _, err := bs.readByte(); err != nil {
				break
			}
			// timestampDelta (zigzag varlong)
			if _, err := bs.readZigzagVarInt(); err != nil {
				break
			}
			// offsetDelta (zigzag varint)
			if _, err := bs.readZigzagVarInt(); err != nil {
				break
			}
			// keyLength (zigzag varint, -1 = null)
			keyLen, err := bs.readZigzagVarInt()
			if err != nil {
				break
			}
			if keyLen > 0 {
				bs.pos += int(keyLen)
			}
			// valueLength (zigzag varint)
			valLen, err := bs.readZigzagVarInt()
			if err != nil {
				break
			}
			if valLen > 0 && bs.pos+int(valLen) <= len(bs.data) {
				value := make([]byte, valLen)
				copy(value, bs.data[bs.pos:])
				bs.pos += int(valLen)
				result = append(result, value)
			}
			// headersCount (zigzag varint)
			headerCount, err := bs.readZigzagVarInt()
			if err != nil {
				break
			}
			for h := int64(0); h < headerCount; h++ {
				kLen, _ := bs.readZigzagVarInt()
				bs.pos += int(kLen)
				vLen, _ := bs.readZigzagVarInt()
				bs.pos += int(vLen)
			}
		}
	}
	return result
}

type byteScanner struct {
	data []byte
	pos  int
}

func (bs *byteScanner) readByte() (byte, error) {
	if bs.pos >= len(bs.data) {
		return 0, io.EOF
	}
	b := bs.data[bs.pos]
	bs.pos++
	return b, nil
}

func (bs *byteScanner) readZigzagVarInt() (int64, error) {
	var uv uint64
	var shift uint
	for {
		if bs.pos >= len(bs.data) {
			return 0, io.EOF
		}
		b := bs.data[bs.pos]
		bs.pos++
		uv |= uint64(b&0x7f) << shift
		if b < 0x80 {
			break
		}
		shift += 7
		if shift >= 64 {
			return 0, io.EOF
		}
	}
	return int64((uv >> 1) ^ -(uv & 1)), nil
}

func encodeRecord(value []byte) []byte {
	var body bytes.Buffer
	body.WriteByte(0)                           // attributes
	writeZigzagVarInt(&body, 0)                 // timestampDelta
	writeZigzagVarInt(&body, 0)                 // offsetDelta
	writeZigzagVarInt(&body, -1)                // keyLength (null)
	writeZigzagVarInt(&body, int64(len(value))) // valueLength
	body.Write(value)
	writeZigzagVarInt(&body, 0) // headersCount

	var rec bytes.Buffer
	writeZigzagVarInt(&rec, int64(body.Len())) // length
	rec.Write(body.Bytes())
	return rec.Bytes()
}

func writeZigzagVarInt(buf *bytes.Buffer, v int64) {
	uv := uint64((v << 1) ^ (v >> 63))
	for uv >= 0x80 {
		buf.WriteByte(byte(uv) | 0x80)
		uv >>= 7
	}
	buf.WriteByte(byte(uv))
}

func writeInt16BE(buf *bytes.Buffer, v int16) {
	var b [2]byte
	binary.BigEndian.PutUint16(b[:], uint16(v))
	buf.Write(b[:])
}

func writeInt32BE(buf *bytes.Buffer, v int32) {
	var b [4]byte
	binary.BigEndian.PutUint32(b[:], uint32(v))
	buf.Write(b[:])
}

func writeInt64BE(buf *bytes.Buffer, v int64) {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], uint64(v))
	buf.Write(b[:])
}

func writeUint32BE(buf *bytes.Buffer, v uint32) {
	var b [4]byte
	binary.BigEndian.PutUint32(b[:], v)
	buf.Write(b[:])
}
