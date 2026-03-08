package protocol

import (
	"math/bits"

	"github.com/KhaiHust/kaf-go/protocol/types"
)

// Writer is a zero-allocation, append-only binary buffer
// that knows how to Write Types
type Writer struct {
	buff []byte
}

// NewWriter creates *Writer with a pre-allocate backing memory.
func NewWriter(capacity int) *Writer {
	return &Writer{buff: make([]byte, 0, capacity)}
}

func (w *Writer) Bytes() []byte {
	return w.buff
}

func (w *Writer) Reset() {
	w.buff = w.buff[:0]
}

func (w *Writer) WriteBool(b bool) {
	if b {
		w.buff = append(w.buff, 1)
	} else {
		w.buff = append(w.buff, 0)
	}
}
func (w *Writer) WriteInt8(value int8) {
	w.buff = append(w.buff, byte(value))
}

func (w *Writer) WriteInt16(value int16) {
	w.buff = append(w.buff, byte(value>>8), byte(value))
}

func (w *Writer) WriteInt32(value int32) {
	w.buff = append(w.buff, byte(value>>24), byte(value>>16), byte(value>>8), byte(value))
}

func (w *Writer) WriteInt64(value int64) {
	w.buff = append(w.buff, byte(value>>56), byte(value>>48), byte(value>>40), byte(value>>32),
		byte(value>>24), byte(value>>16), byte(value>>8), byte(value))
}

func (w *Writer) WriteUVarInt(value uint64) {
	for value >= 0x80 {
		w.buff = append(w.buff, byte(value)|0x80)
		value >>= 7
	}
	w.buff = append(w.buff, byte(value))
}

func (w *Writer) WriteVarInt(value int64) {
	w.WriteUVarInt(uint64((value << 1) ^ (value >> 63)))
}

func (w *Writer) WriteUUID(value [16]byte) {
	w.buff = append(w.buff, value[:]...)
}

func (w *Writer) WriteString(value string) {
	w.WriteInt16(int16(len(value)))
	w.buff = append(w.buff, value...)
}

func (w *Writer) WriteCompactString(compactString types.CompactString) {
	n := len(compactString)
	w.WriteUVarInt(uint64(n + 1))
	w.buff = append(w.buff, compactString...)
}

func (w *Writer) WriteNullableString(value types.NullableString) {
	if value == nil {
		w.WriteInt16(-1)
		return
	}
	w.WriteString(*value)
}

func (w *Writer) WriteCompactNullableString(value types.CompactNullableString) {
	if value == nil {
		w.WriteUVarInt(0)
		return
	}
	w.WriteUVarInt(uint64(len(*value) + 1))
	w.buff = append(w.buff, *value...)
}

func (w *Writer) WriteBytes(value types.Bytes) {
	w.WriteInt32(int32(len(value)))
	w.buff = append(w.buff, value...)
}

func (w *Writer) WriteCompactBytes(value types.CompactBytes) {
	w.WriteUVarInt(uint64(len(value) + 1))
	w.buff = append(w.buff, value...)
}

func (w *Writer) WriteNullableBytes(value types.NullableBytes) {
	if value == nil {
		w.WriteInt32(-1)
		return
	}
	w.WriteInt32(int32(len(value)))
	w.buff = append(w.buff, value...)
}

func (w *Writer) WriteCompactNullableBytes(value types.CompactNullableBytes) {
	if value == nil {
		w.WriteUVarInt(0)
		return
	}
	w.WriteUVarInt(uint64(len(value) + 1))
	w.buff = append(w.buff, value...)
}

func (w *Writer) WriteArrayLen(n int) {
	w.WriteInt32(int32(n))
}

func (w *Writer) WriteCompactArrayLen(n int) {
	w.WriteUVarInt(uint64(n) + 1)
}

func (w *Writer) WriteNullableArrayLen(n int, isNull bool) {
	if isNull {
		w.WriteInt32(-1)
		return
	}
	w.WriteInt32(int32(n))
}

func (w *Writer) WriteRaw(b []byte) {
	w.buff = append(w.buff, b...)
}

func (w *Writer) WriteEmptyTaggedFields() {
	w.buff = append(w.buff, 0x00)
}

func UVarIntSize(v uint64) int {
	if v == 0 {
		return 1
	}
	return (bits.Len64(v) + 6) / 7
}
