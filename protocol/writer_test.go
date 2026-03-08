package protocol

import (
	"bytes"
	"testing"

	"github.com/KhaiHust/kaf-go/protocol/types"
)

func TestNewWriter(t *testing.T) {
	w := NewWriter(100)

	if w == nil {
		t.Fatal("NewWriter() returned nil")
	}

	if len(w.Bytes()) != 0 {
		t.Errorf("NewWriter() initial length = %d, want 0", len(w.Bytes()))
	}

	if cap(w.buff) != 100 {
		t.Errorf("NewWriter() capacity = %d, want 100", cap(w.buff))
	}
}

func TestWriter_Bytes(t *testing.T) {
	w := NewWriter(10)
	w.WriteInt8(1)
	w.WriteInt8(2)

	got := w.Bytes()
	want := []byte{1, 2}

	if !bytes.Equal(got, want) {
		t.Errorf("Bytes() = %v, want %v", got, want)
	}
}

func TestWriter_Reset(t *testing.T) {
	w := NewWriter(10)
	w.WriteInt8(1)
	w.WriteInt8(2)
	w.WriteInt8(3)

	w.Reset()

	if len(w.Bytes()) != 0 {
		t.Errorf("Reset() length = %d, want 0", len(w.Bytes()))
	}

	// Capacity should be preserved
	if cap(w.buff) < 10 {
		t.Errorf("Reset() should preserve capacity")
	}
}

func TestWriter_WriteBool(t *testing.T) {
	tests := []struct {
		name  string
		value bool
		want  []byte
	}{
		{"true", true, []byte{0x01}},
		{"false", false, []byte{0x00}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := NewWriter(10)
			w.WriteBool(tt.value)

			if !bytes.Equal(w.Bytes(), tt.want) {
				t.Errorf("WriteBool(%v) = %v, want %v", tt.value, w.Bytes(), tt.want)
			}
		})
	}
}

func TestWriter_WriteInt8(t *testing.T) {
	tests := []struct {
		name  string
		value int8
		want  []byte
	}{
		{"zero", 0, []byte{0x00}},
		{"positive", 127, []byte{0x7F}},
		{"negative", -1, []byte{0xFF}},
		{"negative min", -128, []byte{0x80}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := NewWriter(10)
			w.WriteInt8(tt.value)

			if !bytes.Equal(w.Bytes(), tt.want) {
				t.Errorf("WriteInt8(%d) = %v, want %v", tt.value, w.Bytes(), tt.want)
			}
		})
	}
}

func TestWriter_WriteInt16(t *testing.T) {
	tests := []struct {
		name  string
		value int16
		want  []byte
	}{
		{"zero", 0, []byte{0x00, 0x00}},
		{"positive", 256, []byte{0x01, 0x00}},
		{"max", 32767, []byte{0x7F, 0xFF}},
		{"negative", -1, []byte{0xFF, 0xFF}},
		{"negative min", -32768, []byte{0x80, 0x00}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := NewWriter(10)
			w.WriteInt16(tt.value)

			if !bytes.Equal(w.Bytes(), tt.want) {
				t.Errorf("WriteInt16(%d) = %v, want %v", tt.value, w.Bytes(), tt.want)
			}
		})
	}
}

func TestWriter_WriteInt32(t *testing.T) {
	tests := []struct {
		name  string
		value int32
		want  []byte
	}{
		{"zero", 0, []byte{0x00, 0x00, 0x00, 0x00}},
		{"positive", 16777216, []byte{0x01, 0x00, 0x00, 0x00}},
		{"max", 2147483647, []byte{0x7F, 0xFF, 0xFF, 0xFF}},
		{"negative", -1, []byte{0xFF, 0xFF, 0xFF, 0xFF}},
		{"negative min", -2147483648, []byte{0x80, 0x00, 0x00, 0x00}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := NewWriter(10)
			w.WriteInt32(tt.value)

			if !bytes.Equal(w.Bytes(), tt.want) {
				t.Errorf("WriteInt32(%d) = %v, want %v", tt.value, w.Bytes(), tt.want)
			}
		})
	}
}

func TestWriter_WriteInt64(t *testing.T) {
	tests := []struct {
		name  string
		value int64
		want  []byte
	}{
		{"zero", 0, []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}},
		{"positive", 72057594037927936, []byte{0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}},
		{"max", 9223372036854775807, []byte{0x7F, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF}},
		{"negative", -1, []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF}},
		{"negative min", -9223372036854775808, []byte{0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := NewWriter(10)
			w.WriteInt64(tt.value)

			if !bytes.Equal(w.Bytes(), tt.want) {
				t.Errorf("WriteInt64(%d) = %v, want %v", tt.value, w.Bytes(), tt.want)
			}
		})
	}
}

func TestWriter_WriteUVarInt(t *testing.T) {
	tests := []struct {
		name  string
		value uint64
		want  []byte
	}{
		{"zero", 0, []byte{0x00}},
		{"one", 1, []byte{0x01}},
		{"127", 127, []byte{0x7F}},
		{"128", 128, []byte{0x80, 0x01}},
		{"255", 255, []byte{0xFF, 0x01}},
		{"300", 300, []byte{0xAC, 0x02}},
		{"16383", 16383, []byte{0xFF, 0x7F}},
		{"16384", 16384, []byte{0x80, 0x80, 0x01}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := NewWriter(20)
			w.WriteUVarInt(tt.value)

			if !bytes.Equal(w.Bytes(), tt.want) {
				t.Errorf("WriteUVarInt(%d) = %v, want %v", tt.value, w.Bytes(), tt.want)
			}
		})
	}
}

func TestWriter_WriteVarInt(t *testing.T) {
	tests := []struct {
		name  string
		value int64
		want  []byte
	}{
		{"zero", 0, []byte{0x00}},
		{"positive one", 1, []byte{0x02}},
		{"negative one", -1, []byte{0x01}},
		{"positive 63", 63, []byte{0x7E}},
		{"negative 64", -64, []byte{0x7F}},
		{"positive 64", 64, []byte{0x80, 0x01}},
		{"negative 65", -65, []byte{0x81, 0x01}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := NewWriter(20)
			w.WriteVarInt(tt.value)

			if !bytes.Equal(w.Bytes(), tt.want) {
				t.Errorf("WriteVarInt(%d) = %v, want %v", tt.value, w.Bytes(), tt.want)
			}
		})
	}
}

func TestWriter_WriteUUID(t *testing.T) {
	uuid := [16]byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
		0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F, 0x10}

	w := NewWriter(20)
	w.WriteUUID(uuid)

	got := w.Bytes()
	want := uuid[:]

	if !bytes.Equal(got, want) {
		t.Errorf("WriteUUID() = %v, want %v", got, want)
	}

	if len(got) != 16 {
		t.Errorf("WriteUUID() length = %d, want 16", len(got))
	}
}

func TestWriter_WriteUUID_Zero(t *testing.T) {
	uuid := [16]byte{}

	w := NewWriter(20)
	w.WriteUUID(uuid)

	got := w.Bytes()
	want := make([]byte, 16)

	if !bytes.Equal(got, want) {
		t.Errorf("WriteUUID(zero) = %v, want %v", got, want)
	}
}

func TestWriter_WriteString(t *testing.T) {
	tests := []struct {
		name  string
		value string
		want  []byte
	}{
		{"empty", "", []byte{0x00, 0x00}},
		{"hello", "hello", []byte{0x00, 0x05, 'h', 'e', 'l', 'l', 'o'}},
		{"single char", "a", []byte{0x00, 0x01, 'a'}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := NewWriter(20)
			w.WriteString(tt.value)

			if !bytes.Equal(w.Bytes(), tt.want) {
				t.Errorf("WriteString(%q) = %v, want %v", tt.value, w.Bytes(), tt.want)
			}
		})
	}
}

func TestWriter_WriteCompactString(t *testing.T) {
	tests := []struct {
		name  string
		value types.CompactString
		want  []byte
	}{
		{"empty", "", []byte{0x01}},                               // length 0 + 1 = 1
		{"hello", "hello", []byte{0x06, 'h', 'e', 'l', 'l', 'o'}}, // length 5 + 1 = 6
		{"single char", "a", []byte{0x02, 'a'}},                   // length 1 + 1 = 2
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := NewWriter(20)
			w.WriteCompactString(tt.value)

			if !bytes.Equal(w.Bytes(), tt.want) {
				t.Errorf("WriteCompactString(%q) = %v, want %v", tt.value, w.Bytes(), tt.want)
			}
		})
	}
}

func TestWriter_WriteNullableString(t *testing.T) {
	hello := "hello"
	empty := ""

	tests := []struct {
		name  string
		value types.NullableString
		want  []byte
	}{
		{"null", nil, []byte{0xFF, 0xFF}}, // -1 in big endian
		{"hello", &hello, []byte{0x00, 0x05, 'h', 'e', 'l', 'l', 'o'}},
		{"empty", &empty, []byte{0x00, 0x00}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := NewWriter(20)
			w.WriteNullableString(tt.value)

			if !bytes.Equal(w.Bytes(), tt.want) {
				t.Errorf("WriteNullableString(%v) = %v, want %v", tt.value, w.Bytes(), tt.want)
			}
		})
	}
}

func TestWriter_WriteCompactNullableString(t *testing.T) {
	hello := "hello"
	empty := ""

	tests := []struct {
		name  string
		value types.CompactNullableString
		want  []byte
	}{
		{"null", nil, []byte{0x00}},                              // 0 indicates null
		{"hello", &hello, []byte{0x06, 'h', 'e', 'l', 'l', 'o'}}, // length+1, then string with its length prefix
		{"empty", &empty, []byte{0x01}},                          // length+1=1, then empty string
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := NewWriter(20)
			w.WriteCompactNullableString(tt.value)

			if !bytes.Equal(w.Bytes(), tt.want) {
				t.Errorf("WriteCompactNullableString(%v) = %v, want %v", tt.value, w.Bytes(), tt.want)
			}
		})
	}
}

func TestWriter_WriteBytes(t *testing.T) {
	tests := []struct {
		name  string
		value types.Bytes
		want  []byte
	}{
		{"empty", types.Bytes{}, []byte{0x00, 0x00, 0x00, 0x00}},
		{"data", types.Bytes{0x01, 0x02, 0x03}, []byte{0x00, 0x00, 0x00, 0x03, 0x01, 0x02, 0x03}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := NewWriter(20)
			w.WriteBytes(tt.value)

			if !bytes.Equal(w.Bytes(), tt.want) {
				t.Errorf("WriteBytes(%v) = %v, want %v", tt.value, w.Bytes(), tt.want)
			}
		})
	}
}

func TestWriter_WriteCompactBytes(t *testing.T) {
	tests := []struct {
		name  string
		value types.CompactBytes
		want  []byte
	}{
		{"empty", types.CompactBytes{}, []byte{0x01}},                                  // length 0 + 1 = 1
		{"data", types.CompactBytes{0x01, 0x02, 0x03}, []byte{0x04, 0x01, 0x02, 0x03}}, // length 3 + 1 = 4
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := NewWriter(20)
			w.WriteCompactBytes(tt.value)

			if !bytes.Equal(w.Bytes(), tt.want) {
				t.Errorf("WriteCompactBytes(%v) = %v, want %v", tt.value, w.Bytes(), tt.want)
			}
		})
	}
}

func TestWriter_WriteNullableBytes(t *testing.T) {
	tests := []struct {
		name  string
		value types.NullableBytes
		want  []byte
	}{
		{"null", nil, []byte{0xFF, 0xFF, 0xFF, 0xFF}}, // -1 in big endian int32
		{"empty", types.NullableBytes{}, []byte{0x00, 0x00, 0x00, 0x00}},
		{"data", types.NullableBytes{0x01, 0x02, 0x03}, []byte{0x00, 0x00, 0x00, 0x03, 0x01, 0x02, 0x03}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := NewWriter(20)
			w.WriteNullableBytes(tt.value)

			if !bytes.Equal(w.Bytes(), tt.want) {
				t.Errorf("WriteNullableBytes(%v) = %v, want %v", tt.value, w.Bytes(), tt.want)
			}
		})
	}
}

func TestWriter_WriteCompactNullableBytes(t *testing.T) {
	tests := []struct {
		name  string
		value types.CompactNullableBytes
		want  []byte
	}{
		{"null", nil, []byte{0x00}},                                                            // 0 indicates null
		{"empty", types.CompactNullableBytes{}, []byte{0x01}},                                  // length 0 + 1 = 1
		{"data", types.CompactNullableBytes{0x01, 0x02, 0x03}, []byte{0x04, 0x01, 0x02, 0x03}}, // length 3 + 1 = 4
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := NewWriter(20)
			w.WriteCompactNullableBytes(tt.value)

			if !bytes.Equal(w.Bytes(), tt.want) {
				t.Errorf("WriteCompactNullableBytes(%v) = %v, want %v", tt.value, w.Bytes(), tt.want)
			}
		})
	}
}

func TestWriter_MultipleWrites(t *testing.T) {
	w := NewWriter(100)

	w.WriteInt8(1)
	w.WriteInt16(256)
	w.WriteInt32(65536)

	want := []byte{
		0x01,       // int8: 1
		0x01, 0x00, // int16: 256
		0x00, 0x01, 0x00, 0x00, // int32: 65536
	}

	if !bytes.Equal(w.Bytes(), want) {
		t.Errorf("MultipleWrites = %v, want %v", w.Bytes(), want)
	}
}

func TestWriter_ResetAndReuse(t *testing.T) {
	w := NewWriter(100)

	// First write
	w.WriteInt32(12345)
	if len(w.Bytes()) != 4 {
		t.Errorf("First write length = %d, want 4", len(w.Bytes()))
	}

	// Reset
	w.Reset()
	if len(w.Bytes()) != 0 {
		t.Errorf("After Reset length = %d, want 0", len(w.Bytes()))
	}

	// Second write
	w.WriteInt16(100)
	want := []byte{0x00, 0x64}
	if !bytes.Equal(w.Bytes(), want) {
		t.Errorf("After Reset and reuse = %v, want %v", w.Bytes(), want)
	}
}

func TestWriter_GrowsBeyondCapacity(t *testing.T) {
	w := NewWriter(2) // Small initial capacity

	// Write more than capacity
	w.WriteInt32(12345)
	w.WriteInt32(67890)

	if len(w.Bytes()) != 8 {
		t.Errorf("Bytes length = %d, want 8", len(w.Bytes()))
	}
}

func TestWriter_WriteArrayLen(t *testing.T) {
	tests := []struct {
		name  string
		value int
		want  []byte
	}{
		{"zero", 0, []byte{0x00, 0x00, 0x00, 0x00}},
		{"positive", 5, []byte{0x00, 0x00, 0x00, 0x05}},
		{"large", 1000, []byte{0x00, 0x00, 0x03, 0xE8}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := NewWriter(10)
			w.WriteArrayLen(tt.value)

			if !bytes.Equal(w.Bytes(), tt.want) {
				t.Errorf("WriteArrayLen(%d) = %v, want %v", tt.value, w.Bytes(), tt.want)
			}
		})
	}
}

func TestWriter_WriteCompactArrayLen(t *testing.T) {
	tests := []struct {
		name  string
		value int
		want  []byte
	}{
		{"zero", 0, []byte{0x01}},        // 0 + 1 = 1
		{"one", 1, []byte{0x02}},         // 1 + 1 = 2
		{"five", 5, []byte{0x06}},        // 5 + 1 = 6
		{"127", 127, []byte{0x80, 0x01}}, // 128 in uvarint
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := NewWriter(10)
			w.WriteCompactArrayLen(tt.value)

			if !bytes.Equal(w.Bytes(), tt.want) {
				t.Errorf("WriteCompactArrayLen(%d) = %v, want %v", tt.value, w.Bytes(), tt.want)
			}
		})
	}
}

func TestWriter_WriteNullableArrayLen(t *testing.T) {
	tests := []struct {
		name   string
		n      int
		isNull bool
		want   []byte
	}{
		{"null", 0, true, []byte{0xFF, 0xFF, 0xFF, 0xFF}},
		{"zero", 0, false, []byte{0x00, 0x00, 0x00, 0x00}},
		{"five", 5, false, []byte{0x00, 0x00, 0x00, 0x05}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := NewWriter(10)
			w.WriteNullableArrayLen(tt.n, tt.isNull)

			if !bytes.Equal(w.Bytes(), tt.want) {
				t.Errorf("WriteNullableArrayLen(%d, %v) = %v, want %v", tt.n, tt.isNull, w.Bytes(), tt.want)
			}
		})
	}
}

func TestWriter_WriteRaw(t *testing.T) {
	w := NewWriter(20)

	w.WriteRaw([]byte{0x01, 0x02, 0x03})
	w.WriteRaw([]byte{0x04, 0x05})

	want := []byte{0x01, 0x02, 0x03, 0x04, 0x05}
	if !bytes.Equal(w.Bytes(), want) {
		t.Errorf("WriteRaw() = %v, want %v", w.Bytes(), want)
	}
}

func TestWriter_WriteEmptyTaggedFields(t *testing.T) {
	w := NewWriter(10)
	w.WriteEmptyTaggedFields()

	want := []byte{0x00}
	if !bytes.Equal(w.Bytes(), want) {
		t.Errorf("WriteEmptyTaggedFields() = %v, want %v", w.Bytes(), want)
	}
}

func TestUVarIntSize(t *testing.T) {
	tests := []struct {
		name  string
		value uint64
		want  int
	}{
		{"zero", 0, 1},
		{"one", 1, 1},
		{"127", 127, 1},
		{"128", 128, 2},
		{"16383", 16383, 2},
		{"16384", 16384, 3},
		{"2097151", 2097151, 3},
		{"2097152", 2097152, 4},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := UVarIntSize(tt.value)
			if got != tt.want {
				t.Errorf("UVarIntSize(%d) = %d, want %d", tt.value, got, tt.want)
			}
		})
	}
}

// Benchmark tests

func BenchmarkWriter_WriteInt8(b *testing.B) {
	w := NewWriter(b.N)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		w.WriteInt8(int8(i))
	}
}

func BenchmarkWriter_WriteInt32(b *testing.B) {
	w := NewWriter(b.N * 4)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		w.WriteInt32(int32(i))
	}
}

func BenchmarkWriter_WriteInt64(b *testing.B) {
	w := NewWriter(b.N * 8)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		w.WriteInt64(int64(i))
	}
}

func BenchmarkWriter_WriteUVarInt(b *testing.B) {
	w := NewWriter(b.N * 10)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		w.WriteUVarInt(uint64(i))
	}
}

func BenchmarkWriter_WriteString(b *testing.B) {
	w := NewWriter(b.N * 20)
	str := "hello world"
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		w.WriteString(str)
	}
}

func BenchmarkWriter_WriteBytes(b *testing.B) {
	w := NewWriter(b.N * 20)
	data := types.Bytes{0x01, 0x02, 0x03, 0x04, 0x05}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		w.WriteBytes(data)
	}
}
