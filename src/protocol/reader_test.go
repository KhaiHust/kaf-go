package protocol

import (
	"io"
	"testing"

	"github.com/gofrs/uuid/v5"
)

func TestNewReader(t *testing.T) {
	data := []byte{0x01, 0x02, 0x03}
	r := NewReader(data)

	if r == nil {
		t.Fatal("NewReader() returned nil")
	}

	if r.Remaining() != 3 {
		t.Errorf("Remaining() = %d, want 3", r.Remaining())
	}

	if r.Done() {
		t.Error("Done() should be false for non-empty buffer")
	}
}

func TestReader_Remaining(t *testing.T) {
	data := []byte{0x01, 0x02, 0x03, 0x04, 0x05}
	r := NewReader(data)

	if r.Remaining() != 5 {
		t.Errorf("Remaining() = %d, want 5", r.Remaining())
	}

	r.ReadInt8()
	if r.Remaining() != 4 {
		t.Errorf("Remaining() after ReadInt8 = %d, want 4", r.Remaining())
	}

	r.ReadInt16()
	if r.Remaining() != 2 {
		t.Errorf("Remaining() after ReadInt16 = %d, want 2", r.Remaining())
	}
}

func TestReader_Done(t *testing.T) {
	data := []byte{0x01}
	r := NewReader(data)

	if r.Done() {
		t.Error("Done() should be false before reading")
	}

	r.ReadInt8()

	if !r.Done() {
		t.Error("Done() should be true after reading all data")
	}
}

func TestReader_Done_EmptyBuffer(t *testing.T) {
	r := NewReader([]byte{})

	if !r.Done() {
		t.Error("Done() should be true for empty buffer")
	}
}

func TestReader_ReadBool(t *testing.T) {
	tests := []struct {
		name    string
		data    []byte
		want    bool
		wantErr bool
	}{
		{"true", []byte{0x01}, true, false},
		{"false", []byte{0x00}, false, false},
		{"non-zero is true", []byte{0xFF}, true, false},
		{"empty", []byte{}, false, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := NewReader(tt.data)
			got, err := r.ReadBool()

			if (err != nil) != tt.wantErr {
				t.Errorf("ReadBool() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !tt.wantErr && got != tt.want {
				t.Errorf("ReadBool() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestReader_ReadInt8(t *testing.T) {
	tests := []struct {
		name    string
		data    []byte
		want    int8
		wantErr bool
	}{
		{"zero", []byte{0x00}, 0, false},
		{"positive", []byte{0x7F}, 127, false},
		{"negative", []byte{0xFF}, -1, false},
		{"negative min", []byte{0x80}, -128, false},
		{"empty", []byte{}, 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := NewReader(tt.data)
			got, err := r.ReadInt8()

			if (err != nil) != tt.wantErr {
				t.Errorf("ReadInt8() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !tt.wantErr && got != tt.want {
				t.Errorf("ReadInt8() = %d, want %d", got, tt.want)
			}
		})
	}
}

func TestReader_ReadInt16(t *testing.T) {
	tests := []struct {
		name    string
		data    []byte
		want    int16
		wantErr bool
	}{
		{"zero", []byte{0x00, 0x00}, 0, false},
		{"positive", []byte{0x01, 0x00}, 256, false},
		{"max", []byte{0x7F, 0xFF}, 32767, false},
		{"negative", []byte{0xFF, 0xFF}, -1, false},
		{"negative min", []byte{0x80, 0x00}, -32768, false},
		{"too short", []byte{0x00}, 0, true},
		{"empty", []byte{}, 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := NewReader(tt.data)
			got, err := r.ReadInt16()

			if (err != nil) != tt.wantErr {
				t.Errorf("ReadInt16() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !tt.wantErr && got != tt.want {
				t.Errorf("ReadInt16() = %d, want %d", got, tt.want)
			}
		})
	}
}

func TestReader_ReadInt32(t *testing.T) {
	tests := []struct {
		name    string
		data    []byte
		want    int32
		wantErr bool
	}{
		{"zero", []byte{0x00, 0x00, 0x00, 0x00}, 0, false},
		{"positive", []byte{0x01, 0x00, 0x00, 0x00}, 16777216, false},
		{"max", []byte{0x7F, 0xFF, 0xFF, 0xFF}, 2147483647, false},
		{"negative", []byte{0xFF, 0xFF, 0xFF, 0xFF}, -1, false},
		{"negative min", []byte{0x80, 0x00, 0x00, 0x00}, -2147483648, false},
		{"too short", []byte{0x00, 0x00, 0x00}, 0, true},
		{"empty", []byte{}, 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := NewReader(tt.data)
			got, err := r.ReadInt32()

			if (err != nil) != tt.wantErr {
				t.Errorf("ReadInt32() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !tt.wantErr && got != tt.want {
				t.Errorf("ReadInt32() = %d, want %d", got, tt.want)
			}
		})
	}
}

func TestReader_ReadInt64(t *testing.T) {
	tests := []struct {
		name    string
		data    []byte
		want    int64
		wantErr bool
	}{
		{"zero", []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}, 0, false},
		{"positive", []byte{0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}, 72057594037927936, false},
		{"max", []byte{0x7F, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF}, 9223372036854775807, false},
		{"negative", []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF}, -1, false},
		{"negative min", []byte{0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}, -9223372036854775808, false},
		{"too short", []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}, 0, true},
		{"empty", []byte{}, 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := NewReader(tt.data)
			got, err := r.ReadInt64()

			if (err != nil) != tt.wantErr {
				t.Errorf("ReadInt64() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !tt.wantErr && got != tt.want {
				t.Errorf("ReadInt64() = %d, want %d", got, tt.want)
			}
		})
	}
}

func TestReader_ReadUint32(t *testing.T) {
	tests := []struct {
		name    string
		data    []byte
		want    uint32
		wantErr bool
	}{
		{"zero", []byte{0x00, 0x00, 0x00, 0x00}, 0, false},
		{"positive", []byte{0x00, 0x00, 0x01, 0x00}, 256, false},
		{"max", []byte{0xFF, 0xFF, 0xFF, 0xFF}, 4294967295, false},
		{"too short", []byte{0x00, 0x00, 0x00}, 0, true},
		{"empty", []byte{}, 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := NewReader(tt.data)
			got, err := r.ReadUint32()

			if (err != nil) != tt.wantErr {
				t.Errorf("ReadUint32() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !tt.wantErr && got != tt.want {
				t.Errorf("ReadUint32() = %d, want %d", got, tt.want)
			}
		})
	}
}

func TestReader_ReadUVarInt(t *testing.T) {
	tests := []struct {
		name    string
		data    []byte
		want    uint64
		wantErr bool
	}{
		{"zero", []byte{0x00}, 0, false},
		{"one", []byte{0x01}, 1, false},
		{"127", []byte{0x7F}, 127, false},
		{"128", []byte{0x80, 0x01}, 128, false},
		{"255", []byte{0xFF, 0x01}, 255, false},
		{"300", []byte{0xAC, 0x02}, 300, false},
		{"16383", []byte{0xFF, 0x7F}, 16383, false},
		{"16384", []byte{0x80, 0x80, 0x01}, 16384, false},
		{"empty", []byte{}, 0, true},
		{"incomplete", []byte{0x80}, 0, true}, // continuation bit set but no more bytes
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := NewReader(tt.data)
			got, err := r.ReadUVarInt()

			if (err != nil) != tt.wantErr {
				t.Errorf("ReadUVarInt() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !tt.wantErr && got != tt.want {
				t.Errorf("ReadUVarInt() = %d, want %d", got, tt.want)
			}
		})
	}
}

func TestReader_ReadVarInt(t *testing.T) {
	tests := []struct {
		name    string
		data    []byte
		want    int64
		wantErr bool
	}{
		{"zero", []byte{0x00}, 0, false},
		{"positive one", []byte{0x02}, 1, false},
		{"negative one", []byte{0x01}, -1, false},
		{"positive 63", []byte{0x7E}, 63, false},
		{"negative 64", []byte{0x7F}, -64, false},
		{"positive 64", []byte{0x80, 0x01}, 64, false},
		{"negative 65", []byte{0x81, 0x01}, -65, false},
		{"empty", []byte{}, 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := NewReader(tt.data)
			got, err := r.ReadVarInt()

			if (err != nil) != tt.wantErr {
				t.Errorf("ReadVarInt() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !tt.wantErr && got != tt.want {
				t.Errorf("ReadVarInt() = %d, want %d", got, tt.want)
			}
		})
	}
}

func TestReader_ReadUUID(t *testing.T) {
	uuidBytes := []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
		0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F, 0x10}

	r := NewReader(uuidBytes)
	got, err := r.ReadUUID()

	if err != nil {
		t.Fatalf("ReadUUID() error = %v", err)
	}

	want, _ := uuid.FromBytes(uuidBytes)
	if got != want {
		t.Errorf("ReadUUID() = %v, want %v", got, want)
	}
}

func TestReader_ReadUUID_TooShort(t *testing.T) {
	r := NewReader([]byte{0x01, 0x02, 0x03})
	_, err := r.ReadUUID()

	if err != io.EOF {
		t.Errorf("ReadUUID() error = %v, want io.EOF", err)
	}
}

func TestReader_ReadString(t *testing.T) {
	tests := []struct {
		name    string
		data    []byte
		want    string
		wantErr bool
	}{
		{"empty", []byte{0x00, 0x00}, "", false},
		{"hello", []byte{0x00, 0x05, 'h', 'e', 'l', 'l', 'o'}, "hello", false},
		{"single char", []byte{0x00, 0x01, 'a'}, "a", false},
		{"too short length", []byte{0x00}, "", true},
		{"insufficient data", []byte{0x00, 0x05, 'h', 'e'}, "", true},
		{"empty buffer", []byte{}, "", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := NewReader(tt.data)
			got, err := r.ReadString()

			if (err != nil) != tt.wantErr {
				t.Errorf("ReadString() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !tt.wantErr && got != tt.want {
				t.Errorf("ReadString() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestReader_ReadNullableString(t *testing.T) {
	hello := "hello"

	tests := []struct {
		name    string
		data    []byte
		want    *string
		wantErr bool
	}{
		{"null", []byte{0xFF, 0xFF}, nil, false},
		{"hello", []byte{0x00, 0x05, 'h', 'e', 'l', 'l', 'o'}, &hello, false},
		{"too short", []byte{0x00}, nil, true},
		{"empty buffer", []byte{}, nil, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := NewReader(tt.data)
			got, err := r.ReadNullableString()

			if (err != nil) != tt.wantErr {
				t.Errorf("ReadNullableString() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !tt.wantErr {
				if tt.want == nil && got != nil {
					t.Errorf("ReadNullableString() = %v, want nil", got)
				} else if tt.want != nil && (got == nil || *got != *tt.want) {
					t.Errorf("ReadNullableString() = %v, want %v", got, tt.want)
				}
			}
		})
	}
}

func TestReader_ReadCompactString(t *testing.T) {
	tests := []struct {
		name    string
		data    []byte
		want    string
		wantErr bool
	}{
		{"empty", []byte{0x01}, "", false},                               // length 0 + 1 = 1
		{"hello", []byte{0x06, 'h', 'e', 'l', 'l', 'o'}, "hello", false}, // length 5 + 1 = 6
		{"single char", []byte{0x02, 'a'}, "a", false},                   // length 1 + 1 = 2
		{"empty buffer", []byte{}, "", true},
		{"insufficient data", []byte{0x06, 'h', 'e'}, "", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := NewReader(tt.data)
			got, err := r.ReadCompactString()

			if (err != nil) != tt.wantErr {
				t.Errorf("ReadCompactString() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !tt.wantErr && string(got) != tt.want {
				t.Errorf("ReadCompactString() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestReader_ReadCompactNullableString(t *testing.T) {
	hello := "hello"

	tests := []struct {
		name    string
		data    []byte
		want    *string
		wantErr bool
	}{
		{"null", []byte{0x01}, nil, false},                              // length 0 + 1 = 1, means null
		{"hello", []byte{0x06, 'h', 'e', 'l', 'l', 'o'}, &hello, false}, // length 5 + 1 = 6
		{"empty buffer", []byte{}, nil, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := NewReader(tt.data)
			got, err := r.ReadCompactNullableString()

			if (err != nil) != tt.wantErr {
				t.Errorf("ReadCompactNullableString() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !tt.wantErr {
				if tt.want == nil && got != nil {
					t.Errorf("ReadCompactNullableString() = %v, want nil", got)
				} else if tt.want != nil && (got == nil || *got != *tt.want) {
					t.Errorf("ReadCompactNullableString() = %v, want %v", got, tt.want)
				}
			}
		})
	}
}

func TestReader_ReadBytes(t *testing.T) {
	tests := []struct {
		name    string
		data    []byte
		want    []byte
		wantErr bool
	}{
		{"empty", []byte{0x00, 0x00, 0x00, 0x00}, []byte{}, false},
		{"data", []byte{0x00, 0x00, 0x00, 0x03, 0x01, 0x02, 0x03}, []byte{0x01, 0x02, 0x03}, false},
		{"too short length", []byte{0x00, 0x00, 0x00}, nil, true},
		{"insufficient data", []byte{0x00, 0x00, 0x00, 0x05, 0x01, 0x02}, nil, true},
		{"empty buffer", []byte{}, nil, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := NewReader(tt.data)
			got, err := r.ReadBytes()

			if (err != nil) != tt.wantErr {
				t.Errorf("ReadBytes() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !tt.wantErr {
				if len(got) != len(tt.want) {
					t.Errorf("ReadBytes() len = %d, want %d", len(got), len(tt.want))
				}
				for i := range got {
					if got[i] != tt.want[i] {
						t.Errorf("ReadBytes()[%d] = %d, want %d", i, got[i], tt.want[i])
					}
				}
			}
		})
	}
}

func TestReader_ReadNullableBytes(t *testing.T) {
	tests := []struct {
		name    string
		data    []byte
		want    []byte
		wantErr bool
	}{
		{"null", []byte{0xFF, 0xFF, 0xFF, 0xFF}, nil, false},
		{"empty", []byte{0x00, 0x00, 0x00, 0x00}, []byte{}, false},
		{"data", []byte{0x00, 0x00, 0x00, 0x03, 0x01, 0x02, 0x03}, []byte{0x01, 0x02, 0x03}, false},
		{"too short length", []byte{0x00, 0x00, 0x00}, nil, true},
		{"empty buffer", []byte{}, nil, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := NewReader(tt.data)
			got, err := r.ReadNullableBytes()

			if (err != nil) != tt.wantErr {
				t.Errorf("ReadNullableBytes() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !tt.wantErr {
				if tt.want == nil && got != nil {
					t.Errorf("ReadNullableBytes() = %v, want nil", got)
				} else if tt.want != nil {
					if len(got) != len(tt.want) {
						t.Errorf("ReadNullableBytes() len = %d, want %d", len(got), len(tt.want))
					}
					for i := range got {
						if got[i] != tt.want[i] {
							t.Errorf("ReadNullableBytes()[%d] = %d, want %d", i, got[i], tt.want[i])
						}
					}
				}
			}
		})
	}
}

func TestReader_ReadCompactBytes(t *testing.T) {
	tests := []struct {
		name    string
		data    []byte
		want    []byte
		wantErr bool
	}{
		{"empty", []byte{0x01}, []byte{}, false},                                  // length 0 + 1 = 1
		{"data", []byte{0x04, 0x01, 0x02, 0x03}, []byte{0x01, 0x02, 0x03}, false}, // length 3 + 1 = 4
		{"empty buffer", []byte{}, nil, true},
		{"insufficient data", []byte{0x04, 0x01, 0x02}, nil, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := NewReader(tt.data)
			got, err := r.ReadCompactBytes()

			if (err != nil) != tt.wantErr {
				t.Errorf("ReadCompactBytes() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !tt.wantErr {
				if len(got) != len(tt.want) {
					t.Errorf("ReadCompactBytes() len = %d, want %d", len(got), len(tt.want))
				}
				for i := range got {
					if got[i] != tt.want[i] {
						t.Errorf("ReadCompactBytes()[%d] = %d, want %d", i, got[i], tt.want[i])
					}
				}
			}
		})
	}
}

func TestReader_ReadCompactNullableBytes(t *testing.T) {
	tests := []struct {
		name    string
		data    []byte
		want    []byte
		wantErr bool
	}{
		{"null", []byte{0x01}, nil, false},                                        // length 0 + 1 = 1, means null
		{"data", []byte{0x04, 0x01, 0x02, 0x03}, []byte{0x01, 0x02, 0x03}, false}, // length 3 + 1 = 4
		{"empty buffer", []byte{}, nil, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := NewReader(tt.data)
			got, err := r.ReadCompactNullableBytes()

			if (err != nil) != tt.wantErr {
				t.Errorf("ReadCompactNullableBytes() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !tt.wantErr {
				if tt.want == nil && got != nil {
					t.Errorf("ReadCompactNullableBytes() = %v, want nil", got)
				} else if tt.want != nil {
					if len(got) != len(tt.want) {
						t.Errorf("ReadCompactNullableBytes() len = %d, want %d", len(got), len(tt.want))
					}
					for i := range got {
						if got[i] != tt.want[i] {
							t.Errorf("ReadCompactNullableBytes()[%d] = %d, want %d", i, got[i], tt.want[i])
						}
					}
				}
			}
		})
	}
}

func TestReader_ReadArrayLen(t *testing.T) {
	tests := []struct {
		name    string
		data    []byte
		want    int
		wantErr bool
	}{
		{"zero", []byte{0x00, 0x00, 0x00, 0x00}, 0, false},
		{"positive", []byte{0x00, 0x00, 0x00, 0x05}, 5, false},
		{"null array", []byte{0xFF, 0xFF, 0xFF, 0xFF}, -1, false},
		{"too short", []byte{0x00, 0x00, 0x00}, 0, true},
		{"empty buffer", []byte{}, 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := NewReader(tt.data)
			got, err := r.ReadArrayLen()

			if (err != nil) != tt.wantErr {
				t.Errorf("ReadArrayLen() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !tt.wantErr && got != tt.want {
				t.Errorf("ReadArrayLen() = %d, want %d", got, tt.want)
			}
		})
	}
}

func TestReader_ReadCompactArrayLen(t *testing.T) {
	tests := []struct {
		name    string
		data    []byte
		want    int
		wantErr bool
	}{
		{"null array", []byte{0x00}, -1, false}, // 0 means null
		{"zero", []byte{0x01}, 0, false},        // 1 - 1 = 0
		{"one", []byte{0x02}, 1, false},         // 2 - 1 = 1
		{"five", []byte{0x06}, 5, false},        // 6 - 1 = 5
		{"empty buffer", []byte{}, 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := NewReader(tt.data)
			got, err := r.ReadCompactArrayLen()

			if (err != nil) != tt.wantErr {
				t.Errorf("ReadCompactArrayLen() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !tt.wantErr && got != tt.want {
				t.Errorf("ReadCompactArrayLen() = %d, want %d", got, tt.want)
			}
		})
	}
}

func TestReader_ReadTaggedFields(t *testing.T) {
	tests := []struct {
		name    string
		data    []byte
		wantErr bool
	}{
		{"empty tagged fields", []byte{0x00}, false},                      // count = 0
		{"one tagged field", []byte{0x01, 0x00, 0x02, 0xAB, 0xCD}, false}, // count=1, tag=0, size=2, data
		{"empty buffer", []byte{}, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := NewReader(tt.data)
			err := r.ReadTaggedFields()

			if (err != nil) != tt.wantErr {
				t.Errorf("ReadTaggedFields() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

// Test roundtrip: write then read
func TestWriter_Reader_Roundtrip_Int8(t *testing.T) {
	values := []int8{0, 1, -1, 127, -128}

	for _, v := range values {
		w := NewWriter(10)
		w.WriteInt8(v)

		r := NewReader(w.Bytes())
		got, err := r.ReadInt8()

		if err != nil {
			t.Errorf("Roundtrip Int8(%d) error = %v", v, err)
		}
		if got != v {
			t.Errorf("Roundtrip Int8(%d) = %d", v, got)
		}
	}
}

func TestWriter_Reader_Roundtrip_Int16(t *testing.T) {
	values := []int16{0, 1, -1, 256, -256, 32767, -32768}

	for _, v := range values {
		w := NewWriter(10)
		w.WriteInt16(v)

		r := NewReader(w.Bytes())
		got, err := r.ReadInt16()

		if err != nil {
			t.Errorf("Roundtrip Int16(%d) error = %v", v, err)
		}
		if got != v {
			t.Errorf("Roundtrip Int16(%d) = %d", v, got)
		}
	}
}

func TestWriter_Reader_Roundtrip_Int32(t *testing.T) {
	values := []int32{0, 1, -1, 65536, -65536, 2147483647, -2147483648}

	for _, v := range values {
		w := NewWriter(10)
		w.WriteInt32(v)

		r := NewReader(w.Bytes())
		got, err := r.ReadInt32()

		if err != nil {
			t.Errorf("Roundtrip Int32(%d) error = %v", v, err)
		}
		if got != v {
			t.Errorf("Roundtrip Int32(%d) = %d", v, got)
		}
	}
}

func TestWriter_Reader_Roundtrip_Int64(t *testing.T) {
	values := []int64{0, 1, -1, 4294967296, -4294967296, 9223372036854775807, -9223372036854775808}

	for _, v := range values {
		w := NewWriter(10)
		w.WriteInt64(v)

		r := NewReader(w.Bytes())
		got, err := r.ReadInt64()

		if err != nil {
			t.Errorf("Roundtrip Int64(%d) error = %v", v, err)
		}
		if got != v {
			t.Errorf("Roundtrip Int64(%d) = %d", v, got)
		}
	}
}

func TestWriter_Reader_Roundtrip_UVarInt(t *testing.T) {
	values := []uint64{0, 1, 127, 128, 255, 300, 16383, 16384, 1000000}

	for _, v := range values {
		w := NewWriter(20)
		w.WriteUVarInt(v)

		r := NewReader(w.Bytes())
		got, err := r.ReadUVarInt()

		if err != nil {
			t.Errorf("Roundtrip UVarInt(%d) error = %v", v, err)
		}
		if got != v {
			t.Errorf("Roundtrip UVarInt(%d) = %d", v, got)
		}
	}
}

func TestWriter_Reader_Roundtrip_VarInt(t *testing.T) {
	values := []int64{0, 1, -1, 63, -64, 64, -65, 1000, -1000}

	for _, v := range values {
		w := NewWriter(20)
		w.WriteVarInt(v)

		r := NewReader(w.Bytes())
		got, err := r.ReadVarInt()

		if err != nil {
			t.Errorf("Roundtrip VarInt(%d) error = %v", v, err)
		}
		if got != v {
			t.Errorf("Roundtrip VarInt(%d) = %d", v, got)
		}
	}
}

func TestWriter_Reader_Roundtrip_String(t *testing.T) {
	values := []string{"", "hello", "hello world", "こんにちは"}

	for _, v := range values {
		w := NewWriter(100)
		w.WriteString(v)

		r := NewReader(w.Bytes())
		got, err := r.ReadString()

		if err != nil {
			t.Errorf("Roundtrip String(%q) error = %v", v, err)
		}
		if got != v {
			t.Errorf("Roundtrip String(%q) = %q", v, got)
		}
	}
}

func TestWriter_Reader_Roundtrip_UUID(t *testing.T) {
	uuidVal := [16]byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
		0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F, 0x10}

	w := NewWriter(20)
	w.WriteUUID(uuidVal)

	r := NewReader(w.Bytes())
	got, err := r.ReadUUID()

	if err != nil {
		t.Errorf("Roundtrip UUID error = %v", err)
	}

	want, _ := uuid.FromBytes(uuidVal[:])
	if got != want {
		t.Errorf("Roundtrip UUID = %v, want %v", got, want)
	}
}

// Benchmark tests

func BenchmarkReader_ReadInt8(b *testing.B) {
	data := make([]byte, b.N)
	for i := range data {
		data[i] = byte(i)
	}
	r := NewReader(data)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		r.ReadInt8()
	}
}

func BenchmarkReader_ReadInt32(b *testing.B) {
	data := make([]byte, b.N*4)
	r := NewReader(data)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		r.ReadInt32()
	}
}

func BenchmarkReader_ReadUVarInt(b *testing.B) {
	// Prepare data with various UVarInt values
	w := NewWriter(b.N * 10)
	for i := 0; i < b.N; i++ {
		w.WriteUVarInt(uint64(i % 1000))
	}

	r := NewReader(w.Bytes())
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		r.ReadUVarInt()
	}
}

func BenchmarkReader_ReadString(b *testing.B) {
	// Prepare data with strings
	w := NewWriter(b.N * 20)
	for i := 0; i < b.N; i++ {
		w.WriteString("hello world")
	}

	r := NewReader(w.Bytes())
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		r.ReadString()
	}
}
