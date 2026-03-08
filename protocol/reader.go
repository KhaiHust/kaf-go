package protocol

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"github.com/KhaiHust/kaf-go/protocol/types"
	"github.com/gofrs/uuid/v5"
)

type Reader struct {
	buff []byte
	pos  int
}

func NewReader(buff []byte) *Reader {
	return &Reader{buff: buff, pos: 0}
}

func (r *Reader) Remaining() int {
	return len(r.buff) - r.pos
}

func (r *Reader) Done() bool {
	return r.pos >= len(r.buff)
}

func (r *Reader) require(n int) error {
	if r.pos+n > len(r.buff) {
		return io.EOF
	}
	return nil
}

func (r *Reader) ReadBool() (bool, error) {
	if err := r.require(1); err != nil {
		return false, err
	}
	v := r.buff[r.pos] != 0
	r.pos++
	return v, nil
}

func (r *Reader) ReadInt8() (int8, error) {
	if err := r.require(1); err != nil {
		return 0, err
	}
	v := int8(r.buff[r.pos])
	r.pos++
	return v, nil
}

func (r *Reader) ReadInt16() (int16, error) {
	if err := r.require(2); err != nil {
		return 0, err
	}
	v := int16(binary.BigEndian.Uint16(r.buff[r.pos:]))
	r.pos += 2
	return v, nil
}

func (r *Reader) ReadInt32() (int32, error) {
	if err := r.require(4); err != nil {
		return 0, err
	}
	v := int32(binary.BigEndian.Uint32(r.buff[r.pos:]))
	r.pos += 4
	return v, nil
}

func (r *Reader) ReadInt64() (int64, error) {
	if err := r.require(8); err != nil {
		return 0, err
	}
	v := int64(binary.BigEndian.Uint64(r.buff[r.pos:]))
	r.pos += 8
	return v, nil
}

func (r *Reader) ReadUint32() (uint32, error) {
	if err := r.require(4); err != nil {
		return 0, err
	}
	v := binary.BigEndian.Uint32(r.buff[r.pos:])
	r.pos += 4
	return v, nil
}

func (r *Reader) ReadUVarInt() (uint64, error) {
	var result uint64
	var shift uint
	for {
		if err := r.require(1); err != nil {
			return 0, err
		}
		b := r.buff[r.pos]
		r.pos++
		if shift == 63 && b > 0x01 {
			return 0, errors.New("uvarint: overflow")
		}
		result |= uint64(b&0x7f) << shift
		if b < 0x80 {
			return result, nil
		}
		shift += 7
		if shift >= 64 {
			return 0, errors.New("uvarint: overflow")
		}
	}
}

func (r *Reader) ReadVarInt() (int64, error) {
	uv, err := r.ReadUVarInt()
	if err != nil {
		return 0, err
	}
	return int64((uv >> 1) ^ -(uv & 1)), nil
}

func (r *Reader) ReadUUID() (uuid.UUID, error) {
	var v [16]byte
	if err := r.require(16); err != nil {
		return uuid.Nil, err
	}

	copy(v[:], r.buff[r.pos:r.pos+16])
	r.pos += 16
	return uuid.FromBytes(v[:])
}

func (r *Reader) ReadString() (string, error) {
	n, err := r.ReadInt16()
	if err != nil {
		return "", err
	}
	if err = r.require(int(n)); err != nil {
		return "", err
	}
	s := string(r.buff[r.pos : r.pos+int(n)])
	r.pos += int(n)
	return s, nil
}

func (r *Reader) ReadNullableString() (types.NullableString, error) {
	n, err := r.ReadInt16()
	if err != nil {
		return nil, err
	}
	if n == -1 {
		return nil, nil
	}
	if n < 1 {
		return nil, errors.New("string: invalid length")
	}
	if err = r.require(int(n)); err != nil {
		return nil, err
	}
	s := string(r.buff[r.pos : r.pos+int(n)])
	r.pos += int(n)
	return &s, nil
}

func (r *Reader) ReadCompactString() (types.CompactString, error) {
	n, err := r.ReadUVarInt()
	if err != nil {
		return "", err
	}
	if n-1 == 0 {
		return "", nil
	}
	if err = r.require(int(n - 1)); err != nil {
		return "", err
	}
	s := string(r.buff[r.pos : r.pos+int(n-1)])
	r.pos += int(n - 1)
	return types.CompactString(s), nil
}

func (r *Reader) ReadCompactNullableString() (types.CompactNullableString, error) {
	wl, err := r.ReadUVarInt()
	if err != nil {
		return nil, err
	}
	if wl == 0 {
		return nil, nil
	}
	n := int(wl) - 1
	if n == 0 {
		empty := ""
		return &empty, nil
	}

	if err = r.require(n); err != nil {
		return nil, err
	}
	s := string(r.buff[r.pos : r.pos+n])
	r.pos += n
	return &s, nil
}

func (r *Reader) ReadBytes() (types.Bytes, error) {
	n, err := r.ReadInt32()
	if err != nil {
		return nil, err
	}
	if err = r.require(int(n)); err != nil {
		return nil, err
	}
	b := make([]byte, n)
	copy(b, r.buff[r.pos:r.pos+int(n)])
	r.pos += int(n)
	return b, nil
}

func (r *Reader) ReadNullableBytes() (types.Bytes, error) {
	n, err := r.ReadInt32()
	if err != nil {
		return nil, err
	}

	if n == -1 {
		return nil, nil
	}
	if n < -1 {
		return nil, errors.New("bytes: invalid length")
	}
	if err = r.require(int(n)); err != nil {
		return nil, err
	}
	b := make([]byte, n)
	copy(b, r.buff[r.pos:r.pos+int(n)])
	r.pos += int(n)
	return b, nil
}

func (r *Reader) ReadCompactBytes() (types.CompactBytes, error) {
	wl, err := r.ReadUVarInt()
	if err != nil {
		return nil, err
	}
	n := int(wl) - 1
	if n < 0 {
		return nil, errors.New("bytes: invalid length")
	}
	if err = r.require(n); err != nil {
		return nil, err
	}
	b := make([]byte, n)
	copy(b, r.buff[r.pos:r.pos+n])
	r.pos += n
	return b, nil
}

func (r *Reader) ReadCompactNullableBytes() (types.CompactBytes, error) {
	wl, err := r.ReadUVarInt()
	if err != nil {
		return nil, err
	}
	n := int(wl) - 1
	if n == 0 {
		return nil, nil
	}
	if n < 0 {
		return nil, errors.New("bytes: invalid length")
	}
	if err = r.require(n); err != nil {
		return nil, err
	}

	b := make([]byte, n)
	copy(b, r.buff[r.pos:r.pos+n])
	r.pos += n
	return b, nil
}

func (r *Reader) ReadArrayLen() (int, error) {
	n, err := r.ReadInt32()
	if err != nil {
		return 0, err
	}
	if n < -1 {
		return 0, fmt.Errorf("array: invalid length %d", n)
	}
	return int(n), nil
}

func (r *Reader) ReadCompactArrayLen() (int, error) {
	wl, err := r.ReadUVarInt()
	if err != nil {
		return 0, err
	}
	if wl == 0 {
		return -1, nil // null array
	}
	return int(wl - 1), nil
}

func (r *Reader) ReadTaggedFields() error {
	count, err := r.ReadUVarInt()
	if err != nil {
		return err
	}
	for i := uint64(0); i < count; i++ {
		// tag key
		if _, err := r.ReadUVarInt(); err != nil {
			return err
		}
		// tag value (compact bytes)
		size, err := r.ReadUVarInt()
		if err != nil {
			return err
		}
		if err := r.require(int(size)); err != nil {
			return err
		}
		r.pos += int(size)
	}
	return nil
}
