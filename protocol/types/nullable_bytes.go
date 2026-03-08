package types

// NullableBytes presents NULLABLE_BYTES primitive.
// A nil slice encodes as null (length = -1).
// A non-nil slice (even empty) encodes as a present value.
type NullableBytes []byte

func NewNullableBytes(bytes []byte) NullableBytes {
	if bytes == nil {
		return nil
	}
	return NullableBytes(bytes)
}

func (bytes NullableBytes) Bytes() []byte {
	return bytes
}

func (bytes NullableBytes) IsNull() bool {
	return bytes == nil
}

func (bytes NullableBytes) IsEmpty() bool {
	return len(bytes) == 0
}

func (bytes NullableBytes) Length() int32 {
	if bytes == nil {
		return -1
	}
	return int32(len(bytes))
}
