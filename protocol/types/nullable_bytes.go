package types

// NullableBytes presents NULLABLE_BYTES primitive.
// A nil slice encodes as null (length = -1).
// A non-nil slice (even empty) encodes as a present value.
type NullableBytes []byte

func (bytes NullableBytes) Bytes() []byte {
	return bytes
}
