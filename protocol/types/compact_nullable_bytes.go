package types

// CompactNullableBytes Represents a raw sequence of bytes.
// First the length N+1 is given as an UNSIGNED_VARINT.
// Then N bytes follow. A null object is represented with a length of 0.
type CompactNullableBytes []byte
