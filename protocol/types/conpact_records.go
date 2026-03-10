package types

// CompactRecords represents a sequence of Kafka records as COMPACT_NULLABLE_BYTES.
// According to Kafka protocol: "Represents a sequence of Kafka records as COMPACT_NULLABLE_BYTES."
// The length N+1 is encoded as UNSIGNED_VARINT, where 0 indicates null, then N bytes follow.
// A nil slice encodes as null (length = 0).
// A non-nil slice (even empty) encodes as a present value.
type CompactRecords []byte
