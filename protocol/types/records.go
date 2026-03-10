package types

// Records represents a sequence of Kafka records as NULLABLE_BYTES.
// According to Kafka protocol: "Represents a sequence of Kafka records as NULLABLE_BYTES."
// The length is encoded as INT32, where -1 indicates null.
// A nil slice encodes as null (length = -1).
// A non-nil slice (even empty) encodes as a present value.
type Records []byte
