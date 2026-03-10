package types

type Record struct {
	Length         Varint
	Attributes     int8
	TimestampDelta Varlong
	OffsetDelta    Varint
	KeyLength      Varint
	Key            []byte
	ValueLength    Varint
	Value          []byte
	HeadersCount   Varint
	Headers        []RecordHeader
}

type RecordHeader struct {
	HeaderKeyLength   Varint
	HeaderKey         string
	HeaderValueLength Varint
	Value             []byte
}
