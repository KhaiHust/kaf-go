package protocol

type Request interface {
	ICodec
	ApiKey() int64
	ApiVersion() int16
	CorrelationID() int32
}
