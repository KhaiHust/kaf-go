package protocol

type Request interface {
	IDecoder
	ApiKey() int64
	ApiVersion() int16
	CorrelationID() int32
}
