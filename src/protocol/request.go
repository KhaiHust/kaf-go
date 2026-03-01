package protocol

type Request interface {
	IEncoder
	ApiKey() int64
	ApiVersion() int16
	CorrelationID() int32
}
