package protocol

type Response interface {
	IEncoder
	ApiKey() int16
}
