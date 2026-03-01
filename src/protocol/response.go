package protocol

type Response interface {
	IDecoder
	ApiKey() int16
}
