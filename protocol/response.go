package protocol

type Response interface {
	ICodec
	ApiKey() int16
}
