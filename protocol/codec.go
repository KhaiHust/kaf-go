package protocol

type IEncoder interface {
	Encode(w *Writer) error
}

type IDecoder interface {
	Decode(r *Reader) error
}

type ICodec interface {
	IEncoder
	IDecoder
}
