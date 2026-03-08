package producer

type ProducerRequest struct {
	TransactionalId *string
	Acks            int16
	TimeoutMs       int32
}

func (p *ProducerRequest) Decode(bytes []byte) (int, error) {
	//TODO implement me
	panic("implement me")
}

func (p *ProducerRequest) Encode() []byte {
	panic("implement me")
}
