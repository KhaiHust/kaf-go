package broker

import (
	"github.com/KhaiHust/kaf-go/constant"
	"github.com/KhaiHust/kaf-go/protocol"
)

type BrokerRegistrationResponse struct {
	ThrottleTimeMs int32
	ErrorCode      int16
	BrokerEpoch    int64
}

func (b *BrokerRegistrationResponse) ApiKey() int16 {
	return constant.ApiKeyBrokerRegistration
}

func (b *BrokerRegistrationResponse) Encode(w *protocol.Writer) error {
	w.WriteInt32(b.ThrottleTimeMs)
	w.WriteInt16(b.ErrorCode)
	w.WriteInt64(b.BrokerEpoch)
	w.WriteEmptyTaggedFields()
	return nil
}

func (b *BrokerRegistrationResponse) Decode(r *protocol.Reader) error {
	var err error
	if b.ThrottleTimeMs, err = r.ReadInt32(); err != nil {
		return err
	}
	if b.ErrorCode, err = r.ReadInt16(); err != nil {
		return err
	}
	if b.BrokerEpoch, err = r.ReadInt64(); err != nil {
		return err
	}
	return r.ReadTaggedFields()
}
