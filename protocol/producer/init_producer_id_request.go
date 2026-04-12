package producer

import (
	"github.com/KhaiHust/kaf-go/constant"
	"github.com/KhaiHust/kaf-go/protocol"
	"github.com/KhaiHust/kaf-go/protocol/types"
)

const InitProducerIdRequestVersion = 5

type InitProducerIdRequest struct {
	TransactionalId      types.CompactNullableString
	TransactionTimeoutMs int32
	ProducerId           int64
	ProducerEpoch        int16
}

func (i *InitProducerIdRequest) ApiKey() int16 {
	return constant.ApiKeyInitProducerId
}

func (i *InitProducerIdRequest) Decode(r *protocol.Reader) error {
	var err error
	if i.TransactionalId, err = r.ReadCompactNullableString(); err != nil {
		return err
	}
	if i.TransactionTimeoutMs, err = r.ReadInt32(); err != nil {
		return err
	}
	if i.ProducerId, err = r.ReadInt64(); err != nil {
		return err
	}
	if i.ProducerEpoch, err = r.ReadInt16(); err != nil {
		return err
	}
	return r.ReadTaggedFields()
}
