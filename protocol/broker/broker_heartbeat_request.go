package broker

import (
	"github.com/KhaiHust/kaf-go/constant"
	"github.com/KhaiHust/kaf-go/protocol"
)

type BrokerHeartbeatRequest struct {
	BrokerId              int32
	BrokerEpoch           int64
	CurrentMetadataOffset int64
	WantFence             bool
	WantShutDown          bool
}

func (b *BrokerHeartbeatRequest) ApiKey() int16 {
	return constant.ApiKeyBrokerHeartbeat
}

func (b *BrokerHeartbeatRequest) Encode(w *protocol.Writer) error {
	w.WriteInt32(b.BrokerId)
	w.WriteInt64(b.BrokerEpoch)
	w.WriteInt64(b.CurrentMetadataOffset)
	w.WriteBool(b.WantFence)
	w.WriteBool(b.WantShutDown)
	w.WriteEmptyTaggedFields()
	return nil
}

func (b *BrokerHeartbeatRequest) Decode(r *protocol.Reader) error {
	var err error
	if b.BrokerId, err = r.ReadInt32(); err != nil {
		return err
	}
	if b.BrokerEpoch, err = r.ReadInt64(); err != nil {
		return err
	}
	if b.CurrentMetadataOffset, err = r.ReadInt64(); err != nil {
		return err
	}
	if b.WantFence, err = r.ReadBool(); err != nil {
		return err
	}
	if b.WantShutDown, err = r.ReadBool(); err != nil {
		return err
	}
	return r.ReadTaggedFields()
}
