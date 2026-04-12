package broker

import (
	"github.com/KhaiHust/kaf-go/constant"
	"github.com/KhaiHust/kaf-go/protocol"
)

type BrokerHeartbeatResponse struct {
	ThrottleTimeMs int32
	ErrorCode      int16
	IsCaughtUp     bool
	IsFenced       bool
	ShouldShutDown bool
}

func (b *BrokerHeartbeatResponse) ApiKey() int16 {
	return constant.ApiKeyBrokerHeartbeat
}

func (b *BrokerHeartbeatResponse) Encode(w *protocol.Writer) error {
	w.WriteInt32(b.ThrottleTimeMs)
	w.WriteInt16(b.ErrorCode)
	w.WriteBool(b.IsCaughtUp)
	w.WriteBool(b.IsFenced)
	w.WriteBool(b.ShouldShutDown)
	w.WriteEmptyTaggedFields()
	return nil
}

func (b *BrokerHeartbeatResponse) Decode(r *protocol.Reader) error {
	var err error
	if b.ThrottleTimeMs, err = r.ReadInt32(); err != nil {
		return err
	}
	if b.ErrorCode, err = r.ReadInt16(); err != nil {
		return err
	}
	if b.IsCaughtUp, err = r.ReadBool(); err != nil {
		return err
	}
	if b.IsFenced, err = r.ReadBool(); err != nil {
		return err
	}
	if b.ShouldShutDown, err = r.ReadBool(); err != nil {
		return err
	}
	return r.ReadTaggedFields()
}
