package broker

import (
	"github.com/KhaiHust/kaf-go/constant"
	"github.com/KhaiHust/kaf-go/protocol"
	"github.com/KhaiHust/kaf-go/protocol/types"
	"github.com/gofrs/uuid/v5"
)

type BrokerListener struct {
	Name             types.CompactString
	Host             types.CompactString
	Port             int32
	SecurityProtocol int16
}

type BrokerFeature struct {
	Name                types.CompactString
	MinSupportedVersion int16
	MaxSupportedVersion int16
}

type BrokerRegistrationRequest struct {
	BrokerId      int32
	ClusterId     types.CompactString
	IncarnationId uuid.UUID
	Listeners     []BrokerListener
	Features      []BrokerFeature
	Rack          types.CompactNullableString
}

func (b *BrokerRegistrationRequest) ApiKey() int16 {
	return constant.ApiKeyBrokerRegistration
}

func (b *BrokerRegistrationRequest) Encode(w *protocol.Writer) error {
	w.WriteInt32(b.BrokerId)
	w.WriteCompactString(b.ClusterId)
	w.WriteUUID(b.IncarnationId)

	w.WriteCompactArrayLen(len(b.Listeners))
	for _, l := range b.Listeners {
		w.WriteCompactString(l.Name)
		w.WriteCompactString(l.Host)
		w.WriteInt32(l.Port)
		w.WriteInt16(l.SecurityProtocol)
		w.WriteEmptyTaggedFields()
	}

	w.WriteCompactArrayLen(len(b.Features))
	for _, f := range b.Features {
		w.WriteCompactString(f.Name)
		w.WriteInt16(f.MinSupportedVersion)
		w.WriteInt16(f.MaxSupportedVersion)
		w.WriteEmptyTaggedFields()
	}

	w.WriteCompactNullableString(b.Rack)
	w.WriteEmptyTaggedFields()
	return nil
}

func (b *BrokerRegistrationRequest) Decode(r *protocol.Reader) error {
	var err error

	if b.BrokerId, err = r.ReadInt32(); err != nil {
		return err
	}
	if b.ClusterId, err = r.ReadCompactString(); err != nil {
		return err
	}
	if b.IncarnationId, err = r.ReadUUID(); err != nil {
		return err
	}

	listenersLen, err := r.ReadCompactArrayLen()
	if err != nil {
		return err
	}
	b.Listeners = make([]BrokerListener, listenersLen)
	for i := range b.Listeners {
		if b.Listeners[i].Name, err = r.ReadCompactString(); err != nil {
			return err
		}
		if b.Listeners[i].Host, err = r.ReadCompactString(); err != nil {
			return err
		}
		if b.Listeners[i].Port, err = r.ReadInt32(); err != nil {
			return err
		}
		if b.Listeners[i].SecurityProtocol, err = r.ReadInt16(); err != nil {
			return err
		}
		if err = r.ReadTaggedFields(); err != nil {
			return err
		}
	}

	featuresLen, err := r.ReadCompactArrayLen()
	if err != nil {
		return err
	}
	b.Features = make([]BrokerFeature, featuresLen)
	for i := range b.Features {
		if b.Features[i].Name, err = r.ReadCompactString(); err != nil {
			return err
		}
		if b.Features[i].MinSupportedVersion, err = r.ReadInt16(); err != nil {
			return err
		}
		if b.Features[i].MaxSupportedVersion, err = r.ReadInt16(); err != nil {
			return err
		}
		if err = r.ReadTaggedFields(); err != nil {
			return err
		}
	}

	if b.Rack, err = r.ReadCompactNullableString(); err != nil {
		return err
	}
	return r.ReadTaggedFields()
}
