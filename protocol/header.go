package protocol

import (
	"github.com/KhaiHust/kaf-go/common"
	"github.com/KhaiHust/kaf-go/protocol/types"
)

type RequestHeader struct {
	ApiKey        int16
	ApiVersion    int16
	CorrelationId int32
	ClientId      types.NullableString
}

func (h *RequestHeader) Encode(w *Writer) error {
	w.WriteInt16(h.ApiKey)
	w.WriteInt16(h.ApiVersion)
	w.WriteInt32(h.CorrelationId)
	w.WriteNullableString(h.ClientId)
	if common.IsFlexible(h.ApiKey, h.ApiVersion) {
		w.WriteEmptyTaggedFields()
	}
	return nil
}

func (h *RequestHeader) Decode(r *Reader) error {
	var err error
	if h.ApiKey, err = r.ReadInt16(); err != nil {
		return err
	}
	if h.ApiVersion, err = r.ReadInt16(); err != nil {
		return err
	}
	if h.CorrelationId, err = r.ReadInt32(); err != nil {
		return err
	}
	if h.ClientId, err = r.ReadNullableString(); err != nil {
		return err
	}
	if common.IsFlexible(h.ApiKey, h.ApiVersion) {
		return r.ReadTaggedFields()
	}
	return nil
}

type ResponseHeader struct {
	CorrelationId int32
	ApiVersion    int16
	ApiKey        int16
}

func (h *ResponseHeader) Encode(w *Writer) error {
	w.WriteInt32(h.CorrelationId)

	if common.IsFlexible(h.ApiKey, h.ApiVersion) {
		w.WriteEmptyTaggedFields()
	}

	return nil
}
func (h *ResponseHeader) Decode(r *Reader) error {
	var err error
	if h.CorrelationId, err = r.ReadInt32(); err != nil {
		return err
	}
	if common.IsFlexible(h.ApiKey, h.ApiVersion) {
		return r.ReadTaggedFields()
	}
	return nil
}
