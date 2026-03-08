package admin

import (
	"github.com/KhaiHust/kaf-go/constant"
	"github.com/KhaiHust/kaf-go/protocol"
)

type ApiVersionResponse struct {
	ErrorCode      int16
	ApiKeys        []ApiKey
	ThrottleTimeMs int32
}

func (a *ApiVersionResponse) ApiKey() int16 {
	return constant.ApiKeyApiVersions
}

func (a *ApiVersionResponse) Encode(w *protocol.Writer) error {
	w.WriteInt16(a.ErrorCode)
	w.WriteCompactArrayLen(len(a.ApiKeys))
	for _, apiKey := range a.ApiKeys {
		err := apiKey.Encode(w)
		if err != nil {
			return err
		}
	}
	w.WriteInt32(a.ThrottleTimeMs)
	w.WriteEmptyTaggedFields()
	return nil
}

func (a *ApiVersionResponse) Decode(r *protocol.Reader) error {
	var err error
	if a.ErrorCode, err = r.ReadInt16(); err != nil {
		return err
	}
	apiKeysLen, err := r.ReadCompactArrayLen()
	if err != nil {
		return err
	}
	apiKeys := make([]ApiKey, apiKeysLen)
	for i := 0; i < apiKeysLen; i++ {
		if err := apiKeys[i].Decode(r); err != nil {
			return err
		}
	}
	a.ApiKeys = apiKeys
	return nil
}

type ApiKey struct {
	ApiKey     int16
	MinVersion int16
	MaxVersion int16
}

func (a *ApiKey) Encode(w *protocol.Writer) error {
	w.WriteInt16(a.ApiKey)
	w.WriteInt16(a.MinVersion)
	w.WriteInt16(a.MaxVersion)
	w.WriteEmptyTaggedFields()
	return nil
}

func (a *ApiKey) Decode(r *protocol.Reader) error {
	var err error
	if a.ApiKey, err = r.ReadInt16(); err != nil {
		return err
	}
	if a.MinVersion, err = r.ReadInt16(); err != nil {
		return err
	}
	if a.MaxVersion, err = r.ReadInt16(); err != nil {
		return err
	}
	return nil
}
