package admin

import (
	"github.com/KhaiHust/kaf-go/constant"
	"github.com/KhaiHust/kaf-go/protocol"
	"github.com/KhaiHust/kaf-go/protocol/types"
)

type ApiVersionRequest struct {
	apiVersion            int16
	ClientSoftwareName    types.CompactString
	ClientSoftwareVersion types.CompactString
}

func (a *ApiVersionRequest) ApiKey() int64 {
	return constant.ApiKeyApiVersions
}

func (a *ApiVersionRequest) ApiVersion() int16 {
	return a.apiVersion
}

func (a *ApiVersionRequest) Encode(w *protocol.Writer) error {
	w.WriteCompactString(a.ClientSoftwareName)
	w.WriteCompactString(a.ClientSoftwareVersion)
	return nil
}
func (a *ApiVersionRequest) Decode(r *protocol.Reader) error {
	var err error
	if a.ClientSoftwareName, err = r.ReadCompactString(); err != nil {
		return err
	}
	if a.ClientSoftwareVersion, err = r.ReadCompactString(); err != nil {
		return err
	}
	return nil
}
