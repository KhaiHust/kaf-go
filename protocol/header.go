package protocol

import (
	"github.com/KhaiHust/kaf-go/common"
	"github.com/KhaiHust/kaf-go/constant"
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
	return nil
}

func responseHeaderVersion(apiKey int16, apiVersion int16) int16 {
	switch apiKey {
	case 0: // Produce          flexibleVersions: 9+
		if apiVersion >= 9 {
			return 1
		}
	case 1: // Fetch            flexibleVersions: 12+
		if apiVersion >= 12 {
			return 1
		}
	case 2: // ListOffsets      flexibleVersions: 6+
		if apiVersion >= 6 {
			return 1
		}
	case constant.ApiKeyMetaData: // Metadata         flexibleVersions: 9+
		if apiVersion >= 9 {
			return 1
		}
	case 8: // OffsetCommit     flexibleVersions: 8+
		if apiVersion >= 8 {
			return 1
		}
	case 9: // OffsetFetch      flexibleVersions: 6+
		if apiVersion >= 6 {
			return 1
		}
	case 10: // FindCoordinator  flexibleVersions: 3+
		if apiVersion >= 3 {
			return 1
		}
	case 11: // JoinGroup        flexibleVersions: 6+
		if apiVersion >= 6 {
			return 1
		}
	case 12: // Heartbeat        flexibleVersions: 4+
		if apiVersion >= 4 {
			return 1
		}
	case 13: // LeaveGroup       flexibleVersions: 4+
		if apiVersion >= 4 {
			return 1
		}
	case 14: // SyncGroup        flexibleVersions: 4+
		if apiVersion >= 4 {
			return 1
		}
	case constant.ApiKeyApiVersions: // ApiVersions      flexibleVersions: 3+  ← EXCEPTION: always v0
		return 0
	case constant.ApiKeyCreateTopics: // CreateTopics     flexibleVersions: 5+
		if apiVersion >= 5 {
			return 1
		}
	case 20: // DeleteTopics     flexibleVersions: 4+
		if apiVersion >= 4 {
			return 1
		}
	case 32: // DescribeConfigs  flexibleVersions: 4+
		if apiVersion >= 4 {
			return 1
		}
	case 37: // CreatePartitions flexibleVersions: 2+
		if apiVersion >= 2 {
			return 1
		}
	}
	return 0 // default: Response Header v0, no TAG_BUFFER
}
