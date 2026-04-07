package consumer

import (
	"github.com/KhaiHust/kaf-go/constant"
	"github.com/KhaiHust/kaf-go/protocol"
	"github.com/KhaiHust/kaf-go/protocol/types"
)

const FindCoordinatorRequestVersion = 6

type FindCoordinatorRequest struct {
	KeyType         int8
	CoordinatorKeys []types.CompactString
}

func (f *FindCoordinatorRequest) ApiKey() int64 {
	return constant.ApiFindCoordinator
}

func (f *FindCoordinatorRequest) ApiVersion() int16 {
	return FindCoordinatorRequestVersion
}

func (f *FindCoordinatorRequest) CorrelationID() int32 {
	//TODO implement me
	panic("implement me")
}

func (f *FindCoordinatorRequest) Decode(r *protocol.Reader) error {
	var err error
	if f.KeyType, err = r.ReadInt8(); err != nil {
		return err
	}
	coordinatorKeysLength, err := r.ReadCompactArrayLen()
	if err != nil {
		return err
	}

	coordinatorKeys := make([]types.CompactString, coordinatorKeysLength)
	for idx, _ := range coordinatorKeys {
		coordinatorKeys[idx], err = r.ReadCompactString()
		if err != nil {
			return err
		}
	}
	f.CoordinatorKeys = coordinatorKeys
	return r.ReadTaggedFields()
}
