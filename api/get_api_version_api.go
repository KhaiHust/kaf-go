package api

import (
	"github.com/KhaiHust/kaf-go/constant"
	"github.com/KhaiHust/kaf-go/protocol"
	"github.com/KhaiHust/kaf-go/protocol/admin"
)

func HandleApiVersionApi(header *protocol.RequestHeader, reader *protocol.Reader) (*protocol.ResponseHeader, protocol.Response, error) {
	responseHeader := &protocol.ResponseHeader{
		CorrelationId: header.CorrelationId,
	}

	apiVersionsResponse := &admin.ApiVersionResponse{
		ErrorCode: 0,
		ApiKeys: []admin.ApiKey{
			{
				ApiKey:     constant.ApiKeyProduce,
				MinVersion: 13,
				MaxVersion: 13,
			},
			{
				ApiKey:     constant.ApiKeyFetch,
				MinVersion: 18,
				MaxVersion: 18,
			},
			{
				ApiKey:     constant.ApiKeyListOffsets,
				MinVersion: 11,
				MaxVersion: 11,
			},
			{
				ApiKey:     constant.ApiKeyApiVersions,
				MinVersion: 0,
				MaxVersion: 3,
			},
			{
				ApiKey:     constant.ApiKeyMetaData,
				MinVersion: 0,
				MaxVersion: 13,
			},
			{
				ApiKey:     constant.ApiKeyCreateTopics,
				MinVersion: 0,
				MaxVersion: 7,
			},
		},
		ThrottleTimeMs: 0,
	}

	return responseHeader, apiVersionsResponse, nil
}
