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
				MinVersion: 10,
				MaxVersion: 10,
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
				ApiKey:     constant.ApiKeyOffsetCommit,
				MinVersion: 8,
				MaxVersion: 8,
			},
			{
				ApiKey:     constant.ApiKeyOffsetFetch,
				MinVersion: 8,
				MaxVersion: 8,
			},
			{
				ApiKey:     constant.ApiFindCoordinator,
				MinVersion: 6,
				MaxVersion: 6,
			},
			{
				ApiKey:     constant.ApiKeyJoinGroup,
				MinVersion: 9,
				MaxVersion: 9,
			},
			{
				ApiKey:     constant.ApiKeyLeaveGroup,
				MinVersion: 5,
				MaxVersion: 5,
			},
			{
				ApiKey:     constant.ApiKeyHeartbeat,
				MinVersion: 4,
				MaxVersion: 4,
			},
			{
				ApiKey:     constant.ApiKeySyncGroup,
				MinVersion: 5,
				MaxVersion: 5,
			},
			{
				ApiKey:     constant.ApiKeyCreateTopics,
				MinVersion: 0,
				MaxVersion: 7,
			},
			{
				ApiKey:     constant.ApiKeyInitProducerId,
				MinVersion: 0,
				MaxVersion: 5,
			},
			{
				ApiKey:     constant.ApiKeyBrokerRegistration,
				MinVersion: 0,
				MaxVersion: 0,
			},
			{
				ApiKey:     constant.ApiKeyBrokerHeartbeat,
				MinVersion: 0,
				MaxVersion: 0,
			},
		},
		ThrottleTimeMs: 0,
	}

	return responseHeader, apiVersionsResponse, nil
}
