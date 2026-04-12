package common

import "github.com/KhaiHust/kaf-go/constant"

// apiFlexibleVersions maps API key → minimum version that is flexible.
// Source: each API's JSON schema "flexibleVersions" field.
// https://github.com/apache/kafka/blob/trunk/clients/src/main/resources/common/message/
var apiFlexibleVersions = map[int16]int16{
	constant.ApiKeyProduce:            9,  // Produce
	constant.ApiKeyFetch:              12, // Fetch
	constant.ApiKeyListOffsets:        6,  // ListOffsets
	constant.ApiKeyMetaData:           9,  // Metadata
	constant.ApiKeyOffsetCommit:       8,  // OffsetCommit
	constant.ApiKeyOffsetFetch:        6,  // OffsetFetch
	constant.ApiFindCoordinator:       3,  // FindCoordinator
	constant.ApiKeyJoinGroup:          6,  // JoinGroup
	constant.ApiKeyHeartbeat:          4,  // Heartbeat
	constant.ApiKeyLeaveGroup:         4,  // LeaveGroup
	constant.ApiKeySyncGroup:          4,  // SyncGroup
	constant.ApiKeyApiVersions:        3,  // ApiVersions — body is flexible but response header is NOT
	constant.ApiKeyCreateTopics:       5,  // CreateTopics
	constant.ApiKeyDeleteTopics:       4,  // DeleteTopics
	constant.ApiKeyInitProducerId:     2,  // InitProducerId — flexible from v2 (per Kafka schema)
	constant.ApiKeyDescribeConfigs:    4,  // DescribeConfigs
	constant.ApiKeyCreatePartitions:   2,  // CreatePartitions
	constant.ApiKeyBrokerRegistration: 0,  // BrokerRegistration (flexible from v0)
	constant.ApiKeyBrokerHeartbeat:    0,  // BrokerHeartbeat (flexible from v0)
}

// IsFlexible reports whether the given API key + version uses
// flexible encoding (compact types + tagged fields).
func IsFlexible(apiKey, apiVersion int16) bool {
	minFlex, ok := apiFlexibleVersions[apiKey]
	if !ok {
		return false
	}
	return apiVersion >= minFlex
}
