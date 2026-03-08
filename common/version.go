package common

import "github.com/KhaiHust/kaf-go/constant"

// apiFlexibleVersions maps API key → minimum version that is flexible.
// Source: each API's JSON schema "flexibleVersions" field.
// https://github.com/apache/kafka/blob/trunk/clients/src/main/resources/common/message/
var apiFlexibleVersions = map[int16]int16{
	0:                           9,  // Produce
	1:                           12, // Fetch
	2:                           6,  // ListOffsets
	constant.ApiKeyMetaData:     9,  // Metadata
	8:                           8,  // OffsetCommit
	9:                           6,  // OffsetFetch
	10:                          3,  // FindCoordinator
	11:                          6,  // JoinGroup
	12:                          4,  // Heartbeat
	13:                          4,  // LeaveGroup
	14:                          4,  // SyncGroup
	constant.ApiKeyApiVersions:  3,  // ApiVersions — body is flexible but response header is NOT
	constant.ApiKeyCreateTopics: 5,  // CreateTopics
	20:                          4,  // DeleteTopics
	32:                          4,  // DescribeConfigs
	37:                          2,  // CreatePartitions
}

// apiMinVersions maps API key → minimum supported version by this broker.
var apiMinVersions = map[int16]int16{
	constant.ApiKeyApiVersions:  0,
	constant.ApiKeyCreateTopics: 7,
}

// apiMaxVersions maps API key → maximum supported version by this broker.
var apiMaxVersions = map[int16]int16{
	constant.ApiKeyApiVersions:  3,
	constant.ApiKeyCreateTopics: 7,
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

// ResponseHeaderFlexible reports whether the response header for this
// API key + version should include a TAG_BUFFER (Response Header v1).
//
// Special case: ApiVersions (18) always uses Response Header v0
// even though its body is flexible — bootstrap compatibility requirement.
func ResponseHeaderFlexible(apiKey, apiVersion int16) bool {
	if apiKey == 18 {
		return false // hardcoded exception per KIP-511
	}
	return IsFlexible(apiKey, apiVersion)
}
