package protocol

// API Keys
const (
	APIKeyProduce         int16 = 0
	APIKeyFetch           int16 = 1
	APIKeyListOffsets     int16 = 2
	APIKeyMetadata        int16 = 3
	APIKeyOffsetCommit    int16 = 8
	APIKeyOffsetFetch     int16 = 9
	APIKeyFindCoordinator int16 = 10
	APIKeyApiVersions     int16 = 18
	APIKeyCreateTopics    int16 = 19
)

// Error Codes
const (
	ErrUnknownServerError       int16 = -1
	ErrNone                     int16 = 0
	ErrOffsetOutOfRange         int16 = 1
	ErrUnknownTopicOrPartition  int16 = 3
	ErrInvalidMessage           int16 = 4
	ErrLeaderNotAvailable       int16 = 5
	ErrNotLeaderForPartition    int16 = 6
	ErrRequestTimedOut          int16 = 7
	ErrMessageTooLarge          int16 = 10
	ErrInvalidTopic             int16 = 17
	ErrUnsupportedVersion       int16 = 35
	ErrTopicAlreadyExists       int16 = 36
	ErrInvalidPartitions        int16 = 37
	ErrInvalidReplicationFactor int16 = 38
)

// APIVersionRange defines the supported version range for an API.
type APIVersionRange struct {
	APIKey     int16
	MinVersion int16
	MaxVersion int16
}

// SupportedAPIVersions lists all APIs supported by this broker.
// Updated to support kafka-go client minimum requirements:
//   - Produce: min v2 (kafka-go negotiates v2,v3,v7)
//   - Fetch: min v2 (kafka-go negotiates v2,v5,v10)
//   - ListOffsets: v1 (kafka-go uses writeListOffsetRequestV1)
//   - Metadata: min v1 (kafka-go negotiates v1,v6)
var SupportedAPIVersions = []APIVersionRange{
	{APIKey: APIKeyProduce, MinVersion: 0, MaxVersion: 2},
	{APIKey: APIKeyFetch, MinVersion: 0, MaxVersion: 2},
	{APIKey: APIKeyListOffsets, MinVersion: 0, MaxVersion: 1},
	{APIKey: APIKeyOffsetCommit, MinVersion: 0, MaxVersion: 0},
	{APIKey: APIKeyOffsetFetch, MinVersion: 0, MaxVersion: 1},
	{APIKey: APIKeyFindCoordinator, MinVersion: 0, MaxVersion: 0},
	{APIKey: APIKeyMetadata, MinVersion: 0, MaxVersion: 1},
	{APIKey: APIKeyCreateTopics, MinVersion: 0, MaxVersion: 1},
	{APIKey: APIKeyApiVersions, MinVersion: 0, MaxVersion: 1},
}
