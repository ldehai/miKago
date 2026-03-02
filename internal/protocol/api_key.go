package protocol

// API Keys
const (
	APIKeyProduce     int16 = 0
	APIKeyFetch       int16 = 1
	APIKeyListOffsets int16 = 2
	APIKeyMetadata    int16 = 3
	APIKeyApiVersions int16 = 18
)

// Error Codes
const (
	ErrNone                      int16 = 0
	ErrOffsetOutOfRange          int16 = 1
	ErrUnknownTopicOrPartition   int16 = 3
	ErrInvalidMessage            int16 = 4
	ErrLeaderNotAvailable        int16 = 5
	ErrNotLeaderForPartition     int16 = 6
	ErrRequestTimedOut           int16 = 7
	ErrMessageTooLarge           int16 = 10
	ErrInvalidTopic              int16 = 17
	ErrUnsupportedVersion        int16 = 35
	ErrTopicAlreadyExists        int16 = 36
	ErrInvalidPartitions         int16 = 37
	ErrInvalidReplicationFactor  int16 = 38
)

// APIVersionRange defines the supported version range for an API.
type APIVersionRange struct {
	APIKey     int16
	MinVersion int16
	MaxVersion int16
}

// SupportedAPIVersions lists all APIs supported by this broker.
var SupportedAPIVersions = []APIVersionRange{
	{APIKey: APIKeyProduce, MinVersion: 0, MaxVersion: 0},
	{APIKey: APIKeyFetch, MinVersion: 0, MaxVersion: 0},
	{APIKey: APIKeyMetadata, MinVersion: 0, MaxVersion: 0},
	{APIKey: APIKeyApiVersions, MinVersion: 0, MaxVersion: 1},
}
