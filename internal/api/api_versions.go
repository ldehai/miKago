package api

import (
	"github.com/andy/mikago/internal/broker"
	"github.com/andy/mikago/internal/protocol"
)

// HandleApiVersions handles ApiVersions requests (api_key=18, v0-v1).
//
// ApiVersions v0 Request: (empty body)
// ApiVersions v0 Response: error_code, [api_versions: api_key, min_version, max_version]
//
// ApiVersions v1 Response: same as v0 + throttle_time_ms at the end
func HandleApiVersions(header *protocol.RequestHeader, body *protocol.Decoder, b *broker.Broker) (protocol.Payload, error) {
	enc := protocol.GetEncoder()

	// Response header
	protocol.EncodeResponseHeader(enc, header.CorrelationID)

	// Error code (0 = no error)
	enc.PutInt16(protocol.ErrNone)

	// Array of supported API versions
	versions := protocol.SupportedAPIVersions
	enc.PutArrayLength(len(versions))
	for _, v := range versions {
		enc.PutInt16(v.APIKey)
		enc.PutInt16(v.MinVersion)
		enc.PutInt16(v.MaxVersion)
	}

	// v1 adds throttle_time_ms
	if header.APIVersion >= 1 {
		enc.PutInt32(0) // throttle_time_ms
	}

	return protocol.EncoderPayload{Encoder: enc}, nil
}
