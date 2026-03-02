package protocol

import "fmt"

// RequestHeader represents a Kafka request header (v0/v1).
// v0: api_key, api_version, correlation_id, client_id
// v1: same as v0 (used for ApiVersions v0)
type RequestHeader struct {
	APIKey        int16
	APIVersion    int16
	CorrelationID int32
	ClientID      *string
}

// DecodeRequestHeader decodes a Kafka request header from the decoder.
// The header version depends on the api_key and api_version, but for
// our MVP we handle v0/v1 headers (no tagged fields).
func DecodeRequestHeader(d *Decoder) (*RequestHeader, error) {
	apiKey, err := d.Int16()
	if err != nil {
		return nil, fmt.Errorf("decoding api_key: %w", err)
	}

	apiVersion, err := d.Int16()
	if err != nil {
		return nil, fmt.Errorf("decoding api_version: %w", err)
	}

	correlationID, err := d.Int32()
	if err != nil {
		return nil, fmt.Errorf("decoding correlation_id: %w", err)
	}

	clientID, err := d.NullableString()
	if err != nil {
		return nil, fmt.Errorf("decoding client_id: %w", err)
	}

	return &RequestHeader{
		APIKey:        apiKey,
		APIVersion:    apiVersion,
		CorrelationID: correlationID,
		ClientID:      clientID,
	}, nil
}

// EncodeResponseHeader writes a Kafka response header (v0).
// v0 response header: correlation_id only.
func EncodeResponseHeader(e *Encoder, correlationID int32) {
	e.PutInt32(correlationID)
}
