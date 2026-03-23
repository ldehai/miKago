package api

import (
	"fmt"

	"github.com/andy/mikago/internal/broker"
	"github.com/andy/mikago/internal/protocol"
)

// Handler dispatches Kafka API requests to the appropriate handler.
type Handler struct {
	Broker *broker.Broker
}

// NewHandler creates a new API handler.
func NewHandler(b *broker.Broker) *Handler {
	return &Handler{Broker: b}
}

// HandleRequest processes a raw Kafka request and returns the response payload.
// The input is the request body (after the 4-byte size prefix has been stripped).
func (h *Handler) HandleRequest(data []byte) (protocol.Payload, error) {
	decoder := protocol.GetDecoder(data)
	defer protocol.PutDecoder(decoder)

	header, err := protocol.DecodeRequestHeader(decoder)
	if err != nil {
		return nil, fmt.Errorf("decoding request header: %w", err)
	}

	// Log detauls removed for performance, keep only crucial errors below.

	var respPayload protocol.Payload

	switch header.APIKey {
	case protocol.APIKeyApiVersions:
		respPayload, err = HandleApiVersions(header, decoder, h.Broker)

	case protocol.APIKeyMetadata:
		respPayload, err = HandleMetadata(header, decoder, h.Broker)

	case protocol.APIKeyProduce:
		respPayload, err = HandleProduce(header, decoder, h.Broker)

	case protocol.APIKeyFetch:
		return HandleFetchZeroCopy(header, decoder, h.Broker)

	case protocol.APIKeyListOffsets:
		respPayload, err = HandleListOffsets(header, decoder, h.Broker)

	case protocol.APIKeyCreateTopics:
		respPayload, err = HandleCreateTopics(header, decoder, h.Broker)

	case protocol.APIKeyOffsetCommit:
		respPayload, err = HandleOffsetCommit(header, decoder, h.Broker)

	case protocol.APIKeyOffsetFetch:
		respPayload, err = HandleOffsetFetch(header, decoder, h.Broker)

	case protocol.APIKeyFindCoordinator:
		respPayload, err = HandleFindCoordinator(header, decoder, h.Broker)

	default:
		return h.unsupportedAPIResponse(header), nil
	}

	if err != nil {
		return nil, err
	}
	return respPayload, nil
}

// unsupportedAPIResponse creates an error response for unsupported API keys.
func (h *Handler) unsupportedAPIResponse(header *protocol.RequestHeader) protocol.Payload {
	enc := protocol.GetEncoder()
	protocol.EncodeResponseHeader(enc, header.CorrelationID)
	enc.PutInt16(protocol.ErrUnsupportedVersion)
	return protocol.EncoderPayload{Encoder: enc}
}
