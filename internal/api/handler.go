package api

import (
	"fmt"
	"log"

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
	decoder := protocol.NewDecoder(data)

	header, err := protocol.DecodeRequestHeader(decoder)
	if err != nil {
		return nil, fmt.Errorf("decoding request header: %w", err)
	}

	log.Printf("[miKago] Request: api_key=%d, api_version=%d, correlation_id=%d, client_id=%v",
		header.APIKey, header.APIVersion, header.CorrelationID, header.ClientID)

	var respBytes []byte

	switch header.APIKey {
	case protocol.APIKeyApiVersions:
		respBytes, err = HandleApiVersions(header, decoder, h.Broker)

	case protocol.APIKeyMetadata:
		respBytes, err = HandleMetadata(header, decoder, h.Broker)

	case protocol.APIKeyProduce:
		respBytes, err = HandleProduce(header, decoder, h.Broker)

	case protocol.APIKeyFetch:
		return HandleFetchZeroCopy(header, decoder, h.Broker)

	case protocol.APIKeyListOffsets:
		respBytes, err = HandleListOffsets(header, decoder, h.Broker)

	case protocol.APIKeyCreateTopics:
		respBytes, err = HandleCreateTopics(header, decoder, h.Broker)

	case protocol.APIKeyOffsetCommit:
		respBytes, err = HandleOffsetCommit(header, decoder, h.Broker)

	case protocol.APIKeyOffsetFetch:
		respBytes, err = HandleOffsetFetch(header, decoder, h.Broker)

	case protocol.APIKeyFindCoordinator:
		respBytes, err = HandleFindCoordinator(header, decoder, h.Broker)

	default:
		log.Printf("[miKago] Unsupported API key: %d", header.APIKey)
		return h.unsupportedAPIResponse(header), nil
	}

	if err != nil {
		return nil, err
	}
	return protocol.BytesPayload(respBytes), nil
}

// unsupportedAPIResponse creates an error response for unsupported API keys.
func (h *Handler) unsupportedAPIResponse(header *protocol.RequestHeader) protocol.Payload {
	enc := protocol.NewEncoder()
	protocol.EncodeResponseHeader(enc, header.CorrelationID)
	enc.PutInt16(protocol.ErrUnsupportedVersion)
	return protocol.BytesPayload(enc.Bytes())
}
