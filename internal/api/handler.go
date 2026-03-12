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

// HandleRequest processes a raw Kafka request and returns the response bytes.
// The input is the request body (after the 4-byte size prefix has been stripped).
func (h *Handler) HandleRequest(data []byte) ([]byte, error) {
	decoder := protocol.NewDecoder(data)

	header, err := protocol.DecodeRequestHeader(decoder)
	if err != nil {
		return nil, fmt.Errorf("decoding request header: %w", err)
	}

	log.Printf("[miKago] Request: api_key=%d, api_version=%d, correlation_id=%d, client_id=%v",
		header.APIKey, header.APIVersion, header.CorrelationID, header.ClientID)

	switch header.APIKey {
	case protocol.APIKeyApiVersions:
		return HandleApiVersions(header, decoder, h.Broker)

	case protocol.APIKeyMetadata:
		return HandleMetadata(header, decoder, h.Broker)

	case protocol.APIKeyProduce:
		return HandleProduce(header, decoder, h.Broker)

	case protocol.APIKeyFetch:
		return HandleFetch(header, decoder, h.Broker)

	case protocol.APIKeyListOffsets:
		return HandleListOffsets(header, decoder, h.Broker)

	case protocol.APIKeyCreateTopics:
		return HandleCreateTopics(header, decoder, h.Broker)

	case protocol.APIKeyOffsetCommit:
		return HandleOffsetCommit(header, decoder, h.Broker)

	case protocol.APIKeyOffsetFetch:
		return HandleOffsetFetch(header, decoder, h.Broker)

	case protocol.APIKeyFindCoordinator:
		return HandleFindCoordinator(header, decoder, h.Broker)

	default:
		log.Printf("[miKago] Unsupported API key: %d", header.APIKey)
		return h.unsupportedAPIResponse(header), nil
	}
}

// unsupportedAPIResponse creates an error response for unsupported API keys.
func (h *Handler) unsupportedAPIResponse(header *protocol.RequestHeader) []byte {
	enc := protocol.NewEncoder()
	protocol.EncodeResponseHeader(enc, header.CorrelationID)
	enc.PutInt16(protocol.ErrUnsupportedVersion)
	return enc.Bytes()
}
