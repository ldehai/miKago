package api

import (
	"github.com/andy/mikago/internal/broker"
	"github.com/andy/mikago/internal/protocol"
)

// HandleFindCoordinator processes a FindCoordinator v0 request (API key 10).
func HandleFindCoordinator(header *protocol.RequestHeader, d *protocol.Decoder, b *broker.Broker) (protocol.Payload, error) {
	// Request structure:
	// group_id (string)

	_, _ = d.String() // group_id, unused since we only have 1 broker

	enc := protocol.GetEncoder()
	protocol.EncodeResponseHeader(enc, header.CorrelationID)

	// Response structure:
	// error_code (int16)
	// coordinator_node_id (int32)
	// coordinator_host (string)
	// coordinator_port (int32)

	enc.PutInt16(protocol.ErrNone)
	enc.PutInt32(b.Config.BrokerID)
	enc.PutString(b.Config.Host)
	enc.PutInt32(b.Config.Port)

	return protocol.EncoderPayload{Encoder: enc}, nil
}
