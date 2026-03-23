package server

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"net"
	"sync"

	"github.com/andy/mikago/internal/api"
	"github.com/andy/mikago/internal/protocol"
)

// Server is the TCP server that speaks the Kafka binary protocol.
type Server struct {
	addr            string
	handler         *api.Handler
	listener        net.Listener
	wg              sync.WaitGroup
	maxRequestBytes int32
}

// NewServer creates a new Server.
func NewServer(addr string, handler *api.Handler, maxRequestBytes int32) *Server {
	if maxRequestBytes <= 0 {
		maxRequestBytes = 100 * 1024 * 1024 // 100MB default
	}
	return &Server{
		addr:            addr,
		handler:         handler,
		maxRequestBytes: maxRequestBytes,
	}
}

// Start begins listening for connections. It blocks until the context is cancelled.
func (s *Server) Start(ctx context.Context) error {
	var err error
	s.listener, err = net.Listen("tcp", s.addr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", s.addr, err)
	}

	log.Printf("[miKago] 🚀 Broker listening on %s", s.addr)

	// Goroutine to close listener when context is cancelled
	go func() {
		<-ctx.Done()
		log.Println("[miKago] Shutting down server...")
		s.listener.Close()
	}()

	for {
		conn, err := s.listener.Accept()
		if err != nil {
			select {
			case <-ctx.Done():
				s.wg.Wait()
				return nil
			default:
				log.Printf("[miKago] Accept error: %v", err)
				continue
			}
		}

		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			s.handleConnection(ctx, conn)
		}()
	}
}

// handleConnection processes a single client connection.
// Kafka uses size-delimited framing: each message starts with a 4-byte big-endian size.
func (s *Server) handleConnection(ctx context.Context, conn net.Conn) {
	defer conn.Close()

	remoteAddr := conn.RemoteAddr().String()
	log.Printf("[miKago] New connection from %s", conn.RemoteAddr())

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		// Read the 4-byte message size
		var sizeBuf [4]byte
		_, err := io.ReadFull(conn, sizeBuf[:])
		if err != nil {
			if err != io.EOF {
				log.Printf("[miKago] Error reading size from %s: %v", remoteAddr, err)
			}
			return
		}

		msgSize := binary.BigEndian.Uint32(sizeBuf[:])
		if msgSize == 0 || msgSize > uint32(s.maxRequestBytes) {
			log.Printf("[miKago] Invalid message size %d from %s", msgSize, remoteAddr)
			return
		}

		// Read the full message body
		msgBuf := make([]byte, msgSize)
		_, err = io.ReadFull(conn, msgBuf)
		if err != nil {
			log.Printf("[miKago] Error reading message body from %s: %v", remoteAddr, err)
			return
		}

		// Handle the request
		responsePayload, err := s.handler.HandleRequest(msgBuf)
		if err != nil {
			log.Printf("[miKago] Error handling request from %s: %v", remoteAddr, err)
			// Try to send an error response if we can extract the correlation ID
			if len(msgBuf) >= 8 {
				corrID := int32(binary.BigEndian.Uint32(msgBuf[4:8]))
				responsePayload = makeErrorResponse(corrID)
			} else {
				return
			}
		}

		// Write the response size prefix first
		var respSizeBuf [4]byte
		binary.BigEndian.PutUint32(respSizeBuf[:], uint32(responsePayload.Size()))
		_, err = conn.Write(respSizeBuf[:])
		if err != nil {
			log.Printf("[miKago] Error writing response size to %s: %v", remoteAddr, err)
			return
		}

		// Write the actual payload (potentially zero-copy sendfile)
		_, err = responsePayload.WriteTo(conn)
		responsePayload.Release()

		if err != nil {
			log.Printf("[miKago] Error writing response payload to %s: %v", remoteAddr, err)
			return
		}
	}
}

// makeErrorResponse creates a minimal error response.
func makeErrorResponse(correlationID int32) protocol.Payload {
	enc := protocol.NewEncoder()
	protocol.EncodeResponseHeader(enc, correlationID)
	enc.PutInt16(-1) // generic error
	return protocol.BytesPayload(enc.Bytes())
}
