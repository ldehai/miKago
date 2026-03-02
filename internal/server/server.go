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
	addr     string
	handler  *api.Handler
	listener net.Listener
	wg       sync.WaitGroup
}

// NewServer creates a new Server.
func NewServer(addr string, handler *api.Handler) *Server {
	return &Server{
		addr:    addr,
		handler: handler,
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
	log.Printf("[miKago] New connection from %s", remoteAddr)

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		// Read the 4-byte message size
		sizeBuf := make([]byte, 4)
		_, err := io.ReadFull(conn, sizeBuf)
		if err != nil {
			if err != io.EOF {
				log.Printf("[miKago] Error reading size from %s: %v", remoteAddr, err)
			}
			return
		}

		msgSize := binary.BigEndian.Uint32(sizeBuf)
		if msgSize == 0 || msgSize > 100*1024*1024 { // max 100MB message
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
		response, err := s.handler.HandleRequest(msgBuf)
		if err != nil {
			log.Printf("[miKago] Error handling request from %s: %v", remoteAddr, err)
			// Try to send an error response if we can extract the correlation ID
			if len(msgBuf) >= 8 {
				corrID := int32(binary.BigEndian.Uint32(msgBuf[4:8]))
				response = makeErrorResponse(corrID)
			} else {
				return
			}
		}

		// Write the response with size prefix
		responseMsg := protocol.WriteSizePrefix(response)
		_, err = conn.Write(responseMsg)
		if err != nil {
			log.Printf("[miKago] Error writing response to %s: %v", remoteAddr, err)
			return
		}
	}
}

// makeErrorResponse creates a minimal error response.
func makeErrorResponse(correlationID int32) []byte {
	enc := protocol.NewEncoder()
	protocol.EncodeResponseHeader(enc, correlationID)
	enc.PutInt16(-1) // generic error
	return enc.Bytes()
}
