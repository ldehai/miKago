package protocol

import (
	"io"
	"os"
)

// Payload represents data that can be sent over a network connection.
// It enables zero-copy operations like sendfile by exposing an io.WriterTo.
type Payload interface {
	Size() int32
	WriteTo(w io.Writer) (int64, error)
	Release() error
}

// BytesPayload wraps a pure byte slice.
type BytesPayload []byte

func (b BytesPayload) Size() int32 {
	return int32(len(b))
}

func (b BytesPayload) WriteTo(w io.Writer) (int64, error) {
	n, err := w.Write(b)
	return int64(n), err
}

func (b BytesPayload) Release() error {
	return nil
}

// EncoderPayload adapts an Encoder buffer to the Payload interface.
type EncoderPayload struct {
	Encoder *Encoder
}

func (e EncoderPayload) Size() int32 {
	return int32(e.Encoder.Len())
}

func (e EncoderPayload) WriteTo(w io.Writer) (int64, error) {
	n, err := w.Write(e.Encoder.Bytes())
	return int64(n), err
}

func (e EncoderPayload) Release() error {
	PutEncoder(e.Encoder)
	return nil
}

// FilePayload represents a contiguous chunk of a file.
// Writing this payload to a net.TCPConn will automatically invoke the
// operating system's zero-copy 'sendfile' system call when supported.
type FilePayload struct {
	File   *os.File
	Offset int64
	Length int64
}

func (f FilePayload) Size() int32 {
	return int32(f.Length)
}

func (f FilePayload) WriteTo(w io.Writer) (int64, error) {
	// Let io.Copy utilize io.ReaderFrom if available (e.g. net.TCPConn)
	// which will seamlessly call sendfile under the hood!
	return io.Copy(w, io.NewSectionReader(f.File, f.Offset, f.Length))
}

func (f FilePayload) Release() error {
	if f.File != nil {
		return f.File.Close()
	}
	return nil
}

// MultiPayload aggregates multiple payloads sequentially.
type MultiPayload struct {
	Payloads []Payload
}

func (m MultiPayload) Size() int32 {
	var total int32
	for _, p := range m.Payloads {
		total += p.Size()
	}
	return total
}

func (m MultiPayload) WriteTo(w io.Writer) (int64, error) {
	var total int64
	for _, p := range m.Payloads {
		n, err := p.WriteTo(w)
		total += n
		if err != nil {
			return total, err
		}
	}
	return total, nil
}

func (m MultiPayload) Release() error {
	var firstErr error
	for _, p := range m.Payloads {
		if err := p.Release(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}
