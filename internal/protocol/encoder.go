package protocol

import (
	"bytes"
	"encoding/binary"
	"sync"
)

var encoderPool = sync.Pool{
	New: func() interface{} {
		return &Encoder{}
	},
}

// GetEncoder retrieves an Encoder from the pool.
func GetEncoder() *Encoder {
	return encoderPool.Get().(*Encoder)
}

// PutEncoder returns an Encoder to the pool after resetting it.
func PutEncoder(e *Encoder) {
	e.Reset()
	encoderPool.Put(e)
}

// Encoder writes Kafka binary protocol data to a buffer.
type Encoder struct {
	buf bytes.Buffer
}

// NewEncoder creates a new Encoder.
func NewEncoder() *Encoder {
	return &Encoder{}
}

// PutInt8 writes a single byte.
func (e *Encoder) PutInt8(v int8) {
	e.buf.WriteByte(byte(v))
}

// PutInt16 writes a big-endian 16-bit integer.
func (e *Encoder) PutInt16(v int16) {
	b := make([]byte, 2)
	binary.BigEndian.PutUint16(b, uint16(v))
	e.buf.Write(b)
}

// PutInt32 writes a big-endian 32-bit integer.
func (e *Encoder) PutInt32(v int32) {
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, uint32(v))
	e.buf.Write(b)
}

// PutInt64 writes a big-endian 64-bit integer.
func (e *Encoder) PutInt64(v int64) {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(v))
	e.buf.Write(b)
}

// PutString writes a length-prefixed string (INT16 length + UTF-8 bytes).
func (e *Encoder) PutString(s string) {
	e.PutInt16(int16(len(s)))
	e.buf.WriteString(s)
}

// PutNullableString writes a nullable string. If nil, writes -1.
func (e *Encoder) PutNullableString(s *string) {
	if s == nil {
		e.PutInt16(-1)
		return
	}
	e.PutString(*s)
}

// PutBytes writes a length-prefixed byte slice (INT32 length + bytes).
func (e *Encoder) PutBytes(b []byte) {
	if b == nil {
		e.PutInt32(-1)
		return
	}
	e.PutInt32(int32(len(b)))
	e.buf.Write(b)
}

// PutBool writes a boolean as a single byte.
func (e *Encoder) PutBool(v bool) {
	if v {
		e.buf.WriteByte(1)
	} else {
		e.buf.WriteByte(0)
	}
}

// PutArrayLength writes an array length as INT32.
func (e *Encoder) PutArrayLength(n int) {
	e.PutInt32(int32(n))
}

// PutRawBytes writes raw bytes without a length prefix.
func (e *Encoder) PutRawBytes(b []byte) {
	e.buf.Write(b)
}

// Bytes returns the accumulated bytes.
func (e *Encoder) Bytes() []byte {
	return e.buf.Bytes()
}

// Len returns the current length of the buffer.
func (e *Encoder) Len() int {
	return e.buf.Len()
}

// Reset clears the buffer.
func (e *Encoder) Reset() {
	e.buf.Reset()
}

// WriteSizePrefix creates a complete message with a 4-byte size prefix.
func WriteSizePrefix(payload []byte) []byte {
	msg := make([]byte, 4+len(payload))
	binary.BigEndian.PutUint32(msg[:4], uint32(len(payload)))
	copy(msg[4:], payload)
	return msg
}
