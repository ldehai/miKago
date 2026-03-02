package protocol

import (
	"encoding/binary"
	"fmt"
)

// Decoder reads Kafka binary protocol data from a byte slice.
type Decoder struct {
	raw []byte
	off int
}

// NewDecoder creates a new Decoder from a byte slice.
func NewDecoder(data []byte) *Decoder {
	return &Decoder{raw: data, off: 0}
}

// Remaining returns the number of unread bytes.
func (d *Decoder) Remaining() int {
	return len(d.raw) - d.off
}

// Offset returns the current read position.
func (d *Decoder) Offset() int {
	return d.off
}

// Int8 reads a single byte as int8.
func (d *Decoder) Int8() (int8, error) {
	if d.Remaining() < 1 {
		return 0, fmt.Errorf("decoder: not enough bytes for Int8, remaining=%d", d.Remaining())
	}
	v := int8(d.raw[d.off])
	d.off++
	return v, nil
}

// Int16 reads a big-endian 16-bit integer.
func (d *Decoder) Int16() (int16, error) {
	if d.Remaining() < 2 {
		return 0, fmt.Errorf("decoder: not enough bytes for Int16, remaining=%d", d.Remaining())
	}
	v := int16(binary.BigEndian.Uint16(d.raw[d.off:]))
	d.off += 2
	return v, nil
}

// Int32 reads a big-endian 32-bit integer.
func (d *Decoder) Int32() (int32, error) {
	if d.Remaining() < 4 {
		return 0, fmt.Errorf("decoder: not enough bytes for Int32, remaining=%d", d.Remaining())
	}
	v := int32(binary.BigEndian.Uint32(d.raw[d.off:]))
	d.off += 4
	return v, nil
}

// Int64 reads a big-endian 64-bit integer.
func (d *Decoder) Int64() (int64, error) {
	if d.Remaining() < 8 {
		return 0, fmt.Errorf("decoder: not enough bytes for Int64, remaining=%d", d.Remaining())
	}
	v := int64(binary.BigEndian.Uint64(d.raw[d.off:]))
	d.off += 8
	return v, nil
}

// String reads a length-prefixed string (INT16 length + UTF-8 bytes).
func (d *Decoder) String() (string, error) {
	length, err := d.Int16()
	if err != nil {
		return "", fmt.Errorf("decoder: reading string length: %w", err)
	}
	if length < 0 {
		return "", fmt.Errorf("decoder: negative string length %d", length)
	}
	if d.Remaining() < int(length) {
		return "", fmt.Errorf("decoder: not enough bytes for String, need=%d remaining=%d", length, d.Remaining())
	}
	s := string(d.raw[d.off : d.off+int(length)])
	d.off += int(length)
	return s, nil
}

// NullableString reads a nullable length-prefixed string. Returns nil for length -1.
func (d *Decoder) NullableString() (*string, error) {
	length, err := d.Int16()
	if err != nil {
		return nil, fmt.Errorf("decoder: reading nullable string length: %w", err)
	}
	if length == -1 {
		return nil, nil
	}
	if length < 0 {
		return nil, fmt.Errorf("decoder: invalid nullable string length %d", length)
	}
	if d.Remaining() < int(length) {
		return nil, fmt.Errorf("decoder: not enough bytes for NullableString, need=%d remaining=%d", length, d.Remaining())
	}
	s := string(d.raw[d.off : d.off+int(length)])
	d.off += int(length)
	return &s, nil
}

// Bytes reads a length-prefixed byte slice (INT32 length + bytes). Returns nil for length -1.
func (d *Decoder) Bytes() ([]byte, error) {
	length, err := d.Int32()
	if err != nil {
		return nil, fmt.Errorf("decoder: reading bytes length: %w", err)
	}
	if length == -1 {
		return nil, nil
	}
	if length < 0 {
		return nil, fmt.Errorf("decoder: invalid bytes length %d", length)
	}
	if d.Remaining() < int(length) {
		return nil, fmt.Errorf("decoder: not enough bytes for Bytes, need=%d remaining=%d", length, d.Remaining())
	}
	b := make([]byte, length)
	copy(b, d.raw[d.off:d.off+int(length)])
	d.off += int(length)
	return b, nil
}

// RawBytes reads exactly n raw bytes without a length prefix.
func (d *Decoder) RawBytes(n int) ([]byte, error) {
	if d.Remaining() < n {
		return nil, fmt.Errorf("decoder: not enough bytes for RawBytes, need=%d remaining=%d", n, d.Remaining())
	}
	b := make([]byte, n)
	copy(b, d.raw[d.off:d.off+n])
	d.off += n
	return b, nil
}

// ArrayLength reads an array length as INT32.
func (d *Decoder) ArrayLength() (int, error) {
	n, err := d.Int32()
	if err != nil {
		return 0, err
	}
	return int(n), nil
}

// Bool reads a boolean (single byte, 0=false, non-zero=true).
func (d *Decoder) Bool() (bool, error) {
	v, err := d.Int8()
	if err != nil {
		return false, err
	}
	return v != 0, nil
}
