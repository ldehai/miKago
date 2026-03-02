package storage

import (
	"encoding/binary"
	"fmt"
	"io"
	"time"
)

// Record represents a single message stored on disk.
type Record struct {
	Offset    int64
	Timestamp time.Time
	Key       []byte
	Value     []byte
}

// RecordHeaderSize is the fixed size of a record header in bytes.
// offset(8) + timestamp(8) + keyLen(4) + valueLen(4) = 24
const RecordHeaderSize = 24

// RecordSize returns the total size of the record on disk.
func RecordSize(key, value []byte) int {
	return RecordHeaderSize + len(key) + len(value)
}

// EncodeRecord writes a record to the writer.
// Format: offset(8) | timestamp(8) | keyLen(4) | key(var) | valueLen(4) | value(var)
func EncodeRecord(w io.Writer, r *Record) error {
	var buf [RecordHeaderSize]byte

	binary.BigEndian.PutUint64(buf[0:8], uint64(r.Offset))
	binary.BigEndian.PutUint64(buf[8:16], uint64(r.Timestamp.UnixMilli()))

	keyLen := len(r.Key)
	if r.Key == nil {
		keyLen = -1
	}
	binary.BigEndian.PutUint32(buf[16:20], uint32(int32(keyLen)))

	valueLen := len(r.Value)
	if r.Value == nil {
		valueLen = -1
	}
	binary.BigEndian.PutUint32(buf[20:24], uint32(int32(valueLen)))

	if _, err := w.Write(buf[:]); err != nil {
		return fmt.Errorf("write record header: %w", err)
	}

	if keyLen > 0 {
		if _, err := w.Write(r.Key); err != nil {
			return fmt.Errorf("write record key: %w", err)
		}
	}

	if valueLen > 0 {
		if _, err := w.Write(r.Value); err != nil {
			return fmt.Errorf("write record value: %w", err)
		}
	}

	return nil
}

// DecodeRecord reads a record from the reader.
func DecodeRecord(r io.Reader) (*Record, error) {
	var buf [RecordHeaderSize]byte

	if _, err := io.ReadFull(r, buf[:]); err != nil {
		return nil, err
	}

	offset := int64(binary.BigEndian.Uint64(buf[0:8]))
	tsMillis := int64(binary.BigEndian.Uint64(buf[8:16]))
	keyLen := int32(binary.BigEndian.Uint32(buf[16:20]))
	valueLen := int32(binary.BigEndian.Uint32(buf[20:24]))

	rec := &Record{
		Offset:    offset,
		Timestamp: time.UnixMilli(tsMillis),
	}

	if keyLen >= 0 {
		rec.Key = make([]byte, keyLen)
		if _, err := io.ReadFull(r, rec.Key); err != nil {
			return nil, fmt.Errorf("read record key: %w", err)
		}
	}

	if valueLen >= 0 {
		rec.Value = make([]byte, valueLen)
		if _, err := io.ReadFull(r, rec.Value); err != nil {
			return nil, fmt.Errorf("read record value: %w", err)
		}
	}

	return rec, nil
}
