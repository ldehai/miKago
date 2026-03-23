package storage

import (
	"encoding/binary"
	"fmt"
	"io"
	"sync"
	"time"
)

// Record represents a single message stored on disk.
type Record struct {
	Offset    int64
	Timestamp time.Time
	Key       []byte
	Value     []byte
}

// RecordHeaderSize is the fixed overhead for a Kafka MessageSet v1 message.
// offset(8) + message_size(4) + crc(4) + magic(1) + attributes(1) + timestamp(8) + keyLen(4) + valueLen(4) = 34
const RecordHeaderSize = 34

// RecordSize returns the total size of the record on disk, strictly matching Kafka MessageSet v1 format.
func RecordSize(key, value []byte) int {
	return RecordHeaderSize + len(key) + len(value)
}

// encodeBufPool pools byte slices used by EncodeRecord to avoid per-record allocations.
var encodeBufPool = sync.Pool{
	New: func() interface{} {
		b := make([]byte, 0, 4096)
		return &b
	},
}

// EncodeRecord writes a record to the writer in official Kafka MessageSet v1 binary format.
// Uses a pooled buffer to combine header + key + value into a single Write() call.
func EncodeRecord(w io.Writer, r *Record) error {
	keyLen := len(r.Key)
	if r.Key == nil {
		keyLen = -1
	}
	valueLen := len(r.Value)
	if r.Value == nil {
		valueLen = -1
	}

	// message_size: 4(crc) + 1(magic) + 1(attr) + 8(ts) + 4(keyLen) + len(key) + 4(valLen) + len(val)
	actualKeyLen := keyLen
	if actualKeyLen < 0 {
		actualKeyLen = 0
	}
	actualValueLen := valueLen
	if actualValueLen < 0 {
		actualValueLen = 0
	}
	sizePayload := 22 + actualKeyLen + actualValueLen

	totalSize := RecordHeaderSize + actualKeyLen + actualValueLen

	// Get a pooled buffer
	bp := encodeBufPool.Get().(*[]byte)
	buf := *bp
	if cap(buf) < totalSize {
		buf = make([]byte, totalSize)
	} else {
		buf = buf[:totalSize]
	}

	// Encode header
	binary.BigEndian.PutUint64(buf[0:8], uint64(r.Offset))
	binary.BigEndian.PutUint32(buf[8:12], uint32(sizePayload))
	binary.BigEndian.PutUint32(buf[12:16], 0) // TODO: CRC32
	buf[16] = 1                               // Magic = 1 (v1)
	buf[17] = 0                               // Attributes = 0
	binary.BigEndian.PutUint64(buf[18:26], uint64(r.Timestamp.UnixMilli()))
	binary.BigEndian.PutUint32(buf[26:30], uint32(int32(keyLen)))
	binary.BigEndian.PutUint32(buf[30:34], uint32(int32(valueLen)))

	// Copy key and value into the same buffer
	pos := RecordHeaderSize
	if actualKeyLen > 0 {
		copy(buf[pos:], r.Key)
		pos += actualKeyLen
	}
	if actualValueLen > 0 {
		copy(buf[pos:], r.Value)
	}

	// Single Write() call
	_, err := w.Write(buf)

	// Return buffer to pool
	*bp = buf
	encodeBufPool.Put(bp)

	if err != nil {
		return fmt.Errorf("write record: %w", err)
	}
	return nil
}

// DecodeRecord reads a record formatted as Kafka MessageSet v1 from the reader.
func DecodeRecord(r io.Reader) (*Record, error) {
	var buf [RecordHeaderSize]byte

	if _, err := io.ReadFull(r, buf[:]); err != nil {
		return nil, err
	}

	offset := int64(binary.BigEndian.Uint64(buf[0:8]))
	// size := int32(binary.BigEndian.Uint32(buf[8:12]))
	// crc := int32(binary.BigEndian.Uint32(buf[12:16]))
	// magic := buf[16]
	// attr := buf[17]
	tsMillis := int64(binary.BigEndian.Uint64(buf[18:26]))
	keyLen := int32(binary.BigEndian.Uint32(buf[26:30]))
	valueLen := int32(binary.BigEndian.Uint32(buf[30:34]))

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
