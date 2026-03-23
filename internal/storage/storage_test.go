package storage

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// --- Record Tests ---

func TestRecordEncodeDecode(t *testing.T) {
	original := &Record{
		Offset:    42,
		Timestamp: time.UnixMilli(1700000000000),
		Key:       []byte("test-key"),
		Value:     []byte("test-value"),
	}

	var buf bytes.Buffer
	if err := EncodeRecord(&buf, original); err != nil {
		t.Fatalf("encode error: %v", err)
	}

	decoded, err := DecodeRecord(&buf)
	if err != nil {
		t.Fatalf("decode error: %v", err)
	}

	if decoded.Offset != original.Offset {
		t.Fatalf("offset: got %d, want %d", decoded.Offset, original.Offset)
	}
	if decoded.Timestamp.UnixMilli() != original.Timestamp.UnixMilli() {
		t.Fatalf("timestamp: got %v, want %v", decoded.Timestamp, original.Timestamp)
	}
	if !bytes.Equal(decoded.Key, original.Key) {
		t.Fatalf("key: got %q, want %q", decoded.Key, original.Key)
	}
	if !bytes.Equal(decoded.Value, original.Value) {
		t.Fatalf("value: got %q, want %q", decoded.Value, original.Value)
	}
	t.Logf("✅ Record encode/decode round-trip: offset=%d, key=%q, value=%q",
		decoded.Offset, decoded.Key, decoded.Value)
}

func TestRecordNullKey(t *testing.T) {
	original := &Record{
		Offset:    0,
		Timestamp: time.Now(),
		Key:       nil,
		Value:     []byte("value-only"),
	}

	var buf bytes.Buffer
	EncodeRecord(&buf, original)
	decoded, _ := DecodeRecord(&buf)

	if decoded.Key != nil {
		t.Fatalf("expected nil key, got %q", decoded.Key)
	}
	if !bytes.Equal(decoded.Value, original.Value) {
		t.Fatalf("value mismatch")
	}
	t.Log("✅ Null key record round-trip")
}

// --- Segment Tests ---

func TestSegmentAppendAndRead(t *testing.T) {
	dir := t.TempDir()

	seg, err := NewSegment(dir, 0)
	if err != nil {
		t.Fatalf("create segment: %v", err)
	}
	defer seg.Close()

	// Append 25 records (more than IndexInterval=10 to test sparse index)
	for i := 0; i < 25; i++ {
		rec := &Record{
			Offset:    int64(i),
			Timestamp: time.Now(),
			Key:       []byte("key"),
			Value:     []byte("value"),
		}
		if _, err := seg.Append(rec); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}

	if seg.NextOffset() != 25 {
		t.Fatalf("nextOffset: got %d, want 25", seg.NextOffset())
	}
	t.Logf("✅ Appended 25 records, logSize=%d bytes, indexCount=%d", seg.Size(), seg.indexCount)

	// Flush buffer to disk for reading
	if err := seg.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	// Read from offset 15
	records, err := seg.ReadFrom(15, 1024*1024)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if len(records) != 10 {
		t.Fatalf("expected 10 records, got %d", len(records))
	}
	if records[0].Offset != 15 {
		t.Fatalf("first offset: got %d, want 15", records[0].Offset)
	}
	t.Logf("✅ ReadFrom(15) returned %d records, first offset=%d", len(records), records[0].Offset)
}

func TestSegmentRecover(t *testing.T) {
	dir := t.TempDir()

	// Create and write
	seg, _ := NewSegment(dir, 0)
	for i := 0; i < 10; i++ {
		rec := &Record{
			Offset:    int64(i),
			Timestamp: time.Now(),
			Key:       []byte("k"),
			Value:     []byte("v"),
		}
		seg.Append(rec)
	}
	seg.Close()

	// Reopen and recover
	seg2, err := NewSegment(dir, 0)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer seg2.Close()

	if err := seg2.Recover(); err != nil {
		t.Fatalf("recover: %v", err)
	}

	if seg2.NextOffset() != 10 {
		t.Fatalf("recovered nextOffset: got %d, want 10", seg2.NextOffset())
	}
	t.Logf("✅ Segment recovery: nextOffset=%d", seg2.NextOffset())
}

// --- Log Tests ---

func TestLogAppendAndFetch(t *testing.T) {
	dir := t.TempDir()

	l, err := NewLog(dir)
	if err != nil {
		t.Fatalf("create log: %v", err)
	}
	defer l.Close()

	// Append messages
	for i := 0; i < 5; i++ {
		offset, err := l.Append([]byte("key"), []byte("hello world"))
		if err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
		if offset != int64(i) {
			t.Fatalf("offset: got %d, want %d", offset, i)
		}
	}

	if l.HighWaterMark() != 5 {
		t.Fatalf("hwm: got %d, want 5", l.HighWaterMark())
	}

	// Fetch all
	records, hwm := l.Fetch(0, 1024*1024)
	if hwm != 5 {
		t.Fatalf("fetch hwm: got %d, want 5", hwm)
	}
	if len(records) != 5 {
		t.Fatalf("records: got %d, want 5", len(records))
	}
	t.Logf("✅ Log: appended 5, fetched %d, hwm=%d", len(records), hwm)
}

func TestLogSegmentRolling(t *testing.T) {
	dir := t.TempDir()

	// Small segment size to force rolling
	l, err := NewLogWithConfig(dir, 200) // 200 bytes per segment
	if err != nil {
		t.Fatalf("create log: %v", err)
	}
	defer l.Close()

	// Append enough to trigger segment roll
	for i := 0; i < 30; i++ {
		_, err := l.Append([]byte("key"), []byte("some-value-data"))
		if err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}

	segCount := l.SegmentCount()
	if segCount <= 1 {
		t.Fatalf("expected multiple segments, got %d", segCount)
	}
	t.Logf("✅ Segment rolling: %d segments for 30 records", segCount)

	// Fetch across segments
	records, _ := l.Fetch(0, 1024*1024)
	if len(records) != 30 {
		t.Fatalf("cross-segment fetch: got %d, want 30", len(records))
	}
	// Verify order
	for i, r := range records {
		if r.Offset != int64(i) {
			t.Fatalf("record %d offset: got %d, want %d", i, r.Offset, i)
		}
	}
	t.Logf("✅ Cross-segment fetch: %d records in correct order", len(records))
}

func TestLogCrashRecovery(t *testing.T) {
	dir := t.TempDir()

	// Write some data
	l, _ := NewLog(dir)
	for i := 0; i < 10; i++ {
		l.Append([]byte("key"), []byte("crash-test"))
	}
	l.Close()

	// Reopen — should recover all records
	l2, err := NewLog(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer l2.Close()

	if l2.HighWaterMark() != 10 {
		t.Fatalf("recovered hwm: got %d, want 10", l2.HighWaterMark())
	}

	records, _ := l2.Fetch(0, 1024*1024)
	if len(records) != 10 {
		t.Fatalf("recovered records: got %d, want 10", len(records))
	}
	if string(records[0].Value) != "crash-test" {
		t.Fatalf("data corrupted: got %q", records[0].Value)
	}
	t.Logf("✅ Crash recovery: hwm=%d, records=%d, data intact", l2.HighWaterMark(), len(records))
}

func TestLogSegmentFiles(t *testing.T) {
	dir := t.TempDir()
	l, _ := NewLogWithConfig(dir, 100)

	for i := 0; i < 20; i++ {
		l.Append([]byte("k"), []byte("v"))
	}

	files := l.ListSegmentFiles()
	for _, f := range files {
		base := filepath.Base(f)
		t.Logf("  Segment: %s", base)
		stat, _ := os.Stat(f)
		if stat == nil {
			t.Fatalf("segment file missing: %s", f)
		}
	}
	t.Logf("✅ %d segment files on disk", len(files))
	l.Close()
}

func TestLogRetentionCleanup(t *testing.T) {
	dir := t.TempDir()

	// Small segments to force multiple
	l, err := NewLogWithConfig(dir, 100)
	if err != nil {
		t.Fatalf("create log: %v", err)
	}

	// Write 20 records to create multiple segments
	for i := 0; i < 20; i++ {
		l.Append([]byte("k"), []byte("retention-test"))
	}

	segsBefore := l.SegmentCount()
	if segsBefore <= 1 {
		t.Fatalf("expected multiple segments, got %d", segsBefore)
	}

	// Manually backdate old segments' timestamps
	l.mu.Lock()
	for i := 0; i < len(l.segments)-1; i++ {
		l.segments[i].lastTimestamp = time.Now().Add(-48 * time.Hour) // 2 days ago
	}
	l.mu.Unlock()

	// Clean with 24h retention — should delete all but active segment
	deleted := l.CleanExpired(24 * 60 * 60 * 1000) // 24 hours in ms
	segsAfter := l.SegmentCount()

	if deleted == 0 {
		t.Fatal("expected some segments to be deleted")
	}
	if segsAfter >= segsBefore {
		t.Fatalf("expected fewer segments after cleanup: before=%d, after=%d", segsBefore, segsAfter)
	}
	if segsAfter < 1 {
		t.Fatal("active segment was deleted!")
	}

	t.Logf("✅ Retention cleanup: %d → %d segments, deleted %d", segsBefore, segsAfter, deleted)
	l.Close()
}
