package storage

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	// DefaultMaxSegmentBytes is the default max size of a single segment (1MB for MVP).
	DefaultMaxSegmentBytes int64 = 1 * 1024 * 1024
)

// Log manages an ordered list of segments for a single partition.
// It handles appending, fetching, segment rolling, and crash recovery.
type Log struct {
	mu              sync.RWMutex
	dir             string
	segments        []*Segment
	activeSegment   *Segment
	maxSegmentBytes int64
	stopCh          chan struct{}
}

// NewLog creates or opens a log in the given directory.
func NewLog(dir string) (*Log, error) {
	return NewLogWithConfig(dir, DefaultMaxSegmentBytes)
}

// NewLogWithConfig creates a log with custom segment size.
func NewLogWithConfig(dir string, maxSegmentBytes int64) (*Log, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("create log dir %s: %w", dir, err)
	}

	l := &Log{
		dir:             dir,
		maxSegmentBytes: maxSegmentBytes,
		stopCh:          make(chan struct{}),
	}

	if err := l.recover(); err != nil {
		return nil, fmt.Errorf("recover log: %w", err)
	}

	if len(l.segments) == 0 {
		seg, err := NewSegment(dir, 0)
		if err != nil {
			return nil, fmt.Errorf("create initial segment: %w", err)
		}
		l.segments = append(l.segments, seg)
		l.activeSegment = seg
	}

	return l, nil
}

// StartRetentionCleaner starts a background goroutine that periodically
// cleans up expired segments.
func (l *Log) StartRetentionCleaner(retentionMs int64, intervalMs int64) {
	if retentionMs <= 0 {
		return
	}
	if intervalMs <= 0 {
		intervalMs = 60000
	}

	go func() {
		ticker := time.NewTicker(time.Duration(intervalMs) * time.Millisecond)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				deleted := l.CleanExpired(retentionMs)
				if deleted > 0 {
					log.Printf("[miKago] Cleaned %d expired segment(s) in %s", deleted, l.dir)
				}
			case <-l.stopCh:
				return
			}
		}
	}()
}

// CleanExpired removes segments whose last record is older than retentionMs.
// Never deletes the active (writable) segment. Returns count of deleted segments.
func (l *Log) CleanExpired(retentionMs int64) int {
	l.mu.Lock()
	defer l.mu.Unlock()

	if len(l.segments) <= 1 {
		return 0
	}

	cutoff := time.Now().Add(-time.Duration(retentionMs) * time.Millisecond)
	deleted := 0
	var remaining []*Segment

	for i, seg := range l.segments {
		isActive := i == len(l.segments)-1
		if !isActive && !seg.lastTimestamp.IsZero() && seg.lastTimestamp.Before(cutoff) {
			if err := seg.DeleteFiles(); err != nil {
				log.Printf("[miKago] Error deleting segment %d: %v", seg.baseOffset, err)
				remaining = append(remaining, seg)
			} else {
				deleted++
			}
		} else {
			remaining = append(remaining, seg)
		}
	}

	l.segments = remaining
	return deleted
}

// recover discovers existing segment files and recovers state.
func (l *Log) recover() error {
	entries, err := os.ReadDir(l.dir)
	if err != nil {
		return fmt.Errorf("read dir %s: %w", l.dir, err)
	}

	baseOffsets := make([]int64, 0)
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".log") {
			continue
		}
		name := strings.TrimSuffix(e.Name(), ".log")
		offset, err := strconv.ParseInt(name, 10, 64)
		if err != nil {
			continue
		}
		baseOffsets = append(baseOffsets, offset)
	}

	sort.Slice(baseOffsets, func(i, j int) bool {
		return baseOffsets[i] < baseOffsets[j]
	})

	for _, baseOffset := range baseOffsets {
		seg, err := NewSegment(l.dir, baseOffset)
		if err != nil {
			return fmt.Errorf("open segment %d: %w", baseOffset, err)
		}
		if err := seg.Recover(); err != nil {
			seg.Close()
			return fmt.Errorf("recover segment %d: %w", baseOffset, err)
		}
		l.segments = append(l.segments, seg)
	}

	if len(l.segments) > 0 {
		l.activeSegment = l.segments[len(l.segments)-1]
	}

	return nil
}

// Append adds a message to the log. Returns the assigned offset.
func (l *Log) Append(key, value []byte) (int64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.activeSegment.Size() >= l.maxSegmentBytes {
		if err := l.rollSegment(); err != nil {
			return 0, fmt.Errorf("roll segment: %w", err)
		}
	}

	offset := l.activeSegment.NextOffset()
	rec := &Record{
		Offset:    offset,
		Timestamp: time.Now(),
		Key:       key,
		Value:     value,
	}

	if _, err := l.activeSegment.Append(rec); err != nil {
		return 0, err
	}

	return offset, nil
}

func (l *Log) rollSegment() error {
	if err := l.activeSegment.Flush(); err != nil {
		return err
	}

	newBaseOffset := l.activeSegment.NextOffset()
	seg, err := NewSegment(l.dir, newBaseOffset)
	if err != nil {
		return err
	}

	l.segments = append(l.segments, seg)
	l.activeSegment = seg
	return nil
}

// Fetch retrieves records starting from startOffset, up to maxBytes.
func (l *Log) Fetch(startOffset int64, maxBytes int32) ([]*Record, int64) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	hwm := l.activeSegment.NextOffset()
	if startOffset >= hwm || len(l.segments) == 0 {
		return nil, hwm
	}

	segIdx := l.findSegment(startOffset)
	if segIdx < 0 {
		return nil, hwm
	}

	var allRecords []*Record
	var totalSize int32

	for i := segIdx; i < len(l.segments); i++ {
		remaining := maxBytes - totalSize
		if remaining <= 0 {
			break
		}

		records, err := l.segments[i].ReadFrom(startOffset, remaining)
		if err != nil {
			break
		}

		for _, rec := range records {
			recSize := int32(RecordSize(rec.Key, rec.Value))
			if totalSize+recSize > maxBytes && len(allRecords) > 0 {
				return allRecords, hwm
			}
			allRecords = append(allRecords, rec)
			totalSize += recSize
		}

		if i+1 < len(l.segments) {
			startOffset = l.segments[i+1].BaseOffset()
		}
	}

	return allRecords, hwm
}

func (l *Log) findSegment(offset int64) int {
	n := len(l.segments)
	if n == 0 {
		return -1
	}

	idx := sort.Search(n, func(i int) bool {
		return l.segments[i].BaseOffset() > offset
	})

	if idx == 0 {
		return 0
	}
	return idx - 1
}

// HighWaterMark returns the next offset to be written.
func (l *Log) HighWaterMark() int64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.activeSegment.NextOffset()
}

// Flush syncs all segments to disk.
func (l *Log) Flush() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.activeSegment.Flush()
}

// Close closes all segments and stops the retention cleaner.
func (l *Log) Close() error {
	select {
	case <-l.stopCh:
	default:
		close(l.stopCh)
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	for _, seg := range l.segments {
		if err := seg.Close(); err != nil {
			return err
		}
	}
	return nil
}

// SegmentCount returns the number of segments (for testing).
func (l *Log) SegmentCount() int {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return len(l.segments)
}

// DataDir returns the log directory (for testing).
func (l *Log) DataDir() string {
	return l.dir
}

// ListSegmentFiles returns log file paths (for debugging).
func (l *Log) ListSegmentFiles() []string {
	l.mu.RLock()
	defer l.mu.RUnlock()

	var files []string
	for _, seg := range l.segments {
		files = append(files, filepath.Join(seg.dir, segmentFileName(seg.baseOffset, ".log")))
	}
	return files
}
