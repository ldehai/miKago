package storage

import (
	"fmt"
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
}

// NewLog creates or opens a log in the given directory.
// On startup, it discovers existing segments and recovers state.
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
	}

	if err := l.recover(); err != nil {
		return nil, fmt.Errorf("recover log: %w", err)
	}

	// If no segments exist, create the first one
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

// recover discovers existing segment files and recovers state.
func (l *Log) recover() error {
	entries, err := os.ReadDir(l.dir)
	if err != nil {
		return fmt.Errorf("read dir %s: %w", l.dir, err)
	}

	// Find all base offsets from .log files
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

	// Open and recover each segment
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

	// Roll segment if needed
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

// rollSegment creates a new active segment.
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
// Searches across segments to find the right starting point.
func (l *Log) Fetch(startOffset int64, maxBytes int32) ([]*Record, int64) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	hwm := l.activeSegment.NextOffset()

	if startOffset >= hwm || len(l.segments) == 0 {
		return nil, hwm
	}

	// Find the segment containing startOffset using binary search
	segIdx := l.findSegment(startOffset)
	if segIdx < 0 {
		return nil, hwm
	}

	var allRecords []*Record
	var totalSize int32

	// Read from the found segment and subsequent segments
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

		// For next segment, start from its base offset
		if i+1 < len(l.segments) {
			startOffset = l.segments[i+1].BaseOffset()
		}
	}

	return allRecords, hwm
}

// findSegment returns the index of the segment that contains (or could contain) the given offset.
func (l *Log) findSegment(offset int64) int {
	n := len(l.segments)
	if n == 0 {
		return -1
	}

	// Binary search: find the last segment with baseOffset <= offset
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

// Close closes all segments.
func (l *Log) Close() error {
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
