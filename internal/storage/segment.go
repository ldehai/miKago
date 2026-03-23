package storage

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"
)

const (
	// IndexEntrySize is the size of a single index entry: relativeOffset(4) + position(4)
	IndexEntrySize = 8

	// IndexInterval is how often we write an index entry (every N records)
	IndexInterval = 10
)

// Segment represents a pair of .log and .index files for a contiguous
// range of message offsets.
type Segment struct {
	baseOffset    int64     // first offset in this segment
	nextOffset    int64     // next offset to be written
	logFile       *os.File  // append-only message data
	indexFile     *os.File  // sparse offset → position index
	logSize       int64     // current size of log file in bytes
	indexCount    int       // number of index entries written
	recordCount   int       // number of records since last index entry
	dir           string    // directory path
	lastTimestamp time.Time // timestamp of the last record written
	logWriter     *bufio.Writer // buffered writer for log file
	indexWriter   *bufio.Writer // buffered writer for index file
}

// segmentFileName generates the 20-digit zero-padded filename for a segment.
func segmentFileName(baseOffset int64, ext string) string {
	return fmt.Sprintf("%020d%s", baseOffset, ext)
}

// NewSegment creates or opens a segment in the given directory with the given base offset.
func NewSegment(dir string, baseOffset int64) (*Segment, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("create segment dir: %w", err)
	}

	logPath := filepath.Join(dir, segmentFileName(baseOffset, ".log"))
	indexPath := filepath.Join(dir, segmentFileName(baseOffset, ".index"))

	logFile, err := os.OpenFile(logPath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("open log file: %w", err)
	}

	indexFile, err := os.OpenFile(indexPath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		logFile.Close()
		return nil, fmt.Errorf("open index file: %w", err)
	}

	logStat, err := logFile.Stat()
	if err != nil {
		logFile.Close()
		indexFile.Close()
		return nil, fmt.Errorf("stat log file: %w", err)
	}

	indexStat, err := indexFile.Stat()
	if err != nil {
		logFile.Close()
		indexFile.Close()
		return nil, fmt.Errorf("stat index file: %w", err)
	}

	s := &Segment{
		baseOffset:  baseOffset,
		nextOffset:  baseOffset,
		logFile:     logFile,
		indexFile:   indexFile,
		logSize:     logStat.Size(),
		indexCount:  int(indexStat.Size() / IndexEntrySize),
		dir:         dir,
		logWriter:   bufio.NewWriterSize(logFile, 256*1024),
		indexWriter: bufio.NewWriterSize(indexFile, 4*1024),
	}

	return s, nil
}

// Recover scans the log file to rebuild the next offset and record count.
// Called on startup when loading existing segments.
func (s *Segment) Recover() error {
	if _, err := s.logFile.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("seek log start: %w", err)
	}

	var lastOffset int64 = s.baseOffset - 1
	var count int

	for {
		rec, err := DecodeRecord(s.logFile)
		if err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
			return fmt.Errorf("recover record: %w", err)
		}
		lastOffset = rec.Offset
		s.lastTimestamp = rec.Timestamp
		count++
	}

	if count > 0 {
		s.nextOffset = lastOffset + 1
	} else {
		s.nextOffset = s.baseOffset
	}
	s.recordCount = count % IndexInterval

	// Seek back to end for appending
	if _, err := s.logFile.Seek(0, io.SeekEnd); err != nil {
		return fmt.Errorf("seek log end: %w", err)
	}

	return nil
}

// Append writes a record to the segment. Returns the byte position in the log file.
func (s *Segment) Append(rec *Record) (int64, error) {
	position := s.logSize

	// Write index entry every N records
	if s.recordCount%IndexInterval == 0 {
		if err := s.writeIndexEntry(int32(rec.Offset-s.baseOffset), int32(position)); err != nil {
			return 0, err
		}
	}

	// Write record to bufio.Writer instead of file directly
	if err := EncodeRecord(s.logWriter, rec); err != nil {
		return 0, fmt.Errorf("append record: %w", err)
	}

	recordSize := int64(RecordSize(rec.Key, rec.Value))
	s.logSize += recordSize
	s.nextOffset = rec.Offset + 1
	s.recordCount++
	s.lastTimestamp = rec.Timestamp

	return position, nil
}

// writeIndexEntry writes a relative-offset → position mapping to the index file.
func (s *Segment) writeIndexEntry(relativeOffset int32, position int32) error {
	var buf [IndexEntrySize]byte
	binary.BigEndian.PutUint32(buf[0:4], uint32(relativeOffset))
	binary.BigEndian.PutUint32(buf[4:8], uint32(position))

	if _, err := s.indexWriter.Write(buf[:]); err != nil {
		return fmt.Errorf("write index entry: %w", err)
	}
	s.indexCount++
	return nil
}

// ReadAt reads a record at the given byte position in the log file.
func (s *Segment) ReadAt(position int64) (*Record, error) {
	f, err := os.Open(filepath.Join(s.dir, segmentFileName(s.baseOffset, ".log")))
	if err != nil {
		return nil, err
	}
	defer f.Close()

	if _, err := f.Seek(position, io.SeekStart); err != nil {
		return nil, err
	}

	return DecodeRecord(f)
}

// ReadFrom reads records starting from startOffset, up to maxBytes.
// Uses the index for efficient seeking.
func (s *Segment) ReadFrom(startOffset int64, maxBytes int32) ([]*Record, error) {
	if startOffset >= s.nextOffset {
		return nil, nil
	}

	// Find byte position using index
	position, err := s.findPosition(startOffset)
	if err != nil {
		return nil, err
	}

	// Open a separate file handle for reading
	f, err := os.Open(filepath.Join(s.dir, segmentFileName(s.baseOffset, ".log")))
	if err != nil {
		return nil, err
	}
	defer f.Close()

	if _, err := f.Seek(position, io.SeekStart); err != nil {
		return nil, err
	}

	var records []*Record
	var totalSize int32

	for {
		rec, err := DecodeRecord(f)
		if err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
			return nil, err
		}

		// Skip records before startOffset (we may have seeked to an earlier position)
		if rec.Offset < startOffset {
			continue
		}

		recSize := int32(RecordSize(rec.Key, rec.Value))
		if totalSize+recSize > maxBytes && len(records) > 0 {
			break
		}

		records = append(records, rec)
		totalSize += recSize
	}

	return records, nil
}

// findPosition looks up the byte position for the given offset using the sparse index.
// Returns the position of the nearest index entry at or before the target offset.
func (s *Segment) findPosition(targetOffset int64) (int64, error) {
	if s.indexCount == 0 {
		return 0, nil // scan from start
	}

	relTarget := int32(targetOffset - s.baseOffset)

	// Read all index entries
	idxPath := filepath.Join(s.dir, segmentFileName(s.baseOffset, ".index"))
	idxData, err := os.ReadFile(idxPath)
	if err != nil {
		return 0, fmt.Errorf("read index: %w", err)
	}

	// Binary search for the largest entry <= relTarget
	entryCount := len(idxData) / IndexEntrySize
	bestPosition := int64(0)

	lo, hi := 0, entryCount-1
	for lo <= hi {
		mid := (lo + hi) / 2
		off := int32(binary.BigEndian.Uint32(idxData[mid*IndexEntrySize : mid*IndexEntrySize+4]))
		pos := int32(binary.BigEndian.Uint32(idxData[mid*IndexEntrySize+4 : mid*IndexEntrySize+8]))

		if off <= relTarget {
			bestPosition = int64(pos)
			lo = mid + 1
		} else {
			hi = mid - 1
		}
	}

	return bestPosition, nil
}

// BaseOffset returns the base offset of this segment.
func (s *Segment) BaseOffset() int64 {
	return s.baseOffset
}

// NextOffset returns the next offset to be written.
func (s *Segment) NextOffset() int64 {
	return s.nextOffset
}

// Size returns the current log file size in bytes.
func (s *Segment) Size() int64 {
	return s.logSize
}

// Flush pushes memory-buffered data to the operating system's page cache.
func (s *Segment) Flush() error {
	if err := s.logWriter.Flush(); err != nil {
		return err
	}
	return s.indexWriter.Flush()
}

// Sync forces the operating system to flush all data to the physical disk.
func (s *Segment) Sync() error {
	if err := s.Flush(); err != nil {
		return err
	}
	if err := s.logFile.Sync(); err != nil {
		return err
	}
	return s.indexFile.Sync()
}

// Close closes both file handles.
func (s *Segment) Close() error {
	s.Sync() // Final sync before close
	if err := s.logFile.Close(); err != nil {
		return err
	}
	return s.indexFile.Close()
}

// LastTimestamp returns the timestamp of the last record in this segment.
func (s *Segment) LastTimestamp() time.Time {
	return s.lastTimestamp
}

// DeleteFiles closes and removes both .log and .index files.
func (s *Segment) DeleteFiles() error {
	s.Close()
	logPath := filepath.Join(s.dir, segmentFileName(s.baseOffset, ".log"))
	idxPath := filepath.Join(s.dir, segmentFileName(s.baseOffset, ".index"))
	if err := os.Remove(logPath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("remove log %s: %w", logPath, err)
	}
	if err := os.Remove(idxPath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("remove index %s: %w", idxPath, err)
	}
	return nil
}
