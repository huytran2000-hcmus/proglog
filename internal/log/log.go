package log

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"

	log_v1 "github.com/huytran2000-hcmus/proglog/api/v1"
)

var ErrNotFound = errors.New("offset out of range")

type Log struct {
	Config        Config
	mu            sync.RWMutex
	Dir           string
	activeSegment *segment
	segments      []*segment
}

type originReader struct {
	store  *store
	offset int64
}

func NewLog(dir string, c Config) (*Log, error) {
	if c.Segment.MaxStoreBytes == 0 {
		c.Segment.MaxStoreBytes = 1024
	}

	if c.Segment.MaxIndexBytes == 0 {
		c.Segment.MaxIndexBytes = 1024
	}

	l := &Log{
		Dir:    dir,
		Config: c,
	}

	return l, l.setup()
}

func (l *Log) Append(record *log_v1.Record) (uint64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	off, err := l.activeSegment.Append(record)
	if err != nil {
		return 0, fmt.Errorf("append to segment: %w", err)
	}

	if l.activeSegment.IsMaxed() {
		err = l.newSegment(off + 1)
	}

	return off, nil
}

func (l *Log) Read(offset uint64) (*log_v1.Record, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	var s *segment
	for _, seg := range l.segments {
		if seg.InRange(offset) {
			s = seg
		}
	}

	if s == nil {
		return nil, ErrNotFound
	}

	record, err := s.Read(offset)
	if err != nil {
		return nil, fmt.Errorf("read record from offset: %w", err)
	}

	return record, nil
}

func (l *Log) Reader() io.Reader {
	l.mu.RLock()
	defer l.mu.RUnlock()

	readers := make([]io.Reader, len(l.segments))
	for i, s := range l.segments {
		r := &originReader{
			store:  s.store,
			offset: 0,
		}

		readers[i] = r
	}

	return io.MultiReader(readers...)
}

func (o *originReader) Read(p []byte) (int, error) {
	n, err := o.store.ReadAt(p, o.offset)
	o.offset += int64(n)
	return n, err
}

func (l *Log) Truncate(lowest uint64) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	var segments []*segment
	for _, s := range l.segments {
		if s.LessThan(lowest) {
			err := s.Remove()
			if err != nil {
				return err
			}
			continue
		}

		segments = append(segments, s)
	}

	l.segments = segments
	return nil
}

func (l *Log) setup() error {
	entries, err := os.ReadDir(l.Dir)
	if err != nil {
		return fmt.Errorf("read log dir: %w", err)
	}

	var baseOffsets []uint64
	for _, ent := range entries {
		fname := ent.Name()
		ext := filepath.Ext(fname)
		offsetStr := strings.TrimSuffix(fname, ext)
		offset, err := strconv.ParseUint(offsetStr, 10, 64)
		if err != nil {
			return fmt.Errorf("parse offset: %w", err)
		}

		baseOffsets = append(baseOffsets, offset)
	}

	sort.Slice(baseOffsets, func(i, j int) bool {
		return baseOffsets[i] < baseOffsets[j]
	})

	for i := 0; i < len(baseOffsets); i++ {
		err = l.newSegment(baseOffsets[i])
		if err != nil {
			return fmt.Errorf("create segment from existed offset=%d: %w", i, err)
		}

		// skip because duplication of index and store
		i++
	}

	if len(l.segments) == 0 {
		err = l.newSegment(l.Config.Segment.InitialOffset)
		if err != nil {
			return fmt.Errorf("create old segment: %w", err)
		}
	}

	return nil
}

func (l *Log) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	for _, s := range l.segments {
		err := s.Close()
		if err != nil {
			return fmt.Errorf("close segment: %w", err)
		}
	}

	return nil
}

func (l *Log) Remove() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	err := l.Close()
	if err != nil {
		return err
	}

	return os.Remove(l.Dir)
}

func (l *Log) Reset() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	err := l.Remove()
	if err != nil {
		return err
	}

	return l.setup()
}

func (l *Log) LowestOffset() (uint64, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	return l.segments[0].baseOffset, nil
}

func (l *Log) HighestOffset() (uint64, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	offset := l.segments[len(l.segments)-1].nextOffset
	if offset == 0 {
		return 0, nil
	}

	return offset - 1, nil
}

func (l *Log) newSegment(offset uint64) error {
	s, err := newSegment(l.Dir, offset, l.Config)
	if err != nil {
		return fmt.Errorf("create segment for log: %w", err)
	}

	l.segments = append(l.segments, s)
	l.activeSegment = s

	return nil
}
