package log

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path"

	"google.golang.org/protobuf/proto"

	log_v1 "github.com/huytran2000-hcmus/proglog/api/v1"
)

type segment struct {
	store                  *store
	index                  *index
	baseOffset, nextOffset uint64
	config                 Config
}

func newSegment(dir string, baseOffset uint64, c Config) (*segment, error) {
	s := &segment{
		baseOffset: baseOffset,
		config:     c,
	}

	storeFile, err := os.OpenFile(
		path.Join(dir, fmt.Sprintf("%d%s", baseOffset, ".store")),
		os.O_RDWR|os.O_CREATE|os.O_APPEND,
		0644)
	if err != nil {
		return nil, fmt.Errorf("create store file: %w", err)
	}

	s.store, err = newStore(storeFile)
	if err != nil {
		return nil, fmt.Errorf("create store: %w", err)
	}

	indexFile, err := os.OpenFile(
		path.Join(dir, fmt.Sprintf("%d%s", baseOffset, ".index")), os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, fmt.Errorf("create index file: %w", err)
	}

	s.index, err = newIndex(indexFile, c)
	if err != nil {
		return nil, fmt.Errorf("create store: %w", err)
	}

	off, _, err := s.index.Read(-1)
	if err != nil {
		switch {
		case errors.Is(err, io.EOF):
			s.nextOffset = baseOffset
		default:
			return nil, fmt.Errorf("read last entry of index: %w", err)
		}
	} else {
		s.nextOffset = baseOffset + uint64(off) + 1
	}

	return s, nil
}

func (s *segment) Append(record *log_v1.Record) (offset uint64, err error) {
	cur := s.nextOffset
	record.Offset = cur
	p, err := proto.Marshal(record)
	if err != nil {
		return 0, fmt.Errorf("marshal record: %w", err)
	}

	_, pos, err := s.store.Append(p)
	if err != nil {
		return 0, fmt.Errorf("append to store: %w", err)
	}

	err = s.index.Write(
		uint32(s.nextOffset-s.baseOffset),
		pos,
	)
	if err != nil {
		return 0, fmt.Errorf("write index: %w", err)
	}
	s.nextOffset++

	return cur, nil
}

func (s *segment) Read(offset uint64) (*log_v1.Record, error) {
	_, pos, err := s.index.Read(int64(offset - s.baseOffset))
	if err != nil {
		return nil, fmt.Errorf("read index: %w", err)
	}

	p, err := s.store.Read(pos)
	if err != nil {
		return nil, fmt.Errorf("read from store: %w", err)
	}

	var record log_v1.Record
	err = proto.Unmarshal(p, &record)
	if err != nil {
		return nil, fmt.Errorf("unmarshal protobuf")
	}

	return &record, nil
}

func (s *segment) InRange(offset uint64) bool {
	return s.baseOffset <= offset && offset < s.nextOffset
}

func (s *segment) LessThan(offset uint64) bool {
	return s.nextOffset <= offset+1
}

func (s *segment) IsMaxed() bool {
	return s.store.size >= s.config.Segment.MaxStoreBytes ||
		s.index.size >= s.config.Segment.MaxIndexBytes
}

func (s *segment) Remove() error {
	err := s.Close()
	if err != nil {
		return fmt.Errorf("close segment: %w", err)
	}

	err = s.store.Remove()
	if err != nil {
		return fmt.Errorf("remove store: %w", err)
	}

	err = s.index.Remove()
	if err != nil {
		return fmt.Errorf("remove index: %w", err)
	}

	return nil
}

func (s *segment) Close() error {
	err := s.store.Close()
	if err != nil {
		return fmt.Errorf("close store: %w", err)
	}

	err = s.index.Close()
	if err != nil {
		return fmt.Errorf("close index: %w", err)
	}

	return nil
}

func nearestMultiple(j, k uint64) uint64 {
	if j < 0 {
		return ((j - k + 1) / k) * k
	}

	return (j / k) * k
}
