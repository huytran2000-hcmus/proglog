package log

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"os"
	"sync"
)

var enc = binary.BigEndian

const (
	lenWidth = 8
)

type store struct {
	file *os.File
	size uint64
	mu   sync.Mutex
	buf  *bufio.Writer
}

func newStore(f *os.File) (*store, error) {
	fi, err := os.Stat(f.Name())
	if err != nil {
		return nil, fmt.Errorf("get file info of file %s: %w", f.Name(), err)
	}

	size := uint64(fi.Size())
	return &store{
		file: f,
		size: size,
		buf:  bufio.NewWriter(f),
	}, nil
}

func (s *store) Append(b []byte) (n uint64, pos uint64, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	pos = s.size
	err = binary.Write(s.buf, binary.BigEndian, uint64(len(b)))
	if err != nil {
		return 0, 0, fmt.Errorf("write the length of the message: %w", err)
	}

	count, err := s.buf.Write(b)
	if err != nil {
		return 0, 0, fmt.Errorf("write the message: %w", err)
	}

	n = uint64(count + lenWidth)
	s.size += n

	return n, pos, nil
}

func (s *store) Read(pos uint64) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	err := s.buf.Flush()
	if err != nil {
		return nil, fmt.Errorf("flush logs to file: %w", err)
	}

	size := make([]byte, lenWidth)
	_, err = s.file.ReadAt(size, int64(pos))
	if err != nil {
		return nil, fmt.Errorf("read the length of the message: %w", err)
	}

	b := make([]byte, enc.Uint64(size))
	_, err = s.file.ReadAt(b, int64(pos+lenWidth))
	if err != nil {
		return nil, fmt.Errorf("read the message: %w", err)
	}

	return b, nil
}

func (s *store) ReadAt(p []byte, offset int64) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	err := s.buf.Flush()
	if err != nil {
		return 0, fmt.Errorf("flush logs to file: %w", err)
	}

	n, err := s.file.ReadAt(p, offset)
	if err != nil {
		return n, err
	}

	return n, nil
}

func (s *store) Name() string {
	return s.file.Name()
}

func (s *store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	err := s.buf.Flush()
	if err != nil {
		return fmt.Errorf("flush logs to file: %w", err)
	}

	err = s.file.Close()
	if err != nil {
		return fmt.Errorf("close log file: %w", err)
	}

	return nil
}

func (s *store) Remove() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	err := os.Remove(s.file.Name())
	if err != nil {
		return fmt.Errorf("remove store file: %w", err)
	}

	return nil
}
