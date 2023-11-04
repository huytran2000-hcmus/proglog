package log

import (
	"io"
	"os"
	"testing"

	log_v1 "github.com/huytran2000-hcmus/proglog/api/v1"
)

func TestSegment(t *testing.T) {
	var n uint64 = 3
	config := Config{}
	config.Segment.MaxIndexBytes = entryWidth * n
	config.Segment.MaxStoreBytes = 1024

	dir, err := os.MkdirTemp(os.TempDir(), "segment-test")
	if err != nil {
		t.Errorf("unexpected error when create temp dir for segment: %s", err)
	}
	defer os.Remove(dir)

	want := &log_v1.Record{
		Value: []byte("hello world"),
	}

	var baseOffset uint64 = 16
	s, err := newSegment(dir, baseOffset, config)
	if err != nil {
		t.Errorf("unexpected error when create segment: %s", err)
	}

	assertEqual(t, uint64(16), s.nextOffset)
	assertEqual(t, false, s.IsMaxed())

	for i := uint64(0); i < n; i++ {
		offset, err := s.Append(want)
		assertEqual(t, nil, err)

		assertEqual(t, 16+i, offset)
		got, err := s.Read(offset)
		assertEqual(t, nil, err)
		assertEqual(t, want.Value, got.Value)
	}

	_, err = s.Append(want)
	assertError(t, io.EOF, err)

	assertEqual(t, true, s.IsMaxed())

	config.Segment.MaxIndexBytes = entryWidth * n
	config.Segment.MaxStoreBytes = uint64(len(want.Value)) * 3
	s, err = newSegment(dir, 16, config)
	assertEqual(t, nil, err)
	assertEqual(t, uint64(19), s.nextOffset)
	assertEqual(t, true, s.IsMaxed())

	err = s.Remove()
	assertEqual(t, nil, err)
	s, err = newSegment(dir, 16, config)
	assertEqual(t, nil, err)
	assertEqual(t, uint64(16), s.nextOffset)
	assertEqual(t, false, s.IsMaxed())
}
