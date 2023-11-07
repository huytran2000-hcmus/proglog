package log

import (
	"io"
	"os"
	"testing"

	log_v1 "github.com/huytran2000-hcmus/proglog/api/v1"
	"github.com/huytran2000-hcmus/proglog/pkg/testhelper"
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

	testhelper.AssertEqual(t, uint64(16), s.nextOffset)
	testhelper.AssertEqual(t, false, s.IsMaxed())

	for i := uint64(0); i < n; i++ {
		offset, err := s.Append(want)
		testhelper.AssertEqual(t, nil, err)

		testhelper.AssertEqual(t, 16+i, offset)
		got, err := s.Read(offset)
		testhelper.AssertEqual(t, nil, err)
		testhelper.AssertEqual(t, want.Value, got.Value)
	}

	_, err = s.Append(want)
	testhelper.AssertError(t, io.EOF, err)

	testhelper.AssertEqual(t, true, s.IsMaxed())

	config.Segment.MaxIndexBytes = entryWidth * n
	config.Segment.MaxStoreBytes = uint64(len(want.Value)) * 3
	s, err = newSegment(dir, 16, config)
	testhelper.AssertEqual(t, nil, err)
	testhelper.AssertEqual(t, uint64(19), s.nextOffset)
	testhelper.AssertEqual(t, true, s.IsMaxed())

	err = s.Remove()
	testhelper.AssertEqual(t, nil, err)
	s, err = newSegment(dir, 16, config)
	testhelper.AssertEqual(t, nil, err)
	testhelper.AssertEqual(t, uint64(16), s.nextOffset)
	testhelper.AssertEqual(t, false, s.IsMaxed())
}
