package log

import (
	"errors"
	"io"
	"os"
	"testing"

	"github.com/huytran2000-hcmus/proglog/pkg/testhelper"
)

func TestIndex(t *testing.T) {
	f, err := os.CreateTemp(os.TempDir(), "index_test")
	if err != nil {
		t.Errorf("create temp file for index: %s", err)
	}
	defer os.Remove(f.Name())

	c := Config{}
	c.Segment.MaxIndexBytes = 1024
	idx, err := newIndex(f, c)
	if err != nil {
		t.Errorf("create new index: %s", err)
	}
	_, _, err = idx.Read(-1)
	if err == nil {
		t.Errorf("want an error after read empty index file, got none")
	}

	if f.Name() != idx.Name() {
		t.Error("file name should match index name")
	}

	entries := []struct {
		Off uint32
		Pos uint64
	}{
		{
			Off: 0, Pos: 0,
		},
		{
			Off: 1, Pos: 10,
		},
	}

	for _, want := range entries {
		err = idx.Write(want.Off, want.Pos)
		if err != nil {
			t.Errorf("write entry to index: %s", err)
		}

		_, pos, err := idx.Read(int64(want.Off))
		if err != nil {
			t.Errorf("read entry from index: %s", err)
		}

		testhelper.AssertEqual(t, want.Pos, pos)
	}

	_, _, err = idx.Read(int64(len(entries)))
	if err == nil || !errors.Is(err, io.EOF) {
		t.Errorf("want an error after read past existing entries, got %s", err)
	}
	_ = idx.Close()

	f, _ = os.OpenFile(f.Name(), os.O_RDWR, 0600)
	idx, err = newIndex(f, c)
	if err != nil {
		t.Errorf("recreate old index: %s", err)
	}

	off, pos, err := idx.Read(-1)
	if err != nil {
		t.Errorf("read last index entry: %s", err)
	}
	testhelper.AssertEqual(t, entries[len(entries)-1].Off, off)
	testhelper.AssertEqual(t, entries[len(entries)-1].Pos, pos)
}
