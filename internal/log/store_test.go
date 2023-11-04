package log

import (
	"os"
	"testing"
)

var (
	message = []byte("hello world")
	width   = uint64(len(message)) + lenWidth
)

func TestStore_Append_Read(t *testing.T) {
	f, err := os.CreateTemp(os.TempDir(), "store_close_test")
	if err != nil {
		t.Errorf("unexpected error after create log file: %s", err)
	}
	defer os.Remove(f.Name())

	s, err := newStore(f)
	if err != nil {
		t.Errorf("unexpected error after create store: %s", err)
	}

	nLog := 4
	testStore_Append(t, s, nLog)
	testStore_Read(t, s, nLog)
	testStore_ReadAt(t, s, nLog)
}

func TestStoreClose(t *testing.T) {
	f, err := os.CreateTemp(os.TempDir(), "store_close_test")
	if err != nil {
		t.Errorf("unexpected error after create log file: %s", err)
	}
	defer os.Remove(f.Name())

	s, err := newStore(f)
	if err != nil {
		t.Errorf("unexpected error after create store: %s", err)
	}

	_, _, err = s.Append(message)
	if err != nil {
		t.Errorf("unexpected error after append log: %s", err)
	}

	beforeSize := readSize(t, f.Name())
	if err != nil {
		t.Errorf("unexpected error after open log: %s", err)
	}
	afterSize := readSize(t, f.Name())
	if err != nil {
		t.Errorf("unexpected error after open log: %s", err)
	}

	err = s.Close()
	if err != nil {
		t.Errorf("unexpected error after close store: %s", err)
	}

	if beforeSize < afterSize {
		t.Errorf("before log file size is greater or equal to after log file size")
	}
}

func testStore_Append(t *testing.T, s *store, nLog int) {
	for i := uint64(1); i <= uint64(nLog); i++ {
		n, pos, err := s.Append(message)
		if err != nil {
			t.Errorf("unexpected error after append log: %s", err)
		}
		assertEqual(t, pos+n, width*i)
	}
}

func testStore_Read(t *testing.T, s *store, nLog int) {
	var pos uint64
	for i := 0; i < nLog; i++ {
		got, err := s.Read(pos)
		if err != nil {
			t.Errorf("unexpected error after read log: %s", err)
		}
		assertEqual(t, message, got)
		pos += width
	}
}

func testStore_ReadAt(t *testing.T, s *store, nLog int) {
	var offset int64
	for i := 0; i < nLog; i++ {
		b := make([]byte, lenWidth)
		n, err := s.ReadAt(b, offset)
		if err != nil {
			t.Errorf("unexpected error after read at offset %d: %s", offset, err)
		}
		assertEqual(t, lenWidth, n)
		offset += int64(n)

		size := enc.Uint64(b)
		assertEqual(t, len(message), int(size))

		b = make([]byte, size)
		n, err = s.ReadAt(b, offset)
		if err != nil {
			t.Errorf("unexpected error after read at offset %d: %s", offset, err)
		}

		assertEqual(t, int(size), n)
		assertEqual(t, message, b)
		offset += int64(n)
	}
}

func readSize(t *testing.T, name string) int64 {
	f, err := os.Open(name)
	if err != nil {
		t.Errorf("unexpected error after open log file: %s", err)
	}

	fi, err := f.Stat()
	if err != nil {
		t.Errorf("unexpected error after get log file info: %s", err)
	}

	return fi.Size()
}
