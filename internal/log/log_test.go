package log

import (
	"io"
	"os"
	"testing"

	"google.golang.org/protobuf/proto"

	log_v1 "github.com/huytran2000-hcmus/proglog/api/v1"
)

func TestLog(t *testing.T) {
	createLog := func() *Log {
		dir, err := os.MkdirTemp(os.TempDir(), "log-test")
		assertNoError(t, err)

		var c Config
		c.Segment.MaxStoreBytes = 32

		log, err := NewLog(dir, c)
		assertNoError(t, err)

		return log
	}

	t.Run("append then read back", func(t *testing.T) {
		log := createLog()
		testAppendReadLog(t, log)
		defer os.RemoveAll(log.Dir)
	})

	t.Run("out of range", func(t *testing.T) {
		log := createLog()
		testLogOutOfRange(t, log)
		defer os.RemoveAll(log.Dir)
	})

	t.Run("recreate log from file", func(t *testing.T) {
		log := createLog()
		testRecreateLog(t, log)
		defer os.RemoveAll(log.Dir)
	})

	t.Run("reader", func(t *testing.T) {
		log := createLog()
		testLogReader(t, log)
		defer os.RemoveAll(log.Dir)
	})
}

func testAppendReadLog(t *testing.T, log *Log) {
	want := &log_v1.Record{
		Value: []byte("hello world"),
	}

	offset, err := log.Append(want)
	assertNoError(t, err)

	got, err := log.Read(offset)
	assertNoError(t, err)
	assertEqual(t, want.Value, got.Value)
}

func testLogOutOfRange(t *testing.T, log *Log) {
	_, err := log.Read(1000)
	assertError(t, ErrNotFound, err)
}

func testRecreateLog(t *testing.T, log *Log) {
	n := 2
	want := &log_v1.Record{
		Value: []byte("six figure job"),
	}

	for i := 0; i <= n; i++ {
		_, err := log.Append(want)
		assertNoError(t, err)
	}

	offset, err := log.LowestOffset()
	assertNoError(t, err)
	assertEqual(t, uint64(0), offset)

	offset, err = log.HighestOffset()
	assertNoError(t, err)
	assertEqual(t, uint64(n), offset)

	err = log.Close()
	assertNoError(t, err)

	log, err = NewLog(log.Dir, log.Config)
	assertNoError(t, err)

	offset, err = log.LowestOffset()
	assertNoError(t, err)
	assertEqual(t, uint64(0), offset)

	offset, err = log.HighestOffset()
	assertNoError(t, err)
	assertEqual(t, uint64(n), offset)
}

func testLogReader(t *testing.T, log *Log) {
	want := &log_v1.Record{
		Value: []byte("six figure job"),
	}

	offset, err := log.Append(want)
	assertNoError(t, err)
	assertEqual(t, uint64(0), offset)

	got, err := log.Read(offset)
	assertNoError(t, err)
	assertEqual(t, want.Value, got.Value)

	reader := log.Reader()
	b, err := io.ReadAll(reader)
	assertNoError(t, err)

	got = &log_v1.Record{}
	err = proto.Unmarshal(b[lenWidth:], got)
	assertNoError(t, err)
	assertEqual(t, want.Value, got.Value)
}

func testTruncateLog(t *testing.T, log *Log) {
	n := 2
	want := &log_v1.Record{
		Value: []byte("six figure job"),
	}

	for i := 0; i <= n; i++ {
		_, err := log.Append(want)
		assertNoError(t, err)
	}

	err := log.Truncate(1)
	assertNoError(t, err)

	_, err = log.Read(0)
	assertError(t, ErrNotFound, err)
}
