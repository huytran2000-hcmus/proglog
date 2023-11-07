package log

import (
	"io"
	"os"
	"testing"

	"google.golang.org/protobuf/proto"

	api "github.com/huytran2000-hcmus/proglog/api/v1"
	"github.com/huytran2000-hcmus/proglog/pkg/testhelper"
)

func TestLog(t *testing.T) {
	createLog := func() *Log {
		dir, err := os.MkdirTemp(os.TempDir(), "log-test")
		testhelper.AssertNoError(t, err)

		var c Config
		c.Segment.MaxStoreBytes = 32

		log, err := NewLog(dir, c)
		testhelper.AssertNoError(t, err)

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
	want := &api.Record{
		Value: []byte("hello world"),
	}

	offset, err := log.Append(want)
	testhelper.AssertNoError(t, err)

	got, err := log.Read(offset)
	testhelper.AssertNoError(t, err)
	testhelper.AssertEqual(t, want.Value, got.Value)
}

func testLogOutOfRange(t *testing.T, log *Log) {
	_, err := log.Read(1000)
	outOfRangeErr := err.(api.OffsetOutOfRangeError)
	testhelper.AssertEqual(t, uint64(1000), outOfRangeErr.Offset)
}

func testRecreateLog(t *testing.T, log *Log) {
	n := 2
	want := &api.Record{
		Value: []byte("six figure job"),
	}

	for i := 0; i <= n; i++ {
		_, err := log.Append(want)
		testhelper.AssertNoError(t, err)
	}

	offset, err := log.LowestOffset()
	testhelper.AssertNoError(t, err)
	testhelper.AssertEqual(t, uint64(0), offset)

	offset, err = log.HighestOffset()
	testhelper.AssertNoError(t, err)
	testhelper.AssertEqual(t, uint64(n), offset)

	err = log.Close()
	testhelper.AssertNoError(t, err)

	log, err = NewLog(log.Dir, log.Config)
	testhelper.AssertNoError(t, err)

	offset, err = log.LowestOffset()
	testhelper.AssertNoError(t, err)
	testhelper.AssertEqual(t, uint64(0), offset)

	offset, err = log.HighestOffset()
	testhelper.AssertNoError(t, err)
	testhelper.AssertEqual(t, uint64(n), offset)
}

func testLogReader(t *testing.T, log *Log) {
	want := &api.Record{
		Value: []byte("six figure job"),
	}

	offset, err := log.Append(want)
	testhelper.AssertNoError(t, err)
	testhelper.AssertEqual(t, uint64(0), offset)

	got, err := log.Read(offset)
	testhelper.AssertNoError(t, err)
	testhelper.AssertEqual(t, want.Value, got.Value)

	reader := log.Reader()
	b, err := io.ReadAll(reader)
	testhelper.AssertNoError(t, err)

	got = &api.Record{}
	err = proto.Unmarshal(b[lenWidth:], got)
	testhelper.AssertNoError(t, err)
	testhelper.AssertEqual(t, want.Value, got.Value)
}

func testTruncateLog(t *testing.T, log *Log) {
	n := 2
	want := &api.Record{
		Value: []byte("six figure job"),
	}

	for i := 0; i <= n; i++ {
		_, err := log.Append(want)
		testhelper.AssertNoError(t, err)
	}

	err := log.Truncate(1)
	testhelper.AssertNoError(t, err)

	_, err = log.Read(0)
	outOfRangeErr := err.(api.OffsetOutOfRangeError)
	testhelper.AssertEqual(t, uint64(0), outOfRangeErr.Offset)
}
