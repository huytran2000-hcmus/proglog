package log_test

import (
	"errors"
	"fmt"
	"net"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"
	"github.com/travisjeffery/go-dynaport"

	api "github.com/huytran2000-hcmus/proglog/api/v1"
	"github.com/huytran2000-hcmus/proglog/internal/log"
	"github.com/huytran2000-hcmus/proglog/pkg/testhelper"
)

func TestMultipleNode(t *testing.T) {
	var logs []*log.Distributed
	n := 3
	ports := dynaport.Get(n)

	for i := 0; i < n; i++ {
		dataDir, err := os.MkdirTemp(os.TempDir(), fmt.Sprintf("distributed-log-test-%d", i))
		testhelper.AssertNoError(t, err)

		defer func(dir string) {
			_ = os.RemoveAll(dir)
		}(dataDir)

		ln, err := net.Listen(
			"tcp",
			fmt.Sprintf("127.0.0.1:%d", ports[i]),
		)

		testhelper.AssertNoError(t, err)

		var config log.Config
		config.Raft.Stream = log.NewStreamLayer(ln, nil, nil)
		config.Raft.LocalID = raft.ServerID(fmt.Sprintf("%d", i))
		config.Raft.HeartbeatTimeout = 50 * time.Millisecond
		config.Raft.ElectionTimeout = 50 * time.Millisecond
		config.Raft.LeaderLeaseTimeout = 50 * time.Millisecond
		config.Raft.CommitTimeout = 5 * time.Millisecond

		if i == 0 {
			config.Raft.Bootstrap = true
		}

		l, err := log.NewDistributed(dataDir, config)
		testhelper.AssertNoError(t, err)
		defer l.Close()

		if i != 0 {
			err := logs[0].Join(
				fmt.Sprintf("%d", i),
				ln.Addr().String(),
			)
			testhelper.AssertNoError(t, err)
		} else {
			err = l.WaitForLeader(10 * time.Second)
			testhelper.RequireNoError(t, err)
		}

		logs = append(logs, l)
	}

	records := []*api.Record{
		{Value: []byte("first")},
		{Value: []byte("second")},
	}

	for _, record := range records {
		off, err := logs[0].Append(record)
		testhelper.RequireNoError(t, err)
		require.Eventually(t, func() bool {
			for j := 0; j < n; j++ {
				got, err := logs[j].Read(off)
				if err != nil {
					return false
				}

				record.Offset = off
				if !reflect.DeepEqual(got.Value, record.Value) {
					return false
				}
			}

			return true
		}, 5*time.Second, 50*time.Millisecond)
	}

	servers, err := logs[0].GetServers()
	testhelper.RequireNoError(t, err)
	testhelper.AssertEqual(t, 3, len(servers))
	testhelper.AssertEqual(t, true, servers[0].IsLeader)
	testhelper.AssertEqual(t, false, servers[1].IsLeader)
	testhelper.AssertEqual(t, false, servers[2].IsLeader)

	err = logs[0].Leave("1")
	testhelper.AssertNoError(t, err)

	time.Sleep(50 * time.Millisecond)

	servers, err = logs[0].GetServers()
	testhelper.RequireNoError(t, err)
	testhelper.AssertEqual(t, 2, len(servers))
	testhelper.AssertEqual(t, true, servers[0].IsLeader)
	testhelper.AssertEqual(t, false, servers[1].IsLeader)

	off, err := logs[0].Append(&api.Record{
		Value: []byte("third"),
	})
	testhelper.AssertNoError(t, err)

	time.Sleep(50 * time.Millisecond)

	_, err = logs[1].Read(off)
	if !errors.As(err, &api.OffsetOutOfRangeError{}) {
		t.Errorf("expect OffsetOutOfRangeError, got %v", err)
	}

	record, err := logs[2].Read(off)
	testhelper.RequireNoError(t, err)
	testhelper.AssertEqual(t, off, record.Offset)
	testhelper.AssertEqual(t, []byte("third"), record.Value)
}
