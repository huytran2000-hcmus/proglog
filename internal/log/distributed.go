package log

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
	"google.golang.org/protobuf/proto"

	api "github.com/huytran2000-hcmus/proglog/api/v1"
)

type Distributed struct {
	cfg  Config
	log  *Log
	raft *raft.Raft
}

func NewDistributed(dataDir string, config Config) (*Distributed, error) {
	l := &Distributed{
		cfg: config,
	}

	err := l.setupLog(dataDir)
	if err != nil {
		return nil, fmt.Errorf("set up local log: %w", err)
	}

	err = l.setupRaft(dataDir)
	if err != nil {
		return nil, fmt.Errorf("set up raft: %w", err)
	}

	return l, nil
}

func (l *Distributed) Append(record *api.Record) (uint64, error) {
	res, err := l.apply(
		AppendRequestType,
		&api.ProduceRequest{Record: record},
	)
	if err != nil {
		return 0, err
	}

	return res.(*api.ProduceResponse).Offset, nil
}

func (l *Distributed) Read(offset uint64) (*api.Record, error) {
	return l.log.Read(offset)
}

func (l *Distributed) Join(id, addr string) error {
	configFuture := l.raft.GetConfiguration()
	err := configFuture.Error()
	if err != nil {
		return fmt.Errorf("get configuration: %w", err)
	}

	serverID := raft.ServerID(id)
	serverAddr := raft.ServerAddress(addr)
	for _, srv := range configFuture.Configuration().Servers {
		if srv.ID == serverID || srv.Address == serverAddr {
			if srv.ID == serverID && srv.Address == serverAddr {
				return nil
			}

			removeFuture := l.raft.RemoveServer(serverID, 0, 0)
			err = removeFuture.Error()
			if err != nil {
				return fmt.Errorf("remove server: %w", err)
			}
		}
	}

	addFuture := l.raft.AddVoter(serverID, serverAddr, 0, 0)
	err = addFuture.Error()
	if err != nil {
		return err
	}

	return nil
}

func (l *Distributed) Leave(id string) error {
	serverID := raft.ServerID(id)
	removeFuture := l.raft.RemoveServer(serverID, 0, 0)
	return removeFuture.Error()
}

func (l *Distributed) setupLog(dataDir string) error {
	logDir := filepath.Join(dataDir, "log")
	err := os.MkdirAll(logDir, 0755)
	if err != nil {
		return fmt.Errorf("create log data dir: %w", err)
	}

	l.log, err = New(logDir, l.cfg)

	return err
}

func (l *Distributed) GetServers() ([]*api.Server, error) {
	future := l.raft.GetConfiguration()
	err := future.Error()
	if err != nil {
		return nil, err
	}

	var servers []*api.Server
	leader, _ := l.raft.LeaderWithID()
	for _, srv := range future.Configuration().Servers {
		servers = append(servers, &api.Server{
			Id:       string(srv.ID),
			RpcAddr:  string(srv.Address),
			IsLeader: leader == srv.Address,
		})
	}

	return servers, nil
}

func (l *Distributed) WaitForLeader(timeout time.Duration) error {
	timeoutC := time.After(timeout)
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-timeoutC:
			return fmt.Errorf("timed out")
		case <-ticker.C:
			leader, _ := l.raft.LeaderWithID()
			if leader != "" {
				return nil
			}
		}
	}
}

func (l *Distributed) Close() error {
	failed := l.raft.Shutdown()

	err := failed.Error()
	if err != nil {
		return err
	}

	return l.log.Close()
}

func (l *Distributed) setupRaft(dataDir string) error {
	fsm := &fsm{log: l.log}

	logDir := filepath.Join(dataDir, "raft", "log")
	err := os.MkdirAll(logDir, 0755)
	if err != nil {
		return fmt.Errorf("create raft log dir: %w", err)
	}

	logConfig := l.cfg
	logConfig.Segment.InitialOffset = 1
	logStore, err := newLogStore(logDir, logConfig)
	if err != nil {
		return fmt.Errorf("create raft's log store: %w", err)
	}

	stableStore, err := raftboltdb.NewBoltStore(
		filepath.Join(dataDir, "raft", "stable"),
	)
	if err != nil {
		return fmt.Errorf("create raft's stable store: %w", err)
	}

	baseSnapDir := filepath.Join(dataDir, "raft")
	retain := 1
	snapshotStore, err := raft.NewFileSnapshotStore(
		baseSnapDir,
		retain,
		os.Stderr,
	)
	if err != nil {
		return fmt.Errorf("create raft's snapshot store: %w", err)
	}

	maxPool := 5
	timeout := 10 * time.Second
	transport := raft.NewNetworkTransport(l.cfg.Raft.Stream, maxPool, timeout, os.Stderr)

	config := raft.DefaultConfig()
	config.LocalID = l.cfg.Raft.LocalID

	if l.cfg.Raft.HeartbeatTimeout != 0 {
		config.HeartbeatTimeout = l.cfg.Raft.HeartbeatTimeout
	}

	if l.cfg.Raft.ElectionTimeout != 0 {
		config.ElectionTimeout = l.cfg.Raft.ElectionTimeout
	}

	if l.cfg.Raft.LeaderLeaseTimeout != 0 {
		config.LeaderLeaseTimeout = l.cfg.Raft.LeaderLeaseTimeout
	}

	if l.cfg.Raft.CommitTimeout != 0 {
		config.CommitTimeout = l.cfg.Raft.CommitTimeout
	}

	l.raft, err = raft.NewRaft(
		config,
		fsm,
		logStore,
		stableStore,
		snapshotStore,
		transport,
	)
	if err != nil {
		return fmt.Errorf("create raft: %w", err)
	}

	hasState, err := raft.HasExistingState(logStore, stableStore, snapshotStore)
	if err != nil {
		return fmt.Errorf("check if has existing state: %w", err)
	}

	if l.cfg.Raft.Bootstrap && !hasState {
		config := raft.Configuration{
			Servers: []raft.Server{
				{
					ID:      config.LocalID,
					Address: transport.LocalAddr(),
				},
			},
		}
		err = l.raft.BootstrapCluster(config).Error()
	}

	return err
}

func (l *Distributed) apply(reqType RequestType, req proto.Message) (interface{}, error) {
	var buf bytes.Buffer
	_, err := buf.Write([]byte{byte(reqType)})
	if err != nil {
		return nil, fmt.Errorf("write request type: %w", err)
	}

	b, err := proto.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("protobuf marshal the request: %w", err)
	}

	_, err = buf.Write(b)
	if err != nil {
		return nil, fmt.Errorf("write request: %w", err)
	}

	timeout := 10 * time.Second
	future := l.raft.Apply(buf.Bytes(), timeout)
	if future.Error() != nil {
		return nil, future.Error()
	}

	res := future.Response()
	if err, ok := res.(error); ok {
		return nil, err
	}

	return res, nil
}
