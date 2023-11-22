package agent

import (
	"bytes"
	"crypto/tls"
	"fmt"
	"io"
	glog "log"
	"net"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	"github.com/huytran2000-hcmus/gopkg/logger"
	"github.com/soheilhy/cmux"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"github.com/huytran2000-hcmus/proglog/internal/auth"
	"github.com/huytran2000-hcmus/proglog/internal/discovery"
	"github.com/huytran2000-hcmus/proglog/internal/log"
	"github.com/huytran2000-hcmus/proglog/internal/server"
)

type Agent struct {
	Config

	log *log.Distributed

	server     *grpc.Server
	membership *discovery.Membership
	mux        cmux.CMux

	shutdown     bool
	shutdowns    chan struct{}
	shutdownLock sync.Mutex
}

type Config struct {
	Bootstrap bool

	DataDir       string
	MaxStoreBytes uint64
	MaxIndexBytes uint64

	ServerTLSConfig *tls.Config
	PeerTLSConfig   *tls.Config

	BindAddr        string
	RPCPort         int
	NodeName        string
	StartPointAddrs []string

	ACLModelFile  string
	ACLPolicyFile string
}

func New(config Config) (*Agent, error) {
	a := &Agent{
		Config:    config,
		shutdown:  false,
		shutdowns: make(chan struct{}),
	}

	err := a.setupMux()
	if err != nil {
		return nil, fmt.Errorf("setup mux: %w", err)
	}
	err = a.setupLog()
	if err != nil {
		return nil, fmt.Errorf("setup log: %w", err)
	}
	err = a.setupServer()
	if err != nil {
		return nil, fmt.Errorf("setup server: %w", err)
	}
	err = a.setupMembership()
	if err != nil {
		return nil, fmt.Errorf("setup membership: %w", err)
	}

	go a.serve()
	return a, nil
}

func (a *Agent) Shutdown() error {
	a.shutdownLock.Lock()
	defer a.shutdownLock.Unlock()
	if a.shutdown {
		return nil
	}

	a.shutdown = true
	close(a.shutdowns)

	shutdowns := []func() error{
		a.membership.Leave,
		func() error {
			a.server.GracefulStop()
			return nil
		},
		a.log.Close,
	}

	for _, fn := range shutdowns {
		err := fn()
		if err != nil {
			return nil
		}
	}

	return nil
}

func (cfg Config) RPCAddr() (string, error) {
	host, _, err := net.SplitHostPort(cfg.BindAddr)
	if err != nil {
		return "", fmt.Errorf("split host port: %w", err)
	}

	return fmt.Sprintf("%s:%d", host, cfg.RPCPort), nil
}

func (a *Agent) setupMembership() error {
	rpcAddr, err := a.RPCAddr()
	if err != nil {
		return fmt.Errorf("invalid rpc address: %w", err)
	}

	config := discovery.Config{
		NodeName: a.NodeName,
		BindAddr: a.BindAddr,
		Tags: map[string]string{
			discovery.RPCTagKey: rpcAddr,
		},
		StartPointAddrs: a.StartPointAddrs,
	}

	a.membership, err = discovery.NewMemberShip(a.log, config)
	return err
}

func (a *Agent) setupServer() error {
	authorizer := auth.New(a.ACLModelFile, a.ACLPolicyFile)

	config := &server.Config{
		CommitLog:   a.log,
		Authorizer:  authorizer,
		GetServerer: a.log,
	}

	var opts []grpc.ServerOption
	if a.ServerTLSConfig != nil {
		creds := credentials.NewTLS(a.ServerTLSConfig)
		opts = append(opts, grpc.Creds(creds))
	}

	var err error
	a.server, err = server.NewGRPCServer(config, opts...)
	if err != nil {
		return fmt.Errorf("create grpc server: %w", err)
	}

	grpcLn := a.mux.Match(cmux.Any())
	go func() {
		err := a.server.Serve(grpcLn)
		if err != nil {
			_ = a.Shutdown()
		}
	}()

	return err
}

func (a *Agent) setupLog() error {
	raftLn := a.mux.Match(func(r io.Reader) bool {
		b := make([]byte, 1)
		_, err := r.Read(b)
		if err != nil {
			return false
		}

		return bytes.Compare(b, []byte{byte(log.RaftRPC)}) == 0
	})
	logConfig := log.Config{}
	logConfig.Raft.Stream = log.NewStreamLayer(raftLn, a.ServerTLSConfig, a.PeerTLSConfig)
	rpcAddr, err := a.Config.RPCAddr()
	if err != nil {
		return err
	}
	logConfig.Raft.BindAddr = rpcAddr
	logConfig.Raft.LocalID = raft.ServerID(a.NodeName)
	logConfig.Raft.Bootstrap = a.Bootstrap

	a.log, err = log.NewDistributed(a.DataDir, logConfig)
	if err != nil {
		return err
	}

	if a.Bootstrap {
		err = a.log.WaitForLeader(5 * time.Second)
	}

	return err
}

func (a *Agent) setupMux() error {
	addr, err := net.ResolveTCPAddr("tcp", a.BindAddr)
	if err != nil {
		return fmt.Errorf("resolve tcp addr: %w", err)
	}
	rpcAddr := fmt.Sprintf("%s:%d", addr.IP.String(), a.RPCPort)

	ln, err := net.Listen("tcp", rpcAddr)
	if err != nil {
		return fmt.Errorf("listen on address %s: %w", rpcAddr, err)
	}
	a.mux = cmux.New(ln)

	return nil
}

func (a *Agent) setupLogger() error {
	lgr, err := logger.New("prodlog", logger.Production, logger.Info)
	if err != nil {
		return err
	}

	logger.ReplaceGlobals(lgr)
	return nil
}

func (a *Agent) serve() error {
	err := a.mux.Serve()
	if err != nil {
		anotherErr := a.Shutdown()
		glog.Printf("%s: %s\n", anotherErr, err)
	}

	return nil
}
