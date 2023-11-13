package agent

import (
	"crypto/tls"
	"fmt"
	"net"
	"sync"

	"github.com/huytran2000-hcmus/gopkg/logger"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	api "github.com/huytran2000-hcmus/proglog/api/v1"
	"github.com/huytran2000-hcmus/proglog/internal/auth"
	"github.com/huytran2000-hcmus/proglog/internal/discovery"
	"github.com/huytran2000-hcmus/proglog/internal/log"
	"github.com/huytran2000-hcmus/proglog/internal/server"
)

type Agent struct {
	Config
	log        *log.Log
	server     *grpc.Server
	membership *discovery.Membership
	replicator *log.Replicator

	shutdown     bool
	shutdowns    chan struct{}
	shutdownLock sync.Mutex
}

type Config struct {
	DataDir       string
	MaxStoreBytes uint64
	MaxIndexBytes uint64

	ServerTLS *tls.Config
	PeerTLS   *tls.Config

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

	for _, fn := range []func() error{
		// "logger": a.setupLogger,
		a.setupLog,
		a.setupServer,
		a.setupMember,
	} {
		err := fn()
		if err != nil {
			return nil, fmt.Errorf("setup: %w", err)
		}
	}

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
		a.replicator.Close,
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

func (a *Agent) setupMember() error {
	rpcAddr, err := a.RPCAddr()
	if err != nil {
		return fmt.Errorf("invalid rpc address: %w", err)
	}

	var opts []grpc.DialOption
	if a.PeerTLS != nil {
		creds := grpc.WithTransportCredentials(credentials.NewTLS(a.PeerTLS))
		opts = append(opts, creds)
	}

	conn, err := grpc.Dial(rpcAddr, opts...)
	if err != nil {
		return fmt.Errorf("dial to local server: %w", err)
	}

	client := api.NewLogClient(conn)
	replicator := log.Replicator{
		DialOpts:    opts,
		LocalServer: client,
	}
	a.replicator = &replicator

	config := &discovery.Config{
		NodeName: a.NodeName,
		BindAddr: a.BindAddr,
		Tags: map[string]string{
			discovery.RPCTagKey: rpcAddr,
		},
		StartPointAddrs: a.StartPointAddrs,
	}

	a.membership, err = discovery.NewMemberShip(&replicator, *config)
	return err
}

func (a *Agent) setupServer() error {
	authorizer := auth.New(a.ACLModelFile, a.ACLPolicyFile)

	config := &server.Config{
		CommitLog:  a.log,
		Authorizer: authorizer,
	}

	var opts []grpc.ServerOption
	if a.ServerTLS != nil {
		creds := credentials.NewTLS(a.ServerTLS)
		opts = append(opts, grpc.Creds(creds))
	}

	var err error
	a.server, err = server.NewGRPCServer(config, opts...)
	if err != nil {
		return fmt.Errorf("create grpc server: %w", err)
	}

	rpcAddr, err := a.RPCAddr()
	if err != nil {
		return fmt.Errorf("invalid rpc address: %w", err)
	}

	ln, err := net.Listen("tcp", rpcAddr)
	if err != nil {
		return fmt.Errorf("listen at %s: %w", rpcAddr, err)
	}

	go func() {
		err := a.server.Serve(ln)
		if err != nil {
			_ = a.Shutdown()
		}
	}()

	return nil
}

func (a *Agent) setupLog() error {
	var cfg log.Config
	cfg.Segment.MaxStoreBytes = a.MaxStoreBytes
	cfg.Segment.MaxIndexBytes = a.MaxIndexBytes

	log, err := log.New(a.DataDir, cfg)
	if err != nil {
		return err
	}
	fmt.Printf("log: %+v\n", log)
	a.log = log

	return err
}

func (a *Agent) setupLogger() error {
	lgr, err := logger.New("prodlog", logger.Production, logger.Info)
	if err != nil {
		return err
	}

	logger.ReplaceGlobals(lgr)
	return nil
}
