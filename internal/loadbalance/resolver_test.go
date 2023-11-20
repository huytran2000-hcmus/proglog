package loadbalance_test

import (
	"net"
	"net/url"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/attributes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/serviceconfig"

	api "github.com/huytran2000-hcmus/proglog/api/v1"
	"github.com/huytran2000-hcmus/proglog/internal/config"
	"github.com/huytran2000-hcmus/proglog/internal/loadbalance"
	"github.com/huytran2000-hcmus/proglog/internal/server"
	"github.com/huytran2000-hcmus/proglog/pkg/testhelper"
)

func TestResolver(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	testhelper.RequireNoError(t, err)

	tlsConfig, err := config.SetupTLSConfig(config.TLSConfig{
		CertFile:      config.ServerCertFile,
		KeyFile:       config.ServerKeyFile,
		CAFile:        config.CAFile,
		ServerAddress: "127.0.0.1",
		IsServer:      true,
	})
	testhelper.RequireNoError(t, err)

	serverCreds := credentials.NewTLS(tlsConfig)
	srv, err := server.NewGRPCServer(&server.Config{
		GetServerer: &getServers{},
	}, grpc.Creds(serverCreds))
	testhelper.RequireNoError(t, err)

	go srv.Serve(ln)

	conn := &clientConn{}
	tlsConfig, err = config.SetupTLSConfig(config.TLSConfig{
		CertFile:      config.RootClientCertFile,
		KeyFile:       config.RootClientKeyFile,
		CAFile:        config.CAFile,
		ServerAddress: "127.0.0.1",
		IsServer:      false,
	})
	testhelper.RequireNoError(t, err)
	clientCreds := credentials.NewTLS(tlsConfig)
	opts := resolver.BuildOptions{
		DisableServiceConfig: false,
		DialCreds:            clientCreds,
	}

	r := &loadbalance.Resolver{}
	url, err := url.Parse(loadbalance.Name + "://" + ln.Addr().String())
	testhelper.RequireNoError(t, err)
	target := resolver.Target{URL: *url}
	t.Log(target.Endpoint())
	_, err = r.Build(
		target,
		conn,
		opts,
	)
	testhelper.RequireNoError(t, err)

	wantState := resolver.State{
		Addresses: []resolver.Address{
			{
				Addr:       "localhost:9000",
				Attributes: attributes.New("is_leader", true),
			},
			{
				Addr:       "localhost:9001",
				Attributes: attributes.New("is_leader", false),
			},
		},
	}

	testhelper.AssertEqual(t, wantState, conn.state)

	conn.state.Addresses = nil
	r.ResolveNow(resolver.ResolveNowOptions{})
	testhelper.AssertEqual(t, wantState, conn.state)
}

type getServers struct{}

func (g *getServers) GetServers() ([]*api.Server, error) {
	return []*api.Server{
		{
			Id:       "leader",
			RpcAddr:  "localhost:9000",
			IsLeader: true,
		},
		{
			Id:       "follower",
			RpcAddr:  "localhost:9001",
			IsLeader: false,
		},
	}, nil
}

type clientConn struct {
	resolver.ClientConn
	state resolver.State
}

func (c *clientConn) UpdateState(state resolver.State) error {
	c.state = state
	return nil
}
func (c *clientConn) ReportError(error)                       {}
func (c *clientConn) NewAddress(addresses []resolver.Address) {}
func (c *clientConn) NewServiceConfig(serviceConfig string)   {}
func (c *clientConn) ParseServiceConfig(serviceConfigJSON string) *serviceconfig.ParseResult {
	return nil
}
