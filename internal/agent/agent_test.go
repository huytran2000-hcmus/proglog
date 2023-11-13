package agent_test

import (
	"context"
	"crypto/tls"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/travisjeffery/go-dynaport"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	api "github.com/huytran2000-hcmus/proglog/api/v1"
	"github.com/huytran2000-hcmus/proglog/internal/agent"
	"github.com/huytran2000-hcmus/proglog/internal/config"
	"github.com/huytran2000-hcmus/proglog/pkg/testhelper"
)

func TestAgent(t *testing.T) {
	serverTLSCfg, err := config.SetupTLSConfig(config.TLSConfig{
		CertFile:      config.ServerCertFile,
		KeyFile:       config.ServerKeyFile,
		CAFile:        config.CAFile,
		ServerAddress: "127.0.0.1",
		IsServer:      true,
	})
	testhelper.AssertNoError(t, err)

	peerTLSCfg, err := config.SetupTLSConfig(config.TLSConfig{
		CertFile:      config.RootClientCertFile,
		KeyFile:       config.RootClientKeyFile,
		CAFile:        config.CAFile,
		ServerAddress: "127.0.0.1",
		IsServer:      false,
	})

	testhelper.AssertNoError(t, err)
	var agents []*agent.Agent
	for i := 0; i < 3; i++ {
		ports := dynaport.Get(2)
		bindAddr := fmt.Sprintf("%s:%d", "127.0.0.1", ports[0])
		rpcPort := ports[1]

		dir, err := os.MkdirTemp(os.TempDir(), "agent-test-log")
		testhelper.AssertNoError(t, err)

		var startJoinAddrs []string
		if i != 0 {
			startJoinAddrs = append(startJoinAddrs, agents[0].BindAddr)
		}

		agent, err := agent.New(agent.Config{
			DataDir:         dir,
			MaxStoreBytes:   0,
			MaxIndexBytes:   0,
			ServerTLS:       serverTLSCfg,
			PeerTLS:         peerTLSCfg,
			BindAddr:        bindAddr,
			RPCPort:         rpcPort,
			NodeName:        fmt.Sprint(i),
			StartPointAddrs: startJoinAddrs,
			ACLModelFile:    config.ACLModelFile,
			ACLPolicyFile:   config.ACLPolicyFile,
		},
		)
		testhelper.AssertNoError(t, err)

		agents = append(agents, agent)
	}

	defer func() {
		for _, agent := range agents {
			err := agent.Shutdown()
			testhelper.AssertNoError(t, err)
			testhelper.AssertNoError(t, os.RemoveAll(agent.DataDir))
		}
	}()

	leaderClient := client(t, agents[0], peerTLSCfg)
	ctx := context.Background()
	want := []byte("what's up")
	produce, err := leaderClient.Produce(
		ctx,
		&api.ProduceRequest{
			Record: &api.Record{
				Value: want,
			},
		},
	)
	testhelper.AssertNoError(t, err)

	consume, err := leaderClient.Consume(ctx, &api.ConsumeRequest{Offset: produce.Offset})
	testhelper.AssertNoError(t, err)
	testhelper.AssertEqual(t, want, consume.Record.Value)

	// consume, err = leaderClient.Consume(ctx, &api.ConsumeRequest{Offset: produce.Offset + 1})
	// testhelper.AssertEqual(t, (*api.ConsumeResponse)(nil), consume)
	// gotErr := status.Code(err)
	// wantErr := status.Code(api.OffsetOutOfRangeError{}.GRPCStatus().Err())
	// testhelper.AssertEqual(t, wantErr, gotErr)

	time.Sleep(2 * time.Second)

	followerClient := client(t, agents[1], peerTLSCfg)
	consume, err = followerClient.Consume(ctx, &api.ConsumeRequest{Offset: produce.Offset})
	testhelper.AssertNoError(t, err)
	testhelper.AssertEqual(t, want, consume.Record.Value)
}

func client(t *testing.T, agent *agent.Agent, tlsConfig *tls.Config) api.LogClient {
	tlsCreds := credentials.NewTLS(tlsConfig)
	opts := []grpc.DialOption{grpc.WithTransportCredentials(tlsCreds)}
	rpcAddr, err := agent.RPCAddr()
	testhelper.AssertNoError(t, err)

	conn, err := grpc.Dial(rpcAddr, opts...)
	testhelper.AssertNoError(t, err)

	client := api.NewLogClient(conn)

	return client
}
