package server

import (
	"context"
	"net"
	"os"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"

	api "github.com/huytran2000-hcmus/proglog/api/v1"
	"github.com/huytran2000-hcmus/proglog/internal/auth"
	"github.com/huytran2000-hcmus/proglog/internal/config"
	"github.com/huytran2000-hcmus/proglog/internal/log"
	"github.com/huytran2000-hcmus/proglog/pkg/testhelper"
)

func TestGRPCServer(t *testing.T) {
	t.Run("produce and consume", func(t *testing.T) {
		rootClient, _, teardown := setupServer(t)
		defer teardown()
		testProduceConsume(t, rootClient)
	})

	t.Run("consume past boundary", func(t *testing.T) {
		rootClient, _, teardown := setupServer(t)
		defer teardown()
		testConsumePastBoundary(t, rootClient)
	})

	t.Run("produce and consume stream", func(t *testing.T) {
		rootClient, _, teardown := setupServer(t)
		defer teardown()
		testProduceConsumeStream(t, rootClient)
	})

	t.Run("unauthorized client", func(t *testing.T) {
		_, nobodyClient, teardown := setupServer(t)
		defer teardown()
		testAuthorization(t, nobodyClient)
	})
}

func testProduceConsume(t *testing.T, client api.LogClient) {
	want := &api.Record{
		Value:  []byte("hello-world"),
		Offset: 0,
	}
	produceReq := &api.ProduceRequest{
		Record: want,
	}

	ctx := context.Background()
	produceResp, err := client.Produce(ctx, produceReq)
	testhelper.AssertNoError(t, err)

	consumeReq := &api.ConsumeRequest{
		Offset: produceResp.Offset,
	}
	consumeResp, err := client.Consume(ctx, consumeReq)
	testhelper.AssertNoError(t, err)
	testhelper.AssertNotEqual(t, nil, consumeResp.Record)

	testhelper.AssertEqual(t, want.Value, consumeResp.Record.Value)
	testhelper.AssertEqual(t, want.Offset, consumeResp.Record.Offset)
}

func testConsumePastBoundary(t *testing.T, client api.LogClient) {
	ctx := context.Background()
	produceResp, err := client.Produce(ctx, &api.ProduceRequest{
		Record: &api.Record{
			Value: []byte("hello world"),
		},
	})
	testhelper.AssertNoError(t, err)

	consumeResp, err := client.Consume(ctx, &api.ConsumeRequest{
		Offset: produceResp.Offset + 1,
	})

	if consumeResp != nil {
		testhelper.AssertNotEqual(t, nil, consumeResp.Record)
	}
	gotCode := status.Code(err)
	wantCode := status.Code(api.OffsetOutOfRangeError{}.GRPCStatus().Err())
	testhelper.AssertEqual(t, wantCode, gotCode)
}

func setupServer(t *testing.T) (rootClient api.LogClient, nobodyClient api.LogClient, teardown func()) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	testhelper.AssertNoError(t, err)

	rootClient, rootClientConn, err := setupClient(t, config.RootClientCertFile, config.RootClientKeyFile, l.Addr().String())
	testhelper.AssertNoError(t, err)

	nobodyClient, nobodyClientConn, err := setupClient(t, config.NobodyClientCertFile, config.NobodyClientKeyFile, l.Addr().String())
	testhelper.AssertNoError(t, err)

	dir, err := os.MkdirTemp(os.TempDir(), "server-test")
	testhelper.AssertNoError(t, err)

	log, err := log.NewLog(dir, log.Config{})
	testhelper.AssertNoError(t, err)
	authorizer := auth.New(config.ACLModelFile, config.ACLPolicyFile)
	cfg := &Config{
		CommitLog:  log,
		Authorizer: authorizer,
	}

	serverTLSConfig, err := config.SetupTLSConfig(config.TLSConfig{
		CertFile:      config.ServerCertFile,
		KeyFile:       config.ServerKeyFile,
		CAFile:        config.CAFile,
		ServerAddress: l.Addr().String(),
		IsServer:      true,
	})
	testhelper.AssertNoError(t, err)

	serverCreds := credentials.NewTLS(serverTLSConfig)
	server, err := NewGRPCServer(cfg, grpc.Creds(serverCreds))
	testhelper.AssertNoError(t, err)

	go func() {
		_ = server.Serve(l)
	}()

	return rootClient, nobodyClient, func() {
		rootClientConn.Close()
		nobodyClientConn.Close()
		server.Stop()
		l.Close()
		_ = log.Remove()
		os.Remove(dir)
	}
}

func testProduceConsumeStream(t *testing.T, client api.LogClient) {
	records := []*api.Record{
		{
			Value: []byte("first message"),
		},
		{
			Value: []byte("second message"),
		},
	}
	ctx := context.Background()

	{
		stream, err := client.ProduceStream(ctx)
		testhelper.AssertNoError(t, err)

		for offset, record := range records {
			err := stream.Send(&api.ProduceRequest{
				Record: record,
			})
			testhelper.AssertNoError(t, err)
			resp, err := stream.Recv()
			testhelper.AssertNoError(t, err)

			if resp.Offset != uint64(offset) {
				t.Fatalf("got offset %d, want: %d", resp.Offset, offset)
			}
		}
	}

	{
		stream, err := client.ConsumeStream(ctx, &api.ConsumeRequest{Offset: 0})
		testhelper.AssertNoError(t, err)

		for offset, record := range records {
			resp, err := stream.Recv()
			testhelper.AssertNoError(t, err)
			testhelper.AssertEqual(t, uint64(offset), resp.Record.Offset)
			testhelper.AssertEqual(t, record.Value, resp.Record.Value)
		}
	}
}

func setupClient(t *testing.T, certPath, keyPath, address string) (api.LogClient, *grpc.ClientConn, error) {
	clientTLSConfig, err := config.SetupTLSConfig(config.TLSConfig{
		CertFile: certPath,
		KeyFile:  keyPath,
		CAFile:   config.CAFile,
		IsServer: false,
	})
	testhelper.AssertNoError(t, err)

	clientCreds := credentials.NewTLS(clientTLSConfig)

	clientOpt := grpc.WithTransportCredentials(clientCreds)
	conn, err := grpc.Dial(address, clientOpt)
	testhelper.AssertNoError(t, err)

	client := api.NewLogClient(conn)

	return client, conn, err
}

func testAuthorization(t *testing.T, nobodyClient api.LogClient) {
	produceReq := &api.ProduceRequest{
		Record: &api.Record{
			Value:  []byte("hello-world"),
			Offset: 0,
		},
	}

	ctx := context.Background()
	_, err := nobodyClient.Produce(ctx, produceReq)
	wantCode := codes.PermissionDenied
	gotCode := status.Code(err)
	testhelper.AssertEqual(t, wantCode, gotCode)

	consumeReq := &api.ConsumeRequest{
		Offset: 0,
	}
	_, err = nobodyClient.Consume(ctx, consumeReq)
	wantCode = codes.PermissionDenied
	gotCode = status.Code(err)
	testhelper.AssertEqual(t, wantCode, gotCode)
}
