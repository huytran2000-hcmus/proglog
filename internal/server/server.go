package server

import (
	"context"
	"fmt"
	"io"

	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_auth "github.com/grpc-ecosystem/go-grpc-middleware/auth"
	grpc_zap "github.com/grpc-ecosystem/go-grpc-middleware/logging/zap"
	grpc_ctxtags "github.com/grpc-ecosystem/go-grpc-middleware/tags"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"

	api "github.com/huytran2000-hcmus/proglog/api/v1"
)

const (
	produceAction  = "produce"
	consumeAction  = "consume"
	objectWildCard = "*"
)

const (
	serverName = "proglog"
)

type grpcServer struct {
	api.UnimplementedLogServer
	*Config
}

type Config struct {
	CommitLog   CommitLog
	Authorizer  Authorizer
	GetServerer GetServerer
}

type CommitLog interface {
	Append(*api.Record) (uint64, error)
	Read(uint64) (*api.Record, error)
}

type Authorizer interface {
	Authorize(subject, object, acttion string) error
}

type GetServerer interface {
	GetServers() ([]*api.Server, error)
}

type subjectContextKey struct{}

func NewGRPCServer(config *Config, opts ...grpc.ServerOption) (*grpc.Server, error) {
	logger := zap.L().Named(serverName)
	// grpc_zap.ReplaceGrpcLoggerV2(logger)

	zapOpts := []grpc_zap.Option{
		grpc_zap.WithDurationField(grpc_zap.DurationToTimeMillisField),
	}

	opts = append(opts,
		grpc.StreamInterceptor(
			grpc_middleware.ChainStreamServer(
				grpc_ctxtags.StreamServerInterceptor(),
				grpc_zap.StreamServerInterceptor(logger, zapOpts...),
				grpc_auth.StreamServerInterceptor(authenticate),
			),
		),
		grpc.UnaryInterceptor(
			grpc_middleware.ChainUnaryServer(
				grpc_ctxtags.UnaryServerInterceptor(),
				grpc_zap.UnaryServerInterceptor(logger, zapOpts...),
				grpc_auth.UnaryServerInterceptor(authenticate),
			),
		),
		grpc.StatsHandler(otelgrpc.NewServerHandler()),
	)
	grpcSrv := grpc.NewServer(opts...)

	hsrv := health.NewServer()
	hsrv.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING)
	grpc_health_v1.RegisterHealthServer(grpcSrv, hsrv)

	srv, err := newGRPCServer(config)
	if err != nil {
		return nil, fmt.Errorf("create new grpc log server: %w", err)
	}

	api.RegisterLogServer(grpcSrv, srv)

	return grpcSrv, nil
}

func newGRPCServer(config *Config) (*grpcServer, error) {
	return &grpcServer{
		Config: config,
	}, nil
}

func (s *grpcServer) Produce(ctx context.Context, req *api.ProduceRequest) (*api.ProduceResponse, error) {
	err := s.Authorizer.Authorize(subject(ctx), objectWildCard, produceAction)
	if err != nil {
		return nil, fmt.Errorf("failed authorization: %w", err)
	}
	offset, err := s.CommitLog.Append(req.Record)
	if err != nil {
		return nil, fmt.Errorf("produce a record: %w", err)
	}

	resp := &api.ProduceResponse{
		Offset: offset,
	}

	return resp, nil
}

func (s *grpcServer) Consume(ctx context.Context, req *api.ConsumeRequest) (*api.ConsumeResponse, error) {
	err := s.Authorizer.Authorize(subject(ctx), objectWildCard, consumeAction)
	if err != nil {
		return nil, fmt.Errorf("failed authorization: %w", err)
	}

	record, err := s.CommitLog.Read(req.Offset)
	if err != nil {
		return nil, err
	}

	resp := &api.ConsumeResponse{
		Record: record,
	}

	return resp, nil
}

func (s *grpcServer) ProduceStream(stream api.Log_ProduceStreamServer) error {
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			break
		}

		if err != nil {
			return fmt.Errorf("receive from bidirectional stream: %w", err)
		}

		resp, err := s.Produce(stream.Context(), req)
		if err != nil {
			return fmt.Errorf("produce from request: %w", err)
		}

		err = stream.Send(resp)
		if err != nil {
			return fmt.Errorf("send a produced record to bidirectional stream: %w", err)
		}
	}

	return nil
}

func (s *grpcServer) ConsumeStream(req *api.ConsumeRequest, stream api.Log_ConsumeStreamServer) error {
	for {
		select {
		case <-stream.Context().Done():
			return nil
		default:
			res, err := s.Consume(stream.Context(), req)

			switch err.(type) {
			case nil:
			case api.OffsetOutOfRangeError:
				continue
			default:
				return err
			}

			err = stream.Send(res)
			if err != nil {
				return err
			}
			req.Offset++
		}
	}
}

func (s *grpcServer) GetServers(ctx context.Context, req *api.GetServersRequest) (*api.GetServersResponse, error) {
	servers, err := s.GetServerer.GetServers()
	if err != nil {
		return nil, err
	}

	return &api.GetServersResponse{
		Servers: servers,
	}, nil
}

func authenticate(ctx context.Context) (context.Context, error) {
	peer, ok := peer.FromContext(ctx)
	if !ok {
		return ctx, status.New(codes.Unknown, "couldn't find peer error").Err()
	}

	if peer.AuthInfo == nil {
		return context.WithValue(ctx, subjectContextKey{}, ""), nil
	}

	tlsInfo := peer.AuthInfo.(credentials.TLSInfo)
	subject := tlsInfo.State.VerifiedChains[0][0].Subject.CommonName
	ctx = context.WithValue(ctx, subjectContextKey{}, subject)

	return ctx, nil
}

func subject(ctx context.Context) string {
	return ctx.Value(subjectContextKey{}).(string)
}
