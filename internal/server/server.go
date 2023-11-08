package server

import (
	"context"
	"fmt"
	"io"

	"google.golang.org/grpc"

	api "github.com/huytran2000-hcmus/proglog/api/v1"
)

type grpcServer struct {
	api.UnimplementedLogServer
	*Config
}

type Config struct {
	CommitLog CommitLog
}

type CommitLog interface {
	Append(*api.Record) (uint64, error)
	Read(uint64) (*api.Record, error)
}

func NewGRPCServer(config *Config, opts ...grpc.ServerOption) (*grpc.Server, error) {
	grpcSrv := grpc.NewServer(opts...)
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
	record, err := s.CommitLog.Read(req.Offset)
	if err != nil {
		return nil, fmt.Errorf("read a record: %w", err)
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
