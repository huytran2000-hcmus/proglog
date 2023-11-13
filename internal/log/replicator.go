package log

import (
	"context"
	"sync"

	"go.uber.org/zap"
	"google.golang.org/grpc"

	api "github.com/huytran2000-hcmus/proglog/api/v1"
)

type Replicator struct {
	DialOpts     []grpc.DialOption
	LocalServer  api.LogClient
	logger       *zap.Logger
	mu           sync.Mutex
	closeServers map[string]chan struct{}
	close        chan struct{}
	closed       bool
}

func (rt *Replicator) Join(name, addr string) error {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	rt.init()

	if rt.closed {
		return nil
	}

	_, ok := rt.closeServers[name]
	if ok {
		return nil
	}

	rt.closeServers[name] = make(chan struct{})

	go rt.replicate(addr, rt.closeServers[name])

	return nil
}

func (rt *Replicator) Leave(name string) error {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	rt.init()

	ch, ok := rt.closeServers[name]
	if !ok {
		return nil
	}
	close(ch)

	delete(rt.closeServers, name)

	return nil
}

func (rt *Replicator) Close() error {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	rt.init()

	if rt.closed {
		return nil
	}

	rt.closed = true
	close(rt.close)

	return nil
}

func (rt *Replicator) replicate(addr string, leave chan struct{}) {
	cc, err := grpc.Dial(addr, rt.DialOpts...)
	if err != nil {
		rt.logError(err, "failed to dial", addr)
	}
	defer cc.Close()

	client := api.NewLogClient(cc)

	ctx := context.Background()
	stream, err := client.ConsumeStream(ctx, &api.ConsumeRequest{
		Offset: 0,
	})
	if err != nil {
		rt.logError(err, "failed to create consume stream", addr)
	}

	records := make(chan *api.Record)
	go func() {
		for {
			consume, err := stream.Recv()
			if err != nil {
				rt.logError(err, "failed to consume from stream", addr)
				return
			}

			records <- consume.Record
		}
	}()

	for {
		select {
		case <-rt.close:
			return
		case <-leave:
			return
		case rec := <-records:
			req := &api.ProduceRequest{
				Record: rec,
			}

			_, err := rt.LocalServer.Produce(ctx, req)
			if err != nil {
				rt.logError(err, "failed to produce", addr)
			}
		}
	}
}

func (rt *Replicator) init() {
	if rt.logger == nil {
		rt.logger = zap.L().Named("Replicator")
	}

	if rt.closeServers == nil {
		rt.closeServers = make(map[string]chan struct{})
	}

	if rt.close == nil {
		rt.close = make(chan struct{})
	}
}

func (rt *Replicator) logError(err error, msg, addr string) {
	rt.logger.Error(
		msg,
		zap.Error(err),
		zap.String("addr", addr),
	)
}
