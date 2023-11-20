package loadbalance

import (
	"context"
	"fmt"
	"sync"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/attributes"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/serviceconfig"

	api "github.com/huytran2000-hcmus/proglog/api/v1"
)

const Name = "proglog"

var (
	_ resolver.Builder  = (*Resolver)(nil)
	_ resolver.Resolver = (*Resolver)(nil)
)

func init() {
	resolver.Register(&Resolver{})
}

type Resolver struct {
	mu            sync.Mutex
	clientConn    resolver.ClientConn
	resolverConn  *grpc.ClientConn
	serviceConfig *serviceconfig.ParseResult
	logger        *zap.Logger
}

func (r *Resolver) Build(target resolver.Target, cc resolver.ClientConn, opts resolver.BuildOptions) (resolver.Resolver, error) {
	r.logger = zap.L().Named("resolver")
	r.clientConn = cc
	var dialOpts []grpc.DialOption
	if opts.DialCreds != nil {
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(opts.DialCreds))
	}

	r.serviceConfig = r.clientConn.ParseServiceConfig(
		fmt.Sprintf(`{"loadBalancingConfig":[{"%s": {}}]}`, Name),
	)

	var err error
	r.resolverConn, err = grpc.Dial(target.URL.Host, dialOpts...)
	if err != nil {
		return nil, fmt.Errorf("dial resolver conn: %w", err)
	}

	r.ResolveNow(resolver.ResolveNowOptions{})

	return r, nil
}

func (r *Resolver) ResolveNow(resolver.ResolveNowOptions) {
	r.mu.Lock()
	defer r.mu.Unlock()

	client := api.NewLogClient(r.resolverConn)
	ctx := context.Background()
	res, err := client.GetServers(ctx, &api.GetServersRequest{})
	if err != nil {
		r.logger.Error("failed to resolve server", zap.Error(err))
		return
	}

	var addrs []resolver.Address
	for _, srv := range res.Servers {
		addrs = append(addrs, resolver.Address{
			Addr: srv.RpcAddr,
			Attributes: attributes.New(
				"is_leader",
				srv.IsLeader,
			),
		})
	}

	err = r.clientConn.UpdateState(resolver.State{
		Addresses:     addrs,
		ServiceConfig: r.serviceConfig,
	})
	r.logger.Error("failed to update client state", zap.Error(err))
}

func (r *Resolver) Scheme() string {
	return Name
}

func (r *Resolver) Close() {
	err := r.resolverConn.Close()
	if err != nil {
		r.logger.Error(
			"failed to close conn",
			zap.Error(err),
		)
	}
}
