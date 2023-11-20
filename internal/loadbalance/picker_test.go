package loadbalance_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/attributes"
	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/balancer/base"
	"google.golang.org/grpc/resolver"

	"github.com/huytran2000-hcmus/proglog/internal/loadbalance"
	"github.com/huytran2000-hcmus/proglog/pkg/testhelper"
)

func TestPickerProduceToLeader(t *testing.T) {
	picker, subConns := setupTest()
	info := balancer.PickInfo{
		FullMethodName: "log.Produce##",
	}

	for i := 0; i < 5; i++ {
		result, err := picker.Pick(info)
		testhelper.RequireNoError(t, err)
		if subConns[0] != result.SubConn {
			t.Errorf("got %s, want %s", result.SubConn, subConns[0])
		}
	}
}

func TestPickerConsumeFromFollowers(t *testing.T) {
	picker, subConns := setupTest()
	info := balancer.PickInfo{
		FullMethodName: "log.Consume##",
	}

	for i := 0; i < 5; i++ {
		result, err := picker.Pick(info)
		testhelper.RequireNoError(t, err)
		require.Equal(t, subConns[i%2+1], result.SubConn)
	}
}

func TestPickNoSubConnAvailable(t *testing.T) {
	picker := &loadbalance.Picker{}

	for _, method := range []string{
		"/log.Produce",
		"/log.Consume",
	} {
		info := balancer.PickInfo{
			FullMethodName: method,
		}

		result, err := picker.Pick(info)
		testhelper.AssertEqual(t, nil, result.SubConn)
		testhelper.AssertError(t, balancer.ErrNoSubConnAvailable, err)
	}
}

func setupTest() (*loadbalance.Picker, []*subConn) {
	var subConns []*subConn
	buildInfo := base.PickerBuildInfo{
		ReadySCs: make(map[balancer.SubConn]base.SubConnInfo),
	}

	for i := 0; i < 3; i++ {
		sc := &subConn{}
		addr := resolver.Address{
			Attributes: attributes.New("is_leader", i == 0),
		}

		sc.UpdateAddresses([]resolver.Address{addr})
		buildInfo.ReadySCs[sc] = base.SubConnInfo{Address: addr}
		subConns = append(subConns, sc)
	}

	picker := &loadbalance.Picker{}
	picker.Build(buildInfo)

	return picker, subConns
}

type subConn struct {
	addrs []resolver.Address
}

func (c *subConn) UpdateAddresses(addresses []resolver.Address) {
	c.addrs = addresses
}

func (c *subConn) Connect() {}

func (c *subConn) GetOrBuildProducer(balancer.ProducerBuilder) (p balancer.Producer, close func()) {
	return nil, func() {}
}

func (c *subConn) Shutdown() {}
