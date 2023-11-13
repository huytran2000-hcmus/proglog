package discovery

import (
	"fmt"
	"testing"
	"time"

	"github.com/hashicorp/serf/serf"
	"github.com/travisjeffery/go-dynaport"

	"github.com/huytran2000-hcmus/proglog/pkg/testhelper"
)

func TestMemberShip(t *testing.T) {
	m, h := setupMember(t, nil)
	m, _ = setupMember(t, m)
	m, _ = setupMember(t, m)

	eventually := func(t *testing.T, f func() bool, failedMsg string) {
		ticker := time.NewTicker(250 * time.Millisecond)
		for {
			select {
			case <-ticker.C:
				ok := f()
				if ok {
					return
				}
			case <-time.After(3 * time.Second):
				t.Fatal(failedMsg)
			}
		}
	}

	eventually(t, func() bool {
		return len(h.joins) == 2 &&
			len(m[0].Members()) == 3 &&
			len(h.leaves) == 0
	}, "initial states is not correct")

	testhelper.AssertNoError(t, m[2].Leave())

	eventually(t, func() bool {
		return len(h.joins) == 2 &&
			len(m[0].Members()) == 3 &&
			len(h.leaves) == 1 &&
			serf.StatusLeft == m[0].Members()[2].Status
	}, "states after 1 member leave is not correct")
}

func setupMember(t *testing.T, members []*Membership) ([]*Membership, *handler) {
	id := len(members)
	ports := dynaport.Get(1)
	addr := fmt.Sprintf("%s:%d", "127.0.0.1", ports[0])
	tags := map[string]string{
		RPCTagKey: addr,
	}

	c := Config{
		NodeName: fmt.Sprintf("%d", id),
		BindAddr: addr,
		Tags:     tags,
	}

	h := &handler{}
	if len(members) == 0 {
		h.joins = make(chan map[string]string, 3)
		h.leaves = make(chan string, 3)
	} else {
		c.StartPointAddrs = append(c.StartPointAddrs, members[0].BindAddr)
	}

	m, err := NewMemberShip(h, c)
	testhelper.AssertNoError(t, err)
	members = append(members, m)

	return members, h
}

type handler struct {
	joins  chan map[string]string
	leaves chan string
}

func (h *handler) Join(id, addr string) error {
	h.joins <- map[string]string{
		"id":   id,
		"addr": addr,
	}

	return nil
}

func (h *handler) Leave(id string) error {
	h.leaves <- id

	return nil
}
