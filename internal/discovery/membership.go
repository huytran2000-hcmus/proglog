package discovery

import (
	"errors"
	"fmt"
	"net"

	"github.com/hashicorp/raft"
	"github.com/hashicorp/serf/serf"
	"go.uber.org/zap"
)

const (
	RPCTagKey = "rpc_addr"
)

type Membership struct {
	Config
	handler Handler
	serf    *serf.Serf
	events  chan serf.Event
	logger  *zap.Logger
}

type Handler interface {
	Join(name, addr string) error
	Leave(name string) error
}

type Config struct {
	NodeName        string
	BindAddr        string
	Tags            map[string]string
	StartPointAddrs []string
}

func NewMemberShip(handler Handler, config Config) (*Membership, error) {
	ms := &Membership{
		Config:  config,
		handler: handler,
		logger:  zap.L(),
	}

	err := ms.setupSerf()
	if err != nil {
		return nil, fmt.Errorf("set up serf: %w", err)
	}
	return ms, nil
}

func (ms *Membership) Members() []serf.Member {
	return ms.serf.Members()
}

func (ms *Membership) Leave() error {
	return ms.serf.Leave()
}

func (ms *Membership) setupSerf() error {
	addr, err := net.ResolveTCPAddr("tcp", ms.BindAddr)
	if err != nil {
		return fmt.Errorf("resolve bind address %s: %w", ms.BindAddr, err)
	}

	config := serf.DefaultConfig()
	config.Init()
	config.MemberlistConfig.BindAddr = addr.IP.String()
	config.MemberlistConfig.BindPort = addr.Port

	ms.events = make(chan serf.Event)
	config.EventCh = ms.events
	config.Tags = ms.Tags
	config.NodeName = ms.NodeName
	ms.serf, err = serf.Create(config)
	if err != nil {
		return fmt.Errorf("create serf agent: %w", err)
	}

	go ms.eventHandler()
	if ms.StartPointAddrs != nil {
		_, err := ms.serf.Join(ms.StartPointAddrs, true)
		if err != nil {
			return fmt.Errorf("join with start point addresses %v: %w", ms.StartPointAddrs, err)
		}
	}

	return nil
}

func (ms *Membership) eventHandler() {
	for e := range ms.events {
		switch e.EventType() {
		case serf.EventMemberJoin:
			memberEvent, ok := e.(serf.MemberEvent)
			if !ok {
				ms.logger.Warn("event type mismatch")
			}

			for _, member := range memberEvent.Members {
				if ms.isLocal(member) {
					continue
				}
				ms.handleJoin(member)
			}
		case serf.EventMemberLeave, serf.EventMemberFailed:
			memberEvent, ok := e.(serf.MemberEvent)
			if !ok {
				ms.logger.Warn("event type mismatch")
			}

			for _, member := range memberEvent.Members {
				if ms.isLocal(member) {
					continue
				}
				ms.handleLeave(member)
			}
		}
	}
}

func (ms *Membership) handleJoin(member serf.Member) {
	err := ms.handler.Join(member.Name, member.Tags[RPCTagKey])
	if err != nil {
		ms.logError(err, "failed to join", member)
	}
}

func (ms *Membership) handleLeave(member serf.Member) {
	err := ms.handler.Leave(member.Name)
	if err != nil {
		ms.logError(err, "failed to leave", member)
	}
}

func (ms *Membership) isLocal(member serf.Member) bool {
	return ms.serf.LocalMember().Name == member.Name
}

func (ms *Membership) logError(err error, msg string, member serf.Member) {
	log := ms.logger.Error
	if errors.Is(err, raft.ErrNotLeader) {
		log = ms.logger.Debug
	}

	log(
		msg,
		zap.Error(err),
		zap.String("name", member.Name),
		zap.String("rpc_addr", member.Tags[RPCTagKey]),
	)
}
