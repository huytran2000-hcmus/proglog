package log

import (
	"bytes"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"time"

	"github.com/hashicorp/raft"
)

const RaftRPC = 1

var _ raft.StreamLayer = (*StreamLayer)(nil)

type StreamLayer struct {
	ln              net.Listener
	serverTLSConfig *tls.Config
	peerTLSConfig   *tls.Config
}

func NewStreamLayer(ln net.Listener, serverTLSConfig, peerTLSConfig *tls.Config) *StreamLayer {
	return &StreamLayer{
		ln:              ln,
		serverTLSConfig: serverTLSConfig,
		peerTLSConfig:   peerTLSConfig,
	}
}

func (s *StreamLayer) Dial(addr raft.ServerAddress, timeout time.Duration) (net.Conn, error) {
	dialer := &net.Dialer{
		Timeout: timeout,
	}
	conn, err := dialer.Dial("tcp", string(addr))
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", addr, err)
	}

	_, err = conn.Write([]byte{byte(RaftRPC)})
	if err != nil {
		return nil, fmt.Errorf("write raft rpc byte: %w", err)
	}

	if s.peerTLSConfig != nil {
		conn = tls.Client(conn, s.peerTLSConfig)
	}

	return conn, err
}

func (s *StreamLayer) Accept() (net.Conn, error) {
	conn, err := s.ln.Accept()
	if err != nil {
		return nil, fmt.Errorf("accept conn: %w", err)
	}

	b := make([]byte, 1)
	_, err = conn.Read(b)
	if err != nil {
		return nil, fmt.Errorf("write raft rpc byte: %w", err)
	}

	if bytes.Compare(b, []byte{byte(RaftRPC)}) != 0 {
		return nil, errors.New("not a raft rpc")
	}

	if s.serverTLSConfig != nil {
		conn = tls.Server(conn, s.serverTLSConfig)
	}

	return conn, nil
}

func (s *StreamLayer) Close() error {
	return s.ln.Close()
}

func (s *StreamLayer) Addr() net.Addr {
	return s.ln.Addr()
}
