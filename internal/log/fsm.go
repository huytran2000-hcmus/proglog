package log

import (
	"bytes"
	"errors"
	"fmt"
	"io"

	"github.com/hashicorp/raft"
	"google.golang.org/protobuf/proto"

	api "github.com/huytran2000-hcmus/proglog/api/v1"
)

type RequestType uint8

const (
	AppendRequestType RequestType = 0
)

var _ raft.FSM = (*fsm)(nil)

type fsm struct {
	log *Log
}

type snapshot struct {
	reader io.Reader
}

func (l *fsm) Apply(record *raft.Log) interface{} {
	buf := record.Data
	reqType := RequestType(buf[0])

	switch reqType {
	case AppendRequestType:
		return l.applyAppend(buf[1:])
	}

	return nil
}

func (l *fsm) Snapshot() (raft.FSMSnapshot, error) {
	r := l.log.Reader()
	return &snapshot{reader: r}, nil
}

func (l *fsm) Restore(r io.ReadCloser) error {
	bLen := make([]byte, lenWidth)
	var buf bytes.Buffer
	for i := 0; ; i++ {
		_, err := io.ReadFull(r, bLen)
		if errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			return fmt.Errorf("read log length: %w", err)
		}

		size := int64(enc.Uint64(bLen))
		_, err = io.CopyN(&buf, r, size)
		if err != nil {
			return fmt.Errorf("read log message: %w", err)
		}

		record := &api.Record{}
		err = proto.Unmarshal(buf.Bytes(), record)
		if err != nil {
			return fmt.Errorf("unmarshal protobuf message: %w", err)
		}

		if i == 0 {
			l.log.Config.Segment.InitialOffset = record.Offset
			err = l.log.Reset()
			if err != nil {
				return fmt.Errorf("reset log: %w", err)
			}
		}

		_, err = l.log.Append(record)
		if err != nil {
			return fmt.Errorf("append to log: %w", err)
		}

		buf.Reset()
	}
	return nil
}

func (l *fsm) applyAppend(b []byte) interface{} {
	var req api.ProduceRequest
	err := proto.Unmarshal(b, &req)
	if err != nil {
		return fmt.Errorf("unmarshal protobuf: %w", err)
	}

	offset, err := l.log.Append(req.Record)
	if err != nil {
		return fmt.Errorf("append to log: %w", err)
	}

	return &api.ProduceResponse{Offset: offset}
}

func (s *snapshot) Persist(sink raft.SnapshotSink) error {
	_, err := io.Copy(sink, s.reader)
	if err != nil {
		_ = sink.Cancel()
		return err
	}

	return sink.Close()
}

func (s *snapshot) Release() {}
