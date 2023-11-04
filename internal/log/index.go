package log

import (
	"fmt"
	"io"
	"os"

	"github.com/tysonmote/gommap"
)

var (
	offWidth   uint64 = 4
	posWidth   uint64 = 8
	entryWidth uint64 = offWidth * posWidth
)

type index struct {
	file *os.File
	mmap gommap.MMap
	size uint64
}

func newIndex(f *os.File, c Config) (*index, error) {
	fi, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("get index file info: %w", err)
	}

	size := uint64(fi.Size())
	err = os.Truncate(f.Name(), int64(c.Segment.MaxIndexBytes))
	if err != nil {
		return nil, fmt.Errorf("grow index file to %d bytes: %w", c.Segment.MaxIndexBytes, err)
	}

	mmap, err := gommap.Map(f.Fd(), gommap.PROT_READ|gommap.PROT_WRITE, gommap.MAP_SHARED)
	if err != nil {
		return nil, err
	}

	return &index{
		file: f,
		mmap: mmap,
		size: size,
	}, nil
}

func (i *index) Read(in int64) (out uint32, pos uint64, err error) {
	if i.size == 0 {
		return 0, 0, io.EOF
	}

	if in == -1 {
		out = uint32((i.size / entryWidth) - 1)
	} else {
		out = uint32(in)
	}

	pos = uint64(out) * entryWidth
	if i.size < pos+entryWidth {
		return 0, 0, io.EOF
	}

	out = enc.Uint32(i.mmap[pos : pos+offWidth])
	pos = enc.Uint64(i.mmap[pos+offWidth : pos+entryWidth])

	return out, pos, nil
}

func (i *index) Write(off uint32, pos uint64) error {
	if uint64(len(i.mmap)) < i.size+entryWidth {
		return io.EOF
	}

	enc.PutUint32(i.mmap[i.size:i.size+offWidth], off)
	enc.PutUint64(i.mmap[i.size+offWidth:i.size+entryWidth], pos)
	i.size += entryWidth

	return nil
}

func (i *index) Name() string {
	return i.file.Name()
}

func (i *index) Close() error {
	err := i.mmap.Sync(gommap.MS_SYNC)
	if err != nil {
		return fmt.Errorf("sync index memory-mapped file to disk")
	}

	err = i.file.Sync()
	if err != nil {
		return fmt.Errorf("sync index file to disk")
	}

	err = i.file.Truncate(int64(i.size))
	if err != nil {
		return fmt.Errorf("set the file size to actual logs size: %w", err)
	}

	err = i.file.Close()
	if err != nil {
		return fmt.Errorf("close index file: %w", err)
	}

	return nil
}

func (s *index) Remove() error {
	err := os.Remove(s.file.Name())
	if err != nil {
		return fmt.Errorf("remove index file: %w", err)
	}

	return nil
}
