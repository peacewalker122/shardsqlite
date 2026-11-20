package replay

import (
	"errors"
	"fmt"
	"os"
	"sync"

	"shardsqlite/internal/raftlog"
	"shardsqlite/internal/wal"
)

var ErrInvalidPageSize = errors.New("replay: invalid page size")

type WAL struct {
	path     string
	shm      bool
	isLeader func() bool
	mu       sync.Mutex
}

func NewWAL(path string) *WAL {
	return &WAL{path: path}
}

func (w *WAL) WithSHM(isLeader func() bool) *WAL {
	w.shm = true
	w.isLeader = isLeader
	return w
}

func (w *WAL) Apply(batch raftlog.Batch) error {
	if batch.PageSize < 512 || batch.PageSize > 65536 || batch.PageSize&(batch.PageSize-1) != 0 {
		return fmt.Errorf("%w: got %d", ErrInvalidPageSize, batch.PageSize)
	}
	w.mu.Lock()
	defer w.mu.Unlock()

	file, err := os.OpenFile(w.path, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return err
	}
	defer file.Close()

	if len(batch.Header) >= wal.HeaderSize {
		if _, err := file.WriteAt(batch.Header[:wal.HeaderSize], 0); err != nil {
			return err
		}
	}
	frameSize := int64(wal.FrameHeaderSize) + int64(batch.PageSize)
	for index, frame := range batch.Frames {
		raw, err := wal.EncodeFrame(frame, batch.PageSize)
		if err != nil {
			return err
		}
		position := int64(batch.BaseFrame) - 1 + int64(index)
		offset := int64(wal.HeaderSize) + position*frameSize
		if _, err := file.WriteAt(raw, offset); err != nil {
			return err
		}
	}
	if err := file.Sync(); err != nil {
		return err
	}
	if !w.shm || (w.isLeader != nil && w.isLeader()) {
		return nil
	}
	header := batch.Header
	if len(header) < wal.HeaderSize {
		header = make([]byte, wal.HeaderSize)
		if _, err := file.ReadAt(header, 0); err != nil {
			return err
		}
	}
	return w.updateSHM(batch, header[:wal.HeaderSize])
}
