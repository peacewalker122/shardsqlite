package raftvfs

import (
	"encoding/binary"
	"errors"
	"fmt"
	"sync"

	"github.com/ncruces/go-sqlite3/util/vfsutil"
	"github.com/ncruces/go-sqlite3/vfs"

	"shardsqlite/internal/ids"
	"shardsqlite/internal/raftlog"
	"shardsqlite/internal/wal"
)

var (
	ErrNotLeader    = errors.New("raftvfs: not leader")
	ErrProposalFail = errors.New("raftvfs: proposal failed")
	ErrBadWALWrite  = errors.New("raftvfs: bad WAL write")
)

type Proposer interface {
	Propose(batch raftlog.Batch) error
	IsLeader() bool
}

type VFS struct {
	vfs.VFS
	proposer  Proposer
	group     ids.Group
	shard     ids.Shard
	db        ids.DBName
	maxFrames int
}

func Wrap(base vfs.VFS, proposer Proposer, group ids.Group, shard ids.Shard, db ids.DBName, maxFrames int) *VFS {
	if maxFrames <= 0 {
		maxFrames = 64
	}
	return &VFS{
		VFS:       base,
		proposer:  proposer,
		group:     group,
		shard:     shard,
		db:        db,
		maxFrames: maxFrames,
	}
}

func (v *VFS) Open(name string, flags vfs.OpenFlag) (vfs.File, vfs.OpenFlag, error) {
	if name == "" {
		return v.OpenFilename(nil, flags)
	}
	return v.openWith(vfsutil.WrapOpen(v.VFS, name, flags))
}

func (v *VFS) OpenFilename(name *vfs.Filename, flags vfs.OpenFlag) (vfs.File, vfs.OpenFlag, error) {
	return v.openWith(vfsutil.WrapOpenFilename(v.VFS, name, flags))
}

func (v *VFS) openWith(file vfs.File, outFlags vfs.OpenFlag, err error) (vfs.File, vfs.OpenFlag, error) {
	if err != nil {
		return nil, outFlags, err
	}
	if outFlags&vfs.OPEN_WAL == 0 {
		return file, outFlags, nil
	}
	return &walFile{File: file, owner: v}, outFlags, nil
}

type pendingChunk struct {
	offset int64
	data   []byte
}

type walFile struct {
	vfs.File
	owner      *VFS
	mu         sync.Mutex
	header     []byte
	pageSize   uint32
	generation ids.WALGeneration
	chunks     []pendingChunk
	frameBase  int64
	scanOffset int64
}

func (f *walFile) WriteAt(data []byte, offset int64) (int, error) {
	if offset == 0 && len(data) >= wal.HeaderSize {
		return f.writeHeader(data)
	}
	if err := f.ensureHeader(); err != nil {
		return 0, err
	}
	return f.appendFrames(data, offset)
}

func (f *walFile) writeHeader(data []byte) (int, error) {
	header, err := wal.ParseHeader(data[:wal.HeaderSize])
	if err != nil {
		return 0, err
	}
	f.mu.Lock()
	f.header = append([]byte(nil), data[:wal.HeaderSize]...)
	f.pageSize = header.PageSize
	f.generation = ids.WALGeneration(uint64(header.Salt1)<<32 | uint64(header.Salt2))
	f.chunks = nil
	f.frameBase = int64(wal.HeaderSize)
	f.scanOffset = int64(wal.HeaderSize)
	f.mu.Unlock()
	return f.File.WriteAt(data, 0)
}

func (f *walFile) ensureHeader() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.pageSize != 0 {
		return nil
	}
	raw := make([]byte, wal.HeaderSize)
	if _, err := f.File.ReadAt(raw, 0); err != nil {
		return err
	}
	header, err := wal.ParseHeader(raw)
	if err != nil {
		return err
	}
	f.header = raw
	f.pageSize = header.PageSize
	f.generation = ids.WALGeneration(uint64(header.Salt1)<<32 | uint64(header.Salt2))
	f.frameBase = int64(wal.HeaderSize)
	f.scanOffset = int64(wal.HeaderSize)
	return nil
}

func (f *walFile) appendFrames(data []byte, offset int64) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if offset < int64(wal.HeaderSize) {
		return 0, fmt.Errorf("%w: offset %d", ErrBadWALWrite, offset)
	}
	f.chunks = append(f.chunks, pendingChunk{offset: offset, data: append([]byte(nil), data...)})
	frameSize := int64(wal.FrameHeaderSize) + int64(f.pageSize)
	for {
		frameHeader := f.readPending(f.scanOffset, wal.FrameHeaderSize)
		if frameHeader == nil {
			break
		}
		if binary.BigEndian.Uint32(frameHeader[4:8]) == 0 {
			f.scanOffset += frameSize
			continue
		}
		if f.readPending(f.scanOffset, int(frameSize)) == nil {
			break
		}
		return len(data), f.commitLocked(f.scanOffset)
	}
	return len(data), nil
}

func (f *walFile) commitLocked(commitOffset int64) error {
	if !f.owner.proposer.IsLeader() {
		return ErrNotLeader
	}
	frameSize := int64(wal.FrameHeaderSize) + int64(f.pageSize)
	frameCount := (commitOffset-f.frameBase)/frameSize + 1
	baseFrame := uint32((f.frameBase-int64(wal.HeaderSize))/frameSize) + 1
	frames := make([]wal.Frame, 0, frameCount)
	for index := int64(0); index < frameCount; index++ {
		raw := f.readPending(f.frameBase+index*frameSize, int(frameSize))
		frame, err := wal.ParseFrame(raw, f.pageSize)
		if err != nil {
			return err
		}
		frames = append(frames, frame)
	}
	header := []byte(nil)
	if baseFrame == 1 {
		header = append([]byte(nil), f.header...)
	}
	batch := raftlog.Batch{
		Group:     f.owner.group,
		Shard:     f.owner.shard,
		DB:        f.owner.db,
		Gen:       f.generation,
		PageSize:  f.pageSize,
		BaseFrame: baseFrame,
		Header:    header,
		Frames:    frames,
		Commit:    true,
	}
	parts, err := raftlog.Chunk(batch, f.owner.maxFrames)
	if err != nil {
		return err
	}
	for _, part := range parts {
		if err := f.owner.proposer.Propose(part); err != nil {
			f.chunks = nil
			return fmt.Errorf("%w: %v", ErrProposalFail, err)
		}
	}
	f.chunks = nil
	f.frameBase = commitOffset + frameSize
	f.scanOffset = f.frameBase
	return nil
}

func (f *walFile) readPending(offset int64, size int) []byte {
	result := make([]byte, size)
	remaining := size
	position := offset
	for _, chunk := range f.chunks {
		chunkEnd := chunk.offset + int64(len(chunk.data))
		if position >= chunkEnd {
			continue
		}
		if position < chunk.offset {
			return nil
		}
		start := position - chunk.offset
		copied := copy(result[size-remaining:], chunk.data[start:])
		remaining -= copied
		position += int64(copied)
		if remaining == 0 {
			return result
		}
	}
	return nil
}

func (f *walFile) Unwrap() vfs.File {
	return f.File
}

func (f *walFile) SharedMemory() vfs.SharedMemory {
	return vfsutil.WrapSharedMemory(f.File)
}

func (f *walFile) LockState() vfs.LockLevel {
	return vfsutil.WrapLockState(f.File)
}

func (f *walFile) PersistWAL() bool {
	return vfsutil.WrapPersistWAL(f.File)
}

func (f *walFile) SetPersistWAL(keep bool) {
	vfsutil.WrapSetPersistWAL(f.File, keep)
}

func (f *walFile) PowersafeOverwrite() bool {
	return vfsutil.WrapPowersafeOverwrite(f.File)
}

func (f *walFile) SetPowersafeOverwrite(value bool) {
	vfsutil.WrapSetPowersafeOverwrite(f.File, value)
}

func (f *walFile) ChunkSize(size int) {
	vfsutil.WrapChunkSize(f.File, size)
}

func (f *walFile) SizeHint(size int64) error {
	return vfsutil.WrapSizeHint(f.File, size)
}

func (f *walFile) HasMoved() (bool, error) {
	return vfsutil.WrapHasMoved(f.File)
}

func (f *walFile) Overwrite() error {
	return vfsutil.WrapOverwrite(f.File)
}

func (f *walFile) SyncSuper(super string) error {
	return vfsutil.WrapSyncSuper(f.File, super)
}

func (f *walFile) CommitPhaseTwo() error {
	return vfsutil.WrapCommitPhaseTwo(f.File)
}

func (f *walFile) BeginAtomicWrite() error {
	return vfsutil.WrapBeginAtomicWrite(f.File)
}

func (f *walFile) CommitAtomicWrite() error {
	return vfsutil.WrapCommitAtomicWrite(f.File)
}

func (f *walFile) RollbackAtomicWrite() error {
	return vfsutil.WrapRollbackAtomicWrite(f.File)
}

func (f *walFile) CheckpointStart() {
	vfsutil.WrapCheckpointStart(f.File)
}

func (f *walFile) CheckpointDone() {
	vfsutil.WrapCheckpointDone(f.File)
}

func (f *walFile) BusyHandler(handler func() bool) {
	vfsutil.WrapBusyHandler(f.File, handler)
}

func (f *walFile) Pragma(name, value string) (string, error) {
	return vfsutil.WrapPragma(f.File, name, value)
}
