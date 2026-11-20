package replay

import (
	"encoding/binary"
	"errors"
	"io"
	"os"
	"strings"

	"golang.org/x/sys/unix"

	"shardsqlite/internal/raftlog"
	"shardsqlite/internal/wal"
)

const (
	walIndexVersion   = 3007000
	walIndexPageBytes = 32768
	hashPageOnePages  = 4062
	hashPagePages     = 4096
	hashSlots         = 8192
	shmWriteLockByte  = 120
)

func (w *WAL) updateSHM(batch raftlog.Batch, walHeader []byte) error {
	if len(batch.Frames) == 0 {
		return nil
	}
	header, err := wal.ParseHeader(walHeader)
	if err != nil {
		return err
	}
	lastFrame := batch.BaseFrame + uint32(len(batch.Frames)) - 1
	requiredBytes := (walFramePage(lastFrame) + 1) * walIndexPageBytes

	shmPath := strings.TrimSuffix(w.path, "-wal") + "-shm"
	file, err := os.OpenFile(shmPath, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return err
	}
	defer file.Close()
	if err := lockShm(file, unix.F_WRLCK); err != nil {
		return err
	}
	defer lockShm(file, unix.F_UNLCK)

	info, err := file.Stat()
	if err != nil {
		return err
	}
	size := requiredBytes
	if info.Size() > int64(size) {
		size = int(info.Size())
	}
	data := make([]byte, size)
	if _, err := file.ReadAt(data, 0); err != nil && !errors.Is(err, io.EOF) {
		return err
	}
	if size > requiredBytes {
		data = data[:size]
	}

	if !shmHeaderValid(data) {
		clear(data[:136])
		binary.NativeEndian.PutUint32(data[0:4], walIndexVersion)
		data[12] = 1
		if header.BigEndianChecksum {
			data[13] = 1
		}
		pageSizeField := uint16(batch.PageSize)
		if batch.PageSize == 65536 {
			pageSizeField = 1
		}
		binary.NativeEndian.PutUint16(data[14:16], pageSizeField)
		sum, err := wal.InitialChecksum(walHeader, header.BigEndianChecksum)
		if err != nil {
			return err
		}
		binary.NativeEndian.PutUint32(data[24:28], sum.S0)
		binary.NativeEndian.PutUint32(data[28:32], sum.S1)
		binary.BigEndian.PutUint32(data[32:36], header.Salt1)
		binary.BigEndian.PutUint32(data[36:40], header.Salt2)
	}

	initialChecksum, err := wal.InitialChecksum(walHeader, header.BigEndianChecksum)
	if err != nil {
		return err
	}
	frameChecksum := wal.Checksum{
		S0: binary.NativeEndian.Uint32(data[24:28]),
		S1: binary.NativeEndian.Uint32(data[28:32]),
	}
	if binary.NativeEndian.Uint32(data[16:20]) == 0 && frameChecksum == (wal.Checksum{}) {
		frameChecksum = initialChecksum
	}
	for index, frame := range batch.Frames {
		frameIndex := batch.BaseFrame + uint32(index)
		appendShmIndex(data, frameIndex, frame.PageNumber)
		raw, err := wal.EncodeFrame(frame, batch.PageSize)
		if err != nil {
			return err
		}
		frameChecksum, err = wal.FrameChecksum(raw, batch.PageSize, frameChecksum, header.BigEndianChecksum)
		if err != nil {
			return err
		}
	}

	mxFrame := binary.NativeEndian.Uint32(data[16:20])
	if lastFrame > mxFrame {
		mxFrame = lastFrame
	}
	nPage := binary.NativeEndian.Uint32(data[20:24])
	for index := len(batch.Frames) - 1; index >= 0; index-- {
		if batch.Frames[index].DatabaseSize != 0 {
			nPage = batch.Frames[index].DatabaseSize
			break
		}
	}
	iChange := binary.NativeEndian.Uint32(data[8:12]) + 1

	pageSizeField := uint16(batch.PageSize)
	if batch.PageSize == 65536 {
		pageSizeField = 1
	}
	hdr := make([]byte, 48)
	binary.NativeEndian.PutUint32(hdr[0:4], walIndexVersion)
	binary.NativeEndian.PutUint32(hdr[8:12], iChange)
	hdr[12] = 1
	if header.BigEndianChecksum {
		hdr[13] = 1
	}
	binary.NativeEndian.PutUint16(hdr[14:16], pageSizeField)
	binary.NativeEndian.PutUint32(hdr[16:20], mxFrame)
	binary.NativeEndian.PutUint32(hdr[20:24], nPage)
	binary.NativeEndian.PutUint32(hdr[24:28], frameChecksum.S0)
	binary.NativeEndian.PutUint32(hdr[28:32], frameChecksum.S1)
	copy(hdr[32:40], walHeader[16:24])
	checksum := wal.Accumulate(hdr[:40], false, wal.Checksum{})
	binary.NativeEndian.PutUint32(hdr[40:44], checksum.S0)
	binary.NativeEndian.PutUint32(hdr[44:48], checksum.S1)
	copy(data[0:48], hdr)
	copy(data[48:96], hdr)

	if err := file.Truncate(int64(len(data))); err != nil {
		return err
	}
	if _, err := file.WriteAt(data, 0); err != nil {
		return err
	}
	return file.Sync()
}

func shmHeaderValid(data []byte) bool {
	if len(data) < 48 || binary.NativeEndian.Uint32(data[0:4]) != walIndexVersion || data[12] != 1 {
		return false
	}
	checksum := wal.Accumulate(data[:40], false, wal.Checksum{})
	return binary.NativeEndian.Uint32(data[40:44]) == checksum.S0 && binary.NativeEndian.Uint32(data[44:48]) == checksum.S1
}

func walFramePage(frame uint32) int {
	return int((frame + hashPagePages - hashPageOnePages - 1) / hashPagePages)
}

func appendShmIndex(data []byte, frameIndex uint32, pageNumber uint32) {
	page := walFramePage(frameIndex)
	pageBase := page * walIndexPageBytes
	iZero := uint32(0)
	if page > 0 {
		iZero = hashPageOnePages + uint32(page-1)*hashPagePages
	}
	index := frameIndex - iZero

	pageMapStart := pageBase
	pageMapLength := hashPagePages
	if page == 0 {
		pageMapStart += 136
		pageMapLength = hashPageOnePages
	}
	hashStart := pageBase + 16384
	if index == 1 {
		clear(data[pageMapStart : pageMapStart+pageMapLength*4])
		clear(data[hashStart : hashStart+hashSlots*2])
	}
	mapOffset := pageMapStart + int(index-1)*4
	binary.NativeEndian.PutUint32(data[mapOffset:mapOffset+4], pageNumber)

	slot := int((uint64(pageNumber) * 383) % hashSlots)
	for binary.NativeEndian.Uint16(data[hashStart+slot*2:hashStart+slot*2+2]) != 0 {
		slot = (slot + 1) & (hashSlots - 1)
	}
	binary.NativeEndian.PutUint16(data[hashStart+slot*2:hashStart+slot*2+2], uint16(index))
}

func lockShm(file *os.File, lockType int16) error {
	lock := unix.Flock_t{
		Type:   lockType,
		Whence: 0,
		Start:  shmWriteLockByte,
		Len:    1,
	}
	command := unix.F_SETLKW
	if lockType == unix.F_UNLCK {
		command = unix.F_SETLK
	}
	return unix.FcntlFlock(file.Fd(), command, &lock)
}
