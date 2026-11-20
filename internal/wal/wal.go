package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
)

const (
	HeaderSize      = 32
	FrameHeaderSize = 24
	walVersion      = 3007000
	magicLittle     = 0x377f0682
	magicBig        = 0x377f0683
)

var (
	ErrShortHeader  = errors.New("wal: short header")
	ErrBadMagic     = errors.New("wal: bad magic")
	ErrBadVersion   = errors.New("wal: bad version")
	ErrBadPageSize  = errors.New("wal: bad page size")
	ErrShortFrame   = errors.New("wal: short frame")
	ErrSaltMismatch = errors.New("wal: salt mismatch")
	ErrChecksum     = errors.New("wal: checksum mismatch")
)

type Header struct {
	PageSize          uint32
	Salt1             uint32
	Salt2             uint32
	BigEndianChecksum bool
}

type Frame struct {
	PageNumber   uint32
	DatabaseSize uint32
	Salt1        uint32
	Salt2        uint32
	Checksum1    uint32
	Checksum2    uint32
	Page         []byte
}

type Checksum struct {
	S0 uint32
	S1 uint32
}

type Chain struct {
	Frames  []Frame
	MXFrame int
}

type accumulator struct {
	order binary.ByteOrder
	sum   Checksum
}

func ParseHeader(data []byte) (Header, error) {
	if len(data) < HeaderSize {
		return Header{}, ErrShortHeader
	}
	magic := binary.BigEndian.Uint32(data[0:4])
	bigEndian := false
	switch magic {
	case magicLittle:
	case magicBig:
		bigEndian = true
	default:
		return Header{}, ErrBadMagic
	}
	if version := binary.BigEndian.Uint32(data[4:8]); version != walVersion {
		return Header{}, fmt.Errorf("%w: got %d", ErrBadVersion, version)
	}
	pageSize := binary.BigEndian.Uint32(data[8:12])
	if pageSize < 512 || pageSize > 65536 || pageSize&(pageSize-1) != 0 {
		return Header{}, fmt.Errorf("%w: got %d", ErrBadPageSize, pageSize)
	}
	return Header{
		PageSize:          pageSize,
		Salt1:             binary.BigEndian.Uint32(data[16:20]),
		Salt2:             binary.BigEndian.Uint32(data[20:24]),
		BigEndianChecksum: bigEndian,
	}, nil
}

func BuildHeader(pageSize, salt1, salt2 uint32, bigEndian bool) []byte {
	header := make([]byte, HeaderSize)
	magic := uint32(magicLittle)
	if bigEndian {
		magic = magicBig
	}
	binary.BigEndian.PutUint32(header[0:4], magic)
	binary.BigEndian.PutUint32(header[4:8], walVersion)
	binary.BigEndian.PutUint32(header[8:12], pageSize)
	binary.BigEndian.PutUint32(header[12:16], 0)
	binary.BigEndian.PutUint32(header[16:20], salt1)
	binary.BigEndian.PutUint32(header[20:24], salt2)
	return header
}

func InitialChecksum(header []byte, bigEndian bool) (Checksum, error) {
	if len(header) < HeaderSize {
		return Checksum{}, ErrShortHeader
	}
	return Accumulate(header[:24], bigEndian, Checksum{}), nil
}

func Accumulate(data []byte, bigEndian bool, initial Checksum) Checksum {
	acc := newAccumulator(bigEndian)
	acc.sum = initial
	acc.update(data)
	return acc.sum
}

func ParseFrame(data []byte, pageSize uint32) (Frame, error) {
	required := int(pageSize) + FrameHeaderSize
	if len(data) < required {
		return Frame{}, fmt.Errorf("%w: got %d want %d", ErrShortFrame, len(data), required)
	}
	frame := Frame{
		PageNumber:   binary.BigEndian.Uint32(data[0:4]),
		DatabaseSize: binary.BigEndian.Uint32(data[4:8]),
		Salt1:        binary.BigEndian.Uint32(data[8:12]),
		Salt2:        binary.BigEndian.Uint32(data[12:16]),
		Checksum1:    binary.BigEndian.Uint32(data[16:20]),
		Checksum2:    binary.BigEndian.Uint32(data[20:24]),
		Page:         append([]byte(nil), data[FrameHeaderSize:required]...),
	}
	return frame, nil
}

func BuildFrame(pageNumber, databaseSize, salt1, salt2 uint32, page []byte, header Header, previous Checksum) ([]byte, error) {
	if len(page) != int(header.PageSize) {
		return nil, fmt.Errorf("%w: page got %d want %d", ErrBadPageSize, len(page), header.PageSize)
	}
	frame := make([]byte, FrameHeaderSize+len(page))
	binary.BigEndian.PutUint32(frame[0:4], pageNumber)
	binary.BigEndian.PutUint32(frame[4:8], databaseSize)
	binary.BigEndian.PutUint32(frame[8:12], salt1)
	binary.BigEndian.PutUint32(frame[12:16], salt2)
	copy(frame[FrameHeaderSize:], page)
	acc := newAccumulator(header.BigEndianChecksum)
	acc.sum = previous
	acc.update(frame[:8])
	acc.update(page)
	binary.BigEndian.PutUint32(frame[16:20], acc.sum.S0)
	binary.BigEndian.PutUint32(frame[20:24], acc.sum.S1)
	return frame, nil
}

func EncodeFrame(frame Frame, pageSize uint32) ([]byte, error) {
	if len(frame.Page) != int(pageSize) {
		return nil, fmt.Errorf("%w: page got %d want %d", ErrBadPageSize, len(frame.Page), pageSize)
	}
	raw := make([]byte, FrameHeaderSize+len(frame.Page))
	binary.BigEndian.PutUint32(raw[0:4], frame.PageNumber)
	binary.BigEndian.PutUint32(raw[4:8], frame.DatabaseSize)
	binary.BigEndian.PutUint32(raw[8:12], frame.Salt1)
	binary.BigEndian.PutUint32(raw[12:16], frame.Salt2)
	binary.BigEndian.PutUint32(raw[16:20], frame.Checksum1)
	binary.BigEndian.PutUint32(raw[20:24], frame.Checksum2)
	copy(raw[FrameHeaderSize:], frame.Page)
	return raw, nil
}

func FrameChecksum(raw []byte, pageSize uint32, previous Checksum, bigEndian bool) (Checksum, error) {
	required := FrameHeaderSize + int(pageSize)
	if len(raw) < required {
		return Checksum{}, fmt.Errorf("%w: got %d want %d", ErrShortFrame, len(raw), required)
	}
	acc := newAccumulator(bigEndian)
	acc.sum = previous
	acc.update(raw[:8])
	acc.update(raw[FrameHeaderSize:required])
	return acc.sum, nil
}

func VerifyChain(headerBytes []byte, rawFrames [][]byte) (Chain, error) {
	header, err := ParseHeader(headerBytes)
	if err != nil {
		return Chain{}, err
	}
	sum, err := InitialChecksum(headerBytes, header.BigEndianChecksum)
	if err != nil {
		return Chain{}, err
	}
	chain := Chain{}
	for position, raw := range rawFrames {
		frame, err := ParseFrame(raw, header.PageSize)
		if err != nil {
			return Chain{}, fmt.Errorf("frame %d: %w", position+1, err)
		}
		if frame.Salt1 != header.Salt1 || frame.Salt2 != header.Salt2 {
			return Chain{}, fmt.Errorf("frame %d: %w", position+1, ErrSaltMismatch)
		}
		acc := newAccumulator(header.BigEndianChecksum)
		acc.sum = sum
		acc.update(raw[:8])
		acc.update(raw[FrameHeaderSize : FrameHeaderSize+int(header.PageSize)])
		if frame.Checksum1 != acc.sum.S0 || frame.Checksum2 != acc.sum.S1 {
			return Chain{}, fmt.Errorf("frame %d: %w", position+1, ErrChecksum)
		}
		sum = acc.sum
		chain.Frames = append(chain.Frames, frame)
		if frame.DatabaseSize != 0 {
			chain.MXFrame = position + 1
		}
	}
	return chain, nil
}

func LastCommitIndex(frames []Frame) int {
	last := 0
	for index, frame := range frames {
		if frame.DatabaseSize != 0 {
			last = index + 1
		}
	}
	return last
}

func newAccumulator(bigEndian bool) accumulator {
	order := binary.ByteOrder(binary.LittleEndian)
	if bigEndian {
		order = binary.BigEndian
	}
	return accumulator{order: order}
}

func (a *accumulator) update(data []byte) {
	for offset := 0; offset+8 <= len(data); offset += 8 {
		x0 := a.order.Uint32(data[offset : offset+4])
		x1 := a.order.Uint32(data[offset+4 : offset+8])
		a.sum.S0 += x0 + a.sum.S1
		a.sum.S1 += x1 + a.sum.S0
	}
}
