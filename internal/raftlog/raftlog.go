package raftlog

import (
	"encoding/json"
	"errors"
	"fmt"

	"shardsqlite/internal/ids"
	"shardsqlite/internal/wal"
)

var (
	ErrEmptyChunkSize = errors.New("raftlog: chunk size must be positive")
	ErrEmptyParts     = errors.New("raftlog: no parts to join")
	ErrPartMismatch   = errors.New("raftlog: part belongs to another batch")
	ErrEarlyCommit    = errors.New("raftlog: commit flag before final part")
)

type Batch struct {
	Group     ids.Group         `json:"group"`
	Shard     ids.Shard         `json:"shard"`
	DB        ids.DBName        `json:"db"`
	Gen       ids.WALGeneration `json:"generation"`
	PageSize  uint32            `json:"page_size"`
	BaseFrame uint32            `json:"base_frame"`
	Header    []byte            `json:"header,omitempty"`
	Frames    []wal.Frame       `json:"frames"`
	Commit    bool              `json:"commit"`
}

func Encode(batch Batch) ([]byte, error) {
	return json.Marshal(batch)
}

func Decode(data []byte) (Batch, error) {
	var batch Batch
	if err := json.Unmarshal(data, &batch); err != nil {
		return Batch{}, err
	}
	return batch, nil
}

func Chunk(batch Batch, maxFrames int) ([]Batch, error) {
	if maxFrames <= 0 {
		return nil, ErrEmptyChunkSize
	}
	if len(batch.Frames) == 0 {
		return []Batch{{
			Group:     batch.Group,
			Shard:     batch.Shard,
			DB:        batch.DB,
			Gen:       batch.Gen,
			PageSize:  batch.PageSize,
			BaseFrame: batch.BaseFrame,
			Header:    batch.Header,
			Frames:    []wal.Frame{},
			Commit:    batch.Commit,
		}}, nil
	}
	parts := make([]Batch, 0, (len(batch.Frames)+maxFrames-1)/maxFrames)
	for start := 0; start < len(batch.Frames); start += maxFrames {
		end := start + maxFrames
		if end > len(batch.Frames) {
			end = len(batch.Frames)
		}
		part := Batch{
			Group:     batch.Group,
			Shard:     batch.Shard,
			DB:        batch.DB,
			Gen:       batch.Gen,
			PageSize:  batch.PageSize,
			BaseFrame: batch.BaseFrame + uint32(start),
			Frames:    batch.Frames[start:end],
			Commit:    false,
		}
		if start == 0 {
			part.Header = batch.Header
		}
		if end == len(batch.Frames) {
			part.Commit = batch.Commit
		}
		parts = append(parts, part)
	}
	return parts, nil
}

func Join(parts []Batch) (Batch, error) {
	if len(parts) == 0 {
		return Batch{}, ErrEmptyParts
	}
	first := parts[0]
	joined := Batch{
		Group:     first.Group,
		Shard:     first.Shard,
		DB:        first.DB,
		Gen:       first.Gen,
		PageSize:  first.PageSize,
		BaseFrame: first.BaseFrame,
		Header:    first.Header,
		Frames:    []wal.Frame{},
	}
	nextFrame := first.BaseFrame
	for index, part := range parts {
		if part.Group != first.Group || part.Shard != first.Shard || part.DB != first.DB || part.Gen != first.Gen || part.PageSize != first.PageSize {
			return Batch{}, fmt.Errorf("%w: part %d", ErrPartMismatch, index)
		}
		if part.BaseFrame != nextFrame {
			return Batch{}, fmt.Errorf("%w: part %d frame %d", ErrPartMismatch, index, part.BaseFrame)
		}
		if index != len(parts)-1 && part.Commit {
			return Batch{}, fmt.Errorf("%w: part %d", ErrEarlyCommit, index)
		}
		joined.Frames = append(joined.Frames, part.Frames...)
		nextFrame += uint32(len(part.Frames))
	}
	joined.Commit = parts[len(parts)-1].Commit
	return joined, nil
}
