package applied

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"shardsqlite/internal/ids"
)

var (
	ErrWALMismatch   = errors.New("applied: WAL generation mismatch")
	ErrNonContiguous = errors.New("applied: non-contiguous raft index")
	ErrStale         = errors.New("applied: stale raft index")
)

type Marker struct {
	Index ids.RaftIndex     `json:"index"`
	Gen   ids.WALGeneration `json:"generation"`
}

type Store struct {
	path    string
	current Marker
}

func Open(path string) (*Store, error) {
	store := &Store{path: path}
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return store, nil
	}
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(data, &store.current); err != nil {
		return nil, fmt.Errorf("applied: decode %s: %w", path, err)
	}
	return store, nil
}

func (s *Store) Current() Marker {
	return s.current
}

func (s *Store) ShouldApply(index ids.RaftIndex, gen ids.WALGeneration) (bool, error) {
	return s.shouldApply(index, gen, false)
}

func (s *Store) ShouldApplyReset(index ids.RaftIndex, gen ids.WALGeneration) (bool, error) {
	return s.shouldApply(index, gen, true)
}

func (s *Store) shouldApply(index ids.RaftIndex, gen ids.WALGeneration, allowGenerationChange bool) (bool, error) {
	if s.current.Index != 0 && s.current.Gen != 0 && s.current.Gen != gen && !allowGenerationChange {
		return false, fmt.Errorf("%w: stored %d got %d", ErrWALMismatch, s.current.Gen, gen)
	}
	if index <= s.current.Index {
		return false, nil
	}
	if s.current.Index == 0 {
		return true, nil
	}
	if index != s.current.Index+1 {
		return false, fmt.Errorf("%w: stored %d got %d", ErrNonContiguous, s.current.Index, index)
	}
	return true, nil
}

func (s *Store) Advance(index ids.RaftIndex, gen ids.WALGeneration) error {
	return s.advance(index, gen, false)
}

func (s *Store) AdvanceReset(index ids.RaftIndex, gen ids.WALGeneration) error {
	return s.advance(index, gen, true)
}

func (s *Store) advance(index ids.RaftIndex, gen ids.WALGeneration, allowGenerationChange bool) error {
	apply, err := s.shouldApply(index, gen, allowGenerationChange)
	if err != nil {
		return err
	}
	if !apply {
		return fmt.Errorf("%w: stored %d got %d", ErrStale, s.current.Index, index)
	}
	return s.write(Marker{Index: index, Gen: gen})
}

func (s *Store) Reset(marker Marker) error {
	return s.write(marker)
}

func (s *Store) write(next Marker) error {
	data, err := json.Marshal(next)
	if err != nil {
		return err
	}
	directory := filepath.Dir(s.path)
	temporary, err := os.CreateTemp(directory, ".applied-*")
	if err != nil {
		return err
	}
	defer os.Remove(temporary.Name())
	if _, err := temporary.Write(data); err != nil {
		temporary.Close()
		return err
	}
	if err := temporary.Sync(); err != nil {
		temporary.Close()
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	if err := os.Rename(temporary.Name(), s.path); err != nil {
		return err
	}
	directoryFile, err := os.Open(directory)
	if err != nil {
		return err
	}
	if err := directoryFile.Sync(); err != nil {
		directoryFile.Close()
		return err
	}
	if err := directoryFile.Close(); err != nil {
		return err
	}
	s.current = next
	return nil
}
