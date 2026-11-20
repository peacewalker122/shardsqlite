package raftlog

import (
	"errors"
	"reflect"
	"testing"

	"shardsqlite/internal/ids"
	"shardsqlite/internal/wal"
)

func TestProperty_TXBatchBoundary(t *testing.T) {
	original := Batch{
		Group:     ids.Group(1),
		Shard:     ids.Shard(2),
		DB:        ids.DBName("main"),
		Gen:       ids.WALGeneration(3),
		PageSize:  4096,
		BaseFrame: 1,
		Header:    []byte{1, 2, 3},
		Frames: []wal.Frame{
			{PageNumber: 1, Page: []byte{1}},
			{PageNumber: 2, Page: []byte{2}},
			{PageNumber: 3, Page: []byte{3}},
			{PageNumber: 4, DatabaseSize: 4, Page: []byte{4}},
			{PageNumber: 5, Page: []byte{5}},
		},
		Commit: true,
	}

	parts, err := Chunk(original, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(parts) != 3 {
		t.Fatalf("part count: got %d want 3", len(parts))
	}
	wantLengths := []int{2, 2, 1}
	wantCommits := []bool{false, false, true}
	for index, part := range parts {
		if len(part.Frames) != wantLengths[index] {
			t.Fatalf("part %d length: got %d want %d", index, len(part.Frames), wantLengths[index])
		}
		if part.Commit != wantCommits[index] {
			t.Fatalf("part %d commit: got %v want %v", index, part.Commit, wantCommits[index])
		}
	}

	joined, err := Join(parts)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(joined, original) {
		t.Fatalf("round trip mismatch:\n got %+v\nwant %+v", joined, original)
	}

	encoded, err := Encode(original)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := Decode(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(decoded, original) {
		t.Fatalf("encode/decode mismatch:\n got %+v\nwant %+v", decoded, original)
	}

	bad := append([]Batch{}, parts...)
	bad[0].Commit = true
	if _, err := Join(bad); !errors.Is(err, ErrEarlyCommit) {
		t.Fatalf("early commit: got %v want %v", err, ErrEarlyCommit)
	}
}
