package applied

import (
	"errors"
	"path/filepath"
	"testing"

	"shardsqlite/internal/ids"
)

func TestProperty_AppliedMonotonic(t *testing.T) {
	path := filepath.Join(t.TempDir(), "applied.json")
	store, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	generation := ids.WALGeneration(7)

	apply, err := store.ShouldApply(1, generation)
	if err != nil || !apply {
		t.Fatalf("first entry rejected: apply=%v err=%v", apply, err)
	}
	if err := store.Advance(1, generation); err != nil {
		t.Fatal(err)
	}
	if err := store.Advance(1, generation); !errors.Is(err, ErrStale) {
		t.Fatalf("stale entry: got %v want %v", err, ErrStale)
	}
	if err := store.Advance(2, generation); err != nil {
		t.Fatal(err)
	}
	if err := store.Advance(4, generation); !errors.Is(err, ErrNonContiguous) {
		t.Fatalf("gap: got %v want %v", err, ErrNonContiguous)
	}
	if err := store.Advance(3, ids.WALGeneration(8)); !errors.Is(err, ErrWALMismatch) {
		t.Fatalf("generation change: got %v want %v", err, ErrWALMismatch)
	}
	apply, err = store.ShouldApplyReset(3, ids.WALGeneration(8))
	if err != nil || !apply {
		t.Fatalf("WAL reset rejected: apply=%v err=%v", apply, err)
	}
	if err := store.AdvanceReset(3, ids.WALGeneration(8)); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if reopened.Current() != (Marker{Index: 3, Gen: ids.WALGeneration(8)}) {
		t.Fatalf("persisted marker: got %+v", reopened.Current())
	}
	apply, err = reopened.ShouldApply(4, ids.WALGeneration(8))
	if err != nil || !apply {
		t.Fatalf("next entry rejected after reopen: apply=%v err=%v", apply, err)
	}

	baselinePath := filepath.Join(t.TempDir(), "baseline.json")
	baselineStore, err := Open(baselinePath)
	if err != nil {
		t.Fatal(err)
	}
	apply, err = baselineStore.ShouldApply(3, generation)
	if err != nil || !apply {
		t.Fatalf("raft baseline rejected: apply=%v err=%v", apply, err)
	}
	if err := baselineStore.Advance(3, generation); err != nil {
		t.Fatal(err)
	}
	if err := baselineStore.Advance(5, generation); !errors.Is(err, ErrNonContiguous) {
		t.Fatalf("gap after baseline: got %v want %v", err, ErrNonContiguous)
	}
}
