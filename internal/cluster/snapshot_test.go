package cluster

import (
	"bytes"
	"database/sql"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"

	_ "github.com/ncruces/go-sqlite3/driver"

	"shardsqlite/internal/applied"
	"shardsqlite/internal/ids"
	"shardsqlite/internal/replay"
)

type memorySink struct {
	bytes.Buffer
}

func (s *memorySink) Close() error {
	return nil
}

func (s *memorySink) ID() string {
	return "snapshot-test"
}

func (s *memorySink) Cancel() error {
	return nil
}

func TestProperty_SnapshotRestoreRoundTrip(t *testing.T) {
	directory := t.TempDir()
	sourcePath := filepath.Join(directory, "source.db")
	source, err := sql.Open("sqlite3", fmt.Sprintf("file:%s?_pragma=journal_mode(WAL)", sourcePath))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := source.Exec("CREATE TABLE items (id INTEGER PRIMARY KEY, value TEXT)"); err != nil {
		t.Fatal(err)
	}
	if _, err := source.Exec("INSERT INTO items (id, value) VALUES (1, 'snapshot')"); err != nil {
		t.Fatal(err)
	}
	if err := source.Close(); err != nil {
		t.Fatal(err)
	}

	databaseBytes, err := os.ReadFile(sourcePath)
	if err != nil {
		t.Fatal(err)
	}
	targetPath := filepath.Join(directory, "target.db")
	if err := os.WriteFile(targetPath, databaseBytes, 0666); err != nil {
		t.Fatal(err)
	}
	markerPath := filepath.Join(directory, "applied.json")
	marker, err := applied.Open(markerPath)
	if err != nil {
		t.Fatal(err)
	}
	generation := ids.WALGeneration(11)
	if err := marker.Advance(7, generation); err != nil {
		t.Fatal(err)
	}
	fsm := NewFSM(replay.NewWAL(targetPath+"-wal"), marker, targetPath, "", directory)
	snapshot, err := fsm.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	sink := &memorySink{}
	if err := snapshot.Persist(sink); err != nil {
		t.Fatal(err)
	}

	restorePath := filepath.Join(directory, "restored.db")
	restoreMarker, err := applied.Open(filepath.Join(directory, "restored-applied.json"))
	if err != nil {
		t.Fatal(err)
	}
	restoreFSM := NewFSM(replay.NewWAL(restorePath+"-wal"), restoreMarker, restorePath, "", directory)
	if err := restoreFSM.Restore(io.NopCloser(bytes.NewReader(sink.Bytes()))); err != nil {
		t.Fatal(err)
	}
	if restoreMarker.Current() != (applied.Marker{Index: 7}) {
		t.Fatalf("restored marker: got %+v", restoreMarker.Current())
	}
	apply, err := restoreMarker.ShouldApply(8, ids.WALGeneration(99))
	if err != nil || !apply {
		t.Fatalf("post-restore apply rejected: apply=%v err=%v", apply, err)
	}

	restored, err := sql.Open("sqlite3", fmt.Sprintf("file:%s?mode=ro", restorePath))
	if err != nil {
		t.Fatal(err)
	}
	defer restored.Close()
	var value string
	if err := restored.QueryRow("SELECT value FROM items WHERE id = 1").Scan(&value); err != nil {
		t.Fatal(err)
	}
	if value != "snapshot" {
		t.Fatalf("restored value: got %q", value)
	}
}
