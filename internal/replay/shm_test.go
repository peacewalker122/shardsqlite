package replay_test

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	_ "github.com/ncruces/go-sqlite3/driver"

	"shardsqlite/internal/ids"
	"shardsqlite/internal/raftlog"
	"shardsqlite/internal/replay"
	"shardsqlite/internal/wal"
)

func TestProperty_DirectSHMLiveApply(t *testing.T) {
	directory := t.TempDir()
	baselinePath := filepath.Join(directory, "baseline.db")
	baseline, err := sql.Open("sqlite3", fmt.Sprintf("file:%s?_pragma=journal_mode(WAL)", baselinePath))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := baseline.Exec("CREATE TABLE baseline (id INTEGER PRIMARY KEY)"); err != nil {
		t.Fatal(err)
	}
	if err := baseline.Close(); err != nil {
		t.Fatal(err)
	}
	baselineBytes, err := os.ReadFile(baselinePath)
	if err != nil {
		t.Fatal(err)
	}

	sourcePath := filepath.Join(directory, "source.db")
	if err := os.WriteFile(sourcePath, baselineBytes, 0666); err != nil {
		t.Fatal(err)
	}
	source, err := sql.Open("sqlite3", fmt.Sprintf("file:%s?_pragma=journal_mode(WAL)&_pragma=wal_autocheckpoint(0)&_pragma=synchronous(FULL)", sourcePath))
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	transaction, err := source.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := transaction.Exec("CREATE TABLE items (id INTEGER PRIMARY KEY, value TEXT)"); err != nil {
		t.Fatal(err)
	}
	if _, err := transaction.Exec("INSERT INTO items (id, value) VALUES (1, 'shm-live')"); err != nil {
		t.Fatal(err)
	}
	if err := transaction.Commit(); err != nil {
		t.Fatal(err)
	}

	walBytes, err := os.ReadFile(sourcePath + "-wal")
	if err != nil {
		t.Fatal(err)
	}
	header, err := wal.ParseHeader(walBytes[:wal.HeaderSize])
	if err != nil {
		t.Fatal(err)
	}
	frameSize := int(wal.FrameHeaderSize) + int(header.PageSize)
	rawFrames := make([][]byte, 0)
	for offset := wal.HeaderSize; offset+frameSize <= len(walBytes); offset += frameSize {
		rawFrames = append(rawFrames, walBytes[offset:offset+frameSize])
	}
	chain, err := wal.VerifyChain(walBytes[:wal.HeaderSize], rawFrames)
	if err != nil {
		t.Fatal(err)
	}
	batch := raftlog.Batch{
		Group:     ids.Group(1),
		Shard:     ids.Shard(1),
		DB:        ids.DBName("main"),
		Gen:       ids.WALGeneration(42),
		PageSize:  header.PageSize,
		BaseFrame: 1,
		Header:    append([]byte(nil), walBytes[:wal.HeaderSize]...),
		Frames:    chain.Frames,
		Commit:    true,
	}

	targetPath := filepath.Join(directory, "target.db")
	if err := os.WriteFile(targetPath, baselineBytes, 0666); err != nil {
		t.Fatal(err)
	}
	reader, err := sql.Open("sqlite3", fmt.Sprintf("file:%s?_pragma=journal_mode(WAL)&_pragma=wal_autocheckpoint(0)", targetPath))
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	reader.SetMaxOpenConns(1)
	var initial int
	if err := reader.QueryRow("SELECT count(*) FROM sqlite_schema").Scan(&initial); err != nil {
		t.Fatal(err)
	}

	applier := replay.NewWAL(targetPath + "-wal").WithSHM(func() bool { return false })
	if err := applier.Apply(batch); err != nil {
		t.Fatal(err)
	}

	var value string
	if err := reader.QueryRow("SELECT value FROM items WHERE id = 1").Scan(&value); err != nil {
		t.Fatalf("long-lived reader did not see applied frame: %v", err)
	}
	if value != "shm-live" {
		t.Fatalf("value: got %q want %q", value, "shm-live")
	}
}
