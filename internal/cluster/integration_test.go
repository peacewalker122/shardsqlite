package cluster

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	_ "github.com/ncruces/go-sqlite3/driver"
	"github.com/ncruces/go-sqlite3/vfs"

	"shardsqlite/internal/ids"
	"shardsqlite/internal/raftvfs"
	"shardsqlite/internal/replay"
)

func TestProperty_CommitGatedByQuorum(t *testing.T) {
	directory := t.TempDir()
	dbPath := filepath.Join(directory, "shard.db")
	appliedPath := filepath.Join(directory, "applied.json")

	_, transport := raft.NewInmemTransport("")
	applier := replay.NewWAL(dbPath + "-wal")
	node, err := Start(Config{
		DataDir:     directory,
		NodeID:      ids.Node(1),
		Transport:   transport,
		Applier:     applier,
		AppliedPath: appliedPath,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer node.Close()

	deadline := time.Now().Add(5 * time.Second)
	for node.Raft.State() != raft.Leader {
		if time.Now().After(deadline) {
			t.Fatal("single node did not become leader")
		}
		time.Sleep(10 * time.Millisecond)
	}

	vfsName := fmt.Sprintf("shardsqlite-test-%d", time.Now().UnixNano())
	vfs.Register(vfsName, raftvfs.Wrap(vfs.Find(""), node.Proposer, ids.Group(1), ids.Shard(1), ids.DBName("main"), 64))
	dsn := fmt.Sprintf("file:%s?vfs=%s&_pragma=journal_mode(WAL)&_pragma=wal_autocheckpoint(0)&_pragma=synchronous(FULL)", dbPath, vfsName)
	db, err := sql.Open("sqlite3", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	transaction, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := transaction.Exec("CREATE TABLE items (id INTEGER PRIMARY KEY, value TEXT)"); err != nil {
		t.Fatal(err)
	}
	if _, err := transaction.Exec("INSERT INTO items (id, value) VALUES (1, 'committed')"); err != nil {
		t.Fatal(err)
	}
	if err := transaction.Commit(); err != nil {
		t.Fatal(err)
	}

	var value string
	if err := db.QueryRow("SELECT value FROM items WHERE id = 1").Scan(&value); err != nil {
		t.Fatal(err)
	}
	if value != "committed" {
		t.Fatalf("committed value: got %q", value)
	}

	if err := node.Close(); err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec("INSERT INTO items (id, value) VALUES (2, 'rejected')")
	if err == nil {
		t.Fatal("write succeeded without quorum")
	}
	var count int
	if err := db.QueryRow("SELECT count(*) FROM items").Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("row count after rejected write: got %d want 1", count)
	}
}

func TestProperty_StaleReadsOnly(t *testing.T) {
	directory := t.TempDir()
	baselinePath := filepath.Join(directory, "baseline.db")
	baseline, err := sql.Open("sqlite3", fmt.Sprintf("file:%s?_pragma=journal_mode(WAL)", baselinePath))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := baseline.Exec("CREATE TABLE items (id INTEGER PRIMARY KEY, value TEXT)"); err != nil {
		t.Fatal(err)
	}
	if _, err := baseline.Exec("INSERT INTO items (id, value) VALUES (1, 'readable')"); err != nil {
		t.Fatal(err)
	}
	if err := baseline.Close(); err != nil {
		t.Fatal(err)
	}
	databaseBytes, err := os.ReadFile(baselinePath)
	if err != nil {
		t.Fatal(err)
	}
	dbPath := filepath.Join(directory, "shard.db")
	if err := os.WriteFile(dbPath, databaseBytes, 0666); err != nil {
		t.Fatal(err)
	}

	_, transport := raft.NewInmemTransport("")
	node, err := Start(Config{
		DataDir:     directory,
		NodeID:      ids.Node(1),
		Transport:   transport,
		Applier:     replay.NewWAL(dbPath + "-wal"),
		AppliedPath: filepath.Join(directory, "applied.json"),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer node.Close()

	deadline := time.Now().Add(5 * time.Second)
	for node.Raft.State() != raft.Leader {
		if time.Now().After(deadline) {
			t.Fatal("single node did not become leader")
		}
		time.Sleep(10 * time.Millisecond)
	}

	if err := node.Read.Wait(); err != nil {
		t.Fatalf("stale read rejected: %v", err)
	}
	_ = node.Read.AppliedIndex()
	if err := node.Close(); err != nil {
		t.Fatal(err)
	}
	if err := node.Read.Wait(); err != nil {
		t.Fatalf("stale read rejected after close: %v", err)
	}
}
