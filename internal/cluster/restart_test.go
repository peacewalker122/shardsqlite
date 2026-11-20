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

func TestProperty_RestartRecovery(t *testing.T) {
	directory := t.TempDir()
	baselinePath := filepath.Join(directory, "baseline.db")
	baseline, err := sql.Open("sqlite3", fmt.Sprintf("file:%s?_pragma=journal_mode(WAL)", baselinePath))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := baseline.Exec("CREATE TABLE items (id INTEGER PRIMARY KEY, value TEXT)"); err != nil {
		t.Fatal(err)
	}
	if err := baseline.Close(); err != nil {
		t.Fatal(err)
	}
	baselineBytes, err := os.ReadFile(baselinePath)
	if err != nil {
		t.Fatal(err)
	}
	dbPath := filepath.Join(directory, "shard.db")
	if err := os.WriteFile(dbPath, baselineBytes, 0666); err != nil {
		t.Fatal(err)
	}

	transportAddress := raft.ServerAddress("restart-node")
	startNode := func(vfsName string) (*Node, *sql.DB) {
		_, transport := raft.NewInmemTransport(transportAddress)
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
		deadline := time.Now().Add(5 * time.Second)
		for node.Raft.State() != raft.Leader {
			if time.Now().After(deadline) {
				t.Fatal("node did not become leader")
			}
			time.Sleep(10 * time.Millisecond)
		}
		vfs.Register(vfsName, raftvfs.Wrap(vfs.Find(""), node.Proposer, ids.Group(1), ids.Shard(1), ids.DBName("main"), 64))
		database, err := sql.Open("sqlite3", databaseDSN(dbPath, vfsName))
		if err != nil {
			t.Fatal(err)
		}
		return node, database
	}

	first, leaderDB := startNode(fmt.Sprintf("restart-%d-a", time.Now().UnixNano()))
	if _, err := leaderDB.Exec("INSERT INTO items (id, value) VALUES (1, 'survived')"); err != nil {
		t.Fatal(err)
	}
	leaderDB.Close()
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}

	second, restartedDB := startNode(fmt.Sprintf("restart-%d-b", time.Now().UnixNano()))
	defer restartedDB.Close()
	defer second.Close()
	var value string
	if err := restartedDB.QueryRow("SELECT value FROM items WHERE id = 1").Scan(&value); err != nil {
		t.Fatal(err)
	}
	if value != "survived" {
		t.Fatalf("value after restart: got %q", value)
	}
}
