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

	"shardsqlite/internal/checkpoint"
	"shardsqlite/internal/ids"
	"shardsqlite/internal/raftvfs"
	"shardsqlite/internal/replay"
	"shardsqlite/internal/wal"
)

func TestProperty_WALResetPropagation(t *testing.T) {
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

	transports := make([]*raft.InmemTransport, 2)
	servers := make([]raft.Server, 2)
	for index := range transports {
		_, transport := raft.NewInmemTransport("")
		transports[index] = transport
		servers[index] = raft.Server{
			ID:       raft.ServerID(fmt.Sprintf("%d", index+1)),
			Address:  transport.LocalAddr(),
			Suffrage: raft.Voter,
		}
	}
	for left := range transports {
		for right := range transports {
			if left != right {
				transports[left].Connect(transports[right].LocalAddr(), transports[right])
			}
		}
	}

	nodes := make([]*Node, 2)
	dbPaths := make([]string, 2)
	vfsNames := make([]string, 2)
	for index := range nodes {
		nodeDir := filepath.Join(directory, fmt.Sprintf("node-%d", index))
		if err := os.MkdirAll(nodeDir, 0755); err != nil {
			t.Fatal(err)
		}
		dbPath := filepath.Join(nodeDir, "shard.db")
		vfsName := fmt.Sprintf("reset-test-%d-%d", time.Now().UnixNano(), index)
		dbPaths[index] = dbPath
		vfsNames[index] = vfsName
		if err := os.WriteFile(dbPath, baselineBytes, 0666); err != nil {
			t.Fatal(err)
		}
		var node *Node
		applier := replay.NewWAL(dbPath + "-wal").WithSHM(func() bool {
			return node != nil && node.Raft.State() == raft.Leader
		})
		node, err = Start(Config{
			DataDir:     nodeDir,
			NodeID:      ids.Node(index + 1),
			Transport:   transports[index],
			Applier:     applier,
			AppliedPath: filepath.Join(nodeDir, "applied.json"),
			Servers:     servers,
		})
		if err != nil {
			t.Fatal(err)
		}
		nodes[index] = node
		defer node.Close()
		vfs.Register(vfsName, raftvfs.Wrap(vfs.Find(""), node.Proposer, ids.Group(1), ids.Shard(1), ids.DBName("main"), 64))
	}

	leader := -1
	deadline := time.Now().Add(10 * time.Second)
	for leader < 0 {
		for index, node := range nodes {
			if node.Raft.State() == raft.Leader {
				leader = index
				break
			}
		}
		if time.Now().After(deadline) {
			t.Fatal("no leader elected")
		}
		time.Sleep(20 * time.Millisecond)
	}
	follower := (leader + 1) % len(nodes)

	leaderDB, err := sql.Open("sqlite3", databaseDSN(dbPaths[leader], vfsNames[leader]))
	if err != nil {
		t.Fatal(err)
	}
	defer leaderDB.Close()
	if _, err := leaderDB.Exec("INSERT INTO items (id, value) VALUES (1, 'before-reset')"); err != nil {
		t.Fatal(err)
	}

	initialFollowerDB, err := sql.Open("sqlite3", databaseDSN(dbPaths[follower], vfsNames[follower]))
	if err != nil {
		t.Fatal(err)
	}
	initialValue := ""
	initialDeadline := time.Now().Add(5 * time.Second)
	for initialValue != "before-reset" {
		err = initialFollowerDB.QueryRow("SELECT value FROM items WHERE id = 1").Scan(&initialValue)
		if err != nil && time.Now().After(initialDeadline) {
			t.Fatalf("follower did not see initial write: %v", err)
		}
		if time.Now().After(initialDeadline) {
			t.Fatal("follower did not see initial write")
		}
		time.Sleep(20 * time.Millisecond)
	}
	initialFollowerDB.Close()

	firstWAL, err := os.ReadFile(dbPaths[leader] + "-wal")
	if err != nil {
		t.Fatal(err)
	}
	firstHeader, err := wal.ParseHeader(firstWAL[:wal.HeaderSize])
	if err != nil {
		t.Fatal(err)
	}

	if err := checkpoint.New(dbPaths[leader], vfsNames[leader]).Run(); err != nil {
		t.Fatal(err)
	}

	followerDB, err := sql.Open("sqlite3", databaseDSN(dbPaths[follower], vfsNames[follower]))
	if err != nil {
		t.Fatal(err)
	}
	defer followerDB.Close()
	followerDB.SetMaxOpenConns(1)
	var before string
	if err := followerDB.QueryRow("SELECT value FROM items WHERE id = 1").Scan(&before); err != nil {
		t.Fatal(err)
	}
	if before != "before-reset" {
		t.Fatalf("value before reset: got %q", before)
	}

	if _, err := leaderDB.Exec("INSERT INTO items (id, value) VALUES (2, 'after-reset')"); err != nil {
		t.Fatal(err)
	}

	value := ""
	readDeadline := time.Now().Add(5 * time.Second)
	for value != "after-reset" {
		err = followerDB.QueryRow("SELECT value FROM items WHERE id = 2").Scan(&value)
		if err != nil && time.Now().After(readDeadline) {
			t.Fatalf("follower did not see post-reset write: %v", err)
		}
		if time.Now().After(readDeadline) {
			t.Fatal("follower did not see post-reset write")
		}
		time.Sleep(20 * time.Millisecond)
	}

	leaderWAL, err := os.ReadFile(dbPaths[leader] + "-wal")
	if err != nil {
		t.Fatal(err)
	}
	followerWAL, err := os.ReadFile(dbPaths[follower] + "-wal")
	if err != nil {
		t.Fatal(err)
	}
	leaderHeader, err := wal.ParseHeader(leaderWAL[:wal.HeaderSize])
	if err != nil {
		t.Fatal(err)
	}
	followerHeader, err := wal.ParseHeader(followerWAL[:wal.HeaderSize])
	if err != nil {
		t.Fatal(err)
	}
	if leaderHeader.Salt1 == firstHeader.Salt1 && leaderHeader.Salt2 == firstHeader.Salt2 {
		t.Fatal("leader WAL salt did not change after checkpoint")
	}
	if followerHeader.Salt1 != leaderHeader.Salt1 || followerHeader.Salt2 != leaderHeader.Salt2 {
		t.Fatalf("follower WAL salt mismatch: got %d/%d want %d/%d", followerHeader.Salt1, followerHeader.Salt2, leaderHeader.Salt1, leaderHeader.Salt2)
	}
}
