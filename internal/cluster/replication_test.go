package cluster

import (
	"database/sql"
	"fmt"
	"io"
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

func TestProperty_ReplicationAcrossNodes(t *testing.T) {
	directory := t.TempDir()
	transports := make([]*raft.InmemTransport, 3)
	servers := make([]raft.Server, 3)
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

	nodes := make([]*Node, 3)
	vfsNames := make([]string, 3)
	dbPaths := make([]string, 3)
	for index := range nodes {
		nodeDir := filepath.Join(directory, fmt.Sprintf("node-%d", index))
		if err := os.MkdirAll(nodeDir, 0755); err != nil {
			t.Fatal(err)
		}
		dbPath := filepath.Join(nodeDir, "shard.db")
		vfsName := fmt.Sprintf("replication-test-%d-%d", time.Now().UnixNano(), index)
		dbPaths[index] = dbPath
		vfsNames[index] = vfsName
		var node *Node
		applier := replay.NewWAL(dbPath + "-wal").WithSHM(func() bool {
			return node != nil && node.Raft.State() == raft.Leader
		})
		node, err := Start(Config{
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

	baselinePath := filepath.Join(directory, "baseline.db")
	baselineDB, err := sql.Open("sqlite3", databaseDSN(baselinePath, ""))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := baselineDB.Exec("CREATE TABLE baseline (id INTEGER PRIMARY KEY)"); err != nil {
		t.Fatal(err)
	}
	if err := baselineDB.Close(); err != nil {
		t.Fatal(err)
	}
	baseline, err := os.ReadFile(baselinePath)
	if err != nil {
		t.Fatal(err)
	}
	for index := range nodes {
		if err := os.WriteFile(dbPaths[index], baseline, 0666); err != nil {
			t.Fatal(err)
		}
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

	longLivedFollower := (leader + 1) % len(nodes)
	longLivedDB, err := sql.Open("sqlite3", databaseDSN(dbPaths[longLivedFollower], vfsNames[longLivedFollower]))
	if err != nil {
		t.Fatal(err)
	}
	defer longLivedDB.Close()
	longLivedDB.SetMaxOpenConns(1)
	var schemaCount int
	if err := longLivedDB.QueryRow("SELECT count(*) FROM sqlite_schema").Scan(&schemaCount); err != nil {
		t.Fatal(err)
	}

	leaderDB, err := sql.Open("sqlite3", databaseDSN(dbPaths[leader], vfsNames[leader]))
	if err != nil {
		t.Fatal(err)
	}
	defer leaderDB.Close()
	transaction, err := leaderDB.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := transaction.Exec("CREATE TABLE items (id INTEGER PRIMARY KEY, value TEXT)"); err != nil {
		t.Fatal(err)
	}
	if _, err := transaction.Exec("INSERT INTO items (id, value) VALUES (1, 'replicated')"); err != nil {
		t.Fatal(err)
	}
	if err := transaction.Commit(); err != nil {
		t.Fatal(err)
	}

	for index, node := range nodes {
		markerPath := filepath.Join(filepath.Dir(dbPaths[index]), "applied.json")
		applyDeadline := time.Now().Add(5 * time.Second)
		for {
			_, err := os.Stat(markerPath)
			if err == nil {
				break
			}
			if time.Now().After(applyDeadline) {
				t.Fatalf("node %d did not apply: %v (state %v)", index, err, node.Raft.State())
			}
			time.Sleep(20 * time.Millisecond)
		}
	}
	var longLivedValue string
	if err := longLivedDB.QueryRow("SELECT value FROM items WHERE id = 1").Scan(&longLivedValue); err != nil {
		t.Fatalf("long-lived follower did not see applied frame: %v", err)
	}
	if longLivedValue != "replicated" {
		t.Fatalf("long-lived follower value: got %q", longLivedValue)
	}

	for index := range nodes {
		followerDB, err := sql.Open("sqlite3", databaseDSN(dbPaths[index], vfsNames[index]))
		if err != nil {
			t.Fatal(err)
		}
		value := ""
		readDeadline := time.Now().Add(5 * time.Second)
		for value != "replicated" {
			err = followerDB.QueryRow("SELECT value FROM items WHERE id = 1").Scan(&value)
			if err != nil && time.Now().After(readDeadline) {
				t.Fatalf("node %d read failed: %v", index, err)
			}
			if time.Now().After(readDeadline) {
				t.Fatalf("node %d did not replicate", index)
			}
			time.Sleep(20 * time.Millisecond)
		}
		followerDB.Close()
	}
}

func databaseDSN(path, vfsName string) string {
	return fmt.Sprintf("file:%s?vfs=%s&_pragma=journal_mode(WAL)&_pragma=wal_autocheckpoint(0)&_pragma=synchronous(FULL)", path, vfsName)
}

func TestE2E_ReplicationOverTCP(t *testing.T) {
	directory := t.TempDir()
	transports := make([]*raft.NetworkTransport, 3)
	servers := make([]raft.Server, 3)
	for index := range transports {
		transport, err := raft.NewTCPTransport("127.0.0.1:0", nil, 3, 5*time.Second, io.Discard)
		if err != nil {
			t.Fatal(err)
		}
		transports[index] = transport
		servers[index] = raft.Server{
			ID:       raft.ServerID(fmt.Sprintf("%d", index+1)),
			Address:  transport.LocalAddr(),
			Suffrage: raft.Voter,
		}
	}
	defer func() {
		for _, transport := range transports {
			transport.Close()
		}
	}()

	nodes := make([]*Node, 3)
	vfsNames := make([]string, 3)
	dbPaths := make([]string, 3)
	for index := range nodes {
		nodeDir := filepath.Join(directory, fmt.Sprintf("node-%d", index))
		if err := os.MkdirAll(nodeDir, 0755); err != nil {
			t.Fatal(err)
		}
		dbPath := filepath.Join(nodeDir, "shard.db")
		vfsName := fmt.Sprintf("tcp-replication-test-%d-%d", time.Now().UnixNano(), index)
		dbPaths[index] = dbPath
		vfsNames[index] = vfsName
		var node *Node
		applier := replay.NewWAL(dbPath + "-wal").WithSHM(func() bool {
			return node != nil && node.Raft.State() == raft.Leader
		})
		node, err := Start(Config{
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

	baselinePath := filepath.Join(directory, "baseline.db")
	baselineDB, err := sql.Open("sqlite3", databaseDSN(baselinePath, ""))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := baselineDB.Exec("CREATE TABLE baseline (id INTEGER PRIMARY KEY)"); err != nil {
		t.Fatal(err)
	}
	if err := baselineDB.Close(); err != nil {
		t.Fatal(err)
	}
	baseline, err := os.ReadFile(baselinePath)
	if err != nil {
		t.Fatal(err)
	}
	for index := range nodes {
		if err := os.WriteFile(dbPaths[index], baseline, 0666); err != nil {
			t.Fatal(err)
		}
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
			t.Fatal("no leader elected over TCP")
		}
		time.Sleep(20 * time.Millisecond)
	}
	leaderAddr := nodes[leader].Raft.Leader()
	for index, node := range nodes {
		if node.Raft.Leader() != leaderAddr {
			t.Fatalf("node %d sees leader %q want %q", index, node.Raft.Leader(), leaderAddr)
		}
	}

	leaderDB, err := sql.Open("sqlite3", databaseDSN(dbPaths[leader], vfsNames[leader]))
	if err != nil {
		t.Fatal(err)
	}
	defer leaderDB.Close()
	transaction, err := leaderDB.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := transaction.Exec("CREATE TABLE items (id INTEGER PRIMARY KEY, value TEXT)"); err != nil {
		t.Fatal(err)
	}
	if _, err := transaction.Exec("INSERT INTO items (id, value) VALUES (1, 'tcp-replicated')"); err != nil {
		t.Fatal(err)
	}
	if err := transaction.Commit(); err != nil {
		t.Fatal(err)
	}

	for index := range nodes {
		nodeDB, err := sql.Open("sqlite3", databaseDSN(dbPaths[index], vfsNames[index]))
		if err != nil {
			t.Fatal(err)
		}
		value := ""
		readDeadline := time.Now().Add(5 * time.Second)
		for value != "tcp-replicated" {
			err = nodeDB.QueryRow("SELECT value FROM items WHERE id = 1").Scan(&value)
			if err != nil && time.Now().After(readDeadline) {
				t.Fatalf("node %d read failed: %v", index, err)
			}
			if time.Now().After(readDeadline) {
				t.Fatalf("node %d did not replicate over TCP", index)
			}
			time.Sleep(20 * time.Millisecond)
		}
		nodeDB.Close()
	}
}

func TestProperty_RollbackNoCommit(t *testing.T) {
	directory := t.TempDir()
	dbPath := filepath.Join(directory, "shard.db")

	_, transport := raft.NewInmemTransport("")
	var node *Node
	applier := replay.NewWAL(dbPath + "-wal").WithSHM(func() bool {
		return node != nil && node.Raft.State() == raft.Leader
	})
	node, err := Start(Config{
		DataDir:     directory,
		NodeID:      ids.Node(1),
		Transport:   transport,
		Applier:     applier,
		AppliedPath: filepath.Join(directory, "applied.json"),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer node.Close()

	deadline := time.Now().Add(5 * time.Second)
	for node.Raft.State() != raft.Leader {
		if time.Now().After(deadline) {
			t.Fatal("node did not become leader")
		}
		time.Sleep(10 * time.Millisecond)
	}

	vfsName := fmt.Sprintf("rollback-test-%d", time.Now().UnixNano())
	vfs.Register(vfsName, raftvfs.Wrap(vfs.Find(""), node.Proposer, ids.Group(1), ids.Shard(1), ids.DBName("main"), 64))
	db, err := sql.Open("sqlite3", databaseDSN(dbPath, vfsName))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	if _, err := db.Exec("PRAGMA cache_size=1"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("CREATE TABLE items (id INTEGER PRIMARY KEY, value TEXT)"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("INSERT INTO items (id, value) VALUES (1, 'committed')"); err != nil {
		t.Fatal(err)
	}

	markerPath := filepath.Join(directory, "applied.json")
	before, err := os.ReadFile(markerPath)
	if err != nil {
		t.Fatal(err)
	}

	transaction, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	for index := 2; index < 80; index++ {
		if _, err := transaction.Exec("INSERT INTO items (id, value) VALUES (?, 'rolled-back')", index); err != nil {
			t.Fatal(err)
		}
	}
	if err := transaction.Rollback(); err != nil {
		t.Fatal(err)
	}

	after, err := os.ReadFile(markerPath)
	if err != nil {
		t.Fatal(err)
	}
	if string(after) != string(before) {
		t.Fatalf("rollback advanced applied marker: before %s after %s", before, after)
	}
	var rolledBack int
	if err := db.QueryRow("SELECT count(*) FROM items WHERE value = 'rolled-back'").Scan(&rolledBack); err != nil {
		t.Fatal(err)
	}
	if rolledBack != 0 {
		t.Fatalf("rolled-back rows visible: %d", rolledBack)
	}

	if _, err := db.Exec("INSERT INTO items (id, value) VALUES (100, 'after-rollback')"); err != nil {
		t.Fatal(err)
	}
	var value string
	if err := db.QueryRow("SELECT value FROM items WHERE id = 100").Scan(&value); err != nil {
		t.Fatal(err)
	}
	if value != "after-rollback" {
		t.Fatalf("post-rollback value: got %q", value)
	}
}
