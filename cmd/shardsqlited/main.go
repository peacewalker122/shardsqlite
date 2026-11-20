package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/hashicorp/raft"
	"github.com/ncruces/go-sqlite3/vfs"

	"shardsqlite/internal/cluster"
	"shardsqlite/internal/ids"
	"shardsqlite/internal/raftvfs"
	"shardsqlite/internal/replay"
)

func main() {
	nodeID := flag.Uint64("node-id", 1, "raft node id")
	dataDir := flag.String("data-dir", "data", "raft and applied-index directory")
	databasePath := flag.String("db", "shard.db", "shard database path")
	bindAddr := flag.String("bind", "127.0.0.1:9200", "raft bind address")
	vfsName := flag.String("vfs", "shardsqlite", "registered SQLite VFS name")
	groupID := flag.Uint64("group", 1, "raft group id")
	shardID := flag.Uint64("shard", 1, "shard id")
	dbName := flag.String("dbname", "main", "SQLite schema name")
	peers := flag.String("peers", "", "comma separated id@address peers")
	flag.Parse()

	transport, err := raft.NewTCPTransport(*bindAddr, (*net.TCPAddr)(nil), 3, 5*time.Second, os.Stderr)
	if err != nil {
		log.Fatal(err)
	}
	node, err := cluster.Start(cluster.Config{
		DataDir:      *dataDir,
		BindAddr:     *bindAddr,
		NodeID:       ids.Node(*nodeID),
		Transport:    transport,
		Applier:      replay.NewWAL(*databasePath + "-wal"),
		AppliedPath:  *dataDir + "/applied.json",
		Servers:      parsePeers(*peers),
		DatabasePath: *databasePath,
		VFSName:      *vfsName,
	})
	if err != nil {
		log.Fatal(err)
	}
	vfs.Register(*vfsName, raftvfs.Wrap(vfs.Find(""), node.Proposer, ids.Group(*groupID), ids.Shard(*shardID), ids.DBName(*dbName), 64))
	fmt.Printf("node %d listening on %s\n", *nodeID, *bindAddr)

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	<-signals
	if err := node.Close(); err != nil {
		log.Fatal(err)
	}
}

func parsePeers(value string) []raft.Server {
	if value == "" {
		return nil
	}
	servers := []raft.Server{}
	for _, entry := range strings.Split(value, ",") {
		parts := strings.SplitN(entry, "@", 2)
		if len(parts) != 2 {
			log.Fatalf("invalid peer %q", entry)
		}
		id, err := strconv.ParseUint(parts[0], 10, 64)
		if err != nil {
			log.Fatal(err)
		}
		servers = append(servers, raft.Server{
			ID:       raft.ServerID(strconv.FormatUint(id, 10)),
			Address:  raft.ServerAddress(parts[1]),
			Suffrage: raft.Voter,
		})
	}
	return servers
}
