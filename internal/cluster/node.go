package cluster

import (
	"errors"
	"fmt"
	"io"
	"net"
	"path/filepath"
	"sync"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"

	"shardsqlite/internal/applied"
	"shardsqlite/internal/ids"
	"shardsqlite/internal/raftlog"
	"shardsqlite/internal/readgate"
	"shardsqlite/internal/wal"
)

type Applier interface {
	Apply(batch raftlog.Batch) error
}

type Config struct {
	DataDir       string
	BindAddr      string
	NodeID        ids.Node
	Transport     raft.Transport
	LogStore      raft.LogStore
	StableStore   raft.StableStore
	SnapshotStore raft.SnapshotStore
	Applier       Applier
	AppliedPath   string
	Servers       []raft.Server
	Logger        hclog.Logger
	DatabasePath  string
	VFSName       string
}

type Node struct {
	Raft     *raft.Raft
	FSM      *FSM
	Proposer *Proposer
	Read     *readgate.Gate
	store    io.Closer
}

func (n *Node) Close() error {
	err := n.Raft.Shutdown().Error()
	if n.store != nil {
		if closeErr := n.store.Close(); err == nil {
			err = closeErr
		}
	}
	return err
}

type FSM struct {
	applier      Applier
	applied      *applied.Store
	databasePath string
	vfsName      string
	dataDir      string
	mu           sync.Mutex
}

func NewFSM(applier Applier, appliedStore *applied.Store, databasePath, vfsName, dataDir string) *FSM {
	return &FSM{
		applier:      applier,
		applied:      appliedStore,
		databasePath: databasePath,
		vfsName:      vfsName,
		dataDir:      dataDir,
	}
}

func (f *FSM) AppliedIndex() uint64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return uint64(f.applied.Current().Index)
}

func (f *FSM) Apply(entry *raft.Log) interface{} {
	if entry.Type != raft.LogCommand {
		return nil
	}
	batch, err := raftlog.Decode(entry.Data)
	if err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	reset := len(batch.Header) >= wal.HeaderSize
	var apply bool
	if reset {
		apply, err = f.applied.ShouldApplyReset(ids.RaftIndex(entry.Index), batch.Gen)
	} else {
		apply, err = f.applied.ShouldApply(ids.RaftIndex(entry.Index), batch.Gen)
	}
	if err != nil {
		return err
	}
	if !apply {
		return nil
	}
	if err := f.applier.Apply(batch); err != nil {
		return err
	}
	if reset {
		return f.applied.AdvanceReset(ids.RaftIndex(entry.Index), batch.Gen)
	}
	return f.applied.Advance(ids.RaftIndex(entry.Index), batch.Gen)
}

func (f *FSM) Snapshot() (raft.FSMSnapshot, error) {
	return f.createSnapshot()
}

func (f *FSM) Restore(source io.ReadCloser) error {
	return f.restoreSnapshot(source)
}

type Proposer struct {
	raft *raft.Raft
}

func NewProposer(node *raft.Raft) *Proposer {
	return &Proposer{raft: node}
}

func (p *Proposer) Propose(batch raftlog.Batch) error {
	data, err := raftlog.Encode(batch)
	if err != nil {
		return err
	}
	future := p.raft.Apply(data, 10*time.Second)
	if err := future.Error(); err != nil {
		return err
	}
	if response, ok := future.Response().(error); ok {
		return response
	}
	return nil
}

func (p *Proposer) IsLeader() bool {
	return p.raft.State() == raft.Leader
}

func Start(config Config) (*Node, error) {
	store := config.LogStore
	stable := config.StableStore
	var createdStore *raftboltdb.BoltStore
	if store == nil || stable == nil {
		created, err := raftboltdb.NewBoltStore(filepath.Join(config.DataDir, "raft.db"))
		if err != nil {
			return nil, err
		}
		createdStore = created
		if store == nil {
			store = created
		}
		if stable == nil {
			stable = created
		}
	}
	snapshots := config.SnapshotStore
	if snapshots == nil {
		created, err := raft.NewFileSnapshotStore(config.DataDir, 2, nil)
		if err != nil {
			return nil, err
		}
		snapshots = created
	}
	transport := config.Transport
	if transport == nil {
		if config.BindAddr == "" {
			return nil, errors.New("cluster: bind address required")
		}
		created, err := raft.NewTCPTransport(config.BindAddr, &net.TCPAddr{}, 3, 5*time.Second, nil)
		if err != nil {
			return nil, err
		}
		transport = created
	}
	appliedStore, err := applied.Open(config.AppliedPath)
	if err != nil {
		return nil, err
	}
	fsm := NewFSM(config.Applier, appliedStore, config.DatabasePath, config.VFSName, config.DataDir)
	raftConfig := raft.DefaultConfig()
	raftConfig.LocalID = raft.ServerID(fmt.Sprintf("%d", config.NodeID))
	raftConfig.Logger = hclog.NewNullLogger()
	if config.Logger != nil {
		raftConfig.Logger = config.Logger
	}
	hasState, err := raft.HasExistingState(store, stable, snapshots)
	if err != nil {
		return nil, err
	}
	if !hasState {
		servers := config.Servers
		if len(servers) == 0 {
			servers = []raft.Server{{
				ID:       raftConfig.LocalID,
				Address:  transport.LocalAddr(),
				Suffrage: raft.Voter,
			}}
		}
		configuration := raft.Configuration{Servers: servers}
		if err := raft.BootstrapCluster(raftConfig, store, stable, snapshots, transport, configuration); err != nil {
			return nil, err
		}
	}
	node, err := raft.NewRaft(raftConfig, fsm, store, stable, snapshots, transport)
	if err != nil {
		return nil, err
	}
	return &Node{
		Raft:     node,
		FSM:      fsm,
		Proposer: NewProposer(node),
		Read:     readgate.New(fsm.AppliedIndex),
		store:    createdStore,
	}, nil
}
