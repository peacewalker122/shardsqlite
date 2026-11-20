package cluster

import (
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"

	"github.com/hashicorp/raft"
	sqlite3 "github.com/ncruces/go-sqlite3"

	"shardsqlite/internal/applied"
	"shardsqlite/internal/ids"
)

type SnapshotPayload struct {
	Group   ids.Group         `json:"group"`
	Shard   ids.Shard         `json:"shard"`
	DBFile  []byte            `json:"db_file"`
	Applied ids.RaftIndex     `json:"applied"`
	Gen     ids.WALGeneration `json:"generation"`
}

type snapshot struct {
	payload SnapshotPayload
}

func (s *snapshot) Persist(sink raft.SnapshotSink) error {
	data, err := json.Marshal(s.payload)
	if err != nil {
		sink.Cancel()
		return err
	}
	if _, err := sink.Write(data); err != nil {
		sink.Cancel()
		return err
	}
	return sink.Close()
}

func (s *snapshot) Release() {}

func (f *FSM) createSnapshot() (raft.FSMSnapshot, error) {
	if f.databasePath == "" {
		return nil, errors.New("cluster: database path required for snapshot")
	}
	f.mu.Lock()
	defer f.mu.Unlock()

	sourceURI := "file:" + f.databasePath
	if f.vfsName != "" {
		sourceURI += "?vfs=" + f.vfsName + "&_pragma=journal_mode(WAL)&_pragma=wal_autocheckpoint(0)"
	}
	source, err := sqlite3.Open(sourceURI)
	if err != nil {
		return nil, err
	}
	defer source.Close()

	backupPath := filepath.Join(f.dataDir, "snapshot-backup.db")
	_ = os.Remove(backupPath)
	if err := source.Backup("main", "file:"+backupPath); err != nil {
		return nil, err
	}
	defer os.Remove(backupPath)
	databaseFile, err := os.ReadFile(backupPath)
	if err != nil {
		return nil, err
	}
	marker := f.applied.Current()
	return &snapshot{payload: SnapshotPayload{
		DBFile:  databaseFile,
		Applied: marker.Index,
		Gen:     marker.Gen,
	}}, nil
}

func (f *FSM) restoreSnapshot(source io.ReadCloser) error {
	defer source.Close()
	data, err := io.ReadAll(source)
	if err != nil {
		return err
	}
	var payload SnapshotPayload
	if err := json.Unmarshal(data, &payload); err != nil {
		return err
	}
	if f.databasePath == "" {
		return errors.New("cluster: database path required for restore")
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := os.WriteFile(f.databasePath, payload.DBFile, 0666); err != nil {
		return err
	}
	if err := os.Remove(f.databasePath + "-wal"); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err := os.Remove(f.databasePath + "-shm"); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return f.applied.Reset(applied.Marker{Index: payload.Applied})
}
