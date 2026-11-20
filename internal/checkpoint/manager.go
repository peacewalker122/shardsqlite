package checkpoint

import (
	sqlite3 "github.com/ncruces/go-sqlite3"
)

type Manager struct {
	databasePath string
	vfsName      string
}

func New(databasePath, vfsName string) *Manager {
	return &Manager{databasePath: databasePath, vfsName: vfsName}
}

func (m *Manager) Run() error {
	uri := "file:" + m.databasePath + "?_pragma=journal_mode(WAL)&_pragma=wal_autocheckpoint(0)"
	if m.vfsName != "" {
		uri += "&vfs=" + m.vfsName
	}
	connection, err := sqlite3.Open(uri)
	if err != nil {
		return err
	}
	defer connection.Close()
	_, _, err = connection.WALCheckpoint("main", sqlite3.CHECKPOINT_TRUNCATE)
	return err
}
