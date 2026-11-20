package checkpoint

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	_ "github.com/ncruces/go-sqlite3/driver"
)

func TestProperty_LocalCheckpoint(t *testing.T) {
	directory := t.TempDir()
	dbPath := filepath.Join(directory, "shard.db")
	dsn := fmt.Sprintf("file:%s?_pragma=journal_mode(WAL)&_pragma=wal_autocheckpoint(0)&_pragma=synchronous(FULL)", dbPath)
	database, err := sql.Open("sqlite3", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	if _, err := database.Exec("CREATE TABLE items (id INTEGER PRIMARY KEY, value TEXT)"); err != nil {
		t.Fatal(err)
	}
	if _, err := database.Exec("INSERT INTO items (id, value) VALUES (1, 'checkpointed')"); err != nil {
		t.Fatal(err)
	}
	before, err := os.Stat(dbPath + "-wal")
	if err != nil {
		t.Fatal(err)
	}
	if before.Size() <= 32 {
		t.Fatalf("WAL empty before checkpoint: %d bytes", before.Size())
	}
	if err := New(dbPath, "").Run(); err != nil {
		t.Fatal(err)
	}
	after, err := os.Stat(dbPath + "-wal")
	if err != nil {
		t.Fatal(err)
	}
	if after.Size() != 0 {
		t.Fatalf("WAL not truncated: %d bytes", after.Size())
	}
	var value string
	if err := database.QueryRow("SELECT value FROM items WHERE id = 1").Scan(&value); err != nil {
		t.Fatal(err)
	}
	if value != "checkpointed" {
		t.Fatalf("value after checkpoint: got %q", value)
	}
}
