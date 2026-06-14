package tests

import (
	"os"
	"testing"

	db "github.com/abhishek-agrl/transaction-db"
	"github.com/abhishek-agrl/transaction-db/storage"
)

// TestStorageInterfaceConformity verifies that the storage Engine compiles and adheres
// to the TransactionDB interface contracts defined in Phase 1.
func TestStorageInterfaceConformity(t *testing.T) {
	// Assert interface implementation
	var _ db.TransactionDB = (*storage.Engine)(nil)

	tmpDir, err := os.MkdirTemp("", "engine_test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	engine, err := storage.NewEngine(tmpDir)
	if err != nil {
		t.Fatalf("failed to construct storage engine: %v", err)
	}
	defer engine.Close()

	// Verify the default behavior of Read for an empty database
	_, err = engine.Read(db.Key("test-key"))
	if err != db.ErrKeyNotFound {
		t.Errorf("expected %v error, got %v", db.ErrKeyNotFound, err)
	}
}
