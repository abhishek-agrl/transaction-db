package storage

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	db "github.com/abhishek-agrl/transaction-db"
)

// TestWALWriteAndRecovery asserts that writing values, updates, and tombstones to the WAL
// results in a correctly reconstructed SkipList state on recovery.
func TestWALWriteAndRecovery(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "wal_test")
	if err != nil {
		t.Fatalf("failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	walPath := filepath.Join(tmpDir, "test.wal")

	// 1. Write records to WAL
	wal, err := OpenWAL(walPath)
	if err != nil {
		t.Fatalf("failed to open WAL: %v", err)
	}

	operations := []struct {
		key       db.Key
		value     db.Value
		isDeleted bool
	}{
		{db.Key("key1"), db.Value("value1"), false},
		{db.Key("key2"), db.Value("value2"), false},
		{db.Key("key1"), db.Value("value1_updated"), false}, // Update
		{db.Key("key3"), db.Value("value3"), false},
		{db.Key("key2"), nil, true}, // Tombstone delete
	}

	for _, op := range operations {
		if err := wal.Write(op.key, op.value, op.isDeleted); err != nil {
			t.Fatalf("failed to write record %+v: %v", op, err)
		}
	}

	if err := wal.Close(); err != nil {
		t.Fatalf("failed to close WAL: %v", err)
	}

	// 2. Perform recovery into a new SkipList
	sl := NewSkipList(10, 0.5)
	count, err := Recover(walPath, sl)
	if err != nil {
		t.Fatalf("failed to recover: %v", err)
	}

	if count != len(operations) {
		t.Errorf("expected to recover %d operations, got %d", len(operations), count)
	}

	// 3. Verify state
	// key1 should be updated
	node, found := sl.Search(db.Key("key1"))
	if !found {
		t.Error("key1 was not recovered")
	} else if !bytes.Equal(node.Value(), db.Value("value1_updated")) {
		t.Errorf("key1 value mismatched. Expected value1_updated, got %s", node.Value())
	}

	// key2 should be marked as deleted (tombstone)
	node, found = sl.Search(db.Key("key2"))
	if !found {
		t.Error("key2 was not recovered")
	} else if !node.IsDeleted() {
		t.Error("key2 was recovered but is not marked as deleted")
	}

	// key3 should be normal
	node, found = sl.Search(db.Key("key3"))
	if !found {
		t.Error("key3 was not recovered")
	} else if !bytes.Equal(node.Value(), db.Value("value3")) {
		t.Errorf("key3 value mismatched. Expected value3, got %s", node.Value())
	}
}

// TestWALCorrupt verifies that the recovery subsystem detects corrupted or incomplete binary records.
func TestWALCorrupt(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "wal_corrupt_test")
	if err != nil {
		t.Fatalf("failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	walPath := filepath.Join(tmpDir, "corrupt.wal")

	// Write one clean record
	wal, err := OpenWAL(walPath)
	if err != nil {
		t.Fatalf("failed to open WAL: %v", err)
	}
	if err := wal.Write(db.Key("k1"), db.Value("v1"), false); err != nil {
		t.Fatalf("failed to write record: %v", err)
	}
	if err := wal.Close(); err != nil {
		t.Fatalf("failed to close WAL: %v", err)
	}

	// Append corrupt/partial bytes to the file
	file, err := os.OpenFile(walPath, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		t.Fatalf("failed to open file for corruption: %v", err)
	}
	// Write 2 partial bytes instead of a full 4-byte key length header
	if _, err := file.Write([]byte{0x00, 0x01}); err != nil {
		t.Fatalf("failed to append partial bytes: %v", err)
	}
	file.Close()

	// Attempt recovery
	sl := NewSkipList(10, 0.5)
	_, err = Recover(walPath, sl)
	if err != ErrCorruptRecord {
		t.Errorf("expected recovery to return ErrCorruptRecord, got %v", err)
	}
}
