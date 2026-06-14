package tests

import (
	"bytes"
	"os"
	"strings"
	"testing"

	db "github.com/abhishek-agrl/transaction-db"
	"github.com/abhishek-agrl/transaction-db/storage"
)

// TestEnginePutAndRead verifies basic Put and Read operations.
func TestEnginePutAndRead(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "engine_put_read")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	engine, err := storage.NewEngine(tmpDir)
	if err != nil {
		t.Fatalf("failed to initialize engine: %v", err)
	}
	defer engine.Close()

	// 1. Basic put and read
	err = engine.Put(db.Key("foo"), db.Value("bar"))
	if err != nil {
		t.Errorf("failed to put: %v", err)
	}

	val, err := engine.Read(db.Key("foo"))
	if err != nil {
		t.Errorf("failed to read: %v", err)
	}
	if !bytes.Equal(val, db.Value("bar")) {
		t.Errorf("expected bar, got %s", val)
	}

	// 2. Read missing key
	_, err = engine.Read(db.Key("notexist"))
	if err != db.ErrKeyNotFound {
		t.Errorf("expected ErrKeyNotFound, got %v", err)
	}

	// 3. Put empty key should fail
	err = engine.Put(db.Key(""), db.Value("val"))
	if err != db.ErrEmptyKey {
		t.Errorf("expected ErrEmptyKey, got %v", err)
	}
}

// TestEngineDelete verifies key deletion and tombstone propagation.
func TestEngineDelete(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "engine_delete")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	engine, err := storage.NewEngine(tmpDir)
	if err != nil {
		t.Fatalf("failed to initialize engine: %v", err)
	}
	defer engine.Close()

	// Put then Delete
	engine.Put(db.Key("key1"), db.Value("val1"))
	err = engine.Delete(db.Key("key1"))
	if err != nil {
		t.Errorf("failed to delete: %v", err)
	}

	// Read deleted key
	_, err = engine.Read(db.Key("key1"))
	if err != db.ErrKeyNotFound {
		t.Errorf("expected key to be deleted, got error: %v", err)
	}

	// Delete non-existent key (should log tombstone cleanly)
	err = engine.Delete(db.Key("nonexistent"))
	if err != nil {
		t.Errorf("expected nil error on nonexistent delete, got %v", err)
	}
}

// TestEngineReadKeyRange verifies range queries and tombstone filtering.
func TestEngineReadKeyRange(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "engine_range")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	engine, err := storage.NewEngine(tmpDir)
	if err != nil {
		t.Fatalf("failed to initialize: %v", err)
	}
	defer engine.Close()

	// Insert items
	engine.Put(db.Key("a"), db.Value("1"))
	engine.Put(db.Key("b"), db.Value("2"))
	engine.Put(db.Key("c"), db.Value("3"))
	engine.Put(db.Key("d"), db.Value("4"))

	// Delete one key in range
	engine.Delete(db.Key("c"))

	// Read range [b, d)
	entries, err := engine.ReadKeyRange(db.Key("b"), db.Key("d"))
	if err != nil {
		t.Fatalf("failed to read range: %v", err)
	}

	// c is deleted, d is exclusive boundary, so only b should remain
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(entries))
	}

	if !bytes.Equal(entries[0].Key, db.Key("b")) {
		t.Errorf("expected key b, got %s", entries[0].Key)
	}
}

// TestEngineBatchPut verifies bulk writing and atomic I/O.
func TestEngineBatchPut(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "engine_batch")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	engine, err := storage.NewEngine(tmpDir)
	if err != nil {
		t.Fatalf("failed to initialize: %v", err)
	}
	defer engine.Close()

	keys := []db.Key{db.Key("k1"), db.Key("k2"), db.Key("k3")}
	vals := []db.Value{db.Value("v1"), db.Value("v2"), db.Value("v3")}

	err = engine.BatchPut(keys, vals)
	if err != nil {
		t.Fatalf("failed to BatchPut: %v", err)
	}

	// Verify reads
	for i, k := range keys {
		val, err := engine.Read(k)
		if err != nil {
			t.Errorf("failed to read %s: %v", k, err)
		}
		if !bytes.Equal(val, vals[i]) {
			t.Errorf("value mismatch: expected %s, got %s", vals[i], val)
		}
	}
}

// TestEngineRecovery verifies database reconstruction from disk state.
func TestEngineRecovery(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "engine_recovery")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// 1. Start engine, write data, and close it
	engine, err := storage.NewEngine(tmpDir)
	if err != nil {
		t.Fatalf("failed to initialize first engine: %v", err)
	}

	engine.Put(db.Key("k1"), db.Value("v1"))
	engine.Put(db.Key("k2"), db.Value("v2"))
	engine.Delete(db.Key("k1"))
	engine.BatchPut(
		[]db.Key{db.Key("k3"), db.Key("k4")},
		[]db.Value{db.Value("v3"), db.Value("v4")},
	)

	engine.Close()

	// 2. Open a new engine on the same folder and recover
	recoveredEngine, err := storage.NewEngine(tmpDir)
	if err != nil {
		t.Fatalf("failed to initialize recovered engine: %v", err)
	}
	defer recoveredEngine.Close()

	// Assert k1 is deleted
	_, err = recoveredEngine.Read(db.Key("k1"))
	if err != db.ErrKeyNotFound {
		t.Errorf("expected k1 to be deleted, got error %v", err)
	}

	// Assert k2 is recovered
	v2, err := recoveredEngine.Read(db.Key("k2"))
	if err != nil {
		t.Errorf("k2 was not recovered: %v", err)
	}
	if !bytes.Equal(v2, db.Value("v2")) {
		t.Errorf("k2 value mismatched: expected v2, got %s", v2)
	}

	// Assert k3 and k4 are recovered
	v3, err := recoveredEngine.Read(db.Key("k3"))
	if err != nil || !bytes.Equal(v3, db.Value("v3")) {
		t.Errorf("k3 value mismatched: got %s, error %v", v3, err)
	}

	v4, err := recoveredEngine.Read(db.Key("k4"))
	if err != nil || !bytes.Equal(v4, db.Value("v4")) {
		t.Errorf("k4 value mismatched: got %s, error %v", v4, err)
	}
}

// TestEngineLSMFlushingAndRecovery verifies that Memtable flushes are triggered correctly,
// writes are correctly distributed across multiple SSTables on disk, and reading/scanning
// across the active Memtable and SSTables behaves correctly. It also verifies that after
// a crash, the database recovers the correct state from the combination of SSTables and WAL.
func TestEngineLSMFlushingAndRecovery(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "engine_lsm_flush")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Initialize engine
	engine, err := storage.NewEngine(tmpDir)
	if err != nil {
		t.Fatalf("failed to create engine: %v", err)
	}
	// Configure small memtable size to trigger frequent flushes (every 3 entries)
	engine.SetMaxMemtableSize(3)

	// Write 8 keys. Since max size is 3, this will trigger:
	// - Write 1, 2, 3 -> Memtable size is 3 -> Flush 1 (sstable_1.db created, keys 1,2,3) -> Memtable cleared
	// - Write 4, 5, 6 -> Memtable size is 3 -> Flush 2 (sstable_2.db created, keys 4,5,6) -> Memtable cleared
	// - Write 7, 8 -> Memtable size is 2 -> remains in Memtable
	keys := []string{"k1", "k2", "k3", "k4", "k5", "k6", "k7", "k8"}
	vals := []string{"v1", "v2", "v3", "v4", "v5", "v6", "v7", "v8"}

	for i := range keys {
		err := engine.Put(db.Key(keys[i]), db.Value(vals[i]))
		if err != nil {
			t.Fatalf("failed to put key %s: %v", keys[i], err)
		}
	}

	// 1. Verify that SSTable files were actually created on disk
	files, err := os.ReadDir(tmpDir)
	if err != nil {
		t.Fatalf("failed to read dir: %v", err)
	}
	sstableCount := 0
	for _, f := range files {
		if strings.HasPrefix(f.Name(), "sstable_") && strings.HasSuffix(f.Name(), ".db") {
			sstableCount++
		}
	}
	if sstableCount != 2 {
		t.Errorf("expected 2 SSTables on disk, found %d", sstableCount)
	}

	// 2. Verify we can read all keys (some from SSTable 1, some from SSTable 2, some from active Memtable)
	for i := range keys {
		val, err := engine.Read(db.Key(keys[i]))
		if err != nil {
			t.Errorf("failed to read key %s: %v", keys[i], err)
		}
		if !bytes.Equal(val, db.Value(vals[i])) {
			t.Errorf("value mismatch for %s: expected %s, got %s", keys[i], vals[i], val)
		}
	}

	// 3. Test range query scanning across both SSTables and active Memtable
	// Keys are k1..k8. Scan [k2, k7)
	entries, err := engine.ReadKeyRange(db.Key("k2"), db.Key("k7"))
	if err != nil {
		t.Fatalf("failed to scan range: %v", err)
	}
	// Expected: k2, k3, k4, k5, k6
	if len(entries) != 5 {
		t.Fatalf("expected 5 entries, got %d", len(entries))
	}
	expectedKeys := []string{"k2", "k3", "k4", "k5", "k6"}
	for i, entry := range entries {
		if !bytes.Equal(entry.Key, db.Key(expectedKeys[i])) {
			t.Errorf("range key mismatch: expected %s, got %s", expectedKeys[i], entry.Key)
		}
	}

	// 4. Overwrite k3 (was in sstable_1.db) with a delete tombstone
	err = engine.Delete(db.Key("k3"))
	if err != nil {
		t.Fatalf("failed to delete k3: %v", err)
	}

	// Verify k3 is not readable
	_, err = engine.Read(db.Key("k3"))
	if err != db.ErrKeyNotFound {
		t.Errorf("expected k3 to be deleted, got error: %v", err)
	}

	// Close engine
	engine.Close()

	// 5. Crash Recovery: Open a new engine on the same folder
	recoveredEngine, err := storage.NewEngine(tmpDir)
	if err != nil {
		t.Fatalf("failed to load recovered engine: %v", err)
	}
	defer recoveredEngine.Close()

	// Verify k3 is still deleted in the new instance
	_, err = recoveredEngine.Read(db.Key("k3"))
	if err != db.ErrKeyNotFound {
		t.Errorf("expected k3 to remain deleted, got %v", err)
	}

	// Verify all other keys are still readable and correct
	for i, k := range keys {
		if k == "k3" {
			continue
		}
		val, err := recoveredEngine.Read(db.Key(k))
		if err != nil {
			t.Errorf("recovered engine failed to read key %s: %v", k, err)
		}
		if !bytes.Equal(val, db.Value(vals[i])) {
			t.Errorf("recovered value mismatch for %s: expected %s, got %s", k, vals[i], val)
		}
	}
}
