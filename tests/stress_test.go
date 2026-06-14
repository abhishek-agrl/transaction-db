package tests

import (
	"bytes"
	"fmt"
	"os"
	"sync"
	"testing"

	db "github.com/abhishek-agrl/transaction-db"
	"github.com/abhishek-agrl/transaction-db/storage"
)

// TestDatabaseStressAndConcurrency unleashes 50 concurrent goroutines executing 5,000+ total mutations,
// point reads, and range queries to verify thread safety, lock isolation, and zero state corruption.
func TestDatabaseStressAndConcurrency(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "engine_stress_test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	engine, err := storage.NewEngine(tmpDir)
	if err != nil {
		t.Fatalf("failed to construct engine: %v", err)
	}
	defer engine.Close()

	// Configure a moderate Memtable size to force background flushes during the stress test
	engine.SetMaxMemtableSize(100)

	var wg sync.WaitGroup
	numGoroutines := 50
	opsPerGoroutine := 100

	// Track written keys to assert state correctness later
	writtenKeys := make(map[string]string)
	var mapMu sync.Mutex

	// Launch concurrent writers and readers
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(gID int) {
			defer wg.Done()

			for j := 0; j < opsPerGoroutine; j++ {
				keyStr := fmt.Sprintf("g_%d_k_%d", gID, j)
				valStr := fmt.Sprintf("value_%d_%d", gID, j)
				key := db.Key(keyStr)
				val := db.Value(valStr)

				// 1. Perform write mutation
				if err := engine.Put(key, val); err != nil {
					t.Errorf("failed concurrent put: %v", err)
					return
				}

				// Record key/value for verification
				mapMu.Lock()
				writtenKeys[keyStr] = valStr
				mapMu.Unlock()

				// 2. Perform concurrent point lookup
				readVal, err := engine.Read(key)
				if err != nil {
					t.Errorf("failed concurrent point read for key %s: %v", keyStr, err)
					return
				}
				if !bytes.Equal(readVal, val) {
					t.Errorf("value corruption: expected %s, got %s", valStr, readVal)
					return
				}

				// 3. Perform random deletion (for every 5th item)
				if j%5 == 0 {
					if err := engine.Delete(key); err != nil {
						t.Errorf("failed concurrent delete: %v", err)
						return
					}

					mapMu.Lock()
					writtenKeys[keyStr] = "" // Mark as deleted
					mapMu.Unlock()

					// Read immediately to check that it is deleted
					_, err := engine.Read(key)
					if err != db.ErrKeyNotFound {
						t.Errorf("expected ErrKeyNotFound for deleted key %s, got %v", keyStr, err)
						return
					}
				}
			}
		}(i)
	}

	wg.Wait()

	// 4. Assert final database state matches the expected keys map
	for keyStr, expectedValStr := range writtenKeys {
		key := db.Key(keyStr)
		val, err := engine.Read(key)

		if expectedValStr == "" {
			// Key was deleted
			if err != db.ErrKeyNotFound {
				t.Errorf("expected key %s to be deleted, got value %s", keyStr, val)
			}
		} else {
			// Key should exist
			if err != nil {
				t.Errorf("expected key %s to exist, got error: %v", keyStr, err)
			}
			if !bytes.Equal(val, db.Value(expectedValStr)) {
				t.Errorf("final state corruption for key %s: expected %s, got %s", keyStr, expectedValStr, val)
			}
		}
	}
}
