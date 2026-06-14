package tests

import (
	"bytes"
	"fmt"
	"os"
	"testing"

	db "github.com/abhishek-agrl/transaction-db"
	"github.com/abhishek-agrl/transaction-db/storage"
)

// TestCrashSimulationSuite runs a rigorous multi-stage crash and recovery simulation.
// It executes mutations, simulates crash shutdowns, restarts, and asserts state consistency.
func TestCrashSimulationSuite(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "engine_crash_simulation")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	expectedState := make(map[string]string)

	// Stage 1: Initial writes and flushes
	engine, err := storage.NewEngine(tmpDir)
	if err != nil {
		t.Fatalf("failed to initialize: %v", err)
	}
	engine.SetMaxMemtableSize(5) // force flushes every 5 entries

	// Write 12 keys (should trigger 2 flushes, 2 remaining in Memtable)
	for i := 0; i < 12; i++ {
		k := fmt.Sprintf("key_%d", i)
		v := fmt.Sprintf("val_%d", i)
		if err := engine.Put(db.Key(k), db.Value(v)); err != nil {
			t.Fatalf("failed to put %s: %v", k, err)
		}
		expectedState[k] = v
	}

	// Abrupt shutdown simulation (close engine to release file lock, mimicking crash)
	engine.Close()

	// Stage 2: Restart and verify
	engine, err = storage.NewEngine(tmpDir)
	if err != nil {
		t.Fatalf("failed to restart: %v", err)
	}
	engine.SetMaxMemtableSize(5)

	verifyState(t, engine, expectedState)

	// Stage 3: Perform deletes and overrides
	// Overwrite key_1, key_2. Delete key_3, key_4.
	engine.Put(db.Key("key_1"), db.Value("new_val_1"))
	engine.Put(db.Key("key_2"), db.Value("new_val_2"))
	engine.Delete(db.Key("key_3"))
	engine.Delete(db.Key("key_4"))

	expectedState["key_1"] = "new_val_1"
	expectedState["key_2"] = "new_val_2"
	expectedState["key_3"] = "" // deleted
	expectedState["key_4"] = "" // deleted

	// Write 10 more keys to force more SSTable flushes
	for i := 100; i < 110; i++ {
		k := fmt.Sprintf("key_%d", i)
		v := fmt.Sprintf("val_%d", i)
		if err := engine.Put(db.Key(k), db.Value(v)); err != nil {
			t.Fatalf("failed to put %s: %v", k, err)
		}
		expectedState[k] = v
	}

	// Abrupt crash
	engine.Close()

	// Stage 4: Recover and verify final state
	recoveredEngine, err := storage.NewEngine(tmpDir)
	if err != nil {
		t.Fatalf("failed to recover: %v", err)
	}
	defer recoveredEngine.Close()

	verifyState(t, recoveredEngine, expectedState)
}

// Helper to verify that storage Engine state matches the expected state map.
func verifyState(t *testing.T, engine *storage.Engine, expected map[string]string) {
	for k, expectedVal := range expected {
		val, err := engine.Read(db.Key(k))
		if expectedVal == "" {
			if err != db.ErrKeyNotFound {
				t.Errorf("expected key %s to be deleted, but read returned %v (val=%s)", k, err, val)
			}
		} else {
			if err != nil {
				t.Errorf("expected key %s to exist, got error: %v", k, err)
			}
			if !bytes.Equal(val, db.Value(expectedVal)) {
				t.Errorf("value mismatch for key %s: expected %s, got %s", k, expectedVal, val)
			}
		}
	}
}
