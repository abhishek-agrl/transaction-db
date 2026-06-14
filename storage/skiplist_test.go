package storage

import (
	"bytes"
	"fmt"
	"sync"
	"testing"

	db "github.com/abhishek-agrl/transaction-db"
)

// TestSkipListBasic verifies standard insert, point query, and in-place updates.
func TestSkipListBasic(t *testing.T) {
	sl := NewSkipList(8, 0.5)

	// Test search on empty list
	if _, found := sl.Search(db.Key("foo")); found {
		t.Error("expected not found, but key was found")
	}

	// Insert items
	if !sl.Insert(db.Key("key1"), db.Value("val1"), false) {
		t.Error("expected true (new key), got false")
	}
	if !sl.Insert(db.Key("key2"), db.Value("val2"), false) {
		t.Error("expected true (new key), got false")
	}

	// Verify size
	if sl.Size() != 2 {
		t.Errorf("expected size 2, got %d", sl.Size())
	}

	// Retrieve items
	node, found := sl.Search(db.Key("key1"))
	if !found {
		t.Fatal("expected key1 to be found")
	}
	if !bytes.Equal(node.Value(), db.Value("val1")) {
		t.Errorf("expected val1, got %s", node.Value())
	}

	// Update items in-place
	if sl.Insert(db.Key("key1"), db.Value("newval1"), false) {
		t.Error("expected false (update), got true")
	}

	// Verify size remains same after update
	if sl.Size() != 2 {
		t.Errorf("expected size 2, got %d", sl.Size())
	}

	node, found = sl.Search(db.Key("key1"))
	if !found {
		t.Fatal("expected key1 to be found")
	}
	if !bytes.Equal(node.Value(), db.Value("newval1")) {
		t.Errorf("expected newval1, got %s", node.Value())
	}
}

// TestSkipListRangeSearch verifies sorted range scanning with inclusive bounds.
func TestSkipListRangeSearch(t *testing.T) {
	sl := NewSkipList(8, 0.5)

	keys := []string{"e", "c", "a", "d", "b"}
	for _, k := range keys {
		sl.Insert(db.Key(k), db.Value("val_"+k), false)
	}

	// Search range [b, d]
	nodes := sl.RangeSearch(db.Key("b"), db.Key("d"))
	if len(nodes) != 3 {
		t.Fatalf("expected 3 nodes, got %d", len(nodes))
	}

	expected := []string{"b", "c", "d"}
	for i, node := range nodes {
		if !bytes.Equal(node.Key(), db.Key(expected[i])) {
			t.Errorf("expected key %s, got %s", expected[i], node.Key())
		}
	}

	// Search range [x, z] (no matches)
	nodes = sl.RangeSearch(db.Key("x"), db.Key("z"))
	if len(nodes) != 0 {
		t.Errorf("expected 0 nodes, got %d", len(nodes))
	}

	// Search range [a, f] (all matches)
	nodes = sl.RangeSearch(db.Key("a"), db.Key("f"))
	if len(nodes) != 5 {
		t.Errorf("expected 5 nodes, got %d", len(nodes))
	}
}

// TestSkipListTombstone verifies tombstone insertion and retrieval.
func TestSkipListTombstone(t *testing.T) {
	sl := NewSkipList(8, 0.5)

	sl.Insert(db.Key("k1"), db.Value("v1"), false)
	sl.Insert(db.Key("k1"), nil, true) // Tombstone

	node, found := sl.Search(db.Key("k1"))
	if !found {
		t.Fatal("expected k1 to be found (even if tombstoned)")
	}
	if !node.IsDeleted() {
		t.Error("expected node to be marked deleted (tombstone)")
	}
}

// TestSkipListConcurrency validates concurrent operations without data races.
func TestSkipListConcurrency(t *testing.T) {
	sl := NewSkipList(12, 0.5)
	var wg sync.WaitGroup
	workers := 10
	opsPerWorker := 100

	// Concurrent Inserts
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for j := 0; j < opsPerWorker; j++ {
				key := db.Key(fmt.Sprintf("w_%d_k_%d", workerID, j))
				val := db.Value(fmt.Sprintf("v_%d", j))
				sl.Insert(key, val, false)
			}
		}(i)
	}

	// Concurrent Searches
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for j := 0; j < opsPerWorker; j++ {
				key := db.Key(fmt.Sprintf("w_%d_k_%d", workerID, j))
				sl.Search(key)
			}
		}(i)
	}

	wg.Wait()

	expectedSize := workers * opsPerWorker
	if sl.Size() != expectedSize {
		t.Errorf("expected size %d, got %d", expectedSize, sl.Size())
	}
}
