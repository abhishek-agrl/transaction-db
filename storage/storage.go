package storage

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	db "github.com/abhishek-agrl/transaction-db"
)

const (
	// DefaultMaxMemtableSize is the threshold of entries before a Memtable is flushed to an SSTable.
	DefaultMaxMemtableSize = 1000
)

// Engine implements the db.TransactionDB interface as a persistent LSM-Tree.
type Engine struct {
	mu              sync.RWMutex
	wal             *WAL
	memtable        *SkipList
	sstables        []*SSTable // Ordered from newest to oldest
	dir             string
	maxMemtableSize int
	nextSSTableID   int64
}

// Ensure Engine implements the db.TransactionDB interface at compile time.
var _ db.TransactionDB = (*Engine)(nil)

// NewEngine creates, recovers, and initializes a new storage engine instance.
// It automatically loads existing SSTables and replays the WAL log at startup.
func NewEngine(dir string) (*Engine, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}

	// 1. Find and load all existing SSTable files from disk
	files, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	var sstIDs []int64
	for _, f := range files {
		if !f.IsDir() && strings.HasPrefix(f.Name(), "sstable_") && strings.HasSuffix(f.Name(), ".db") {
			var id int64
			if _, err := fmt.Sscanf(f.Name(), "sstable_%d.db", &id); err == nil {
				sstIDs = append(sstIDs, id)
			}
		}
	}

	// Sort IDs descending (newest first) to prioritize fresh updates
	sort.Slice(sstIDs, func(i, j int) bool {
		return sstIDs[i] > sstIDs[j]
	})

	var sstables []*SSTable
	var maxID int64
	for _, id := range sstIDs {
		if id > maxID {
			maxID = id
		}
		path := filepath.Join(dir, fmt.Sprintf("sstable_%d.db", id))
		// Load SSTable sparse index (interval = 16)
		sst, err := LoadSSTable(path, id, 16)
		if err != nil {
			return nil, err
		}
		sstables = append(sstables, sst)
	}

	memtable := NewSkipList(16, 0.5)
	walPath := filepath.Join(dir, "wal.db")

	// 2. Replay active WAL log transactions
	if _, err := Recover(walPath, memtable); err != nil {
		return nil, err
	}

	// 3. Open WAL for write-only append operations
	wal, err := OpenWAL(walPath)
	if err != nil {
		return nil, err
	}

	return &Engine{
		dir:             dir,
		memtable:        memtable,
		wal:             wal,
		sstables:        sstables,
		maxMemtableSize: DefaultMaxMemtableSize,
		nextSSTableID:   maxID + 1,
	}, nil
}

// SetMaxMemtableSize overrides the default Memtable flush threshold.
// Useful for configuring small sizes in tests to force flush triggers.
func (e *Engine) SetMaxMemtableSize(size int) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.maxMemtableSize = size
}

// FlushActiveMemtable flushes the current active Memtable entries to a sorted SSTable on disk,
// updates the internal SSTable slice, truncates the WAL, and spins up a fresh WAL.
// Must be called with the Engine write lock held.
func (e *Engine) FlushActiveMemtable() error {
	if e.memtable.Size() == 0 {
		return nil
	}

	// 1. Gather all active Skip List nodes
	var entries []db.Entry
	var isDeleted []bool
	curr := e.memtable.head.next[0]
	for curr != nil {
		entries = append(entries, db.Entry{
			Key:   curr.key,
			Value: curr.value,
		})
		isDeleted = append(isDeleted, curr.isDeleted)
		curr = curr.next[0]
	}

	// 2. Write new SSTable
	sstableID := e.nextSSTableID
	e.nextSSTableID++
	sstablePath := filepath.Join(e.dir, fmt.Sprintf("sstable_%d.db", sstableID))

	sstable, err := WriteSSTable(sstablePath, sstableID, entries, isDeleted, 16)
	if err != nil {
		return err
	}

	// Prepend new SSTable to list (newest first)
	e.sstables = append([]*SSTable{sstable}, e.sstables...)

	// 3. Reset Memtable and clear active WAL
	e.memtable = NewSkipList(16, 0.5)

	if err := e.wal.Close(); err != nil {
		return err
	}

	walPath := filepath.Join(e.dir, "wal.db")
	if err := os.Remove(walPath); err != nil && !os.IsNotExist(err) {
		return err
	}

	// Open a fresh WAL
	wal, err := OpenWAL(walPath)
	if err != nil {
		return err
	}
	e.wal = wal

	return nil
}

// Put writes a key-value pair to the database.
func (e *Engine) Put(key db.Key, value db.Value) error {
	if len(key) == 0 {
		return db.ErrEmptyKey
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	// 1. Append transaction to WAL and sync
	if err := e.wal.Write(key, value, false); err != nil {
		return err
	}

	// 2. Insert to in-memory index
	e.memtable.Insert(key, value, false)

	// 3. Flush memtable if size threshold is exceeded
	if e.memtable.Size() >= e.maxMemtableSize {
		if err := e.FlushActiveMemtable(); err != nil {
			return err
		}
	}

	return nil
}

// Read retrieves the value associated with a key.
func (e *Engine) Read(key db.Key) (db.Value, error) {
	if len(key) == 0 {
		return nil, db.ErrEmptyKey
	}

	e.mu.RLock()
	defer e.mu.RUnlock()

	// 1. Query the active in-memory Memtable
	node, found := e.memtable.Search(key)
	if found {
		if node.IsDeleted() {
			return nil, db.ErrKeyNotFound
		}
		return node.Value(), nil
	}

	// 2. Query the SSTables from newest to oldest
	for _, sst := range e.sstables {
		val, sstFound, sstDeleted, err := sst.Search(key)
		if err != nil {
			return nil, err
		}
		if sstFound {
			if sstDeleted {
				return nil, db.ErrKeyNotFound
			}
			return val, nil
		}
	}

	return nil, db.ErrKeyNotFound
}

// entryState keeps track of merged values and deletion status during range scanning.
type entryState struct {
	value     db.Value
	isDeleted bool
}

// ReadKeyRange retrieves all active key-value pairs within a sorted range [startKey, endKey).
func (e *Engine) ReadKeyRange(startKey db.Key, endKey db.Key) ([]db.Entry, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	merged := make(map[string]entryState)

	// 1. Scan SSTables from oldest to newest to let newer entries overwrite naturally
	for i := len(e.sstables) - 1; i >= 0; i-- {
		sstEntries, sstDeleted, err := e.sstables[i].RangeScan(startKey, endKey)
		if err != nil {
			return nil, err
		}
		for j, entry := range sstEntries {
			merged[string(entry.Key)] = entryState{
				value:     entry.Value,
				isDeleted: sstDeleted[j],
			}
		}
	}

	// 2. Scan active Memtable (highest precedence)
	memNodes := e.memtable.RangeSearch(startKey, endKey)
	for _, node := range memNodes {
		// Enforce exclusive endKey boundary
		if bytes.Equal(node.Key(), endKey) {
			continue
		}
		merged[string(node.Key())] = entryState{
			value:     node.Value(),
			isDeleted: node.IsDeleted(),
		}
	}

	// 3. Collect and sort all keys lexicographically
	keys := make([]db.Key, 0, len(merged))
	for k := range merged {
		keys = append(keys, db.Key(k))
	}

	sort.Slice(keys, func(i, j int) bool {
		return bytes.Compare(keys[i], keys[j]) < 0
	})

	// 4. Construct response, omitting tombstones
	var results []db.Entry
	for _, k := range keys {
		state := merged[string(k)]
		if state.isDeleted {
			continue
		}
		results = append(results, db.Entry{
			Key:   k,
			Value: state.value,
		})
	}

	return results, nil
}

// BatchPut atomically or efficiently writes a collection of keys and values.
func (e *Engine) BatchPut(keys []db.Key, values []db.Value) error {
	if len(keys) != len(values) {
		return db.ErrBatchLengthMismatch
	}
	if len(keys) == 0 {
		return nil
	}

	for _, k := range keys {
		if len(k) == 0 {
			return db.ErrEmptyKey
		}
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	// 1. Write batch to the WAL in one operation
	if err := e.wal.WriteBatch(keys, values); err != nil {
		return err
	}

	// 2. Insert sequentially to the active Memtable
	for i := range keys {
		e.memtable.Insert(keys[i], values[i], false)
	}

	// 3. Flush Memtable if threshold is exceeded
	if e.memtable.Size() >= e.maxMemtableSize {
		if err := e.FlushActiveMemtable(); err != nil {
			return err
		}
	}

	return nil
}

// Delete removes a key from the database.
func (e *Engine) Delete(key db.Key) error {
	if len(key) == 0 {
		return db.ErrEmptyKey
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	// 1. Write a tombstone record to the WAL
	if err := e.wal.Write(key, nil, true); err != nil {
		return err
	}

	// 2. Insert tombstone into Memtable
	e.memtable.Insert(key, nil, true)

	// 3. Flush Memtable if threshold is exceeded
	if e.memtable.Size() >= e.maxMemtableSize {
		if err := e.FlushActiveMemtable(); err != nil {
			return err
		}
	}

	return nil
}

// Close safely shuts down the storage engine, closing active WAL handles.
func (e *Engine) Close() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.wal != nil {
		return e.wal.Close()
	}
	return nil
}
