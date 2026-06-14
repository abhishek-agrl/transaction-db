package storage

import (
	"bytes"
	"math/rand"
	"sync"
	"time"

	db "github.com/abhishek-agrl/transaction-db"
)

// SkipNode represents a single element at all heights in the Skip List.
type SkipNode struct {
	key       db.Key
	value     db.Value
	isDeleted bool
	next      []*SkipNode // next[i] points to the next node at level i (0-indexed)
}

// Key returns the node's key.
func (n *SkipNode) Key() db.Key {
	return n.key
}

// Value returns the node's value.
func (n *SkipNode) Value() db.Value {
	return n.value
}

// IsDeleted returns whether this node represents a tombstone.
func (n *SkipNode) IsDeleted() bool {
	return n.isDeleted
}

// SkipList is a thread-safe, probabilistic, ordered in-memory index structure (Memtable).
type SkipList struct {
	mu       sync.RWMutex
	head     *SkipNode
	maxLevel int
	level    int
	p        float64
	size     int
	rng      *rand.Rand
}

// NewSkipList creates and initializes a new SkipList.
func NewSkipList(maxLevel int, p float64) *SkipList {
	if maxLevel <= 0 {
		maxLevel = 16
	}
	if p <= 0 || p >= 1 {
		p = 0.5
	}

	head := &SkipNode{
		next: make([]*SkipNode, maxLevel),
	}

	return &SkipList{
		head:     head,
		maxLevel: maxLevel,
		level:    1,
		p:        p,
		rng:      rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

// Size returns the total number of items stored in the skip list.
func (sl *SkipList) Size() int {
	sl.mu.RLock()
	defer sl.mu.RUnlock()
	return sl.size
}

// randomHeight generates a height for a new node probabilistically.
// This is called inside Insert while holding the write lock.
func (sl *SkipList) randomHeight() int {
	height := 1
	for height < sl.maxLevel && sl.rng.Float64() < sl.p {
		height++
	}
	return height
}

// Insert inserts or updates a key-value pair into the skip list.
// Returns true if a new node was created, or false if an existing key was updated in-place.
func (sl *SkipList) Insert(key db.Key, value db.Value, isDeleted bool) bool {
	sl.mu.Lock()
	defer sl.mu.Unlock()

	// Track the preceding node at each level of the skip list
	update := make([]*SkipNode, sl.maxLevel)
	curr := sl.head

	// Traverse the skip list levels down to 0
	for i := sl.level - 1; i >= 0; i-- {
		for curr.next[i] != nil && bytes.Compare(curr.next[i].key, key) < 0 {
			curr = curr.next[i]
		}
		update[i] = curr
	}

	// Look at the candidate node at the base level (level 0)
	curr = curr.next[0]

	// If the key already exists, perform an in-place update
	if curr != nil && bytes.Equal(curr.key, key) {
		curr.value = value
		curr.isDeleted = isDeleted
		return false
	}

	// Determine height for the new node
	h := sl.randomHeight()
	if h > sl.level {
		for i := sl.level; i < h; i++ {
			update[i] = sl.head
		}
		sl.level = h
	}

	// Create and link the new node
	newNode := &SkipNode{
		key:       key,
		value:     value,
		isDeleted: isDeleted,
		next:      make([]*SkipNode, h),
	}

	for i := 0; i < h; i++ {
		newNode.next[i] = update[i].next[i]
		update[i].next[i] = newNode
	}

	sl.size++
	return true
}

// Search searches for a key in the skip list.
// Returns the matching node and a boolean indicating if it was found.
func (sl *SkipList) Search(key db.Key) (*SkipNode, bool) {
	sl.mu.RLock()
	defer sl.mu.RUnlock()

	curr := sl.head
	for i := sl.level - 1; i >= 0; i-- {
		for curr.next[i] != nil && bytes.Compare(curr.next[i].key, key) < 0 {
			curr = curr.next[i]
		}
	}

	curr = curr.next[0]
	if curr != nil && bytes.Equal(curr.key, key) {
		return curr, true
	}
	return nil, false
}

// RangeSearch retrieves all nodes in the sorted range [startKey, endKey] inclusive.
func (sl *SkipList) RangeSearch(startKey db.Key, endKey db.Key) []*SkipNode {
	sl.mu.RLock()
	defer sl.mu.RUnlock()

	curr := sl.head
	for i := sl.level - 1; i >= 0; i-- {
		for curr.next[i] != nil && bytes.Compare(curr.next[i].key, startKey) < 0 {
			curr = curr.next[i]
		}
	}

	curr = curr.next[0]

	var results []*SkipNode
	for curr != nil && bytes.Compare(curr.key, endKey) <= 0 {
		results = append(results, curr)
		curr = curr.next[0]
	}

	return results
}
