package transactiondb

import "errors"

// Common errors returned by the storage engine.
var (
	ErrKeyNotFound         = errors.New("key not found")
	ErrEmptyKey            = errors.New("key cannot be empty")
	ErrBatchLengthMismatch = errors.New("batch keys and values lengths must be equal")
)

// Key represents the primary identifier in the database.
// It is defined as a byte slice to avoid extra string allocations.
type Key []byte

// Value represents the data payload associated with a Key.
// It is defined as a byte slice to avoid extra string allocations.
type Value []byte

// Entry represents a key-value pair.
type Entry struct {
	Key   Key
	Value Value
}

// TransactionDB defines the core interface for the Key/Value storage engine.
type TransactionDB interface {
	// Put writes a key-value pair to the database.
	Put(key Key, value Value) error

	// Read retrieves the value associated with a key.
	// Returns ErrKeyNotFound if the key does not exist.
	Read(key Key) (Value, error)

	// ReadKeyRange retrieves all key-value pairs within a sorted range [startKey, endKey).
	// The range is startKey inclusive and endKey exclusive.
	ReadKeyRange(startKey Key, endKey Key) ([]Entry, error)

	// BatchPut atomically or efficiently writes a collection of keys and values.
	// The length of keys and values must match; otherwise, ErrBatchLengthMismatch is returned.
	BatchPut(keys []Key, values []Value) error

	// Delete removes a key from the database.
	Delete(key Key) error
}
