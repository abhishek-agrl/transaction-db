package storage

import (
	"encoding/binary"
	"errors"
	"io"
	"os"
	"sync"

	db "github.com/abhishek-agrl/transaction-db"
)

var (
	ErrCorruptRecord = errors.New("wal: corrupt record detected")
)

// WAL provides sequential persistence and guarantees durability for the database.
type WAL struct {
	mu   sync.Mutex
	file *os.File
	path string
}

// OpenWAL opens or creates a Write-Ahead Log file for writing at the specified path.
// It uses os.O_APPEND | os.O_CREATE | os.O_WRONLY as required.
func OpenWAL(path string) (*WAL, error) {
	file, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}

	return &WAL{
		file: file,
		path: path,
	}, nil
}

// Write appends a key-value record to the log.
// A deletion is marked by setting value length to -1 (indicating a tombstone).
// It calls file.Sync() to guarantee persistence before returning.
func (w *WAL) Write(key db.Key, value db.Value, isDeleted bool) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	keyLen := int32(len(key))
	var valLen int32
	if isDeleted {
		valLen = -1
	} else {
		valLen = int32(len(value))
	}

	// Calculate buffer size: 4 bytes (KeyLen) + len(Key) + 4 bytes (ValLen) + len(Val)
	valPayloadSize := valLen
	if isDeleted {
		valPayloadSize = 0
	}
	bufSize := 8 + int(keyLen) + int(valPayloadSize)
	buf := make([]byte, bufSize)

	// Encode Key Length and Key Bytes
	binary.BigEndian.PutUint32(buf[0:4], uint32(keyLen))
	copy(buf[4:4+keyLen], key)

	// Encode Value Length and Value Bytes
	binary.BigEndian.PutUint32(buf[4+keyLen:8+keyLen], uint32(valLen))
	if !isDeleted && valLen > 0 {
		copy(buf[8+keyLen:], value)
	}

	// Write buffer to file
	if _, err := w.file.Write(buf); err != nil {
		return err
	}

	// Force fsync to disk to prevent data loss on crash
	return w.file.Sync()
}

// WriteBatch serializes multiple key-value entries into a single buffer, writes
// them to the file in a single write operation, and calls file.Sync() once.
func (w *WAL) WriteBatch(keys []db.Key, values []db.Value) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Calculate total buffer size
	totalSize := 0
	for i := range keys {
		totalSize += 8 + len(keys[i]) + len(values[i])
	}

	buf := make([]byte, totalSize)
	offset := 0
	for i := range keys {
		keyLen := int32(len(keys[i]))
		valLen := int32(len(values[i]))

		// Encode Key Length and Key Bytes
		binary.BigEndian.PutUint32(buf[offset:offset+4], uint32(keyLen))
		copy(buf[offset+4:offset+4+int(keyLen)], keys[i])
		offset += 4 + int(keyLen)

		// Encode Value Length and Value Bytes
		binary.BigEndian.PutUint32(buf[offset:offset+4], uint32(valLen))
		copy(buf[offset+4:offset+4+int(valLen)], values[i])
		offset += 4 + int(valLen)
	}

	// Write batch buffer to file
	if _, err := w.file.Write(buf); err != nil {
		return err
	}

	// Force fsync to disk
	return w.file.Sync()
}

// Recover reads the WAL linearly from the beginning, populates the provided SkipList,
// and returns the total number of records recovered.
func Recover(path string, sl *SkipList) (int, error) {
	file, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil // No file exists, nothing to recover
		}
		return 0, err
	}
	defer file.Close()

	count := 0
	headerBuf := make([]byte, 4)

	for {
		// 1. Read Key Length
		_, err := io.ReadFull(file, headerBuf)
		if err == io.EOF {
			break // Clean EOF (no more records)
		}
		if err == io.ErrUnexpectedEOF {
			return count, ErrCorruptRecord // Truncated header
		}
		if err != nil {
			return count, err
		}
		keyLen := int32(binary.BigEndian.Uint32(headerBuf))
		if keyLen < 0 {
			return count, ErrCorruptRecord
		}

		// 2. Read Key Bytes
		key := make([]byte, keyLen)
		_, err = io.ReadFull(file, key)
		if err != nil {
			return count, ErrCorruptRecord
		}

		// 3. Read Value Length
		_, err = io.ReadFull(file, headerBuf)
		if err != nil {
			return count, ErrCorruptRecord
		}
		valLen := int32(binary.BigEndian.Uint32(headerBuf))

		var val []byte
		isDeleted := valLen == -1

		// 4. Read Value Bytes (if not deleted and has length > 0)
		if !isDeleted {
			if valLen < 0 {
				return count, ErrCorruptRecord
			}
			val = make([]byte, valLen)
			if valLen > 0 {
				_, err = io.ReadFull(file, val)
				if err != nil {
					return count, ErrCorruptRecord
				}
			}
		}

		// 5. Populate in-memory SkipList
		sl.Insert(key, val, isDeleted)
		count++
	}

	return count, nil
}

// Close closes the active WAL file handle.
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.file == nil {
		return nil
	}
	err := w.file.Close()
	w.file = nil
	return err
}
