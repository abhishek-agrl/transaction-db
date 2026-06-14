package storage

import (
	"bytes"
	"encoding/binary"
	"io"
	"os"
	"sort"

	db "github.com/abhishek-agrl/transaction-db"
)

// IndexEntry represents a sparse index pointer mapping a key to its file offset.
type IndexEntry struct {
	key    db.Key
	offset int64
}

// SSTable represents a Sorted String Table file on disk with a sparse index in RAM.
type SSTable struct {
	id          int64
	path        string
	sparseIndex []IndexEntry
}

// WriteSSTable serializes a sorted slice of database entries to a new SSTable file,
// building and returning an in-memory sparse index at the configured block interval.
func WriteSSTable(path string, id int64, entries []db.Entry, isDeleted []bool, sparseInterval int) (*SSTable, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var sparseIndex []IndexEntry
	var offset int64

	for i, entry := range entries {
		keyLen := int32(len(entry.Key))
		var valLen int32
		if isDeleted[i] {
			valLen = -1
		} else {
			valLen = int32(len(entry.Value))
		}

		// Save a sparse index pointer at configured intervals (e.g. every 16th key)
		if i%sparseInterval == 0 {
			keyCopy := make([]byte, len(entry.Key))
			copy(keyCopy, entry.Key)
			sparseIndex = append(sparseIndex, IndexEntry{
				key:    keyCopy,
				offset: offset,
			})
		}

		valSize := valLen
		if valLen < 0 {
			valSize = 0
		}
		bufSize := 8 + int(keyLen) + int(valSize)
		buf := make([]byte, bufSize)

		binary.BigEndian.PutUint32(buf[0:4], uint32(keyLen))
		copy(buf[4:4+keyLen], entry.Key)
		binary.BigEndian.PutUint32(buf[4+keyLen:8+keyLen], uint32(valLen))
		if valLen > 0 {
			copy(buf[8+keyLen:], entry.Value)
		}

		n, err := file.Write(buf)
		if err != nil {
			return nil, err
		}
		offset += int64(n)
	}

	if err := file.Sync(); err != nil {
		return nil, err
	}

	return &SSTable{
		id:          id,
		path:        path,
		sparseIndex: sparseIndex,
	}, nil
}

// LoadSSTable opens an existing SSTable file and parses its contents to reconstruct the sparse index in RAM.
// It skips reading values, ensuring fast startup recovery and low memory usage.
func LoadSSTable(path string, id int64, sparseInterval int) (*SSTable, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var sparseIndex []IndexEntry
	var offset int64
	headerBuf := make([]byte, 4)
	count := 0

	for {
		// 1. Read Key Length
		_, err := io.ReadFull(file, headerBuf)
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			break
		}
		if err != nil {
			return nil, err
		}
		keyLen := int32(binary.BigEndian.Uint32(headerBuf))

		// 2. Read Key Bytes
		key := make([]byte, keyLen)
		_, err = io.ReadFull(file, key)
		if err != nil {
			return nil, err
		}

		// 3. Read Value Length
		_, err = io.ReadFull(file, headerBuf)
		if err != nil {
			return nil, err
		}
		valLen := int32(binary.BigEndian.Uint32(headerBuf))

		valSize := valLen
		if valLen < 0 {
			valSize = 0
		}

		// 4. Record sparse index pointer
		if count%sparseInterval == 0 {
			sparseIndex = append(sparseIndex, IndexEntry{
				key:    key,
				offset: offset,
			})
		}

		// 5. Seek past value payload to save disk reads
		if valSize > 0 {
			if _, err = file.Seek(int64(valSize), io.SeekCurrent); err != nil {
				return nil, err
			}
		}

		offset += 8 + int64(keyLen) + int64(valSize)
		count++
	}

	return &SSTable{
		id:          id,
		path:        path,
		sparseIndex: sparseIndex,
	}, nil
}

// Search searches for a key in the SSTable file.
// It uses binary search on the sparse index to find the candidate block, seeks there, and scans sequentially.
func (s *SSTable) Search(key db.Key) (db.Value, bool, bool, error) {
	file, err := os.Open(s.path)
	if err != nil {
		return nil, false, false, err
	}
	defer file.Close()

	if len(s.sparseIndex) == 0 {
		return nil, false, false, nil
	}

	// Binary search sparse index to find the block containing the key.
	// Find the first index entry whose key is greater than our search key.
	idx := sort.Search(len(s.sparseIndex), func(i int) bool {
		return bytes.Compare(s.sparseIndex[i].key, key) > 0
	})

	if idx == 0 {
		// Search key is smaller than the first key in the SSTable
		if bytes.Equal(s.sparseIndex[0].key, key) {
			idx = 1
		} else {
			return nil, false, false, nil
		}
	}

	// Seek to the starting offset of the candidate block
	startOffset := s.sparseIndex[idx-1].offset
	if _, err = file.Seek(startOffset, io.SeekStart); err != nil {
		return nil, false, false, err
	}

	headerBuf := make([]byte, 4)

	// Scan sequentially from the start of the block
	for {
		_, err := io.ReadFull(file, headerBuf)
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			break
		}
		if err != nil {
			return nil, false, false, err
		}
		keyLen := int32(binary.BigEndian.Uint32(headerBuf))

		currKey := make([]byte, keyLen)
		if _, err = io.ReadFull(file, currKey); err != nil {
			return nil, false, false, err
		}

		if _, err = io.ReadFull(file, headerBuf); err != nil {
			return nil, false, false, err
		}
		valLen := int32(binary.BigEndian.Uint32(headerBuf))

		cmp := bytes.Compare(currKey, key)
		if cmp == 0 {
			// Found matching key
			isDeleted := valLen == -1
			if isDeleted {
				return nil, true, true, nil // found but marked deleted
			}
			val := make([]byte, valLen)
			if _, err = io.ReadFull(file, val); err != nil {
				return nil, false, false, err
			}
			return val, true, false, nil
		} else if cmp > 0 {
			// Key not found (encountered a larger key in sorted file)
			break
		} else {
			// Seek past value
			valSize := valLen
			if valLen < 0 {
				valSize = 0
			}
			if valSize > 0 {
				if _, err = file.Seek(int64(valSize), io.SeekCurrent); err != nil {
					return nil, false, false, err
				}
			}
		}
	}

	return nil, false, false, nil
}

// RangeScan reads all entries within [startKey, endKey] from the SSTable file.
func (s *SSTable) RangeScan(startKey, endKey db.Key) ([]db.Entry, []bool, error) {
	file, err := os.Open(s.path)
	if err != nil {
		return nil, nil, err
	}
	defer file.Close()

	if len(s.sparseIndex) == 0 {
		return nil, nil, nil
	}

	// Find the index block that could contain startKey
	idx := sort.Search(len(s.sparseIndex), func(i int) bool {
		return bytes.Compare(s.sparseIndex[i].key, startKey) > 0
	})

	if idx > 0 {
		idx--
	}

	startOffset := s.sparseIndex[idx].offset
	if _, err = file.Seek(startOffset, io.SeekStart); err != nil {
		return nil, nil, err
	}

	var entries []db.Entry
	var isDeleted []bool
	headerBuf := make([]byte, 4)

	for {
		_, err := io.ReadFull(file, headerBuf)
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			break
		}
		if err != nil {
			return nil, nil, err
		}
		keyLen := int32(binary.BigEndian.Uint32(headerBuf))

		currKey := make([]byte, keyLen)
		if _, err = io.ReadFull(file, currKey); err != nil {
			return nil, nil, err
		}

		if _, err = io.ReadFull(file, headerBuf); err != nil {
			return nil, nil, err
		}
		valLen := int32(binary.BigEndian.Uint32(headerBuf))

		// Stop scanning if key is greater than or equal to the endKey (exclusive upper bound)
		if bytes.Compare(currKey, endKey) >= 0 {
			break
		}

		deleted := valLen == -1
		var val []byte
		if !deleted && valLen > 0 {
			val = make([]byte, valLen)
			if _, err = io.ReadFull(file, val); err != nil {
				return nil, nil, err
			}
		}

		if bytes.Compare(currKey, startKey) >= 0 {
			entries = append(entries, db.Entry{Key: currKey, Value: val})
			isDeleted = append(isDeleted, deleted)
		}
	}

	return entries, isDeleted, nil
}
