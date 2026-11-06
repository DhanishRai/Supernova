package killitdb

import (
	"fmt"
	"io"
)

// Put inserts or updates a key-value pair.
//
// Concurrency:
// Acquires the global database write lock.
//
// Durability & Consistency:
// The operation is encoded as a single-operation record, appended to the data file,
// and synced to physical storage before updating the in-memory index.
func (db *DB) Put(key, value []byte) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.closed {
		return ErrDatabaseClosed
	}
	if db.unusable {
		return ErrDatabaseUnusable
	}

	keyLen := len(key)
	if keyLen < MinKeyLen || keyLen > MaxKeyLen {
		return ErrInvalidKeyLength
	}

	valLen := len(value)
	if valLen > MaxValueLen {
		return ErrInvalidValueLength
	}

	ops := []Operation{
		{
			Type:  OpPut,
			Key:   key,
			Value: value,
		},
	}

	// appendRecord verifies capacity, encodes, writes via WriteAt, calls Sync,
	// and advances db.size. It does not touch db.index.
	_, indexOps, err := db.appendRecord(ops)
	if err != nil {
		return err
	}

	// Update in-memory index ONLY after record is durably synced.
	db.index.ApplyBatch(indexOps)

	return nil
}

// Delete removes a key by appending a tombstone record to disk.
//
// Concurrency:
// Acquires the global database write lock.
//
// Durability & Consistency:
// A DELETE record is appended and synced before the key is removed from the in-memory index.
// Deleting a non-existent key still appends a durable tombstone record as required by append-only logging.
func (db *DB) Delete(key []byte) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.closed {
		return ErrDatabaseClosed
	}
	if db.unusable {
		return ErrDatabaseUnusable
	}

	keyLen := len(key)
	if keyLen < MinKeyLen || keyLen > MaxKeyLen {
		return ErrInvalidKeyLength
	}

	ops := []Operation{
		{
			Type:  OpDelete,
			Key:   key,
			Value: nil,
		},
	}

	_, indexOps, err := db.appendRecord(ops)
	if err != nil {
		return err
	}

	// Apply deletion to in-memory index only after Sync succeeds.
	db.index.ApplyBatch(indexOps)

	return nil
}

// Get retrieves the value associated with a key.
//
// Concurrency:
// Acquires the global database read lock (RLock).
//
// Integrity:
// Returns a freshly allocated copy of the value read directly from disk via ReadAt.
// An empty value ([]byte{}) is returned for keys stored with length 0 and is distinguished
// from missing keys, which return ErrKeyNotFound.
func (db *DB) Get(key []byte) ([]byte, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	if db.closed {
		return nil, ErrDatabaseClosed
	}
	if db.unusable {
		return nil, ErrDatabaseUnusable
	}

	keyLen := len(key)
	if keyLen < MinKeyLen || keyLen > MaxKeyLen {
		return nil, ErrInvalidKeyLength
	}

	ent, found := db.index.Get(string(key))
	if !found {
		return nil, ErrKeyNotFound
	}

	// Empty value optimization: return non-nil empty slice without disk read
	if ent.valueLen == 0 {
		return []byte{}, nil
	}

	// Allocate new slice to ensure caller never receives internal mutable memory.
	valBuf := make([]byte, ent.valueLen)
	n, err := db.file.ReadAt(valBuf, ent.valueOffset)
	if err != nil && err != io.EOF {
		return nil, fmt.Errorf("read value failed: %w", err)
	}
	if n != int(ent.valueLen) {
		return nil, fmt.Errorf("short read for value at offset %d: expected %d bytes, got %d", ent.valueOffset, ent.valueLen, n)
	}

	return valBuf, nil
}
