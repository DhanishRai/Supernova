package killitdb

import (
	"fmt"
	"io"
	"sort"
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

// KeyCount returns the total number of active keys in the database.
func (db *DB) KeyCount() (int, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	if db.closed {
		return 0, ErrDatabaseClosed
	}
	if db.unusable {
		return 0, ErrDatabaseUnusable
	}

	return db.index.Len(), nil
}

// Keys returns a sorted slice of all active keys in the database.
func (db *DB) Keys() ([]string, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	if db.closed {
		return nil, ErrDatabaseClosed
	}
	if db.unusable {
		return nil, ErrDatabaseUnusable
	}

	keys := db.index.Keys()
	sort.Strings(keys)
	return keys, nil
}

// ForEach executes fn for each active key-value pair in alphabetical order.
// The iteration stops and returns immediately if fn returns a non-nil error.
// The entire iteration is guarded under db.mu.RLock().
func (db *DB) ForEach(fn func(key string, val []byte) error) error {
	db.mu.RLock()
	defer db.mu.RUnlock()

	if db.closed {
		return ErrDatabaseClosed
	}
	if db.unusable {
		return ErrDatabaseUnusable
	}

	keys := db.index.Keys()
	sort.Strings(keys)

	for _, k := range keys {
		ent, found := db.index.Get(k)
		if !found {
			continue
		}

		var valBuf []byte
		if ent.valueLen > 0 {
			valBuf = make([]byte, ent.valueLen)
			n, err := db.file.ReadAt(valBuf, ent.valueOffset)
			if err != nil && err != io.EOF {
				return fmt.Errorf("read value failed for key %s: %w", k, err)
			}
			if n != int(ent.valueLen) {
				return fmt.Errorf("short read for value of key %s at offset %d: expected %d, got %d", k, ent.valueOffset, ent.valueLen, n)
			}
		} else {
			valBuf = []byte{}
		}

		if err := fn(k, valBuf); err != nil {
			return err
		}
	}

	return nil
}
