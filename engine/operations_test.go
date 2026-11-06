package killitdb

import (
	"bytes"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
)

func TestOperations_PutThenGet(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(filepath.Join(dir, "test.db"), 1024*1024)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer db.Close()

	key := []byte("user:101")
	val := []byte("Alice Smith")

	if err := db.Put(key, val); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	got, err := db.Get(key)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if !bytes.Equal(got, val) {
		t.Fatalf("expected '%s', got '%s'", string(val), string(got))
	}

	// Verify GET returns independent memory (mutating returned slice does not corrupt subsequent GETs)
	got[0] = 'X'
	got2, err := db.Get(key)
	if err != nil {
		t.Fatalf("second Get failed: %v", err)
	}
	if string(got2) != "Alice Smith" {
		t.Fatalf("GET returned mutable internal memory: expected 'Alice Smith', got '%s'", string(got2))
	}
}

func TestOperations_UpdateExistingKey(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(filepath.Join(dir, "test.db"), 1024*1024)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer db.Close()

	key := []byte("status")

	if err := db.Put(key, []byte("pending")); err != nil {
		t.Fatalf("initial Put failed: %v", err)
	}

	v1, err := db.Get(key)
	if err != nil || string(v1) != "pending" {
		t.Fatalf("expected 'pending', got '%s' (err=%v)", string(v1), err)
	}

	// Update key
	if err := db.Put(key, []byte("completed-successfully")); err != nil {
		t.Fatalf("update Put failed: %v", err)
	}

	v2, err := db.Get(key)
	if err != nil || string(v2) != "completed-successfully" {
		t.Fatalf("expected 'completed-successfully', got '%s' (err=%v)", string(v2), err)
	}
}

func TestOperations_DeleteExistingKey(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(filepath.Join(dir, "test.db"), 1024*1024)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer db.Close()

	key := []byte("session:abc")
	if err := db.Put(key, []byte("token-123")); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	if err := db.Delete(key); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	_, err = db.Get(key)
	if !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("expected ErrKeyNotFound after Delete, got %v", err)
	}
}

func TestOperations_DeleteMissingKey(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(filepath.Join(dir, "test.db"), 1024*1024)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer db.Close()

	key := []byte("nonexistent-key")

	// Deleting a missing key should succeed and write a tombstone record
	oldSize := db.Size()
	if err := db.Delete(key); err != nil {
		t.Fatalf("Delete on missing key failed: %v", err)
	}

	if db.Size() <= oldSize {
		t.Fatalf("expected file size to grow with tombstone record: old=%d, new=%d", oldSize, db.Size())
	}

	_, err = db.Get(key)
	if !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("expected ErrKeyNotFound, got %v", err)
	}
}

func TestOperations_GetMissingKey(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(filepath.Join(dir, "test.db"), 1024*1024)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer db.Close()

	_, err = db.Get([]byte("does-not-exist"))
	if !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("expected ErrKeyNotFound, got %v", err)
	}
}

func TestOperations_EmptyValue(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(filepath.Join(dir, "test.db"), 1024*1024)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer db.Close()

	emptyKey := []byte("empty-val-key")
	if err := db.Put(emptyKey, []byte{}); err != nil {
		t.Fatalf("Put with empty value failed: %v", err)
	}

	val, err := db.Get(emptyKey)
	if err != nil {
		t.Fatalf("Get on empty value key failed: %v", err)
	}
	if len(val) != 0 {
		t.Fatalf("expected 0-length value, got %d bytes", len(val))
	}

	// Make sure missing key returns ErrKeyNotFound and is not confused with empty value
	_, err = db.Get([]byte("different-missing-key"))
	if !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("expected ErrKeyNotFound for missing key, got %v", err)
	}
}

func TestOperations_MaxKeyAndValueSizes(t *testing.T) {
	dir := t.TempDir()
	// Allow 5 MB file size
	db, err := Open(filepath.Join(dir, "test.db"), 5*1024*1024)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer db.Close()

	maxKey := bytes.Repeat([]byte("k"), MaxKeyLen)   // 1024 bytes
	maxVal := bytes.Repeat([]byte("v"), MaxValueLen) // 1 MB (1,048,576 bytes)

	if err := db.Put(maxKey, maxVal); err != nil {
		t.Fatalf("Put with max key and value failed: %v", err)
	}

	got, err := db.Get(maxKey)
	if err != nil {
		t.Fatalf("Get with max key failed: %v", err)
	}

	if len(got) != MaxValueLen {
		t.Fatalf("expected value length %d, got %d", MaxValueLen, len(got))
	}
	if !bytes.Equal(got, maxVal) {
		t.Fatalf("retrieved max value does not match written value")
	}
}

func TestOperations_InvalidKeyAndValueSizes(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(filepath.Join(dir, "test.db"), 1024*1024)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer db.Close()

	// Zero-length key
	if err := db.Put([]byte{}, []byte("val")); !errors.Is(err, ErrInvalidKeyLength) {
		t.Fatalf("expected ErrInvalidKeyLength for 0-len key, got %v", err)
	}
	if err := db.Delete([]byte{}); !errors.Is(err, ErrInvalidKeyLength) {
		t.Fatalf("expected ErrInvalidKeyLength for 0-len key on Delete, got %v", err)
	}
	if _, err := db.Get([]byte{}); !errors.Is(err, ErrInvalidKeyLength) {
		t.Fatalf("expected ErrInvalidKeyLength for 0-len key on Get, got %v", err)
	}

	// Key exceeding MaxKeyLen (1025 bytes)
	tooLongKey := bytes.Repeat([]byte("k"), MaxKeyLen+1)
	if err := db.Put(tooLongKey, []byte("val")); !errors.Is(err, ErrInvalidKeyLength) {
		t.Fatalf("expected ErrInvalidKeyLength for 1025-byte key, got %v", err)
	}
	if err := db.Delete(tooLongKey); !errors.Is(err, ErrInvalidKeyLength) {
		t.Fatalf("expected ErrInvalidKeyLength for 1025-byte key on Delete, got %v", err)
	}
	if _, err := db.Get(tooLongKey); !errors.Is(err, ErrInvalidKeyLength) {
		t.Fatalf("expected ErrInvalidKeyLength for 1025-byte key on Get, got %v", err)
	}

	// Value exceeding MaxValueLen (1MB + 1)
	tooLongVal := make([]byte, MaxValueLen+1)
	if err := db.Put([]byte("key"), tooLongVal); !errors.Is(err, ErrInvalidValueLength) {
		t.Fatalf("expected ErrInvalidValueLength for >1MB value, got %v", err)
	}
}

func TestOperations_ClosedOrUnusableDatabase(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(filepath.Join(dir, "test.db"), 1024*1024)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	key := []byte("key")
	val := []byte("val")

	if err := db.Put(key, val); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Close the DB
	if err := db.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Verify all operations reject when closed
	if err := db.Put(key, val); !errors.Is(err, ErrDatabaseClosed) {
		t.Fatalf("expected ErrDatabaseClosed on Put, got %v", err)
	}
	if err := db.Delete(key); !errors.Is(err, ErrDatabaseClosed) {
		t.Fatalf("expected ErrDatabaseClosed on Delete, got %v", err)
	}
	if _, err := db.Get(key); !errors.Is(err, ErrDatabaseClosed) {
		t.Fatalf("expected ErrDatabaseClosed on Get, got %v", err)
	}

	// Simulate unusable state
	unusableDB := &DB{
		unusable: true,
		maxBytes: 1024 * 1024,
		index:    newIndex(),
	}
	if err := unusableDB.Put(key, val); !errors.Is(err, ErrDatabaseUnusable) {
		t.Fatalf("expected ErrDatabaseUnusable on Put, got %v", err)
	}
	if err := unusableDB.Delete(key); !errors.Is(err, ErrDatabaseUnusable) {
		t.Fatalf("expected ErrDatabaseUnusable on Delete, got %v", err)
	}
	if _, err := unusableDB.Get(key); !errors.Is(err, ErrDatabaseUnusable) {
		t.Fatalf("expected ErrDatabaseUnusable on Get, got %v", err)
	}
}

func TestOperations_ConcurrentGetPut(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(filepath.Join(dir, "concurrent.db"), 10*1024*1024)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer db.Close()

	var wg sync.WaitGroup
	workers := 8
	opsPerWorker := 50

	// Pre-populate some keys
	for i := 0; i < 20; i++ {
		k := []byte(fmt.Sprintf("key-%d", i))
		v := []byte(fmt.Sprintf("initial-val-%d", i))
		if err := db.Put(k, v); err != nil {
			t.Fatalf("pre-populate failed: %v", err)
		}
	}

	// Writers
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for i := 0; i < opsPerWorker; i++ {
				k := []byte(fmt.Sprintf("key-%d", (workerID*10+i)%20))
				v := []byte(fmt.Sprintf("val-w%d-%d", workerID, i))
				_ = db.Put(k, v)
			}
		}(w)
	}

	// Readers
	for r := 0; r < workers; r++ {
		wg.Add(1)
		go func(readerID int) {
			defer wg.Done()
			for i := 0; i < opsPerWorker; i++ {
				k := []byte(fmt.Sprintf("key-%d", (readerID*5+i)%20))
				_, _ = db.Get(k)
			}
		}(r)
	}

	wg.Wait()
}

func TestOperations_KeysAndForEach(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(filepath.Join(dir, "keys_test.db"), 1024*1024)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer db.Close()

	// Initial empty count
	count, err := db.KeyCount()
	if err != nil || count != 0 {
		t.Fatalf("expected count 0, got %d (err=%v)", count, err)
	}

	// Insert in non-alphabetical order
	db.Put([]byte("banana"), []byte("yellow"))
	db.Put([]byte("apple"), []byte("red"))
	db.Put([]byte("cherry"), []byte("dark-red"))
	db.Put([]byte("empty-item"), []byte{})
	db.Put([]byte("to-delete"), []byte("temporary"))
	db.Delete([]byte("to-delete"))

	// Verify KeyCount
	count, err = db.KeyCount()
	if err != nil || count != 4 {
		t.Fatalf("expected count 4, got %d (err=%v)", count, err)
	}

	// Verify Keys() is sorted and excludes deleted key
	keys, err := db.Keys()
	if err != nil {
		t.Fatalf("Keys() failed: %v", err)
	}
	expectedKeys := []string{"apple", "banana", "cherry", "empty-item"}
	if len(keys) != len(expectedKeys) {
		t.Fatalf("expected %d keys, got %d", len(expectedKeys), len(keys))
	}
	for i, k := range expectedKeys {
		if keys[i] != k {
			t.Errorf("keys[%d]: expected %s, got %s", i, k, keys[i])
		}
	}

	// Verify ForEach() visits each key in order
	var visitedKeys []string
	var visitedVals []string
	err = db.ForEach(func(k string, v []byte) error {
		visitedKeys = append(visitedKeys, k)
		visitedVals = append(visitedVals, string(v))
		return nil
	})
	if err != nil {
		t.Fatalf("ForEach failed: %v", err)
	}

	expectedVals := []string{"red", "yellow", "dark-red", ""}
	for i := range expectedKeys {
		if visitedKeys[i] != expectedKeys[i] {
			t.Errorf("ForEach key[%d]: expected %s, got %s", i, expectedKeys[i], visitedKeys[i])
		}
		if visitedVals[i] != expectedVals[i] {
			t.Errorf("ForEach val[%d]: expected '%s', got '%s'", i, expectedVals[i], visitedVals[i])
		}
	}

	// Test early exit on error from ForEach callback
	var earlyCount int
	stopErr := errors.New("stop iteration")
	err = db.ForEach(func(k string, v []byte) error {
		earlyCount++
		if earlyCount == 2 {
			return stopErr
		}
		return nil
	})
	if !errors.Is(err, stopErr) {
		t.Fatalf("expected stopErr, got %v", err)
	}
	if earlyCount != 2 {
		t.Fatalf("expected earlyCount 2, got %d", earlyCount)
	}
}
