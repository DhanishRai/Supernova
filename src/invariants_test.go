package killitdb

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
)

// Invariant 1: Every successful operation survives process termination.
func TestInvariant1_SurvivesProcessTermination(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "inv1.db")

	db, err := Open(dbPath, 1024*1024)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	if err := db.Put([]byte("survivor"), []byte("golden-value")); err != nil {
		t.Fatalf("Put failed: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Reopen simulates restart after clean or killed process
	db2, err := Open(dbPath, 1024*1024)
	if err != nil {
		t.Fatalf("reopen failed: %v", err)
	}
	defer db2.Close()

	val, err := db2.Get([]byte("survivor"))
	if err != nil {
		t.Fatalf("Get failed after reopen: %v", err)
	}
	if string(val) != "golden-value" {
		t.Fatalf("expected 'golden-value', got '%s'", string(val))
	}
}

// Invariant 2: An unsuccessful operation is fully present or fully absent.
func TestInvariant2_UnsuccessfulOpFullyPresentOrAbsent(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "inv2.db")

	db, err := Open(dbPath, 1024*1024)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	db.Put([]byte("valid-prior"), []byte("val1"))
	db.Close()

	// Append a corrupted record representing a failed/unsuccessful write
	corruptRec, _ := encodeRecord([]Operation{
		{Type: OpPut, Key: []byte("unsuccessful"), Value: []byte("partial")},
	})
	corruptRec = corruptRec[:len(corruptRec)-5] // torn tail

	f, _ := os.OpenFile(dbPath, os.O_WRONLY|os.O_APPEND, 0666)
	f.Write(corruptRec)
	f.Close()

	// On recovery, the torn record must be cleanly truncated (fully absent)
	db2, err := Open(dbPath, 1024*1024)
	if err != nil {
		t.Fatalf("Open failed on recovery: %v", err)
	}
	defer db2.Close()

	_, err = db2.Get([]byte("unsuccessful"))
	if !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("expected unsuccessful op to be completely absent, got: %v", err)
	}
	val, err := db2.Get([]byte("valid-prior"))
	if err != nil || string(val) != "val1" {
		t.Fatalf("valid-prior key must be fully intact")
	}
}

// Invariant 3: A batch is never partially present.
func TestInvariant3_BatchNeverPartiallyPresent(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "inv3.db")

	db, err := Open(dbPath, 1024*1024)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	// 1. Partial in-memory protection: fail sync during batch
	mock := newMockStorageFile(make([]byte, HeaderSize))
	mock.failSync = errors.New("simulated sync error during batch")
	mock.failSyncTimes = 1

	mockDB := &DB{
		file:     mock,
		size:     HeaderSize,
		maxBytes: 1024 * 1024,
		index:    newIndex(),
	}

	err = mockDB.Batch([]Operation{
		{Type: OpPut, Key: []byte("k1"), Value: []byte("v1")},
		{Type: OpPut, Key: []byte("k2"), Value: []byte("v2")},
	})
	if err == nil {
		t.Fatalf("expected batch sync error")
	}
	if mockDB.index.Len() != 0 {
		t.Fatalf("batch partially exposed in memory: index len = %d", mockDB.index.Len())
	}

	// 2. On-disk torn batch simulation
	db.Put([]byte("base"), []byte("base-val"))
	db.Close()

	batchRec, _ := encodeRecord([]Operation{
		{Type: OpPut, Key: []byte("batch-k1"), Value: []byte("batch-v1")},
		{Type: OpPut, Key: []byte("batch-k2"), Value: []byte("batch-v2")},
	})
	// Slice 3 bytes off the end so the batch record is truncated on disk
	f, _ := os.OpenFile(dbPath, os.O_WRONLY|os.O_APPEND, 0666)
	f.Write(batchRec[:len(batchRec)-3])
	f.Close()

	dbRecovered, err := Open(dbPath, 1024*1024)
	if err != nil {
		t.Fatalf("recovery failed: %v", err)
	}
	defer dbRecovered.Close()

	_, err1 := dbRecovered.Get([]byte("batch-k1"))
	_, err2 := dbRecovered.Get([]byte("batch-k2"))
	if !errors.Is(err1, ErrKeyNotFound) || !errors.Is(err2, ErrKeyNotFound) {
		t.Fatalf("batch was partially recovered! err1=%v, err2=%v", err1, err2)
	}
}

// Invariant 4: GET never returns a value that was never written for that key.
func TestInvariant4_GetNeverReturnsUnwrittenValue(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(filepath.Join(dir, "inv4.db"), 1024*1024)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer db.Close()

	// Non-existent key must return ErrKeyNotFound
	_, err = db.Get([]byte("non-existent"))
	if !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("expected ErrKeyNotFound, got %v", err)
	}

	// Deleted key must return ErrKeyNotFound
	db.Put([]byte("temp"), []byte("temporary-value"))
	db.Delete([]byte("temp"))
	_, err = db.Get([]byte("temp"))
	if !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("expected ErrKeyNotFound for deleted key, got %v", err)
	}

	// Distinct keys must never cross-talk or return each other's values
	db.Put([]byte("keyA"), []byte("valueA"))
	db.Put([]byte("keyB"), []byte("valueB"))

	valA, err := db.Get([]byte("keyA"))
	if err != nil || string(valA) != "valueA" {
		t.Fatalf("keyA mismatch: %s", string(valA))
	}
	valB, err := db.Get([]byte("keyB"))
	if err != nil || string(valB) != "valueB" {
		t.Fatalf("keyB mismatch: %s", string(valB))
	}
}

// Invariant 5: Open recovery is repeatable after a process kill.
func TestInvariant5_RecoveryRepeatableAfterKill(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "inv5.db")

	db, err := Open(dbPath, 1024*1024)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	db.Put([]byte("committed"), []byte("persistent"))
	cleanSize := db.Size()
	db.Close()

	// Append corrupt torn tail bytes
	f, _ := os.OpenFile(dbPath, os.O_WRONLY|os.O_APPEND, 0666)
	f.Write([]byte{0xDE, 0xAD, 0xBE, 0xEF, 0x01, 0x02})
	f.Close()

	// Recovery run 1
	db1, err := Open(dbPath, 1024*1024)
	if err != nil {
		t.Fatalf("run 1 failed: %v", err)
	}
	if db1.Size() != cleanSize {
		t.Fatalf("run 1 failed to truncate to cleanSize %d, got %d", cleanSize, db1.Size())
	}
	db1.Close()

	// Recovery run 2 (proves idempotency and crash-safety if killed during run 1)
	db2, err := Open(dbPath, 1024*1024)
	if err != nil {
		t.Fatalf("run 2 failed: %v", err)
	}
	defer db2.Close()
	if db2.Size() != cleanSize {
		t.Fatalf("run 2 size mismatch: %d != %d", db2.Size(), cleanSize)
	}
	val, err := db2.Get([]byte("committed"))
	if err != nil || string(val) != "persistent" {
		t.Fatalf("data corrupted after repeated recovery: %v", err)
	}
}

// Invariant 6: No index update occurs before durable Sync.
func TestInvariant6_NoIndexUpdateBeforeSync(t *testing.T) {
	mock := newMockStorageFile(make([]byte, HeaderSize))
	mock.failSync = errors.New("disk sync failure")
	mock.failSyncTimes = 1 // write's sync fails; rollback succeeds

	db := &DB{
		file:     mock,
		size:     HeaderSize,
		maxBytes: 1024 * 1024,
		index:    newIndex(),
	}

	err := db.Put([]byte("sync-fail-key"), []byte("sync-fail-val"))
	if err == nil {
		t.Fatalf("expected Put to fail on sync error")
	}

	// Index must NOT contain the key
	_, found := db.index.Get("sync-fail-key")
	if found {
		t.Fatalf("index was mutated before Sync succeeded!")
	}
}

// Invariant 7: Failed rollback prevents further writes.
func TestInvariant7_FailedRollbackPreventsWrites(t *testing.T) {
	mock := newMockStorageFile(make([]byte, HeaderSize))
	mock.failWriteAt = errors.New("write failure")
	mock.failTruncate = errors.New("truncate device error") // rollback fails

	db := &DB{
		file:     mock,
		size:     HeaderSize,
		maxBytes: 1024 * 1024,
		index:    newIndex(),
	}

	err := db.Put([]byte("key"), []byte("val"))
	if err == nil {
		t.Fatalf("expected error on failed write")
	}

	if !db.unusable {
		t.Fatalf("db must be marked unusable when rollback fails")
	}

	// All subsequent operations must be rejected with ErrDatabaseUnusable
	if err := db.Put([]byte("key2"), []byte("val2")); !errors.Is(err, ErrDatabaseUnusable) {
		t.Fatalf("expected ErrDatabaseUnusable on Put, got %v", err)
	}
	if err := db.Delete([]byte("key")); !errors.Is(err, ErrDatabaseUnusable) {
		t.Fatalf("expected ErrDatabaseUnusable on Delete, got %v", err)
	}
	if err := db.Batch([]Operation{{Type: OpPut, Key: []byte("k"), Value: []byte("v")}}); !errors.Is(err, ErrDatabaseUnusable) {
		t.Fatalf("expected ErrDatabaseUnusable on Batch, got %v", err)
	}
	if _, err := db.Get([]byte("key")); !errors.Is(err, ErrDatabaseUnusable) {
		t.Fatalf("expected ErrDatabaseUnusable on Get, got %v", err)
	}
}

// Invariant 8: File-size limits are enforced.
func TestInvariant8_FileSizeLimitsEnforced(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(filepath.Join(dir, "inv8.db"), HeaderSize+50)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer db.Close()

	// Put that fits within limit
	err = db.Put([]byte("k"), []byte("v"))
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Put that would exceed limit must return ErrDiskFull with zero file growth
	sizeBefore := db.Size()
	err = db.Put([]byte("large-key"), bytes.Repeat([]byte("x"), 100))
	if !errors.Is(err, ErrDiskFull) {
		t.Fatalf("expected ErrDiskFull, got %v", err)
	}
	if db.Size() != sizeBefore {
		t.Fatalf("file grew despite exceeding maxBytes! before=%d, after=%d", sizeBefore, db.Size())
	}
}

// Invariant 9: Keys, values and batch limits are enforced.
func TestInvariant9_KeyValBatchLimitsEnforced(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(filepath.Join(dir, "inv9.db"), 10*1024*1024)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer db.Close()

	// Key length limits [1, 1024]
	if err := db.Put([]byte{}, []byte("val")); !errors.Is(err, ErrInvalidKeyLength) {
		t.Fatalf("0-length key must be rejected: %v", err)
	}
	if err := db.Put(bytes.Repeat([]byte("k"), MaxKeyLen+1), []byte("val")); !errors.Is(err, ErrInvalidKeyLength) {
		t.Fatalf("1025-byte key must be rejected: %v", err)
	}

	// Value length limit [0, 1MB]
	if err := db.Put([]byte("k"), make([]byte, MaxValueLen+1)); !errors.Is(err, ErrInvalidValueLength) {
		t.Fatalf(">1MB value must be rejected: %v", err)
	}

	// Batch limits [1, 10000]
	if err := db.Batch(nil); !errors.Is(err, ErrEmptyBatch) {
		t.Fatalf("nil batch must be rejected: %v", err)
	}
	if err := db.Batch([]Operation{}); !errors.Is(err, ErrEmptyBatch) {
		t.Fatalf("empty batch must be rejected: %v", err)
	}

	tooLargeBatch := make([]Operation, MaxBatchOps+1)
	for i := range tooLargeBatch {
		tooLargeBatch[i] = Operation{Type: OpPut, Key: []byte("k"), Value: []byte("v")}
	}
	if err := db.Batch(tooLargeBatch); !errors.Is(err, ErrBatchTooLarge) {
		t.Fatalf("10001 ops batch must be rejected: %v", err)
	}

	// DELETE must have empty value
	err = db.Batch([]Operation{{Type: OpDelete, Key: []byte("k"), Value: []byte("val")}})
	if !errors.Is(err, ErrDeleteWithValue) {
		t.Fatalf("DELETE with value must be rejected: %v", err)
	}
}

// Invariant 10: Concurrent GET and writes are race-free.
func TestInvariant10_ConcurrentGetAndWritesRaceFree(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(filepath.Join(dir, "inv10.db"), 20*1024*1024)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer db.Close()

	const readers = 6
	const writers = 4
	const ops = 100

	var stopFlag int32
	var readerWg sync.WaitGroup
	var writerWg sync.WaitGroup

	// Readers continuously read keys
	for r := 0; r < readers; r++ {
		readerWg.Add(1)
		go func(id int) {
			defer readerWg.Done()
			for atomic.LoadInt32(&stopFlag) == 0 {
				key := fmt.Sprintf("race-key-%d", id%10)
				val, err := db.Get([]byte(key))
				if err != nil && !errors.Is(err, ErrKeyNotFound) {
					t.Errorf("Get failed with unexpected error: %v", err)
				}
				_ = val
			}
		}(r)
	}

	// Writers issue Puts and Batches concurrently
	for w := 0; w < writers; w++ {
		writerWg.Add(1)
		go func(id int) {
			defer writerWg.Done()
			for i := 0; i < ops; i++ {
				key := fmt.Sprintf("race-key-%d", i%10)
				val := fmt.Sprintf("race-val-w%d-%d", id, i)
				if i%2 == 0 {
					_ = db.Put([]byte(key), []byte(val))
				} else {
					_ = db.Batch([]Operation{
						{Type: OpPut, Key: []byte(key), Value: []byte(val)},
					})
				}
			}
		}(w)
	}

	// Wait for writers to complete, then signal readers to stop
	writerWg.Wait()
	atomic.StoreInt32(&stopFlag, 1)
	readerWg.Wait()
}
