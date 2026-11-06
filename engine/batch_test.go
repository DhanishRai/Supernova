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

func TestBatch_MultiplePuts(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(filepath.Join(dir, "test.db"), 1024*1024)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer db.Close()

	ops := []Operation{
		{Type: OpPut, Key: []byte("k1"), Value: []byte("v1")},
		{Type: OpPut, Key: []byte("k2"), Value: []byte("v2")},
		{Type: OpPut, Key: []byte("k3"), Value: []byte("v3")},
	}

	if err := db.Batch(ops); err != nil {
		t.Fatalf("Batch failed: %v", err)
	}

	for _, op := range ops {
		v, err := db.Get(op.Key)
		if err != nil {
			t.Fatalf("Get(%s) failed: %v", string(op.Key), err)
		}
		if !bytes.Equal(v, op.Value) {
			t.Fatalf("Get(%s) expected '%s', got '%s'", string(op.Key), string(op.Value), string(v))
		}
	}
}

func TestBatch_WithDelete(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(filepath.Join(dir, "test.db"), 1024*1024)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer db.Close()

	if err := db.Put([]byte("to-delete"), []byte("temporary")); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	ops := []Operation{
		{Type: OpPut, Key: []byte("new-key"), Value: []byte("new-val")},
		{Type: OpDelete, Key: []byte("to-delete"), Value: nil},
	}

	if err := db.Batch(ops); err != nil {
		t.Fatalf("Batch failed: %v", err)
	}

	// New key exists
	v, err := db.Get([]byte("new-key"))
	if err != nil || string(v) != "new-val" {
		t.Fatalf("expected 'new-val', got '%s' (err=%v)", string(v), err)
	}

	// Deleted key does not exist
	_, err = db.Get([]byte("to-delete"))
	if !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("expected ErrKeyNotFound for deleted key, got %v", err)
	}
}

func TestBatch_DuplicateKeys(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(filepath.Join(dir, "test.db"), 1024*1024)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer db.Close()

	ops := []Operation{
		{Type: OpPut, Key: []byte("dup"), Value: []byte("first")},
		{Type: OpPut, Key: []byte("dup"), Value: []byte("second")},
		{Type: OpPut, Key: []byte("dup"), Value: []byte("third-winner")},
	}

	if err := db.Batch(ops); err != nil {
		t.Fatalf("Batch failed: %v", err)
	}

	v, err := db.Get([]byte("dup"))
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if string(v) != "third-winner" {
		t.Fatalf("expected latest value 'third-winner', got '%s'", string(v))
	}
}

func TestBatch_PutThenDeleteSameKey(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(filepath.Join(dir, "test.db"), 1024*1024)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer db.Close()

	ops := []Operation{
		{Type: OpPut, Key: []byte("lifecycle"), Value: []byte("alive")},
		{Type: OpDelete, Key: []byte("lifecycle"), Value: nil},
	}

	if err := db.Batch(ops); err != nil {
		t.Fatalf("Batch failed: %v", err)
	}

	_, err = db.Get([]byte("lifecycle"))
	if !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("expected key to be absent after PUT then DELETE, got %v", err)
	}
}

func TestBatch_DeleteThenPutSameKey(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(filepath.Join(dir, "test.db"), 1024*1024)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer db.Close()

	// Seed existing key
	if err := db.Put([]byte("reborn"), []byte("original")); err != nil {
		t.Fatalf("seed Put failed: %v", err)
	}

	ops := []Operation{
		{Type: OpDelete, Key: []byte("reborn"), Value: nil},
		{Type: OpPut, Key: []byte("reborn"), Value: []byte("resurrected")},
	}

	if err := db.Batch(ops); err != nil {
		t.Fatalf("Batch failed: %v", err)
	}

	v, err := db.Get([]byte("reborn"))
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if string(v) != "resurrected" {
		t.Fatalf("expected 'resurrected', got '%s'", string(v))
	}
}

func TestBatch_InvalidOperationCausesZeroDiskWrites(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")
	db, err := Open(dbPath, 1024*1024)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer db.Close()

	initialSize := db.Size()

	// Batch contains valid operations followed by one invalid operation (key length = 0)
	ops := []Operation{
		{Type: OpPut, Key: []byte("valid1"), Value: []byte("v1")},
		{Type: OpPut, Key: []byte("valid2"), Value: []byte("v2")},
		{Type: OpPut, Key: []byte{}, Value: []byte("v3")}, // Invalid!
	}

	err = db.Batch(ops)
	if !errors.Is(err, ErrInvalidKeyLength) {
		t.Fatalf("expected ErrInvalidKeyLength, got %v", err)
	}

	// Verify ZERO disk writes occurred
	if db.Size() != initialSize {
		t.Fatalf("expected db.Size() unchanged (%d), got %d", initialSize, db.Size())
	}
	stat, err := os.Stat(dbPath)
	if err != nil {
		t.Fatalf("Stat failed: %v", err)
	}
	if stat.Size() != initialSize {
		t.Fatalf("file grew on disk despite invalid batch! expected %d, got %d", initialSize, stat.Size())
	}

	// Verify none of the valid operations in the rejected batch were indexed
	if _, err := db.Get([]byte("valid1")); !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("key 'valid1' was partially applied from rejected batch!")
	}
}

func TestBatch_EmptyBatchRejected(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(filepath.Join(dir, "test.db"), 1024*1024)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer db.Close()

	if err := db.Batch(nil); !errors.Is(err, ErrEmptyBatch) {
		t.Fatalf("expected ErrEmptyBatch for nil, got %v", err)
	}
	if err := db.Batch([]Operation{}); !errors.Is(err, ErrEmptyBatch) {
		t.Fatalf("expected ErrEmptyBatch for empty slice, got %v", err)
	}
}

func TestBatch_MoreThan10000OperationsRejected(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(filepath.Join(dir, "test.db"), 1024*1024)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer db.Close()

	ops := make([]Operation, MaxBatchOps+1)
	for i := range ops {
		ops[i] = Operation{Type: OpPut, Key: []byte("k"), Value: []byte("v")}
	}

	if err := db.Batch(ops); !errors.Is(err, ErrBatchTooLarge) {
		t.Fatalf("expected ErrBatchTooLarge, got %v", err)
	}
}

func TestBatch_MaxSizeRejection(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")
	// maxBytes set to only fit header + small margin
	db, err := Open(dbPath, HeaderSize+20)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer db.Close()

	ops := []Operation{
		{Type: OpPut, Key: []byte("large-batch-key"), Value: []byte("large-batch-value")},
	}

	err = db.Batch(ops)
	if !errors.Is(err, ErrDiskFull) {
		t.Fatalf("expected ErrDiskFull, got %v", err)
	}

	if db.Size() != HeaderSize {
		t.Fatalf("expected db.Size() unchanged (%d), got %d", HeaderSize, db.Size())
	}
}

func TestBatch_IsOneRecord(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "one_record.db")
	db, err := Open(dbPath, 1024*1024)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer db.Close()

	ops := []Operation{
		{Type: OpPut, Key: []byte("k1"), Value: []byte("v1")},
		{Type: OpPut, Key: []byte("k2"), Value: []byte("v2")},
		{Type: OpPut, Key: []byte("k3"), Value: []byte("v3")},
		{Type: OpPut, Key: []byte("k4"), Value: []byte("v4")},
	}

	if err := db.Batch(ops); err != nil {
		t.Fatalf("Batch failed: %v", err)
	}

	// Verify on disk: read entire file after HeaderSize
	fileData, err := os.ReadFile(dbPath)
	if err != nil {
		t.Fatalf("ReadFile failed: %v", err)
	}

	recordData := fileData[HeaderSize:]
	decoded, err := decodeRecord(recordData)
	if err != nil {
		t.Fatalf("decodeRecord failed: %v", err)
	}

	// Proves all 4 operations are contained in EXACTLY ONE record on disk
	if len(decoded) != 4 {
		t.Fatalf("expected 1 record with 4 ops, decoded %d ops", len(decoded))
	}
	if int64(len(fileData)) != db.Size() {
		t.Fatalf("expected on-disk file size %d, got %d", db.Size(), len(fileData))
	}
}

func TestBatch_SyncFailureDoesNotExposePartialIndex(t *testing.T) {
	mock := newMockStorageFile(make([]byte, HeaderSize))
	mock.failSync = errors.New("simulated sync error during batch")
	mock.failSyncTimes = 1

	db := &DB{
		file:     mock,
		size:     HeaderSize,
		maxBytes: 1024 * 1024,
		index:    newIndex(),
	}

	ops := []Operation{
		{Type: OpPut, Key: []byte("k1"), Value: []byte("v1")},
		{Type: OpPut, Key: []byte("k2"), Value: []byte("v2")},
	}

	err := db.Batch(ops)
	if err == nil {
		t.Fatalf("expected batch to fail on sync error")
	}

	// CRITICAL RULE: In-memory index must remain empty!
	if db.index.Len() != 0 {
		t.Fatalf("sync failure exposed partial index updates! expected len 0, got %d", db.index.Len())
	}
}

func TestBatch_ConcurrentGetNeverSeesPartialBatch(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(filepath.Join(dir, "atomic.db"), 20*1024*1024)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer db.Close()

	// Initial state
	key := []byte("atomic-key")
	if err := db.Put(key, []byte("final-0")); err != nil {
		t.Fatalf("initial Put failed: %v", err)
	}

	iterations := 300
	var stopFlag int32
	var partialStateObserved int32

	var wg sync.WaitGroup

	// Reader goroutines continuously check the key
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for atomic.LoadInt32(&stopFlag) == 0 {
				val, err := db.Get(key)
				if err != nil {
					continue
				}
				// The batch temporarily writes "intermediate-state" before "final-N"
				// A reader must NEVER observe "intermediate-state"!
				if string(val) == "intermediate-state" {
					atomic.AddInt32(&partialStateObserved, 1)
				}
			}
		}()
	}

	// Writer applies batches containing an intermediate state overwritten by a final state
	for i := 1; i <= iterations; i++ {
		finalVal := []byte(fmt.Sprintf("final-%d", i))
		ops := []Operation{
			{Type: OpPut, Key: key, Value: []byte("intermediate-state")},
			{Type: OpPut, Key: []byte("other-key"), Value: []byte("other-val")},
			{Type: OpPut, Key: key, Value: finalVal},
		}
		if err := db.Batch(ops); err != nil {
			t.Fatalf("Batch iteration %d failed: %v", i, err)
		}
	}

	atomic.StoreInt32(&stopFlag, 1)
	wg.Wait()

	if partialStateObserved > 0 {
		t.Fatalf("readers observed %d partial/intermediate batch states!", partialStateObserved)
	}

	finalVal, err := db.Get(key)
	if err != nil || string(finalVal) != fmt.Sprintf("final-%d", iterations) {
		t.Fatalf("unexpected final state: %s (err=%v)", string(finalVal), err)
	}
}
