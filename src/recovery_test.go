package killitdb

import (
	"bytes"
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestRecovery_MultiplePutRecords(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "multi_put.db")

	db1, err := Open(dbPath, 1024*1024)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	for i := 0; i < 10; i++ {
		k := []byte(string(rune('a' + i)))
		v := []byte(string(rune('A' + i)))
		if err := db1.Put(k, v); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}
	db1.Close()

	// Reopen and verify index is fully rebuilt from disk
	db2, err := Open(dbPath, 1024*1024)
	if err != nil {
		t.Fatalf("reopen failed: %v", err)
	}
	defer db2.Close()

	for i := 0; i < 10; i++ {
		k := []byte(string(rune('a' + i)))
		expectedV := string(rune('A' + i))
		got, err := db2.Get(k)
		if err != nil || string(got) != expectedV {
			t.Fatalf("key %s: expected '%s', got '%s' (err=%v)", string(k), expectedV, string(got), err)
		}
	}
}

func TestRecovery_Delete(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "delete.db")

	db1, err := Open(dbPath, 1024*1024)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	db1.Put([]byte("alive"), []byte("value"))
	db1.Put([]byte("to-delete"), []byte("temporary"))
	db1.Delete([]byte("to-delete"))
	db1.Close()

	db2, err := Open(dbPath, 1024*1024)
	if err != nil {
		t.Fatalf("reopen failed: %v", err)
	}
	defer db2.Close()

	// 'alive' must be present
	v, err := db2.Get([]byte("alive"))
	if err != nil || string(v) != "value" {
		t.Fatalf("expected 'value', got '%s'", string(v))
	}

	// 'to-delete' must be absent
	_, err = db2.Get([]byte("to-delete"))
	if !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("expected ErrKeyNotFound for deleted key, got %v", err)
	}
}

func TestRecovery_OrderedBatch(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "batch_rec.db")

	db1, err := Open(dbPath, 1024*1024)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	err = db1.Batch([]Operation{
		{Type: OpPut, Key: []byte("k1"), Value: []byte("v1")},
		{Type: OpPut, Key: []byte("k2"), Value: []byte("v2")},
		{Type: OpDelete, Key: []byte("k1"), Value: nil},
		{Type: OpPut, Key: []byte("k3"), Value: []byte("v3")},
	})
	if err != nil {
		t.Fatalf("Batch failed: %v", err)
	}
	db1.Close()

	db2, err := Open(dbPath, 1024*1024)
	if err != nil {
		t.Fatalf("reopen failed: %v", err)
	}
	defer db2.Close()

	if _, err := db2.Get([]byte("k1")); !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("k1 should be absent after recovery")
	}
	if v, err := db2.Get([]byte("k2")); err != nil || string(v) != "v2" {
		t.Fatalf("k2 mismatch: %s (err=%v)", string(v), err)
	}
	if v, err := db2.Get([]byte("k3")); err != nil || string(v) != "v3" {
		t.Fatalf("k3 mismatch: %s (err=%v)", string(v), err)
	}
}

func TestRecovery_DuplicateKeys(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "dup.db")

	db1, err := Open(dbPath, 1024*1024)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	db1.Put([]byte("target"), []byte("val-1"))
	db1.Put([]byte("target"), []byte("val-2"))
	db1.Put([]byte("target"), []byte("val-3-winner"))
	db1.Close()

	db2, err := Open(dbPath, 1024*1024)
	if err != nil {
		t.Fatalf("reopen failed: %v", err)
	}
	defer db2.Close()

	v, err := db2.Get([]byte("target"))
	if err != nil || string(v) != "val-3-winner" {
		t.Fatalf("expected 'val-3-winner', got '%s'", string(v))
	}
}

func TestRecovery_IgnoreIncompleteRecordHeader(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "partial_header.db")

	db1, err := Open(dbPath, 1024*1024)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	db1.Put([]byte("safe-key"), []byte("safe-value"))
	validSize := db1.Size()
	db1.Close()

	// Append 1..7 bytes of partial record header to simulate process kill mid-header write
	for tornBytes := 1; tornBytes < RecordHeaderSize; tornBytes++ {
		f, err := os.OpenFile(dbPath, os.O_WRONLY|os.O_APPEND, 0666)
		if err != nil {
			t.Fatalf("OpenFile failed: %v", err)
		}
		f.Write(bytes.Repeat([]byte{0xEE}, tornBytes))
		f.Close()

		// Recovery must truncate the torn tail and recover valid records
		db2, err := Open(dbPath, 1024*1024)
		if err != nil {
			t.Fatalf("reopen with %d torn header bytes failed: %v", tornBytes, err)
		}

		if db2.Size() != validSize {
			t.Fatalf("expected recovered size %d, got %d", validSize, db2.Size())
		}
		v, err := db2.Get([]byte("safe-key"))
		if err != nil || string(v) != "safe-value" {
			t.Fatalf("safe-key corrupt or missing")
		}
		db2.Close()
	}
}

func TestRecovery_IgnoreIncompletePayload(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "partial_payload.db")

	db1, err := Open(dbPath, 1024*1024)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	db1.Put([]byte("good-key"), []byte("good-value"))
	validSize := db1.Size()
	db1.Close()

	// Append 8-byte header claiming 100 bytes of payload, but only write 20 bytes
	f, err := os.OpenFile(dbPath, os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		t.Fatalf("OpenFile failed: %v", err)
	}
	var tornHeader [RecordHeaderSize]byte
	binary.LittleEndian.PutUint32(tornHeader[0:4], 100) // Declared payload 100 bytes
	binary.LittleEndian.PutUint32(tornHeader[4:8], 0x12345678)
	f.Write(tornHeader[:])
	f.Write(bytes.Repeat([]byte{0xAA}, 20)) // Only write 20 bytes
	f.Close()

	db2, err := Open(dbPath, 1024*1024)
	if err != nil {
		t.Fatalf("reopen failed: %v", err)
	}
	defer db2.Close()

	if db2.Size() != validSize {
		t.Fatalf("expected torn payload truncated to %d, got %d", validSize, db2.Size())
	}
	v, err := db2.Get([]byte("good-key"))
	if err != nil || string(v) != "good-value" {
		t.Fatalf("good-key missing")
	}
}

func TestRecovery_DetectCRCMismatch(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "crc_mismatch.db")

	db1, err := Open(dbPath, 1024*1024)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	db1.Put([]byte("valid"), []byte("data"))
	validSize := db1.Size()
	db1.Close()

	// Append a complete record frame with corrupted CRC
	corruptRec, _ := encodeRecord([]Operation{
		{Type: OpPut, Key: []byte("uncommitted"), Value: []byte("bad-crc")},
	})
	corruptRec[4] ^= 0xFF // Invert CRC byte

	f, err := os.OpenFile(dbPath, os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		t.Fatalf("OpenFile failed: %v", err)
	}
	f.Write(corruptRec)
	f.Close()

	db2, err := Open(dbPath, 1024*1024)
	if err != nil {
		t.Fatalf("reopen failed: %v", err)
	}
	defer db2.Close()

	if db2.Size() != validSize {
		t.Fatalf("expected CRC mismatch record to be truncated to %d, got %d", validSize, db2.Size())
	}
	if _, err := db2.Get([]byte("uncommitted")); !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("uncommitted record with bad CRC should not be recovered")
	}
}

func TestRecovery_DetectInvalidOperationData(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "invalid_op.db")

	db1, err := Open(dbPath, 1024*1024)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	db1.Put([]byte("safe"), []byte("val"))
	validSize := db1.Size()
	db1.Close()

	// Append record with valid CRC but invalid opType (99)
	payload := make([]byte, 4+OpHeaderSize+2)
	binary.LittleEndian.PutUint32(payload[0:4], 1)
	payload[4] = 99 // Invalid opType
	binary.LittleEndian.PutUint16(payload[5:7], 1)
	binary.LittleEndian.PutUint32(payload[7:11], 1)
	payload[11] = 'k'
	payload[12] = 'v'

	rec := craftRecord(payload)

	f, err := os.OpenFile(dbPath, os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		t.Fatalf("OpenFile failed: %v", err)
	}
	f.Write(rec)
	f.Close()

	db2, err := Open(dbPath, 1024*1024)
	if err != nil {
		t.Fatalf("reopen failed: %v", err)
	}
	defer db2.Close()

	if db2.Size() != validSize {
		t.Fatalf("expected invalid op record truncated to %d, got %d", validSize, db2.Size())
	}
}

func TestRecovery_TruncateTornTail_And_Reopen(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "torn_tail.db")

	db1, err := Open(dbPath, 1024*1024)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	db1.Put([]byte("k1"), []byte("v1"))
	validSize := db1.Size()
	db1.Close()

	// Append torn bytes
	f, _ := os.OpenFile(dbPath, os.O_WRONLY|os.O_APPEND, 0666)
	f.Write([]byte("garbage-torn-tail-bytes-12345"))
	f.Close()

	// First reopen truncates torn tail
	db2, err := Open(dbPath, 1024*1024)
	if err != nil {
		t.Fatalf("first recovery failed: %v", err)
	}
	if db2.Size() != validSize {
		t.Fatalf("expected size %d after truncation, got %d", validSize, db2.Size())
	}
	db2.Close()

	// Second reopen verifies file is completely clean and stable
	db3, err := Open(dbPath, 1024*1024)
	if err != nil {
		t.Fatalf("second reopen failed: %v", err)
	}
	defer db3.Close()

	if db3.Size() != validSize {
		t.Fatalf("expected size %d on clean reopen, got %d", validSize, db3.Size())
	}
	v, err := db3.Get([]byte("k1"))
	if err != nil || string(v) != "v1" {
		t.Fatalf("k1 missing on second reopen")
	}
}

func TestRecovery_KillRestartSimulation(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "restart_sim.db")

	// Multi-cycle crash simulation:
	// Cycle 1: Write k1, kill mid-write of k2
	db, err := Open(dbPath, 1024*1024)
	if err != nil {
		t.Fatalf("cycle 1 open failed: %v", err)
	}
	db.Put([]byte("k1"), []byte("v1"))
	db.Close()

	f, _ := os.OpenFile(dbPath, os.O_WRONLY|os.O_APPEND, 0666)
	f.Write([]byte{0x01, 0x02, 0x03}) // partial write
	f.Close()

	// Cycle 2: Recover, write k2, kill mid-write of k3
	db, err = Open(dbPath, 1024*1024)
	if err != nil {
		t.Fatalf("cycle 2 open failed: %v", err)
	}
	db.Put([]byte("k2"), []byte("v2"))
	db.Close()

	f, _ = os.OpenFile(dbPath, os.O_WRONLY|os.O_APPEND, 0666)
	f.Write([]byte{0xDE, 0xAD, 0xBE, 0xEF, 0x00}) // partial write
	f.Close()

	// Cycle 3: Recover, verify k1 and k2 exist, write k3
	db, err = Open(dbPath, 1024*1024)
	if err != nil {
		t.Fatalf("cycle 3 open failed: %v", err)
	}
	v1, err1 := db.Get([]byte("k1"))
	v2, err2 := db.Get([]byte("k2"))
	if err1 != nil || string(v1) != "v1" || err2 != nil || string(v2) != "v2" {
		t.Fatalf("acknowledged records failed to survive crash cycles")
	}
	db.Put([]byte("k3"), []byte("v3"))
	db.Close()

	// Final verification
	db, err = Open(dbPath, 1024*1024)
	if err != nil {
		t.Fatalf("final open failed: %v", err)
	}
	defer db.Close()
	v3, err3 := db.Get([]byte("k3"))
	if err3 != nil || string(v3) != "v3" {
		t.Fatalf("k3 missing")
	}
}

func TestRecovery_CompactionTmpFileRemoval(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "compaction_cleanup.db")
	tmpPath := dbPath + ".tmp"

	// Create an orphaned compaction .tmp file
	if err := os.WriteFile(tmpPath, []byte("dangling-compaction-artifact"), 0666); err != nil {
		t.Fatalf("WriteFile tmp failed: %v", err)
	}

	db, err := Open(dbPath, 1024*1024)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer db.Close()

	// Verify .tmp file was removed
	if _, err := os.Stat(tmpPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("compaction .tmp file was not removed during Open()")
	}
}

func TestRecovery_UnacknowledgedFinalRecordAllOrNothing(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "all_or_nothing.db")

	db1, err := Open(dbPath, 1024*1024)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	db1.Put([]byte("committed"), []byte("yes"))
	db1.Close()

	// Case A: Record is fully written with valid CRC -> recovered in full
	recValid, _ := encodeRecord([]Operation{
		{Type: OpPut, Key: []byte("full-write"), Value: []byte("complete")},
	})
	f, _ := os.OpenFile(dbPath, os.O_WRONLY|os.O_APPEND, 0666)
	f.Write(recValid)
	f.Close()

	dbA, err := Open(dbPath, 1024*1024)
	if err != nil {
		t.Fatalf("open A failed: %v", err)
	}
	vFull, err := dbA.Get([]byte("full-write"))
	if err != nil || string(vFull) != "complete" {
		t.Fatalf("fully written record must be recovered")
	}
	sizeAfterA := dbA.Size()
	dbA.Close()

	// Case B: Record is partially written / interrupted -> fully absent
	f, _ = os.OpenFile(dbPath, os.O_WRONLY|os.O_APPEND, 0666)
	f.Write(recValid[:len(recValid)-4]) // Incomplete by 4 bytes
	f.Close()

	dbB, err := Open(dbPath, 1024*1024)
	if err != nil {
		t.Fatalf("open B failed: %v", err)
	}
	defer dbB.Close()

	if dbB.Size() != sizeAfterA {
		t.Fatalf("partial record must be truncated; expected size %d, got %d", sizeAfterA, dbB.Size())
	}
	if _, err := dbB.Get([]byte("full-write")); err != nil {
		t.Fatalf("previously committed full-write must remain present")
	}
}
