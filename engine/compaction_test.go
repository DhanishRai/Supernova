package killitdb

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestCompaction_ShrinksFileAndPreservesLiveKeys(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "compact_test.db")

	db, err := Open(dbPath, 10*1024*1024)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer db.Close()

	// 1. Write 100 keys
	for i := 0; i < 100; i++ {
		k := fmt.Sprintf("key-%03d", i)
		v := fmt.Sprintf("val-%03d", i)
		if err := db.Put([]byte(k), []byte(v)); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	// 2. Overwrite 50 keys with newer values
	for i := 0; i < 50; i++ {
		k := fmt.Sprintf("key-%03d", i)
		v := fmt.Sprintf("new-val-%03d", i)
		if err := db.Put([]byte(k), []byte(v)); err != nil {
			t.Fatalf("update Put failed: %v", err)
		}
	}

	// 3. Delete 30 keys
	for i := 50; i < 80; i++ {
		k := fmt.Sprintf("key-%03d", i)
		if err := db.Delete([]byte(k)); err != nil {
			t.Fatalf("Delete failed: %v", err)
		}
	}

	sizeBefore := db.Size()

	// 4. Run Compaction
	if err := db.Compact(); err != nil {
		t.Fatalf("Compact failed: %v", err)
	}

	sizeAfter := db.Size()

	// Compaction must significantly shrink the file
	if sizeAfter >= sizeBefore {
		t.Fatalf("expected compacted size %d < old size %d", sizeAfter, sizeBefore)
	}

	// 5. Verify all active keys are intact with expected latest values
	// Keys 0..49 must have updated values
	for i := 0; i < 50; i++ {
		k := fmt.Sprintf("key-%03d", i)
		expected := fmt.Sprintf("new-val-%03d", i)
		got, err := db.Get([]byte(k))
		if err != nil || string(got) != expected {
			t.Fatalf("key %s: expected %s, got %s (err=%v)", k, expected, string(got), err)
		}
	}

	// Keys 50..79 must be NOT FOUND
	for i := 50; i < 80; i++ {
		k := fmt.Sprintf("key-%03d", i)
		_, err := db.Get([]byte(k))
		if !errors.Is(err, ErrKeyNotFound) {
			t.Fatalf("deleted key %s must return ErrKeyNotFound, got %v", k, err)
		}
	}

	// Keys 80..99 must have original values
	for i := 80; i < 100; i++ {
		k := fmt.Sprintf("key-%03d", i)
		expected := fmt.Sprintf("val-%03d", i)
		got, err := db.Get([]byte(k))
		if err != nil || string(got) != expected {
			t.Fatalf("key %s: expected %s, got %s (err=%v)", k, expected, string(got), err)
		}
	}

	// 6. Verify subsequent writes after compaction succeed
	if err := db.Put([]byte("post-compact-key"), []byte("post-val")); err != nil {
		t.Fatalf("Put after compaction failed: %v", err)
	}
	got, err := db.Get([]byte("post-compact-key"))
	if err != nil || string(got) != "post-val" {
		t.Fatalf("Get after compaction failed: %v", err)
	}
}

func TestCompaction_EmptyDatabase(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "compact_empty.db")

	db, err := Open(dbPath, 1024*1024)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer db.Close()

	// Compacting empty DB must produce exactly 16-byte header file
	if err := db.Compact(); err != nil {
		t.Fatalf("Compact empty DB failed: %v", err)
	}

	if db.Size() != HeaderSize {
		t.Fatalf("expected compacted empty size %d, got %d", HeaderSize, db.Size())
	}
	count, _ := db.KeyCount()
	if count != 0 {
		t.Fatalf("expected key count 0, got %d", count)
	}
}

func TestCompaction_InterruptedTmpCleanupOnStartup(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "compact_crash.db")

	db, err := Open(dbPath, 1024*1024)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	db.Put([]byte("survivor"), []byte("golden"))
	originalSize := db.Size()
	db.Close()

	// Simulate process kill during compaction by creating a corrupt .tmp file
	tmpPath := dbPath + ".tmp"
	if err := os.WriteFile(tmpPath, bytes.Repeat([]byte{0xDE, 0xAD}, 50), 0666); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	// Reopen must delete .tmp file and restore original data intact
	reopenedDB, err := Open(dbPath, 1024*1024)
	if err != nil {
		t.Fatalf("Open failed on restart: %v", err)
	}
	defer reopenedDB.Close()

	if reopenedDB.Size() != originalSize {
		t.Fatalf("expected size %d, got %d", originalSize, reopenedDB.Size())
	}
	got, err := reopenedDB.Get([]byte("survivor"))
	if err != nil || string(got) != "golden" {
		t.Fatalf("original key lost or corrupted after interrupted compaction: %v", err)
	}

	// Verify .tmp file was deleted
	if _, err := os.Stat(tmpPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf(".tmp file was not deleted during Open()")
	}
}
