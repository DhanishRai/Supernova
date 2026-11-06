package killitdb

// perf_test.go — Million-key recovery performance test.
//
// Run with:
//
//	go test -v -run TestRecovery_MillionKeys -timeout 300s ./src/
//
// Standard benchmark (alternative):
//
//	go test -bench BenchmarkRecovery_MillionKeys -benchtime=1x -timeout 300s ./src/

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"
)

const (
	millionKeyCount = 1_000_000
	batchSize       = 10_000 // maximum allowed
	valueSize       = 32     // 32-byte values: realistic, keeps file manageable
	keyPrefix       = "bench-key-"
)

// buildMillionKeyDB creates a fresh database containing exactly keyCount distinct
// keys. It writes them in batches of batchSize (maximum allowed) using Batch().
// Each key has the form "bench-key-NNNNNNN" and each value is 32 deterministic bytes.
//
// Returns the path to the written database file.
func buildMillionKeyDB(t testing.TB, dir string, keyCount int) string {
	t.Helper()
	dbPath := filepath.Join(dir, "million.db")

	// Generous maxBytes: each record ≈ 8 (outer) + 4 (opCount) + batchSize*(7+18+32) = ~590 KB per batch.
	// 100 batches × 590 KB ≈ 59 MB. Use 256 MB headroom.
	const maxBytes = 256 * 1024 * 1024

	db, err := Open(dbPath, maxBytes)
	if err != nil {
		t.Fatalf("buildMillionKeyDB: Open failed: %v", err)
	}

	written := 0
	for written < keyCount {
		end := written + batchSize
		if end > keyCount {
			end = keyCount
		}

		ops := make([]Operation, 0, end-written)
		for i := written; i < end; i++ {
			key := fmt.Sprintf("%s%07d", keyPrefix, i)
			val := deterministicValue(i)
			ops = append(ops, Operation{
				Type:  OpPut,
				Key:   []byte(key),
				Value: val,
			})
		}

		if err := db.Batch(ops); err != nil {
			db.Close()
			t.Fatalf("buildMillionKeyDB: Batch(%d..%d) failed: %v", written, end, err)
		}

		written = end
	}

	if err := db.Close(); err != nil {
		t.Fatalf("buildMillionKeyDB: Close failed: %v", err)
	}

	return dbPath
}

// deterministicValue returns a 32-byte deterministic value for key index i.
func deterministicValue(i int) []byte {
	v := make([]byte, valueSize)
	// Simple LCG-like fill: reproducible, cheap, different per key.
	x := uint64(i)*6364136223846793005 + 1442695040888963407
	for j := 0; j < valueSize; j += 8 {
		rem := valueSize - j
		if rem > 8 {
			rem = 8
		}
		for b := 0; b < rem; b++ {
			v[j+b] = byte(x >> (uint(b) * 8))
		}
		x = x*6364136223846793005 + 1442695040888963407
	}
	return v
}

// TestRecovery_MillionKeys creates a 1,000,000-key database, closes it, then
// measures only Open() recovery time. It verifies all keys and spot-checks values.
func TestRecovery_MillionKeys(t *testing.T) {
	dir := t.TempDir()

	t.Log("Phase 1: building database with 1,000,000 keys …")
	buildStart := time.Now()
	dbPath := buildMillionKeyDB(t, dir, millionKeyCount)
	buildElapsed := time.Since(buildStart)

	fi, _ := os.Stat(dbPath)
	t.Logf("Build complete in %s — file size: %.1f MB", buildElapsed, float64(fi.Size())/(1024*1024))

	// Collect baseline memory before recovery.
	var memBefore, memAfter runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&memBefore)

	t.Log("Phase 2: measuring Open() recovery …")
	recoverStart := time.Now()

	db, err := Open(dbPath, 256*1024*1024)
	if err != nil {
		t.Fatalf("Open (recovery) failed: %v", err)
	}
	recoverElapsed := time.Since(recoverStart)

	runtime.ReadMemStats(&memAfter)
	heapGrowthMB := float64(memAfter.HeapAlloc-memBefore.HeapAlloc) / (1024 * 1024)
	totalAllocMB := float64(memAfter.TotalAlloc-memBefore.TotalAlloc) / (1024 * 1024)

	recoveredCount := db.index.Len()

	t.Logf("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	t.Logf("Recovery elapsed:   %s", recoverElapsed)
	t.Logf("Recovered keys:     %d", recoveredCount)
	t.Logf("Heap growth:        %.1f MB (live after GC)", heapGrowthMB)
	t.Logf("Total alloc:        %.1f MB (cumulative during recovery)", totalAllocMB)
	t.Logf("File size:          %.1f MB", float64(fi.Size())/(1024*1024))
	t.Logf("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

	// 1. Verify all 1,000,000 keys are accessible.
	if recoveredCount != millionKeyCount {
		t.Errorf("expected %d recovered keys, got %d", millionKeyCount, recoveredCount)
	}

	// 2. Spot-check 1,000 random keys to confirm values are correct.
	t.Log("Phase 3: spot-checking 1,000 random keys …")
	rng := rand.New(rand.NewSource(42))
	for i := 0; i < 1000; i++ {
		idx := rng.Intn(millionKeyCount)
		key := fmt.Sprintf("%s%07d", keyPrefix, idx)
		expectedVal := deterministicValue(idx)

		got, err := db.Get([]byte(key))
		if err != nil {
			t.Errorf("Get(%s) failed: %v", key, err)
			continue
		}
		if len(got) != len(expectedVal) {
			t.Errorf("key %s: value length mismatch: expected %d, got %d", key, len(expectedVal), len(got))
			continue
		}
		for b := range expectedVal {
			if got[b] != expectedVal[b] {
				t.Errorf("key %s: value byte %d mismatch: expected %02x, got %02x", key, b, expectedVal[b], got[b])
				break
			}
		}
	}

	// 3. Verify first, last, and middle keys explicitly.
	for _, idx := range []int{0, millionKeyCount/2 - 1, millionKeyCount - 1} {
		key := fmt.Sprintf("%s%07d", keyPrefix, idx)
		expectedVal := deterministicValue(idx)
		got, err := db.Get([]byte(key))
		if err != nil {
			t.Errorf("boundary key %s: Get failed: %v", key, err)
		} else if string(got) != string(expectedVal) {
			t.Errorf("boundary key %s: value mismatch", key)
		}
	}

	if err := db.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// 4. Performance assertion.
	const targetSeconds = 2.0
	if recoverElapsed.Seconds() > targetSeconds {
		t.Errorf("PERFORMANCE MISS: recovery took %s, target is under %.0fs", recoverElapsed, targetSeconds)
	} else {
		t.Logf("PERFORMANCE PASS: recovery in %s (target: <%.0fs)", recoverElapsed, targetSeconds)
	}
}

// BenchmarkRecovery_MillionKeys measures Open() recovery of 1,000,000 keys.
// Use -benchtime=1x to run exactly once (the setup cost dominates).
//
//	go test -bench BenchmarkRecovery_MillionKeys -benchtime=1x -timeout 300s ./src/
func BenchmarkRecovery_MillionKeys(b *testing.B) {
	dir := b.TempDir()
	dbPath := buildMillionKeyDB(b, dir, millionKeyCount)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		db, err := Open(dbPath, 256*1024*1024)
		if err != nil {
			b.Fatalf("Open failed: %v", err)
		}
		count := db.index.Len()
		db.Close()
		if count != millionKeyCount {
			b.Fatalf("expected %d keys, got %d", millionKeyCount, count)
		}
	}
}
