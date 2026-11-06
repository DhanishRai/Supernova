package killitdb

import (
	"fmt"
	"testing"
)

func TestIndex_Insert(t *testing.T) {
	idx := newIndex()

	if idx.Len() != 0 {
		t.Fatalf("expected initial len 0, got %d", idx.Len())
	}

	idx.Put("user:101", 1024, 64)
	idx.Put("user:102", 2048, 128)

	if idx.Len() != 2 {
		t.Fatalf("expected len 2, got %d", idx.Len())
	}

	e1, ok1 := idx.Get("user:101")
	if !ok1 || e1.valueOffset != 1024 || e1.valueLen != 64 {
		t.Fatalf("expected user:101 at offset 1024 len 64, got %+v (found=%v)", e1, ok1)
	}

	e2, ok2 := idx.Get("user:102")
	if !ok2 || e2.valueOffset != 2048 || e2.valueLen != 128 {
		t.Fatalf("expected user:102 at offset 2048 len 128, got %+v (found=%v)", e2, ok2)
	}
}

func TestIndex_Update(t *testing.T) {
	idx := newIndex()

	idx.Put("key1", 100, 50)
	if idx.Len() != 1 {
		t.Fatalf("expected len 1, got %d", idx.Len())
	}

	// Overwrite key1 with new location
	idx.Put("key1", 500, 200)
	if idx.Len() != 1 {
		t.Fatalf("expected len 1 after update, got %d", idx.Len())
	}

	e, ok := idx.Get("key1")
	if !ok {
		t.Fatalf("expected key1 to be present")
	}
	if e.valueOffset != 500 || e.valueLen != 200 {
		t.Fatalf("expected updated offset 500 len 200, got %+v", e)
	}
}

func TestIndex_Delete(t *testing.T) {
	idx := newIndex()

	idx.Put("tempKey", 1000, 40)
	if idx.Len() != 1 {
		t.Fatalf("expected len 1, got %d", idx.Len())
	}

	idx.Delete("tempKey")
	if idx.Len() != 0 {
		t.Fatalf("expected len 0 after delete, got %d", idx.Len())
	}

	_, ok := idx.Get("tempKey")
	if ok {
		t.Fatalf("expected tempKey to be absent after deletion")
	}

	// Deleting a non-existent key should be a safe no-op
	idx.Delete("nonExistent")
	if idx.Len() != 0 {
		t.Fatalf("expected len 0, got %d", idx.Len())
	}
}

func TestIndex_MissingKey(t *testing.T) {
	idx := newIndex()

	e, ok := idx.Get("missing")
	if ok {
		t.Fatalf("expected missing key to return ok=false, got ok=true")
	}
	if e != (entry{}) {
		t.Fatalf("expected zero entry for missing key, got %+v", e)
	}
}

func TestIndex_EmptyValue(t *testing.T) {
	idx := newIndex()

	// Put a key with an empty value (len = 0) at offset 512
	idx.Put("emptyKey", 512, 0)

	e, ok := idx.Get("emptyKey")
	if !ok {
		t.Fatalf("expected emptyKey to be found (ok=true)")
	}
	if e.valueOffset != 512 {
		t.Fatalf("expected offset 512, got %d", e.valueOffset)
	}
	if e.valueLen != 0 {
		t.Fatalf("expected valueLen 0, got %d", e.valueLen)
	}

	// Contrast explicitly with a missing key
	_, okMissing := idx.Get("missingKey")
	if okMissing {
		t.Fatalf("expected missingKey to return ok=false, but got true")
	}
}

func TestIndex_DuplicateKeys(t *testing.T) {
	idx := newIndex()

	ops := []IndexOp{
		{Type: OpPut, Key: "counter", ValueOffset: 10, ValueLen: 4},
		{Type: OpPut, Key: "counter", ValueOffset: 20, ValueLen: 4},
		{Type: OpPut, Key: "counter", ValueOffset: 30, ValueLen: 4},
		{Type: OpPut, Key: "counter", ValueOffset: 40, ValueLen: 8},
	}

	idx.ApplyBatch(ops)

	if idx.Len() != 1 {
		t.Fatalf("expected len 1, got %d", idx.Len())
	}

	e, ok := idx.Get("counter")
	if !ok {
		t.Fatalf("expected counter to be present")
	}
	if e.valueOffset != 40 || e.valueLen != 8 {
		t.Fatalf("expected latest value offset 40 len 8, got %+v", e)
	}
}

func TestIndex_OrderedBatchOperations(t *testing.T) {
	idx := newIndex()

	// Case 1: PUT X followed by DELETE X -> X absent
	ops1 := []IndexOp{
		{Type: OpPut, Key: "k1", ValueOffset: 100, ValueLen: 10},
		{Type: OpDelete, Key: "k1"},
	}
	idx.ApplyBatch(ops1)
	if _, ok := idx.Get("k1"); ok {
		t.Fatalf("k1 should be absent after PUT followed by DELETE")
	}

	// Case 2: DELETE X followed by PUT X -> X present with new value
	ops2 := []IndexOp{
		{Type: OpDelete, Key: "k2"},
		{Type: OpPut, Key: "k2", ValueOffset: 200, ValueLen: 20},
	}
	idx.ApplyBatch(ops2)
	e2, ok2 := idx.Get("k2")
	if !ok2 || e2.valueOffset != 200 || e2.valueLen != 20 {
		t.Fatalf("k2 should be present after DELETE followed by PUT, got %+v (ok=%v)", e2, ok2)
	}

	// Case 3: Mixed multi-key batch
	ops3 := []IndexOp{
		{Type: OpPut, Key: "a", ValueOffset: 10, ValueLen: 5},
		{Type: OpPut, Key: "b", ValueOffset: 20, ValueLen: 5},
		{Type: OpPut, Key: "c", ValueOffset: 30, ValueLen: 5},
		{Type: OpDelete, Key: "b"},
		{Type: OpPut, Key: "a", ValueOffset: 40, ValueLen: 10}, // Overwrite a
	}
	idx.ApplyBatch(ops3)

	ea, oka := idx.Get("a")
	if !oka || ea.valueOffset != 40 || ea.valueLen != 10 {
		t.Fatalf("unexpected state for 'a': %+v (ok=%v)", ea, oka)
	}

	if _, okb := idx.Get("b"); okb {
		t.Fatalf("expected 'b' to be deleted")
	}

	ec, okc := idx.Get("c")
	if !okc || ec.valueOffset != 30 || ec.valueLen != 5 {
		t.Fatalf("unexpected state for 'c': %+v (ok=%v)", ec, okc)
	}
}

func TestIndex_ApplySingle(t *testing.T) {
	idx := newIndex()

	idx.Apply(IndexOp{Type: OpPut, Key: "foo", ValueOffset: 999, ValueLen: 33})
	e, ok := idx.Get("foo")
	if !ok || e.valueOffset != 999 || e.valueLen != 33 {
		t.Fatalf("unexpected result for Apply PUT: %+v (ok=%v)", e, ok)
	}

	idx.Apply(IndexOp{Type: OpDelete, Key: "foo"})
	if _, ok := idx.Get("foo"); ok {
		t.Fatalf("foo should be absent after Apply DELETE")
	}
}

func BenchmarkIndex_OneMillionEntries(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		idx := newIndex()
		count := 1000000

		// Populate 1,000,000 entries
		for j := 0; j < count; j++ {
			key := fmt.Sprintf("k%d", j)
			idx.Put(key, int64(j*100), uint32(50))
		}

		if idx.Len() != count {
			b.Fatalf("expected %d entries, got %d", count, idx.Len())
		}

		// Perform lookups
		for j := 0; j < 1000; j++ {
			key := fmt.Sprintf("k%d", j)
			e, ok := idx.Get(key)
			if !ok || e.valueOffset != int64(j*100) {
				b.Fatalf("lookup failed for %s", key)
			}
		}
	}
}
