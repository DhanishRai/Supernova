package killitdb

// entry stores the on-disk location and length of a value.
// Values remain on disk and are read via ReadAt without caching in memory.
type entry struct {
	valueOffset int64
	valueLen    uint32
}

// IndexOp represents an operation with its absolute on-disk value offset
// ready to be applied to the in-memory index.
type IndexOp struct {
	Type        OperationType
	Key         string
	ValueOffset int64
	ValueLen    uint32
}

// Index is the in-memory key-to-offset index backed by a map.
// Note: Index does not contain its own mutex; synchronization is handled
// by the database engine's global sync.RWMutex.
type Index struct {
	entries map[string]entry
}

// newIndex creates a new empty Index.
func newIndex() *Index {
	return &Index{
		entries: make(map[string]entry),
	}
}

// Get looks up a key in the index.
// Returns the entry and true if found, or entry{} and false if absent.
// An entry with valueLen == 0 and found == true represents an existing key with an empty value.
func (idx *Index) Get(key string) (entry, bool) {
	e, ok := idx.entries[key]
	return e, ok
}

// Put updates or inserts a key with its absolute on-disk value offset and length.
func (idx *Index) Put(key string, valueOffset int64, valueLen uint32) {
	idx.entries[key] = entry{
		valueOffset: valueOffset,
		valueLen:    valueLen,
	}
}

// Delete removes a key from the index.
func (idx *Index) Delete(key string) {
	delete(idx.entries, key)
}

// Apply applies a single IndexOp to the index.
func (idx *Index) Apply(op IndexOp) {
	switch op.Type {
	case OpPut:
		idx.Put(op.Key, op.ValueOffset, op.ValueLen)
	case OpDelete:
		idx.Delete(op.Key)
	}
}

// ApplyBatch applies an ordered sequence of IndexOps to the index in order.
func (idx *Index) ApplyBatch(ops []IndexOp) {
	for i := range ops {
		idx.Apply(ops[i])
	}
}

// Len returns the number of active keys in the index.
func (idx *Index) Len() int {
	return len(idx.entries)
}

// Keys returns a slice containing all active keys in the index.
func (idx *Index) Keys() []string {
	keys := make([]string, 0, len(idx.entries))
	for k := range idx.entries {
		keys = append(keys, k)
	}
	return keys
}
