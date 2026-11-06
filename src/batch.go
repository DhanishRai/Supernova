package killitdb

// Batch applies an atomic sequence of operations as exactly ONE durable record.
//
// Concurrency & Atomicity:
//  1. Validates all operations upfront. If any operation is invalid, rejects the entire batch
//     with zero disk writes.
//  2. Acquires the global database write lock (db.mu.Lock).
//  3. Encodes the complete batch into a single contiguous binary record in memory.
//  4. Appends and syncs the single record using appendRecord.
//  5. Applies all operations to the in-memory index in their supplied order under the same
//     write lock ONLY after successful disk sync.
//
// Concurrent readers (GET) cannot observe a partially applied batch because the global
// write lock covers both the durable append and the entire index update.
func (db *DB) Batch(ops []Operation) error {
	opCount := len(ops)
	if opCount < MinBatchOps {
		return ErrEmptyBatch
	}
	if opCount > MaxBatchOps {
		return ErrBatchTooLarge
	}

	// Validate every operation before acquiring lock or writing anything.
	for i := range ops {
		op := &ops[i]
		if op.Type != OpPut && op.Type != OpDelete {
			return ErrInvalidOpType
		}
		keyLen := len(op.Key)
		if keyLen < MinKeyLen || keyLen > MaxKeyLen {
			return ErrInvalidKeyLength
		}
		valLen := len(op.Value)
		if valLen > MaxValueLen {
			return ErrInvalidValueLength
		}
		if op.Type == OpDelete && valLen != 0 {
			return ErrDeleteWithValue
		}
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	if db.closed {
		return ErrDatabaseClosed
	}
	if db.unusable {
		return ErrDatabaseUnusable
	}

	// appendRecord encodes the batch as exactly ONE record, checks capacity,
	// writes at db.size, and calls Sync(). It does not modify db.index.
	_, indexOps, err := db.appendRecord(ops)
	if err != nil {
		return err
	}

	// Apply all batch operations to the in-memory index in order.
	// This occurs under db.mu.Lock after Sync has succeeded.
	db.index.ApplyBatch(indexOps)

	return nil
}
