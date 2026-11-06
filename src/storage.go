package killitdb

import (
	"fmt"
	"io"
)

// appendRecord writes a batch of operations as a single durable record to the authoritative data file.
//
// Lock ownership:
// Caller MUST hold db.mu.Lock before calling appendRecord.
//
// Write ordering & crash-safety guarantees:
//  1. Validates that the database is open and not in an unusable/poisoned state.
//  2. Captures oldSize from db.size.
//  3. Encodes the entire record into memory (encodeRecord).
//  4. Checks if oldSize + recordLength > db.maxBytes; returns ErrDiskFull without disk I/O if exceeded.
//  5. Writes the complete encoded byte slice at offset oldSize using WriteAt.
//  6. Detects short writes or write errors; on failure, attempts rollback(oldSize).
//  7. Issues Sync() to flush file buffers to durable physical storage.
//  8. On Sync() failure, attempts rollback(oldSize).
//  9. Only after successful Sync() advances db.size and calculates index metadata.
//
// 10. Does NOT update the in-memory index; returns indexOps to caller for subsequent in-memory application.
func (db *DB) appendRecord(ops []Operation) (int64, []IndexOp, error) {
	if db.closed {
		return 0, nil, ErrDatabaseClosed
	}
	if db.unusable {
		return 0, nil, ErrDatabaseUnusable
	}

	oldSize := db.size

	// 1. Encode the entire record in memory.
	recordBytes, err := encodeRecord(ops)
	if err != nil {
		return 0, nil, err
	}

	recordLen := int64(len(recordBytes))

	// 2. Capacity check before writing anything to disk.
	if oldSize+recordLen > db.maxBytes {
		return 0, nil, ErrDiskFull
	}

	// 3. Write complete record at oldSize.
	n, writeErr := db.file.WriteAt(recordBytes, oldSize)
	if writeErr != nil || n != len(recordBytes) {
		if writeErr == nil {
			writeErr = io.ErrShortWrite
		}

		rbErr := db.rollback(oldSize)
		if rbErr != nil {
			db.unusable = true
			return 0, nil, fmt.Errorf("write error (%w); rollback failed (%v): database marked unusable", writeErr, rbErr)
		}
		return 0, nil, writeErr
	}

	// 4. Force write to disk before reporting success or updating metadata.
	if syncErr := db.file.Sync(); syncErr != nil {
		rbErr := db.rollback(oldSize)
		if rbErr != nil {
			db.unusable = true
			return 0, nil, fmt.Errorf("sync error (%w); rollback failed (%v): database marked unusable", syncErr, rbErr)
		}
		return 0, nil, syncErr
	}

	// 5. Durability confirmed. Advance database size.
	db.size = oldSize + recordLen

	// 6. Compute absolute on-disk value offsets for index updates.
	// Outer record: 4 bytes payload length + 4 bytes CRC (8 bytes).
	// Payload begins with 4 bytes operation count.
	// Each operation starts at opOffset:
	//   1 byte type + 2 bytes keyLen + 4 bytes valLen (7 bytes)
	//   Key starts at opOffset + 7
	//   Value starts at opOffset + 7 + keyLen
	indexOps := make([]IndexOp, len(ops))
	curOpOffset := oldSize + int64(RecordHeaderSize) + 4 // 8 + 4 = 12 bytes after record start

	for i := range ops {
		op := &ops[i]
		keyLen := int64(len(op.Key))
		valLen := int64(len(op.Value))

		valOffset := curOpOffset + int64(OpHeaderSize) + keyLen

		indexOps[i] = IndexOp{
			Type:        op.Type,
			Key:         string(op.Key),
			ValueOffset: valOffset,
			ValueLen:    uint32(valLen),
		}

		curOpOffset += int64(OpHeaderSize) + keyLen + valLen
	}

	return oldSize, indexOps, nil
}

// rollback attempts to truncate the file back to oldSize and sync the truncation.
// If either Truncate or Sync fails, the DB is marked unusable to prevent further corruption.
// Caller MUST hold db.mu.Lock.
func (db *DB) rollback(oldSize int64) error {
	truncErr := db.file.Truncate(oldSize)
	if truncErr != nil {
		db.unusable = true
		return fmt.Errorf("truncate to %d failed: %w", oldSize, truncErr)
	}

	syncErr := db.file.Sync()
	if syncErr != nil {
		db.unusable = true
		return fmt.Errorf("sync after truncate failed: %w", syncErr)
	}

	db.size = oldSize
	return nil
}
