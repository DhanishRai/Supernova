package killitdb

import (
	"encoding/binary"
	"fmt"
	"os"
)

// Compact rewrites the authoritative data file, reclaiming space occupied by
// overwritten keys, tombstones, and dead records.
//
// Crash-Safety & Durability Order:
//  1. Acquires db.mu.Lock() to guarantee exclusivity against concurrent writers.
//  2. Removes any dangling compaction temporary file (db.path + ".tmp").
//  3. Creates the temporary file (db.path + ".tmp").
//  4. Writes the standard 16-byte header (KVDB0001, version 1, reserved 0).
//  5. Iterates all active keys from db.index, reads each live value via ReadAt from old file,
//     encodes each as an OpPut record, and appends to the temporary file.
//  6. Issues Sync() on the temporary file to ensure physical durability.
//  7. Closes the temporary file handle.
//  8. Closes the active database file handle (mandatory on Windows prior to replacement).
//  9. Atomically renames tmpPath over db.path (os.Rename).
//  10. Re-opens db.path and runs recoverDatabase() to verify integrity and rebuild the index
//     with updated on-disk offsets.
//  11. Updates db.file, db.size, and db.index to the new compacted state.
func (db *DB) Compact() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.closed {
		return ErrDatabaseClosed
	}
	if db.unusable {
		return ErrDatabaseUnusable
	}

	tmpPath := db.path + ".tmp"

	// 1. Clean up any existing .tmp file
	_ = os.Remove(tmpPath)

	// 2. Create the temporary compaction file
	tmpFile, err := os.OpenFile(tmpPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)
	if err != nil {
		return fmt.Errorf("compact: failed to create temporary file: %w", err)
	}

	// Helper for cleanup on failure before rename
	cleanupTmp := func() {
		_ = tmpFile.Close()
		_ = os.Remove(tmpPath)
	}

	// 3. Write 16-byte header to temporary file
	var header [HeaderSize]byte
	copy(header[0:8], HeaderMagic)
	binary.LittleEndian.PutUint32(header[8:12], CurrentVersion)
	binary.LittleEndian.PutUint32(header[12:16], 0)

	if _, err := tmpFile.WriteAt(header[:], 0); err != nil {
		cleanupTmp()
		return fmt.Errorf("compact: failed to write header: %w", err)
	}

	var tmpOffset int64 = HeaderSize

	// 4. Copy all active live key-values into temporary file
	keys := db.index.Keys()
	for _, k := range keys {
		ent, found := db.index.Get(k)
		if !found {
			continue
		}

		var val []byte
		if ent.valueLen > 0 {
			val = make([]byte, ent.valueLen)
			n, err := db.file.ReadAt(val, ent.valueOffset)
			if err != nil && err != os.ErrClosed {
				cleanupTmp()
				return fmt.Errorf("compact: failed to read value for key '%s': %w", k, err)
			}
			if n != int(ent.valueLen) {
				cleanupTmp()
				return fmt.Errorf("compact: short read for key '%s'", k)
			}
		}

		recordBytes, err := encodeRecord([]Operation{
			{
				Type:  OpPut,
				Key:   []byte(k),
				Value: val,
			},
		})
		if err != nil {
			cleanupTmp()
			return fmt.Errorf("compact: failed to encode record for key '%s': %w", k, err)
		}

		// Capacity boundary check against maxBytes
		if tmpOffset+int64(len(recordBytes)) > db.maxBytes {
			cleanupTmp()
			return ErrDiskFull
		}

		if _, err := tmpFile.WriteAt(recordBytes, tmpOffset); err != nil {
			cleanupTmp()
			return fmt.Errorf("compact: failed to write record for key '%s': %w", k, err)
		}

		tmpOffset += int64(len(recordBytes))
	}

	// 5. Sync temporary file to physical storage
	if err := tmpFile.Sync(); err != nil {
		cleanupTmp()
		return fmt.Errorf("compact: failed to sync temporary file: %w", err)
	}

	// 6. Close temporary file
	if err := tmpFile.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("compact: failed to close temporary file: %w", err)
	}

	// 7. Close old file handle before atomic replacement
	if err := db.file.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("compact: failed to close old file: %w", err)
	}

	// 8. Atomically replace old database file with compacted file
	if err := os.Rename(tmpPath, db.path); err != nil {
		// Attempt to reopen old file if rename fails
		reopenFile, reopenErr := os.OpenFile(db.path, os.O_RDWR, 0666)
		if reopenErr == nil {
			db.file = reopenFile
		} else {
			db.unusable = true
		}
		_ = os.Remove(tmpPath)
		return fmt.Errorf("compact: atomic rename failed: %w", err)
	}

	// 9. Reopen the newly compacted database file
	newFile, err := os.OpenFile(db.path, os.O_RDWR, 0666)
	if err != nil {
		db.unusable = true
		return fmt.Errorf("compact: failed to reopen compacted file: %w", err)
	}

	// 10. Rebuild in-memory index from compacted file
	newIndex, recoveredSize, err := recoverDatabase(newFile, db.maxBytes)
	if err != nil {
		_ = newFile.Close()
		db.unusable = true
		return fmt.Errorf("compact: recovery scan on compacted file failed: %w", err)
	}

	db.file = newFile
	db.size = recoveredSize
	db.index = newIndex

	return nil
}
