package killitdb

import (
	"encoding/binary"
	"fmt"
	"os"
)

// recoverDatabase performs crash-safe scanning and rebuilding of the in-memory index
// across the authoritative data file.
//
// Crash-Safety & Integrity Assumptions:
//  1. Crashes produce torn tails at the end of the file, not arbitrary media corruption.
//     A CRC failure or incomplete record encountered during scanning terminates recovery
//     at that offset, treating subsequent bytes as an unacknowledged torn write.
//  2. If file size is less than 16 bytes (e.g. 0 bytes or killed during initial header write),
//     the file is truncated to 0, a fresh 16-byte header is written at offset 0, and synced.
//  3. When an incomplete, truncated, or invalid record is encountered, all subsequent bytes
//     are considered an incomplete write: the file is truncated back to the last valid record
//     offset and synced before returning a usable database.
//  4. Recovery scanning is strictly read-only until the final truncation (if needed), making
//     recovery repeatable if interrupted by another crash during recovery.
//
// Performance notes:
//   - A single record buffer is grown as needed and reused across all records; this avoids
//     one heap allocation per record on the hot path.
//   - decodeRecordForRecovery skips value-byte copies (values stay on disk and are not
//     loaded into memory during recovery).
func recoverDatabase(file storageFile, maxBytes int64) (*Index, int64, error) {
	stat, err := file.Stat()
	if err != nil {
		return nil, 0, fmt.Errorf("failed to stat file during recovery: %w", err)
	}

	fileSize := stat.Size()
	if fileSize > maxBytes {
		return nil, 0, ErrFileExceedsMaxBytes
	}

	// 1. If file is shorter than 16 bytes, safely initialize the header.
	if fileSize < HeaderSize {
		if err := file.Truncate(0); err != nil {
			return nil, 0, fmt.Errorf("failed to truncate partial header file: %w", err)
		}

		var header [HeaderSize]byte
		copy(header[0:8], HeaderMagic)
		binary.LittleEndian.PutUint32(header[8:12], CurrentVersion)
		binary.LittleEndian.PutUint32(header[12:16], 0)

		if _, err := file.WriteAt(header[:], 0); err != nil {
			return nil, 0, fmt.Errorf("failed to write header during recovery: %w", err)
		}
		if err := file.Sync(); err != nil {
			return nil, 0, fmt.Errorf("failed to sync header during recovery: %w", err)
		}

		fileSize = HeaderSize
	}

	// 2. Validate the 16-byte header.
	var headerBuf [HeaderSize]byte
	if _, err := file.ReadAt(headerBuf[:], 0); err != nil {
		return nil, 0, fmt.Errorf("failed to read header during recovery: %w", err)
	}

	if string(headerBuf[0:8]) != HeaderMagic {
		return nil, 0, ErrInvalidMagic
	}
	version := binary.LittleEndian.Uint32(headerBuf[8:12])
	if version != CurrentVersion {
		return nil, 0, ErrUnsupportedVersion
	}
	reserved := binary.LittleEndian.Uint32(headerBuf[12:16])
	if reserved != 0 {
		return nil, 0, ErrInvalidHeaderReserved
	}

	// 3. Scan records starting at offset 16.
	fmt.Fprintln(os.Stderr, "RECOVERY_SCAN_START")
	index := newIndex()
	scanOffset := int64(HeaderSize)

	// Reuse a single growing buffer across all records to avoid one heap
	// allocation per record. The slice is re-sliced to fit each record's
	// exact size without reallocating when the capacity suffices.
	var recBuf []byte

	for {
		remaining := fileSize - scanOffset

		// Verify at least 8 bytes remain for the outer record header.
		if remaining < int64(RecordHeaderSize) {
			// Incomplete outer record header (0 to 7 bytes) -> torn tail.
			break
		}

		// Peek at the outer 8-byte header to learn payloadLen.
		var recHeader [RecordHeaderSize]byte
		if _, err := file.ReadAt(recHeader[:], scanOffset); err != nil {
			// Read error -> stop recovery at last valid offset.
			break
		}

		payloadLen := binary.LittleEndian.Uint32(recHeader[0:4])
		totalRecordSize := int64(RecordHeaderSize) + int64(payloadLen)

		// Verify payload length fits within remaining file size without overflow.
		if totalRecordSize < int64(RecordHeaderSize) || totalRecordSize > remaining {
			// Truncated payload -> torn tail.
			break
		}

		// Grow the shared buffer only when the current record is larger.
		if int64(cap(recBuf)) < totalRecordSize {
			recBuf = make([]byte, totalRecordSize)
		}
		recBuf = recBuf[:totalRecordSize]

		if _, err := file.ReadAt(recBuf, scanOffset); err != nil {
			break
		}

		// Verify CRC and decode operations with relative offsets.
		// decodeRecordForRecovery skips value-byte allocation (values stay on disk).
		decodedOps, err := decodeRecordForRecovery(recBuf)
		if err != nil {
			// CRC mismatch, corrupt payload, or invalid operation data -> stop recovery.
			break
		}

		// Apply operations to the in-memory index in order.
		// absoluteValueOffset = recordStartOffset + relativeValueOffset
		for i := range decodedOps {
			op := &decodedOps[i]
			absValOffset := scanOffset + op.RelativeValOffset
			index.Apply(IndexOp{
				Type:        op.Type,
				Key:         string(op.Key),
				ValueOffset: absValOffset,
				ValueLen:    op.ValueLen,
			})
		}

		scanOffset += totalRecordSize
	}

	// 4. If an incomplete or torn tail exists, truncate and sync.
	if scanOffset < fileSize {
		fmt.Fprintln(os.Stderr, "RECOVERY_TRUNCATE_START")
		if err := file.Truncate(scanOffset); err != nil {
			return nil, 0, fmt.Errorf("failed to truncate torn tail to %d: %w", scanOffset, err)
		}
		if err := file.Sync(); err != nil {
			return nil, 0, fmt.Errorf("failed to sync truncation to %d: %w", scanOffset, err)
		}
	}

	return index, scanOffset, nil
}
