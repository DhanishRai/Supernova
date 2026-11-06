package killitdb

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"sync"
)

const (
	// HeaderSize is the fixed size in bytes of the authoritative file header.
	HeaderSize = 16

	// HeaderMagic is the 8-byte magic identifier at the start of the file.
	HeaderMagic = "KVDB0001"

	// CurrentVersion is the supported database file format version.
	CurrentVersion uint32 = 1
)

var (
	// ErrInvalidMaxBytes indicates maxBytes is less than the 16-byte header size or negative.
	ErrInvalidMaxBytes = errors.New("maxBytes must be at least 16 bytes")
	// ErrFileExceedsMaxBytes indicates the existing file size exceeds maxBytes.
	ErrFileExceedsMaxBytes = errors.New("database file size exceeds maximum configured size")
	// ErrInvalidMagic indicates the file header does not contain the KVDB0001 magic bytes.
	ErrInvalidMagic = errors.New("invalid database header magic")
	// ErrUnsupportedVersion indicates the file version is not supported.
	ErrUnsupportedVersion = errors.New("unsupported database format version")
	// ErrInvalidHeaderReserved indicates the reserved header field is non-zero.
	ErrInvalidHeaderReserved = errors.New("reserved header field must be zero")
	// ErrDatabaseClosed indicates an operation was attempted on a closed database.
	ErrDatabaseClosed = errors.New("database is closed")
	// ErrDatabaseUnusable indicates the database encountered an unrecoverable write or rollback failure.
	ErrDatabaseUnusable = errors.New("database is unusable due to unconfirmed rollback")
	// ErrDiskFull indicates appending the record would exceed the maximum configured database size.
	ErrDiskFull = errors.New("database file size limit reached")
	// ErrRecordsNotSupported indicates the file contains data beyond the 16-byte header
	// before recovery scanning has been implemented.
	ErrRecordsNotSupported = errors.New("database contains records beyond header; recovery not yet implemented")
)

// storageFile defines the file operations required by the storage engine.
// This interface allows fault-injection testing without external libraries.
type storageFile interface {
	ReadAt(p []byte, off int64) (n int, err error)
	WriteAt(p []byte, off int64) (n int, err error)
	Truncate(size int64) error
	Sync() error
	Close() error
	Stat() (os.FileInfo, error)
}

// DB represents an open instance of the key-value database engine.
type DB struct {
	mu       sync.RWMutex
	file     storageFile
	path     string
	size     int64
	maxBytes int64
	index    *Index
	closed   bool
	unusable bool
}

// Open opens an existing database or creates a new one at the specified path.
// If the file is shorter than 16 bytes (or empty/interrupted during previous creation),
// it initializes the 16-byte header at offset 0 and syncs to disk.
func Open(path string, maxBytes int64) (*DB, error) {
	if maxBytes < HeaderSize {
		return nil, ErrInvalidMaxBytes
	}

	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return nil, fmt.Errorf("failed to open database file: %w", err)
	}

	stat, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to stat database file: %w", err)
	}

	fileSize := stat.Size()
	if fileSize > maxBytes {
		file.Close()
		return nil, ErrFileExceedsMaxBytes
	}

	// If the file is shorter than 16 bytes (e.g. 0 bytes or interrupted write),
	// safely write the 16-byte header at offset zero and sync.
	if fileSize < HeaderSize {
		var header [HeaderSize]byte
		copy(header[0:8], HeaderMagic)
		binary.LittleEndian.PutUint32(header[8:12], CurrentVersion)
		binary.LittleEndian.PutUint32(header[12:16], 0) // Reserved uint32

		if _, err := file.WriteAt(header[:], 0); err != nil {
			file.Close()
			return nil, fmt.Errorf("failed to write database header: %w", err)
		}

		if err := file.Sync(); err != nil {
			file.Close()
			return nil, fmt.Errorf("failed to sync database header: %w", err)
		}

		fileSize = HeaderSize
	}

	// Read and validate the 16-byte header
	var headerBuf [HeaderSize]byte
	if _, err := file.ReadAt(headerBuf[:], 0); err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to read database header: %w", err)
	}

	if string(headerBuf[0:8]) != HeaderMagic {
		file.Close()
		return nil, ErrInvalidMagic
	}

	version := binary.LittleEndian.Uint32(headerBuf[8:12])
	if version != CurrentVersion {
		file.Close()
		return nil, ErrUnsupportedVersion
	}

	reserved := binary.LittleEndian.Uint32(headerBuf[12:16])
	if reserved != 0 {
		file.Close()
		return nil, ErrInvalidHeaderReserved
	}

	// In this step, reject files that contain records beyond the header
	// until recovery scanning is implemented in subsequent steps.
	if fileSize > HeaderSize {
		file.Close()
		return nil, ErrRecordsNotSupported
	}

	db := &DB{
		file:     file,
		path:     path,
		size:     fileSize,
		maxBytes: maxBytes,
		index:    newIndex(),
		closed:   false,
	}

	return db, nil
}

// Close flushes and closes the database data file.
// It is safe against repeated calls.
func (db *DB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.closed {
		return nil
	}

	db.closed = true

	var syncErr error
	if db.file != nil {
		syncErr = db.file.Sync()
		closeErr := db.file.Close()
		if syncErr != nil {
			return syncErr
		}
		if closeErr != nil {
			return closeErr
		}
	}

	return nil
}

// Path returns the path to the database file.
func (db *DB) Path() string {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return db.path
}

// Size returns the current size of the database file on disk.
func (db *DB) Size() int64 {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return db.size
}
