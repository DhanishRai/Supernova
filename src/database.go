package killitdb

import (
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
	// ErrKeyNotFound indicates the requested key does not exist in the database.
	ErrKeyNotFound = errors.New("key not found")
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
// It removes any dangling compaction temporary file (.tmp) and performs
// crash-safe recovery, scanning all durable records and rebuilding the in-memory index.
func Open(path string, maxBytes int64) (*DB, error) {
	if maxBytes < HeaderSize {
		return nil, ErrInvalidMaxBytes
	}

	// 1. Remove compaction temporary file (path + ".tmp") if present.
	tmpPath := path + ".tmp"
	if err := os.Remove(tmpPath); err != nil && !os.IsNotExist(err) {
		return nil, fmt.Errorf("failed to clean up compaction temporary file: %w", err)
	}

	// 2. Open or create the authoritative data file.
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return nil, fmt.Errorf("failed to open database file: %w", err)
	}

	// 3. Perform crash-safe recovery (header check, record scan, index rebuild, torn tail truncation).
	index, recoveredSize, err := recoverDatabase(file, maxBytes)
	if err != nil {
		file.Close()
		return nil, err
	}

	db := &DB{
		file:     file,
		path:     path,
		size:     recoveredSize,
		maxBytes: maxBytes,
		index:    index,
		closed:   false,
		unusable: false,
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
