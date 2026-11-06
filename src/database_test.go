package killitdb

import (
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestDB_CreateNewDatabase(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	db, err := Open(dbPath, 1024*1024)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	if db.Path() != dbPath {
		t.Fatalf("expected path %s, got %s", dbPath, db.Path())
	}
	if db.Size() != HeaderSize {
		t.Fatalf("expected size %d, got %d", HeaderSize, db.Size())
	}

	if err := db.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Verify file on disk
	info, err := os.Stat(dbPath)
	if err != nil {
		t.Fatalf("Stat failed: %v", err)
	}
	if info.Size() != HeaderSize {
		t.Fatalf("expected on-disk file size %d, got %d", HeaderSize, info.Size())
	}

	// Verify header bytes
	data, err := os.ReadFile(dbPath)
	if err != nil {
		t.Fatalf("ReadFile failed: %v", err)
	}
	if len(data) != HeaderSize {
		t.Fatalf("expected exactly %d header bytes, got %d", HeaderSize, len(data))
	}
	if string(data[0:8]) != HeaderMagic {
		t.Fatalf("expected magic %s, got %s", HeaderMagic, string(data[0:8]))
	}

	version := binary.LittleEndian.Uint32(data[8:12])
	if version != CurrentVersion {
		t.Fatalf("expected version %d, got %d", CurrentVersion, version)
	}

	reserved := binary.LittleEndian.Uint32(data[12:16])
	if reserved != 0 {
		t.Fatalf("expected reserved 0, got %d", reserved)
	}
}

func TestDB_ReopenEmptyDatabase(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "reopen.db")

	// First open creates the header
	db1, err := Open(dbPath, 1024*1024)
	if err != nil {
		t.Fatalf("initial Open failed: %v", err)
	}
	if err := db1.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Second open reopens without recreating
	db2, err := Open(dbPath, 1024*1024)
	if err != nil {
		t.Fatalf("reopen failed: %v", err)
	}
	if db2.Size() != HeaderSize {
		t.Fatalf("expected size %d on reopen, got %d", HeaderSize, db2.Size())
	}
	if err := db2.Close(); err != nil {
		t.Fatalf("Close after reopen failed: %v", err)
	}
}

func TestDB_InvalidMagic(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "bad_magic.db")

	// Write 16 bytes with bad magic
	var badHeader [HeaderSize]byte
	copy(badHeader[0:8], "BADMAGIC")
	binary.LittleEndian.PutUint32(badHeader[8:12], CurrentVersion)
	binary.LittleEndian.PutUint32(badHeader[12:16], 0)

	if err := os.WriteFile(dbPath, badHeader[:], 0666); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	_, err := Open(dbPath, 1024*1024)
	if !errors.Is(err, ErrInvalidMagic) {
		t.Fatalf("expected ErrInvalidMagic, got %v", err)
	}
}

func TestDB_InvalidVersion(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "bad_version.db")

	// Write 16 bytes with unsupported version 99
	var badHeader [HeaderSize]byte
	copy(badHeader[0:8], HeaderMagic)
	binary.LittleEndian.PutUint32(badHeader[8:12], 99)
	binary.LittleEndian.PutUint32(badHeader[12:16], 0)

	if err := os.WriteFile(dbPath, badHeader[:], 0666); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	_, err := Open(dbPath, 1024*1024)
	if !errors.Is(err, ErrUnsupportedVersion) {
		t.Fatalf("expected ErrUnsupportedVersion, got %v", err)
	}
}

func TestDB_InvalidHeaderReserved(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "bad_reserved.db")

	// Write 16 bytes with non-zero reserved field
	var badHeader [HeaderSize]byte
	copy(badHeader[0:8], HeaderMagic)
	binary.LittleEndian.PutUint32(badHeader[8:12], CurrentVersion)
	binary.LittleEndian.PutUint32(badHeader[12:16], 123)

	if err := os.WriteFile(dbPath, badHeader[:], 0666); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	_, err := Open(dbPath, 1024*1024)
	if !errors.Is(err, ErrInvalidHeaderReserved) {
		t.Fatalf("expected ErrInvalidHeaderReserved, got %v", err)
	}
}

func TestDB_InvalidMaxBytes(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "invalid_max.db")

	// Less than HeaderSize (16)
	_, err := Open(dbPath, 15)
	if !errors.Is(err, ErrInvalidMaxBytes) {
		t.Fatalf("expected ErrInvalidMaxBytes for 15, got %v", err)
	}

	// Negative
	_, err = Open(dbPath, -1)
	if !errors.Is(err, ErrInvalidMaxBytes) {
		t.Fatalf("expected ErrInvalidMaxBytes for -1, got %v", err)
	}

	// 0
	_, err = Open(dbPath, 0)
	if !errors.Is(err, ErrInvalidMaxBytes) {
		t.Fatalf("expected ErrInvalidMaxBytes for 0, got %v", err)
	}
}

func TestDB_FileShorterThanHeader_Recovery(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "interrupted_header.db")

	// Simulate a process killed after writing 5 bytes of header
	if err := os.WriteFile(dbPath, []byte("KVDB0"), 0666); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	// Open should detect file is shorter than 16 bytes, safely re-initialize the full header, and succeed
	db, err := Open(dbPath, 1024*1024)
	if err != nil {
		t.Fatalf("Open failed on partial header: %v", err)
	}
	defer db.Close()

	if db.Size() != HeaderSize {
		t.Fatalf("expected size %d after re-initialization, got %d", HeaderSize, db.Size())
	}

	// Verify on disk
	data, err := os.ReadFile(dbPath)
	if err != nil {
		t.Fatalf("ReadFile failed: %v", err)
	}
	if len(data) != HeaderSize {
		t.Fatalf("expected %d bytes, got %d", HeaderSize, len(data))
	}
	if string(data[0:8]) != HeaderMagic {
		t.Fatalf("expected magic %s, got %s", HeaderMagic, string(data[0:8]))
	}
}

func TestDB_RepeatedClose(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "repeated_close.db")

	db, err := Open(dbPath, 1024*1024)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	// First Close
	if err := db.Close(); err != nil {
		t.Fatalf("first Close failed: %v", err)
	}

	// Second Close must not panic or error
	if err := db.Close(); err != nil {
		t.Fatalf("second Close failed: %v", err)
	}

	// Third Close must also be a safe no-op
	if err := db.Close(); err != nil {
		t.Fatalf("third Close failed: %v", err)
	}
}

func TestDB_OpenExistingFile_PreservesContent(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "existing_records.db")

	// Write 16-byte valid header plus some mock record bytes
	var header [HeaderSize]byte
	copy(header[0:8], HeaderMagic)
	binary.LittleEndian.PutUint32(header[8:12], CurrentVersion)
	binary.LittleEndian.PutUint32(header[12:16], 0)

	payload := []byte("mock-pre-existing-record-content")
	fullFile := append(header[:], payload...)

	if err := os.WriteFile(dbPath, fullFile, 0666); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	// Open should reject because records beyond header are not yet supported in Step 4,
	// and CRUCIALLY must NOT truncate or overwrite the file!
	_, err := Open(dbPath, 1024*1024)
	if !errors.Is(err, ErrRecordsNotSupported) {
		t.Fatalf("expected ErrRecordsNotSupported, got %v", err)
	}

	// Verify the original contents and file size remain completely untouched
	dataAfter, err := os.ReadFile(dbPath)
	if err != nil {
		t.Fatalf("ReadFile failed: %v", err)
	}
	if len(dataAfter) != len(fullFile) {
		t.Fatalf("file was modified! expected length %d, got %d", len(fullFile), len(dataAfter))
	}
	if string(dataAfter[HeaderSize:]) != string(payload) {
		t.Fatalf("file content was corrupted or overwritten!")
	}
}
