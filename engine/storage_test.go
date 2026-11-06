package killitdb

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// mockStorageFile implements storageFile for fault-injection testing.
type mockStorageFile struct {
	data          []byte
	failWriteAt   error
	shortWriteN   int
	failSync      error
	failSyncTimes int
	failTruncate  error
	syncCount     int
	writeCount    int
}

func newMockStorageFile(initialHeader []byte) *mockStorageFile {
	buf := make([]byte, len(initialHeader))
	copy(buf, initialHeader)
	return &mockStorageFile{data: buf}
}

func (m *mockStorageFile) ReadAt(p []byte, off int64) (int, error) {
	if off >= int64(len(m.data)) {
		return 0, io.EOF
	}
	n := copy(p, m.data[off:])
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

func (m *mockStorageFile) WriteAt(p []byte, off int64) (int, error) {
	m.writeCount++
	if m.failWriteAt != nil {
		return 0, m.failWriteAt
	}
	if m.shortWriteN > 0 {
		needed := int(off) + m.shortWriteN
		if needed > len(m.data) {
			m.data = append(m.data, make([]byte, needed-len(m.data))...)
		}
		copy(m.data[off:], p[:m.shortWriteN])
		return m.shortWriteN, nil
	}

	needed := int(off) + len(p)
	if needed > len(m.data) {
		m.data = append(m.data, make([]byte, needed-len(m.data))...)
	}
	copy(m.data[off:], p)
	return len(p), nil
}

func (m *mockStorageFile) Truncate(size int64) error {
	if m.failTruncate != nil {
		return m.failTruncate
	}
	if size <= int64(len(m.data)) {
		m.data = m.data[:size]
	}
	return nil
}

func (m *mockStorageFile) Sync() error {
	m.syncCount++
	if m.failSyncTimes > 0 {
		m.failSyncTimes--
		return m.failSync
	}
	if m.failSyncTimes == -1 {
		return m.failSync
	}
	return nil
}

func (m *mockStorageFile) Close() error {
	return nil
}

type dummyFileInfo struct {
	size int64
}

func (d dummyFileInfo) Name() string       { return "mock" }
func (d dummyFileInfo) Size() int64        { return d.size }
func (d dummyFileInfo) Mode() os.FileMode  { return 0666 }
func (d dummyFileInfo) ModTime() time.Time { return time.Now() }
func (d dummyFileInfo) IsDir() bool        { return false }
func (d dummyFileInfo) Sys() interface{}   { return nil }

func (m *mockStorageFile) Stat() (os.FileInfo, error) {
	return dummyFileInfo{size: int64(len(m.data))}, nil
}

func TestStorage_SuccessfulAppend(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "append.db")

	db, err := Open(dbPath, 1024*1024)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer db.Close()

	ops := []Operation{
		{Type: OpPut, Key: []byte("greeting"), Value: []byte("hello-world")},
	}

	encoded, err := encodeRecord(ops)
	if err != nil {
		t.Fatalf("encodeRecord failed: %v", err)
	}
	expectedRecordLen := int64(len(encoded))

	db.mu.Lock()
	recordOffset, indexOps, err := db.appendRecord(ops)
	db.mu.Unlock()

	if err != nil {
		t.Fatalf("appendRecord failed: %v", err)
	}

	if recordOffset != HeaderSize {
		t.Fatalf("expected recordOffset %d, got %d", HeaderSize, recordOffset)
	}

	expectedNewSize := int64(HeaderSize) + expectedRecordLen
	if db.Size() != expectedNewSize {
		t.Fatalf("expected db.Size() %d, got %d", expectedNewSize, db.Size())
	}

	// Verify file on disk grew by exact record length
	stat, err := os.Stat(dbPath)
	if err != nil {
		t.Fatalf("Stat failed: %v", err)
	}
	if stat.Size() != expectedNewSize {
		t.Fatalf("expected on-disk size %d, got %d", expectedNewSize, stat.Size())
	}

	// Verify indexOps metadata returned
	if len(indexOps) != 1 {
		t.Fatalf("expected 1 indexOp, got %d", len(indexOps))
	}
	if indexOps[0].Key != "greeting" {
		t.Fatalf("expected key 'greeting', got '%s'", indexOps[0].Key)
	}
	if indexOps[0].ValueLen != uint32(len("hello-world")) {
		t.Fatalf("expected valueLen %d, got %d", len("hello-world"), indexOps[0].ValueLen)
	}

	// Verify ReadAt using indexOps[0].ValueOffset
	valBuf := make([]byte, indexOps[0].ValueLen)
	n, err := db.file.ReadAt(valBuf, indexOps[0].ValueOffset)
	if err != nil {
		t.Fatalf("ReadAt value failed: %v", err)
	}
	if n != len(valBuf) || string(valBuf) != "hello-world" {
		t.Fatalf("expected 'hello-world' at offset %d, got '%s'", indexOps[0].ValueOffset, string(valBuf))
	}

	// Decode the appended record from disk
	recordBuf := make([]byte, expectedRecordLen)
	if _, err := db.file.ReadAt(recordBuf, recordOffset); err != nil {
		t.Fatalf("ReadAt record failed: %v", err)
	}
	decoded, err := decodeRecord(recordBuf)
	if err != nil {
		t.Fatalf("decodeRecord failed on appended record: %v", err)
	}
	if len(decoded) != 1 || string(decoded[0].Key) != "greeting" || string(decoded[0].Value) != "hello-world" {
		t.Fatalf("decoded record mismatch: %+v", decoded)
	}
}

func TestStorage_NoIndexChangesOccurHere(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "no_index.db")

	db, err := Open(dbPath, 1024*1024)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer db.Close()

	if db.index.Len() != 0 {
		t.Fatalf("expected index to be empty before append")
	}

	ops := []Operation{
		{Type: OpPut, Key: []byte("foo"), Value: []byte("bar")},
	}

	db.mu.Lock()
	_, _, err = db.appendRecord(ops)
	db.mu.Unlock()

	if err != nil {
		t.Fatalf("appendRecord failed: %v", err)
	}

	// CRITICAL RULE: appendRecord must NOT update index!
	if db.index.Len() != 0 {
		t.Fatalf("appendRecord modified index! expected len 0, got %d", db.index.Len())
	}
}

func TestStorage_SyncCalledBeforeSuccess(t *testing.T) {
	mock := newMockStorageFile(make([]byte, HeaderSize))
	db := &DB{
		file:     mock,
		size:     HeaderSize,
		maxBytes: 1024 * 1024,
		index:    newIndex(),
	}

	ops := []Operation{
		{Type: OpPut, Key: []byte("k"), Value: []byte("v")},
	}

	db.mu.Lock()
	_, _, err := db.appendRecord(ops)
	db.mu.Unlock()

	if err != nil {
		t.Fatalf("appendRecord failed: %v", err)
	}

	if mock.syncCount != 1 {
		t.Fatalf("expected exactly 1 sync call on success, got %d", mock.syncCount)
	}
}

func TestStorage_MaxFileSizeRejectsWithoutModifying(t *testing.T) {
	mock := newMockStorageFile(make([]byte, HeaderSize))
	// Set maxBytes smaller than header + record length
	db := &DB{
		file:     mock,
		size:     HeaderSize,
		maxBytes: HeaderSize + 5, // Not enough room for record
		index:    newIndex(),
	}

	ops := []Operation{
		{Type: OpPut, Key: []byte("large-key"), Value: []byte("large-val")},
	}

	db.mu.Lock()
	_, _, err := db.appendRecord(ops)
	db.mu.Unlock()

	if !errors.Is(err, ErrDiskFull) {
		t.Fatalf("expected ErrDiskFull, got %v", err)
	}

	if db.size != HeaderSize {
		t.Fatalf("expected db.size unchanged (%d), got %d", HeaderSize, db.size)
	}
	if len(mock.data) != HeaderSize {
		t.Fatalf("file length modified on rejected write! got %d", len(mock.data))
	}
	if mock.writeCount != 0 {
		t.Fatalf("WriteAt was called despite exceeding maxBytes!")
	}
}

func TestStorage_SimulatedShortWrite_Rollback(t *testing.T) {
	mock := newMockStorageFile(make([]byte, HeaderSize))
	mock.shortWriteN = 10 // Only write 10 bytes

	db := &DB{
		file:     mock,
		size:     HeaderSize,
		maxBytes: 1024 * 1024,
		index:    newIndex(),
	}

	ops := []Operation{
		{Type: OpPut, Key: []byte("k"), Value: []byte("v")},
	}

	db.mu.Lock()
	_, _, err := db.appendRecord(ops)
	db.mu.Unlock()

	if !errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("expected io.ErrShortWrite, got %v", err)
	}

	// Verify rollback restored file to HeaderSize
	if len(mock.data) != HeaderSize {
		t.Fatalf("expected rollback to HeaderSize (%d), got %d", HeaderSize, len(mock.data))
	}
	if db.size != HeaderSize {
		t.Fatalf("expected db.size to remain %d, got %d", HeaderSize, db.size)
	}
	if db.unusable {
		t.Fatalf("database should not be poisoned when rollback succeeds")
	}
}

func TestStorage_SimulatedWriteError_Rollback(t *testing.T) {
	mock := newMockStorageFile(make([]byte, HeaderSize))
	mock.failWriteAt = errors.New("simulated I/O disk failure")

	db := &DB{
		file:     mock,
		size:     HeaderSize,
		maxBytes: 1024 * 1024,
		index:    newIndex(),
	}

	ops := []Operation{
		{Type: OpPut, Key: []byte("k"), Value: []byte("v")},
	}

	db.mu.Lock()
	_, _, err := db.appendRecord(ops)
	db.mu.Unlock()

	if err == nil || !errors.Is(err, mock.failWriteAt) {
		t.Fatalf("expected simulated write error, got %v", err)
	}

	if len(mock.data) != HeaderSize {
		t.Fatalf("expected file length %d, got %d", HeaderSize, len(mock.data))
	}
	if db.size != HeaderSize {
		t.Fatalf("expected db.size %d, got %d", HeaderSize, db.size)
	}
	if db.unusable {
		t.Fatalf("database should not be marked unusable when rollback succeeds")
	}
}

func TestStorage_SimulatedSyncError_Rollback(t *testing.T) {
	mock := newMockStorageFile(make([]byte, HeaderSize))
	mock.failSync = errors.New("simulated sync failure")
	mock.failSyncTimes = 1 // Fail the write's sync, but allow rollback's sync to succeed

	db := &DB{
		file:     mock,
		size:     HeaderSize,
		maxBytes: 1024 * 1024,
		index:    newIndex(),
	}

	ops := []Operation{
		{Type: OpPut, Key: []byte("k"), Value: []byte("v")},
	}

	db.mu.Lock()
	_, _, err := db.appendRecord(ops)
	db.mu.Unlock()

	if err == nil || !errors.Is(err, mock.failSync) {
		t.Fatalf("expected simulated sync error, got %v", err)
	}

	// Verify rollback truncated back to HeaderSize
	if len(mock.data) != HeaderSize {
		t.Fatalf("expected rollback to %d, got %d", HeaderSize, len(mock.data))
	}
	if db.size != HeaderSize {
		t.Fatalf("expected db.size %d, got %d", HeaderSize, db.size)
	}
	if db.unusable {
		t.Fatalf("database should not be marked unusable when rollback succeeds")
	}
}

func TestStorage_RollbackFailurePoisonsDB(t *testing.T) {
	mock := newMockStorageFile(make([]byte, HeaderSize))
	mock.failWriteAt = errors.New("disk write failure")
	mock.failTruncate = errors.New("truncate device error") // Rollback fails!

	db := &DB{
		file:     mock,
		size:     HeaderSize,
		maxBytes: 1024 * 1024,
		index:    newIndex(),
	}

	ops := []Operation{
		{Type: OpPut, Key: []byte("k"), Value: []byte("v")},
	}

	db.mu.Lock()
	_, _, err := db.appendRecord(ops)
	db.mu.Unlock()

	if err == nil {
		t.Fatalf("expected error on failed write + failed rollback")
	}

	// CRITICAL RULE: DB must now be marked unusable!
	if !db.unusable {
		t.Fatalf("db.unusable must be true when rollback cannot be confirmed")
	}

	// Any subsequent append must immediately fail with ErrDatabaseUnusable
	db.mu.Lock()
	_, _, secondErr := db.appendRecord(ops)
	db.mu.Unlock()

	if !errors.Is(secondErr, ErrDatabaseUnusable) {
		t.Fatalf("expected ErrDatabaseUnusable on poisoned DB, got %v", secondErr)
	}
}
