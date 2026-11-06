package killitdb

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"testing"
)

func TestEncodeRecord_OnePut(t *testing.T) {
	ops := []Operation{
		{
			Type:  OpPut,
			Key:   []byte("test-key"),
			Value: []byte("test-value"),
		},
	}

	buf, err := encodeRecord(ops)
	if err != nil {
		t.Fatalf("unexpected error encoding single PUT: %v", err)
	}

	// Payload = 4 (opCount) + 1 (type) + 2 (keyLen) + 4 (valLen) + 8 (key) + 10 (val) = 29
	expectedPayloadLen := uint32(4 + 1 + 2 + 4 + len("test-key") + len("test-value"))
	expectedTotalLen := RecordHeaderSize + int(expectedPayloadLen)

	if len(buf) != expectedTotalLen {
		t.Fatalf("expected total length %d, got %d", expectedTotalLen, len(buf))
	}

	payloadLen := binary.LittleEndian.Uint32(buf[0:4])
	if payloadLen != expectedPayloadLen {
		t.Fatalf("expected payload length %d, got %d", expectedPayloadLen, payloadLen)
	}

	crc := binary.LittleEndian.Uint32(buf[4:8])

	// Validate CRC
	hasher := crc32.NewIEEE()
	hasher.Write(buf[0:4])
	hasher.Write(buf[8:])
	expectedCRC := hasher.Sum32()

	if crc != expectedCRC {
		t.Fatalf("CRC mismatch: expected %d, got %d", expectedCRC, crc)
	}

	// Validate payload fields
	opCount := binary.LittleEndian.Uint32(buf[8:12])
	if opCount != 1 {
		t.Fatalf("expected opCount 1, got %d", opCount)
	}

	if OperationType(buf[12]) != OpPut {
		t.Fatalf("expected op type %d, got %d", OpPut, buf[12])
	}

	keyLen := binary.LittleEndian.Uint16(buf[13:15])
	if int(keyLen) != len("test-key") {
		t.Fatalf("expected key length %d, got %d", len("test-key"), keyLen)
	}

	valLen := binary.LittleEndian.Uint32(buf[15:19])
	if int(valLen) != len("test-value") {
		t.Fatalf("expected value length %d, got %d", len("test-value"), valLen)
	}

	valStart := 19 + int(keyLen)
	valEnd := valStart + int(valLen)
	if string(buf[19:valStart]) != "test-key" {
		t.Fatalf("expected key 'test-key', got '%s'", string(buf[19:valStart]))
	}

	if string(buf[valStart:valEnd]) != "test-value" {
		t.Fatalf("expected value 'test-value', got '%s'", string(buf[valStart:valEnd]))
	}
}

func TestEncodeRecord_OneDelete(t *testing.T) {
	ops := []Operation{
		{
			Type:  OpDelete,
			Key:   []byte("deleted-key"),
			Value: nil,
		},
	}

	buf, err := encodeRecord(ops)
	if err != nil {
		t.Fatalf("unexpected error encoding single DELETE: %v", err)
	}

	expectedPayloadLen := uint32(4 + 1 + 2 + 4 + len("deleted-key") + 0)
	payloadLen := binary.LittleEndian.Uint32(buf[0:4])
	if payloadLen != expectedPayloadLen {
		t.Fatalf("expected payload length %d, got %d", expectedPayloadLen, payloadLen)
	}

	// Verify DELETE has 0 value length
	valLen := binary.LittleEndian.Uint32(buf[15:19])
	if valLen != 0 {
		t.Fatalf("expected 0 value length for DELETE, got %d", valLen)
	}
}

func TestEncodeRecord_DeleteWithValue_Rejected(t *testing.T) {
	ops := []Operation{
		{
			Type:  OpDelete,
			Key:   []byte("key"),
			Value: []byte("cannot-have-value"),
		},
	}

	_, err := encodeRecord(ops)
	if !errors.Is(err, ErrDeleteWithValue) {
		t.Fatalf("expected ErrDeleteWithValue, got %v", err)
	}
}

func TestEncodeRecord_MultipleOperations(t *testing.T) {
	ops := []Operation{
		{Type: OpPut, Key: []byte("k1"), Value: []byte("v1")},
		{Type: OpPut, Key: []byte("k2"), Value: []byte("v2-longer")},
		{Type: OpDelete, Key: []byte("k1"), Value: nil},
		{Type: OpPut, Key: []byte("k3"), Value: []byte("")},
	}

	buf, err := encodeRecord(ops)
	if err != nil {
		t.Fatalf("unexpected error encoding multiple ops: %v", err)
	}

	opCount := binary.LittleEndian.Uint32(buf[8:12])
	if opCount != uint32(len(ops)) {
		t.Fatalf("expected opCount %d, got %d", len(ops), opCount)
	}

	// Validate CRC on multi-op record
	hasher := crc32.NewIEEE()
	hasher.Write(buf[0:4])
	hasher.Write(buf[8:])
	expectedCRC := hasher.Sum32()

	actualCRC := binary.LittleEndian.Uint32(buf[4:8])
	if actualCRC != expectedCRC {
		t.Fatalf("CRC mismatch: expected %d, got %d", expectedCRC, actualCRC)
	}
}

func TestEncodeRecord_EmptyValue(t *testing.T) {
	ops := []Operation{
		{
			Type:  OpPut,
			Key:   []byte("empty-val-key"),
			Value: []byte{},
		},
	}

	buf, err := encodeRecord(ops)
	if err != nil {
		t.Fatalf("unexpected error with empty value: %v", err)
	}

	valLen := binary.LittleEndian.Uint32(buf[15:19])
	if valLen != 0 {
		t.Fatalf("expected value length 0, got %d", valLen)
	}
}

func TestEncodeRecord_MaximumKeyLength(t *testing.T) {
	maxKey := bytes.Repeat([]byte("k"), MaxKeyLen)
	ops := []Operation{
		{
			Type:  OpPut,
			Key:   maxKey,
			Value: []byte("val"),
		},
	}

	buf, err := encodeRecord(ops)
	if err != nil {
		t.Fatalf("unexpected error with max key length (%d): %v", MaxKeyLen, err)
	}

	keyLen := binary.LittleEndian.Uint16(buf[13:15])
	if keyLen != MaxKeyLen {
		t.Fatalf("expected key length %d, got %d", MaxKeyLen, keyLen)
	}
}

func TestEncodeRecord_InvalidKeyLength(t *testing.T) {
	// Zero length key
	opsZero := []Operation{
		{Type: OpPut, Key: []byte{}, Value: []byte("v")},
	}
	if _, err := encodeRecord(opsZero); !errors.Is(err, ErrInvalidKeyLength) {
		t.Fatalf("expected ErrInvalidKeyLength for 0-len key, got %v", err)
	}

	// Key exceeding MaxKeyLen (1025 bytes)
	tooLongKey := bytes.Repeat([]byte("k"), MaxKeyLen+1)
	opsTooLong := []Operation{
		{Type: OpPut, Key: tooLongKey, Value: []byte("v")},
	}
	if _, err := encodeRecord(opsTooLong); !errors.Is(err, ErrInvalidKeyLength) {
		t.Fatalf("expected ErrInvalidKeyLength for 1025-byte key, got %v", err)
	}
}

func TestEncodeRecord_InvalidValueLength(t *testing.T) {
	tooLongVal := make([]byte, MaxValueLen+1)
	ops := []Operation{
		{Type: OpPut, Key: []byte("k"), Value: tooLongVal},
	}

	if _, err := encodeRecord(ops); !errors.Is(err, ErrInvalidValueLength) {
		t.Fatalf("expected ErrInvalidValueLength for >1MB value, got %v", err)
	}
}

func TestEncodeRecord_InvalidOperationType(t *testing.T) {
	invalidTypes := []OperationType{0, 3, 255}
	for _, typ := range invalidTypes {
		ops := []Operation{
			{Type: typ, Key: []byte("k"), Value: []byte("v")},
		}
		if _, err := encodeRecord(ops); !errors.Is(err, ErrInvalidOpType) {
			t.Fatalf("expected ErrInvalidOpType for type %d, got %v", typ, err)
		}
	}
}

func TestEncodeRecord_EmptyBatch(t *testing.T) {
	if _, err := encodeRecord(nil); !errors.Is(err, ErrEmptyBatch) {
		t.Fatalf("expected ErrEmptyBatch for nil slice, got %v", err)
	}

	if _, err := encodeRecord([]Operation{}); !errors.Is(err, ErrEmptyBatch) {
		t.Fatalf("expected ErrEmptyBatch for empty slice, got %v", err)
	}
}

func TestEncodeRecord_TooManyOperations(t *testing.T) {
	ops := make([]Operation, MaxBatchOps+1)
	for i := range ops {
		ops[i] = Operation{Type: OpPut, Key: []byte("k"), Value: []byte("v")}
	}

	if _, err := encodeRecord(ops); !errors.Is(err, ErrBatchTooLarge) {
		t.Fatalf("expected ErrBatchTooLarge for %d ops, got %v", len(ops), err)
	}
}

func TestEncodeRecord_CRCCalculationCorrectness(t *testing.T) {
	ops := []Operation{
		{Type: OpPut, Key: []byte("hello"), Value: []byte("world")},
	}

	buf, err := encodeRecord(ops)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	storedCRC := binary.LittleEndian.Uint32(buf[4:8])

	// Explicit manual CRC over payload length (4 bytes) + payload
	manualHasher := crc32.NewIEEE()
	manualHasher.Write(buf[0:4])
	manualHasher.Write(buf[8:])
	manualCRC := manualHasher.Sum32()

	if storedCRC != manualCRC {
		t.Fatalf("stored CRC %d != manual CRC %d", storedCRC, manualCRC)
	}

	// Verify CRC does not match if payload length byte is flipped
	tamperedLenBuf := make([]byte, len(buf))
	copy(tamperedLenBuf, buf)
	tamperedLenBuf[0] ^= 0xFF
	hasherTampered := crc32.NewIEEE()
	hasherTampered.Write(tamperedLenBuf[0:4])
	hasherTampered.Write(tamperedLenBuf[8:])
	if hasherTampered.Sum32() == storedCRC {
		t.Fatalf("CRC failed to detect payload length byte corruption")
	}

	// Verify CRC does not match if payload byte is flipped
	tamperedPayloadBuf := make([]byte, len(buf))
	copy(tamperedPayloadBuf, buf)
	tamperedPayloadBuf[len(tamperedPayloadBuf)-1] ^= 0xFF
	hasherTamperedPayload := crc32.NewIEEE()
	hasherTamperedPayload.Write(tamperedPayloadBuf[0:4])
	hasherTamperedPayload.Write(tamperedPayloadBuf[8:])
	if hasherTamperedPayload.Sum32() == storedCRC {
		t.Fatalf("CRC failed to detect payload byte corruption")
	}
}

func TestEncodeRecord_MaxAllowedBatch(t *testing.T) {
	// Verify exact boundary of MaxBatchOps (10,000 operations)
	ops := make([]Operation, MaxBatchOps)
	for i := range ops {
		ops[i] = Operation{Type: OpPut, Key: []byte("k"), Value: nil}
	}

	buf, err := encodeRecord(ops)
	if err != nil {
		t.Fatalf("unexpected error with exactly %d ops: %v", MaxBatchOps, err)
	}

	opCount := binary.LittleEndian.Uint32(buf[8:12])
	if opCount != MaxBatchOps {
		t.Fatalf("expected opCount %d, got %d", MaxBatchOps, opCount)
	}
}
