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

// craftRecord creates a properly framed record with valid CRC for testing internal payload validation.
func craftRecord(payload []byte) []byte {
	buf := make([]byte, RecordHeaderSize+len(payload))
	binary.LittleEndian.PutUint32(buf[0:4], uint32(len(payload)))
	copy(buf[RecordHeaderSize:], payload)

	h := crc32.NewIEEE()
	h.Write(buf[0:4])
	h.Write(buf[RecordHeaderSize:])
	binary.LittleEndian.PutUint32(buf[4:8], h.Sum32())
	return buf
}

func TestDecodeRecord_RoundTrip(t *testing.T) {
	origOps := []Operation{
		{Type: OpPut, Key: []byte("user:101"), Value: []byte("Alice")},
		{Type: OpPut, Key: []byte("user:102"), Value: []byte("Bob")},
		{Type: OpDelete, Key: []byte("user:100"), Value: nil},
		{Type: OpPut, Key: []byte("flag:active"), Value: []byte{}},
	}

	buf, err := encodeRecord(origOps)
	if err != nil {
		t.Fatalf("encodeRecord failed: %v", err)
	}

	decodedOps, err := decodeRecord(buf)
	if err != nil {
		t.Fatalf("decodeRecord failed: %v", err)
	}

	if len(decodedOps) != len(origOps) {
		t.Fatalf("expected %d ops, got %d", len(origOps), len(decodedOps))
	}

	for i := range origOps {
		if decodedOps[i].Type != origOps[i].Type {
			t.Errorf("op[%d] type mismatch: expected %d, got %d", i, origOps[i].Type, decodedOps[i].Type)
		}
		if !bytes.Equal(decodedOps[i].Key, origOps[i].Key) {
			t.Errorf("op[%d] key mismatch: expected %s, got %s", i, origOps[i].Key, decodedOps[i].Key)
		}
		if !bytes.Equal(decodedOps[i].Value, origOps[i].Value) {
			t.Errorf("op[%d] value mismatch: expected %s, got %s", i, origOps[i].Value, decodedOps[i].Value)
		}
	}
}

func TestDecodeRecord_TruncatedHeader(t *testing.T) {
	shortBuffers := [][]byte{
		{},
		{0x01},
		{0x01, 0x02, 0x03, 0x04},
		{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07},
	}

	for _, buf := range shortBuffers {
		_, err := decodeRecord(buf)
		if !errors.Is(err, ErrRecordTooShort) {
			t.Fatalf("expected ErrRecordTooShort for len %d, got %v", len(buf), err)
		}
	}
}

func TestDecodeRecord_CorruptedLength(t *testing.T) {
	origOps := []Operation{
		{Type: OpPut, Key: []byte("k"), Value: []byte("v")},
	}
	buf, err := encodeRecord(origOps)
	if err != nil {
		t.Fatalf("encode failed: %v", err)
	}

	// Tamper payload length to be larger than buffer
	tamperedBuf := make([]byte, len(buf))
	copy(tamperedBuf, buf)
	binary.LittleEndian.PutUint32(tamperedBuf[0:4], binary.LittleEndian.Uint32(buf[0:4])+10)

	_, err = decodeRecord(tamperedBuf)
	if !errors.Is(err, ErrRecordSizeMismatch) {
		t.Fatalf("expected ErrRecordSizeMismatch, got %v", err)
	}

	// Tamper payload length to be smaller than buffer
	copy(tamperedBuf, buf)
	binary.LittleEndian.PutUint32(tamperedBuf[0:4], binary.LittleEndian.Uint32(buf[0:4])-2)

	_, err = decodeRecord(tamperedBuf)
	if !errors.Is(err, ErrRecordSizeMismatch) {
		t.Fatalf("expected ErrRecordSizeMismatch, got %v", err)
	}
}

func TestDecodeRecord_CorruptedCRC(t *testing.T) {
	ops := []Operation{
		{Type: OpPut, Key: []byte("safe-key"), Value: []byte("safe-val")},
	}
	buf, err := encodeRecord(ops)
	if err != nil {
		t.Fatalf("encode failed: %v", err)
	}

	// Corrupt one bit in CRC field
	tampered := make([]byte, len(buf))
	copy(tampered, buf)
	tampered[4] ^= 0x01

	_, err = decodeRecord(tampered)
	if !errors.Is(err, ErrCRCMismatch) {
		t.Fatalf("expected ErrCRCMismatch, got %v", err)
	}

	// Corrupt one bit in payload body without updating CRC
	tamperedPayload := make([]byte, len(buf))
	copy(tamperedPayload, buf)
	tamperedPayload[len(tamperedPayload)-1] ^= 0x01

	_, err = decodeRecord(tamperedPayload)
	if !errors.Is(err, ErrCRCMismatch) {
		t.Fatalf("expected ErrCRCMismatch on tampered payload, got %v", err)
	}
}

func TestDecodeRecord_TruncatedPayload(t *testing.T) {
	ops := []Operation{
		{Type: OpPut, Key: []byte("long-key"), Value: []byte("long-val-body")},
	}
	buf, err := encodeRecord(ops)
	if err != nil {
		t.Fatalf("encode failed: %v", err)
	}

	// Slice off the last 5 bytes without altering header
	truncated := buf[:len(buf)-5]
	_, err = decodeRecord(truncated)
	if !errors.Is(err, ErrRecordSizeMismatch) {
		t.Fatalf("expected ErrRecordSizeMismatch on truncated record, got %v", err)
	}

	// Payload with truncated operation count (< 4 bytes) but valid CRC
	shortPayload := craftRecord([]byte{0x01, 0x02})
	_, err = decodeRecord(shortPayload)
	if !errors.Is(err, ErrTruncatedPayload) {
		t.Fatalf("expected ErrTruncatedPayload for <4 byte payload, got %v", err)
	}

	// Payload with opCount=1 but no operation header bytes
	opCountOnly := make([]byte, 4)
	binary.LittleEndian.PutUint32(opCountOnly, 1)
	partialOp := craftRecord(opCountOnly)
	_, err = decodeRecord(partialOp)
	if !errors.Is(err, ErrTruncatedPayload) {
		t.Fatalf("expected ErrTruncatedPayload for missing op body, got %v", err)
	}
}

func TestDecodeRecord_InvalidOperationType(t *testing.T) {
	// opCount=1, type=99 (invalid), keyLen=2, valLen=2, key="ab", val="cd"
	payload := make([]byte, 4+OpHeaderSize+4)
	binary.LittleEndian.PutUint32(payload[0:4], 1)
	payload[4] = 99 // Invalid op type
	binary.LittleEndian.PutUint16(payload[5:7], 2)
	binary.LittleEndian.PutUint32(payload[7:11], 2)
	copy(payload[11:13], "ab")
	copy(payload[13:15], "cd")

	record := craftRecord(payload)
	_, err := decodeRecord(record)
	if !errors.Is(err, ErrInvalidOpType) {
		t.Fatalf("expected ErrInvalidOpType, got %v", err)
	}
}

func TestDecodeRecord_InvalidOperationCount(t *testing.T) {
	// opCount = 0
	payloadZero := make([]byte, 4)
	binary.LittleEndian.PutUint32(payloadZero, 0)
	recordZero := craftRecord(payloadZero)
	_, err := decodeRecord(recordZero)
	if !errors.Is(err, ErrEmptyBatch) {
		t.Fatalf("expected ErrEmptyBatch for opCount 0, got %v", err)
	}

	// opCount = 10,001
	payloadTooMany := make([]byte, 4)
	binary.LittleEndian.PutUint32(payloadTooMany, MaxBatchOps+1)
	recordTooMany := craftRecord(payloadTooMany)
	_, err = decodeRecord(recordTooMany)
	if !errors.Is(err, ErrBatchTooLarge) {
		t.Fatalf("expected ErrBatchTooLarge for opCount %d, got %v", MaxBatchOps+1, err)
	}
}

func TestDecodeRecord_InvalidKeyLength(t *testing.T) {
	// keyLen = 0
	payloadZeroKey := make([]byte, 4+OpHeaderSize)
	binary.LittleEndian.PutUint32(payloadZeroKey[0:4], 1)
	payloadZeroKey[4] = byte(OpPut)
	binary.LittleEndian.PutUint16(payloadZeroKey[5:7], 0) // keyLen = 0
	binary.LittleEndian.PutUint32(payloadZeroKey[7:11], 0)

	recordZeroKey := craftRecord(payloadZeroKey)
	_, err := decodeRecord(recordZeroKey)
	if !errors.Is(err, ErrInvalidKeyLength) {
		t.Fatalf("expected ErrInvalidKeyLength for keyLen 0, got %v", err)
	}

	// keyLen = 1025 (exceeds MaxKeyLen)
	payloadLongKey := make([]byte, 4+OpHeaderSize)
	binary.LittleEndian.PutUint32(payloadLongKey[0:4], 1)
	payloadLongKey[4] = byte(OpPut)
	binary.LittleEndian.PutUint16(payloadLongKey[5:7], MaxKeyLen+1) // 1025
	binary.LittleEndian.PutUint32(payloadLongKey[7:11], 0)

	recordLongKey := craftRecord(payloadLongKey)
	_, err = decodeRecord(recordLongKey)
	if !errors.Is(err, ErrInvalidKeyLength) {
		t.Fatalf("expected ErrInvalidKeyLength for keyLen 1025, got %v", err)
	}
}

func TestDecodeRecord_InvalidValueLength(t *testing.T) {
	// valLen = MaxValueLen + 1
	payloadLongVal := make([]byte, 4+OpHeaderSize+1)
	binary.LittleEndian.PutUint32(payloadLongVal[0:4], 1)
	payloadLongVal[4] = byte(OpPut)
	binary.LittleEndian.PutUint16(payloadLongVal[5:7], 1)
	binary.LittleEndian.PutUint32(payloadLongVal[7:11], MaxValueLen+1)
	payloadLongVal[11] = 'k'

	recordLongVal := craftRecord(payloadLongVal)
	_, err := decodeRecord(recordLongVal)
	if !errors.Is(err, ErrInvalidValueLength) {
		t.Fatalf("expected ErrInvalidValueLength, got %v", err)
	}

	// DELETE with non-zero value length
	payloadDeleteVal := make([]byte, 4+OpHeaderSize+2)
	binary.LittleEndian.PutUint32(payloadDeleteVal[0:4], 1)
	payloadDeleteVal[4] = byte(OpDelete)
	binary.LittleEndian.PutUint16(payloadDeleteVal[5:7], 1)
	binary.LittleEndian.PutUint32(payloadDeleteVal[7:11], 1) // Non-zero valLen for DELETE
	payloadDeleteVal[11] = 'k'
	payloadDeleteVal[12] = 'v'

	recordDeleteVal := craftRecord(payloadDeleteVal)
	_, err = decodeRecord(recordDeleteVal)
	if !errors.Is(err, ErrDeleteWithValue) {
		t.Fatalf("expected ErrDeleteWithValue for DELETE with value, got %v", err)
	}
}

func TestDecodeRecord_ExtraTrailingBytes(t *testing.T) {
	ops := []Operation{
		{Type: OpPut, Key: []byte("k"), Value: []byte("v")},
	}
	buf, err := encodeRecord(ops)
	if err != nil {
		t.Fatalf("encode failed: %v", err)
	}

	// Append unexpected trailing bytes to outer record
	withTrailing := append(buf, 0xAA, 0xBB, 0xCC)
	_, err = decodeRecord(withTrailing)
	if !errors.Is(err, ErrRecordSizeMismatch) {
		t.Fatalf("expected ErrRecordSizeMismatch for trailing bytes on outer record, got %v", err)
	}

	// Craft payload with valid opCount=1, valid op, but trailing garbage bytes inside payload
	payload := make([]byte, 4+OpHeaderSize+2+3) // 3 extra bytes
	binary.LittleEndian.PutUint32(payload[0:4], 1)
	payload[4] = byte(OpPut)
	binary.LittleEndian.PutUint16(payload[5:7], 1)
	binary.LittleEndian.PutUint32(payload[7:11], 1)
	payload[11] = 'k'
	payload[12] = 'v'
	payload[13] = 0xDE
	payload[14] = 0xAD
	payload[15] = 0xBE

	record := craftRecord(payload)
	_, err = decodeRecord(record)
	if !errors.Is(err, ErrTrailingData) {
		t.Fatalf("expected ErrTrailingData for payload with internal trailing bytes, got %v", err)
	}
}

func TestDecodeRecord_EmptyPutValue(t *testing.T) {
	ops := []Operation{
		{Type: OpPut, Key: []byte("key-with-empty-val"), Value: []byte{}},
	}
	buf, err := encodeRecord(ops)
	if err != nil {
		t.Fatalf("encode failed: %v", err)
	}

	decoded, err := decodeRecord(buf)
	if err != nil {
		t.Fatalf("decode failed: %v", err)
	}

	if len(decoded) != 1 {
		t.Fatalf("expected 1 op, got %d", len(decoded))
	}
	if decoded[0].Type != OpPut {
		t.Fatalf("expected OpPut, got %d", decoded[0].Type)
	}
	if string(decoded[0].Key) != "key-with-empty-val" {
		t.Fatalf("expected key 'key-with-empty-val', got '%s'", string(decoded[0].Key))
	}
	if len(decoded[0].Value) != 0 {
		t.Fatalf("expected empty value, got %v", decoded[0].Value)
	}
}
