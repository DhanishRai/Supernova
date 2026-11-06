package killitdb

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"math"
)

// OperationType defines the type of database operation in a record.
type OperationType uint8

const (
	// OpPut inserts or updates a key-value pair.
	OpPut OperationType = 1
	// OpDelete removes a key.
	OpDelete OperationType = 2
)

const (
	// MinKeyLen is the minimum allowed key length in bytes.
	MinKeyLen = 1
	// MaxKeyLen is the maximum allowed key length in bytes (1 KB).
	MaxKeyLen = 1024
	// MaxValueLen is the maximum allowed value length in bytes (1 MB).
	MaxValueLen = 1024 * 1024
	// MinBatchOps is the minimum number of operations allowed in a batch.
	MinBatchOps = 1
	// MaxBatchOps is the maximum number of operations allowed in a batch.
	MaxBatchOps = 10000

	// RecordHeaderSize is the size of the outer record header:
	// 4 bytes payload length + 4 bytes CRC32.
	RecordHeaderSize = 8

	// OpHeaderSize is the fixed size per operation:
	// 1 byte type + 2 bytes key length + 4 bytes value length.
	OpHeaderSize = 7
)

var (
	// ErrEmptyBatch indicates the batch contains no operations.
	ErrEmptyBatch = errors.New("batch must contain at least 1 operation")
	// ErrBatchTooLarge indicates the batch exceeds MaxBatchOps.
	ErrBatchTooLarge = errors.New("batch exceeds maximum of 10,000 operations")
	// ErrInvalidKeyLength indicates the key length is outside the valid range [1, 1024].
	ErrInvalidKeyLength = errors.New("key length must be between 1 and 1024 bytes")
	// ErrInvalidValueLength indicates the value length exceeds MaxValueLen (1 MB).
	ErrInvalidValueLength = errors.New("value length must be between 0 and 1048576 bytes (1 MB)")
	// ErrDeleteWithValue indicates a DELETE operation was provided with a non-empty value.
	ErrDeleteWithValue = errors.New("DELETE operation must have zero value length")
	// ErrInvalidOpType indicates an unrecognized operation type.
	ErrInvalidOpType = errors.New("invalid operation type")
	// ErrPayloadTooLarge indicates the total payload size exceeds uint32 limit.
	ErrPayloadTooLarge = errors.New("payload length exceeds maximum allowed uint32")
	// ErrRecordTooShort indicates data is smaller than the 8-byte outer record header.
	ErrRecordTooShort = errors.New("record data too short for outer header")
	// ErrRecordSizeMismatch indicates data length does not match payload length + 8.
	ErrRecordSizeMismatch = errors.New("record data size does not match specified payload length")
	// ErrCRCMismatch indicates record CRC32 does not match payload checksum.
	ErrCRCMismatch = errors.New("record CRC32 checksum mismatch")
	// ErrTruncatedPayload indicates the payload ends before all declared fields could be read.
	ErrTruncatedPayload = errors.New("payload is truncated")
	// ErrTrailingData indicates extra bytes remain in the payload after decoding all declared operations.
	ErrTrailingData = errors.New("payload contains unexpected trailing data")
)

// Operation represents a single PUT or DELETE mutation.
type Operation struct {
	Type  OperationType
	Key   []byte
	Value []byte
}

// encodeRecord serializes a batch of operations into the binary record format:
//
// Outer Record:
//   - Payload length: uint32 (little-endian)
//   - CRC32: uint32 (little-endian, IEEE polynomial over payload-length bytes + payload)
//   - Payload:
//   - Operation count: uint32 (little-endian)
//   - Repeated operations:
//   - Type: uint8 (1=PUT, 2=DELETE)
//   - Key length: uint16 (little-endian)
//   - Value length: uint32 (little-endian)
//   - Key bytes
//   - Value bytes
func encodeRecord(ops []Operation) ([]byte, error) {
	opCount := len(ops)
	if opCount < MinBatchOps {
		return nil, ErrEmptyBatch
	}
	if opCount > MaxBatchOps {
		return nil, ErrBatchTooLarge
	}

	// Payload begins with uint32 operation count (4 bytes).
	var payloadLen uint64 = 4

	for i := range ops {
		op := &ops[i]

		if op.Type != OpPut && op.Type != OpDelete {
			return nil, ErrInvalidOpType
		}

		keyLen := len(op.Key)
		if keyLen < MinKeyLen || keyLen > MaxKeyLen {
			return nil, ErrInvalidKeyLength
		}

		valLen := len(op.Value)
		if valLen > MaxValueLen {
			return nil, ErrInvalidValueLength
		}

		if op.Type == OpDelete && valLen != 0 {
			return nil, ErrDeleteWithValue
		}

		// OpHeaderSize (7) + keyLen + valLen
		opSize := uint64(OpHeaderSize) + uint64(keyLen) + uint64(valLen)
		if math.MaxUint64-payloadLen < opSize {
			return nil, ErrPayloadTooLarge
		}
		payloadLen += opSize
	}

	if payloadLen > math.MaxUint32 {
		return nil, ErrPayloadTooLarge
	}

	totalRecordSize := uint64(RecordHeaderSize) + payloadLen
	if totalRecordSize > math.MaxUint32 {
		return nil, ErrPayloadTooLarge
	}

	buf := make([]byte, totalRecordSize)

	// 1. Write payload length (uint32, little-endian) at buf[0:4]
	binary.LittleEndian.PutUint32(buf[0:4], uint32(payloadLen))

	// 2. Write payload starting at buf[8]
	payloadOffset := RecordHeaderSize

	// Operation count
	binary.LittleEndian.PutUint32(buf[payloadOffset:payloadOffset+4], uint32(opCount))
	payloadOffset += 4

	for i := range ops {
		op := &ops[i]

		// Type (1 byte)
		buf[payloadOffset] = byte(op.Type)
		payloadOffset++

		// Key length (2 bytes, little-endian)
		binary.LittleEndian.PutUint16(buf[payloadOffset:payloadOffset+2], uint16(len(op.Key)))
		payloadOffset += 2

		// Value length (4 bytes, little-endian)
		binary.LittleEndian.PutUint32(buf[payloadOffset:payloadOffset+4], uint32(len(op.Value)))
		payloadOffset += 4

		// Key bytes
		copy(buf[payloadOffset:], op.Key)
		payloadOffset += len(op.Key)

		// Value bytes
		if len(op.Value) > 0 {
			copy(buf[payloadOffset:], op.Value)
			payloadOffset += len(op.Value)
		}
	}

	// 3. Compute CRC32 over the four payload-length bytes followed by payload.
	// CRC covers buf[0:4] and buf[8:]. It does not cover buf[4:8] (CRC field).
	hasher := crc32.NewIEEE()
	hasher.Write(buf[0:4])
	hasher.Write(buf[RecordHeaderSize:])
	checksum := hasher.Sum32()

	binary.LittleEndian.PutUint32(buf[4:8], checksum)

	return buf, nil
}

// decodeRecord decodes and validates a complete binary record:
//
// 1. Validates that at least 8 bytes exist for the outer header.
// 2. Reads payload length and checks for integer overflow.
// 3. Verifies that data contains exactly RecordHeaderSize + payloadLength bytes.
// 4. Verifies the IEEE CRC32 checksum over the 4 payload length bytes + payload.
// 5. Decodes and bounds-checks operation count (1–10,000).
// 6. Decodes each operation with strict boundary checks on type, key length, and value length.
// 7. Confirms no unexpected trailing bytes remain in the payload.
//
// The function does not trust any lengths in data and strictly validates bounds
// before allocating or slicing memory.
// DecodedOp represents a decoded operation along with its relative value offset
// from the start of the encoded record frame.
type DecodedOp struct {
	Type              OperationType
	Key               []byte
	Value             []byte
	RelativeValOffset int64
	ValueLen          uint32
}

// decodeRecordWithOffsets decodes and validates a complete binary record, providing
// each operation's value offset relative to the beginning of the record.
func decodeRecordWithOffsets(data []byte) ([]DecodedOp, error) {
	if len(data) < RecordHeaderSize {
		return nil, ErrRecordTooShort
	}

	payloadLen := binary.LittleEndian.Uint32(data[0:4])
	expectedTotalSize := uint64(RecordHeaderSize) + uint64(payloadLen)

	if expectedTotalSize > math.MaxUint32 || uint64(len(data)) != expectedTotalSize {
		return nil, ErrRecordSizeMismatch
	}

	// Validate CRC32
	expectedCRC := binary.LittleEndian.Uint32(data[4:8])
	hasher := crc32.NewIEEE()
	hasher.Write(data[0:4])
	hasher.Write(data[RecordHeaderSize:])
	if hasher.Sum32() != expectedCRC {
		return nil, ErrCRCMismatch
	}

	// Payload must contain at least 4 bytes for operation count.
	if payloadLen < 4 {
		return nil, ErrTruncatedPayload
	}

	payloadOffset := RecordHeaderSize
	opCount := binary.LittleEndian.Uint32(data[payloadOffset : payloadOffset+4])
	payloadOffset += 4

	if opCount < MinBatchOps {
		return nil, ErrEmptyBatch
	}
	if opCount > MaxBatchOps {
		return nil, ErrBatchTooLarge
	}

	payloadEnd := len(data)
	ops := make([]DecodedOp, 0, opCount)

	for i := uint32(0); i < opCount; i++ {
		// Must have at least OpHeaderSize (7 bytes)
		if payloadEnd-payloadOffset < OpHeaderSize {
			return nil, ErrTruncatedPayload
		}

		opType := OperationType(data[payloadOffset])
		payloadOffset++

		if opType != OpPut && opType != OpDelete {
			return nil, ErrInvalidOpType
		}

		keyLen := binary.LittleEndian.Uint16(data[payloadOffset : payloadOffset+2])
		payloadOffset += 2

		if keyLen < MinKeyLen || keyLen > MaxKeyLen {
			return nil, ErrInvalidKeyLength
		}

		valLen := binary.LittleEndian.Uint32(data[payloadOffset : payloadOffset+4])
		payloadOffset += 4

		if valLen > MaxValueLen {
			return nil, ErrInvalidValueLength
		}

		if opType == OpDelete && valLen != 0 {
			return nil, ErrDeleteWithValue
		}

		opDataLen := uint64(keyLen) + uint64(valLen)
		if uint64(payloadEnd-payloadOffset) < opDataLen {
			return nil, ErrTruncatedPayload
		}

		key := make([]byte, keyLen)
		copy(key, data[payloadOffset:payloadOffset+int(keyLen)])
		payloadOffset += int(keyLen)

		relValOffset := int64(payloadOffset)

		var val []byte
		if valLen > 0 {
			val = make([]byte, valLen)
			copy(val, data[payloadOffset:payloadOffset+int(valLen)])
			payloadOffset += int(valLen)
		}

		ops = append(ops, DecodedOp{
			Type:              opType,
			Key:               key,
			Value:             val,
			RelativeValOffset: relValOffset,
			ValueLen:          valLen,
		})
	}

	if payloadOffset != payloadEnd {
		return nil, ErrTrailingData
	}

	return ops, nil
}

// decodeRecord decodes and validates a complete binary record.
func decodeRecord(data []byte) ([]Operation, error) {
	decodedOps, err := decodeRecordWithOffsets(data)
	if err != nil {
		return nil, err
	}

	ops := make([]Operation, len(decodedOps))
	for i := range decodedOps {
		ops[i] = Operation{
			Type:  decodedOps[i].Type,
			Key:   decodedOps[i].Key,
			Value: decodedOps[i].Value,
		}
	}

	return ops, nil
}
