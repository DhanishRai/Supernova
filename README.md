# KillItDB

A crash-proof embedded key-value database engine built for the 24-hour hackathon **"Kill It and Prove It"**.

---

## Project Overview

KillItDB is a high-reliability, append-only embedded key-value storage engine designed to survive unannounced process kills, power outages, and sudden crashes without state corruption or partial batch exposure.

Key guarantees:
- **Crash Safety:** All acknowledged writes are strictly persisted and synced before returning to the caller.
- **Atomicity:** Batch writes are strictly atomic—they either recover in full or are cleanly rolled back / ignored on recovery.
- **Data Integrity:** CRC32 checksum verification protects all record lengths and payloads against partial or corrupt writes.
- **Fast Lookups:** In-memory key-to-offset index (`map[string]entry`) enables $O(1)$ disk reads without loading values into RAM.

---

## Repository Structure & Team Division

To ensure seamless collaboration when teammates pull or push code, the repository is organized into cleanly separated components:

```
srinathon-3.0/
├── cmd/
│   ├── kvdb/                 # [Teammate 2] CLI, daemon server, compaction trigger
│   │   └── README.md
│   └── torture/              # [Teammate 1] Autonomous torture-testing & kill tool
│       └── README.md
├── engine/                   # [Our Component] Core database engine (package killitdb)
│   ├── batch.go              # Atomic multi-operation batch writes
│   ├── database.go           # DB handle, 16-byte header, file lifecycle
│   ├── index.go              # In-memory hash index (key -> disk offset/len)
│   ├── operations.go         # Put, Get, Delete operations
│   ├── record.go             # Binary encoder/decoder, CRC32, boundary validation
│   ├── recovery.go           # Crash-safe sequential recovery scan & torn-tail truncation
│   ├── storage.go            # Durable append, WriteAt, fsync, and rollback
│   └── *_test.go             # 94 unit, concurrency, recovery, and invariant tests
├── torture tool/             # [Teammate 1] Autonomous torture-testing & kill tool
├── go.mod                    # Root module definition (module killitdb)
├── README.md                 # Project architecture & status
└── TEST_PLAN.md              # 11-layer end-to-end testing & crash verification plan
```

### Component Ownership
| Component | Directory | Package | Ownership | Responsibility |
|---|---|---|---|---|
| **Database Engine Core** | `engine/` | `killitdb` | **Our Role** | Storage format, atomic batch, durable sync, recovery, in-memory index. |
| **Torture Testing Tool** | `torture tool/` | Python / harness | **Teammate 1** | Process kill injector (`kill -9`), seed-based fuzzing, crash validation. |

To import the engine in Go:
```go
import "killitdb/engine"
```

---

## Current Implementation Status

- **Status:** Step 10 complete (Engine core verified against all invariants)
- **Engine Core:**
  - Record data structures (`OperationType`, `Operation`) and validation constants implemented.
  - Binary record encoder (`encodeRecord`) implemented with full boundary validation (1–1024 byte keys, 0–1 MB values, 1–10,000 batch operations, zero-value DELETE checks, integer overflow prevention).
  - IEEE CRC32 checksum generation implemented covering payload length and payload bytes.
  - Binary record decoder (`decodeRecord`) and offset-aware variant (`decodeRecordWithOffsets`) implemented with defensive bounds checking before reading/allocating; each decoded operation carries its value's offset relative to the start of its encoded record.
  - Recovery-optimized decoder (`decodeRecordForRecovery`) skips value-byte allocation on the hot recovery path; only offsets and lengths are stored.
  - In-memory hash index (`Index` backed by `map[string]entry`) implemented with ordered batch application.
  - Database file structure and safe initialization (`Open`, `Close`, `DB` struct) implemented with 16-byte header.
  - Internal durable record append (`appendRecord`) with rollback and poison state handling.
  - Core operations (`Put`, `Delete`, `Get`) implemented with global synchronization (`sync.RWMutex`).
  - Atomic multi-operation batch write (`Batch`) implemented:
    - Encodes all batch operations (1–10,000 ops) as **exactly ONE** durable on-disk record.
    - Validates all operations upfront before acquiring write lock or executing disk I/O; rejects invalid batches with zero disk writes.
    - Appends and issues `Sync()` before applying batch operations to the in-memory index.
    - Guarantees strict atomicity under `db.mu.Lock`: readers cannot observe partial, intermediate, or uncommitted batch states.
  - **Crash-safe startup recovery (`recoverDatabase`)** implemented:
    - Removes any `<path>.tmp` file on startup (reserved for teammate's compaction).
    - If file is shorter than 16 bytes, truncates to zero, writes a fresh header and syncs.
    - Validates header magic (`KVDB0001`), version, and reserved field.
    - Sequentially scans records from byte 16; for each record verifies the 8-byte outer header fits, reads `payloadLen`, confirms payload fits within remaining file, reads full payload, validates CRC32, decodes all operations with relative value offsets.
    - Computes `absoluteValueOffset = recordStartOffset + relativeValueOffset` for each PUT and applies operations to the index in order.
    - Stops at the first incomplete header, out-of-bounds payload, CRC mismatch, or invalid operation data.
    - If `scanOffset < fileSize` after scanning (torn tail detected), truncates the file to `scanOffset` and syncs, ensuring recovery is idempotent if killed mid-truncation.
    - `Open()` delegates entirely to `recoverDatabase()` for a consistent startup path whether the file is new or existing.
    - **Record buffer is reused** across all records during scanning (single growing `[]byte`; no per-record heap alloc).
    - **Value bytes are not loaded into RAM** during recovery; `decodeRecordForRecovery` advances past value bytes without copying them.
  - **Comprehensive automated test suite:** **98 tests** covering record serialization, indexing, file lifecycle, fault injection, atomic batches, concurrency, crash recovery edge cases, 1,000,000-key recovery benchmark (469.8 ms), and all 10 core safety invariants.
  - **Torture Test Suite Validation:** Autonomous multi-iteration crash torture testing verified across all modes (`default`, `large-values`, `large-batches`, `compaction`, `disk-full`, `recovery-crash`) with golden template caching, empirical startup/recovery delay calibration ($S = T - R$), multi-variant torn tails (fewer than 8B, length past EOF, bad CRC, 1-byte truncate, clean), chained kills (F2), and direct point-lookup verification. All 96 unit tests in the harness pass cleanly.

---

## Final Engine API

```go
package killitdb

// Core Database Lifecycle
func Open(path string, maxBytes int64) (*DB, error)
func (db *DB) Close() error
func (db *DB) Size() int64
func (db *DB) Path() string

// Core Operations
func (db *DB) Put(key, value []byte) error
func (db *DB) Get(key []byte) ([]byte, error)
func (db *DB) Delete(key []byte) error
func (db *DB) Batch(ops []Operation) error
```

---

## Durability & Write Ordering

Every write (`Put`, `Delete`, `Batch`) follows a strict, non-negotiable ordering:

1. **Input Validation:** Key length (1–1024 B), value length (0–1 MB), batch size (1–10,000 ops), and DELETE zero-value constraints are checked before taking locks or touching the disk.
2. **Lock Acquisition:** `db.mu.Lock()` acquired.
3. **State Check:** Reject immediately if `db.closed` or `db.unusable`.
4. **In-Memory Encoding:** Entire record (outer header, payload length, CRC32, operation count, operations) is encoded into a contiguous byte buffer.
5. **Capacity Check:** Verify `oldSize + recordLength <= db.maxBytes`. Return `ErrDiskFull` without writing if limit would be exceeded.
6. **Disk Write:** Write complete record at `oldSize` using `WriteAt`.
7. **Hardware Sync:** Issue `Sync()` to flush OS page cache to physical storage.
8. **Rollback on Write/Sync Failure:** If `WriteAt` or `Sync()` fails, immediately attempt `Truncate(oldSize)` and `Sync()`. If rollback fails or cannot be confirmed, set `db.unusable = true` to permanently poison the in-memory state and prevent further writes.
9. **Index Update (Only After Durability):** Advance `db.size` and apply all operations to `db.index`.
10. **Unlock & Return:** Release lock and return success to caller. Readers never observe uncommitted, intermediate, or partial writes.

---

## Crash Recovery Procedure

When `Open(path, maxBytes)` is called:
1. **Compaction Cleanup:** Remove any orphaned `path + ".tmp"` file left behind by an interrupted compaction.
2. **Header Initialization:** If file size is < 16 bytes, truncate to 0, write the 16-byte header (`KVDB0001`, version 1, reserved 0), and sync.
3. **Header Validation:** Validate magic, version, and reserved field.
4. **Sequential Scan:** Start at offset 16 and read record by record:
   - Check if at least 8 bytes remain for the outer record header.
   - Read `payloadLen` and compute total record size.
   - Verify `totalRecordSize` fits within remaining file size without overflow.
   - Read complete record into a reused memory buffer.
   - Compute and verify IEEE CRC32 checksum.
   - Decode operations and calculate absolute on-disk value offsets (`recordStart + relValOffset`).
   - Apply operations to in-memory index in order.
5. **Torn Tail Handling:** Stop scanning at the first incomplete record, CRC mismatch, or corrupted operation. If `scanOffset < fileSize`, truncate the file to `scanOffset` and issue `Sync()`.
6. **Idempotence:** Recovery is read-only until the final truncation, ensuring recovery itself is completely repeatable if interrupted by another crash.

---

## Error Behavior

| Error | Condition |
|---|---|
| `ErrKeyNotFound` | Key does not exist in the database or was deleted by a tombstone. |
| `ErrDiskFull` | An operation would cause the file size to exceed `maxBytes`. Zero bytes are written to disk. |
| `ErrDatabaseClosed` | An operation was attempted on a closed database instance. |
| `ErrDatabaseUnusable` | An I/O write or sync failed and the subsequent rollback could not be confirmed. The DB is permanently poisoned to protect data integrity until restarted. |
| `ErrInvalidKeyLength` | Key is empty (0 bytes) or exceeds 1024 bytes. |
| `ErrInvalidValueLength` | Value exceeds 1 MB (1,048,576 bytes). |
| `ErrDeleteWithValue` | A DELETE operation was submitted with non-empty value bytes. |
| `ErrEmptyBatch` | Batch was submitted with 0 operations. |
| `ErrBatchTooLarge` | Batch exceeds 10,000 operations. |
| `ErrInvalidMagic` / `ErrUnsupportedVersion` / `ErrInvalidHeaderReserved` | Authoritative data file header is corrupted or incompatible. |

---

## Known Limitations

1. **In-Memory RAM Index:** The hash index (`map[string]entry`) keeps all keys, value offsets (8 bytes), and value lengths (4 bytes) in RAM. For 1,000,000 keys, live heap memory is ~129 MB. Databases with tens of millions of keys will require sufficient RAM.
2. **Append-Only Growth:** Deletes and updates append new records (tombstones / new values) without immediately reclaiming old space on disk. Space reclamation is delegated to the compaction component owned by teammates (`<path>.tmp` rewrite).
3. **Crash Assumption on Torn Tails:** Recovery assumes unannounced crashes result in torn tails at the end of the file. If an arbitrary middle-of-file bitflip occurs, recovery halts at that corrupted record.
4. **Race Detector on Windows:** Running `go test -race` requires a CGO compiler (GCC/MinGW). In pure Go environments without CGO, race-freedom is verified by architectural design (single `sync.RWMutex`, no shared mutable state outside mutex) and high-concurrency stress tests.

---

## Recovery Performance Results

| Metric | Measured |
|---|---|
| **Keys** | 1,000,000 |
| **File size** | 53.4 MB |
| **Build time** (1 M × Batch write) | ~786 ms |
| **Open() recovery time** | **469 ms** ✅ (target: < 2 s) |
| **Heap growth during recovery** | ~129 MB (live after GC) |
| **Total allocations during recovery** | ~275 MB cumulative |
| **Spot-check correctness** | 1,000 / 1,000 random keys correct |

**Test environment:** Windows 11, Go 1.27.3 (windows/amd64), local SSD, `go test -run TestRecovery_MillionKeys -timeout 300s ./engine/`

**Test command:**
```bash
go test -v -run TestRecovery_MillionKeys -timeout 300s ./engine/
```

**Key optimizations applied during Step 9 (without weakening correctness):**
1. Single growing record buffer reused across all records — eliminates one `make([]byte, N)` per record.
2. `decodeRecordForRecovery` skips value-byte `copy` — eliminates O(valueBytes) allocation per record on the recovery hot path. CRC validation, all length bounds checks, and all op validation are still performed.



---

## Fixed Storage Format

KillItDB uses a single authoritative data file.

### 1. File Header (16 bytes)
| Field | Type | Size | Description |
|---|---|---|---|
| Magic | `[8]byte` | 8 bytes | ASCII string `"KVDB0001"` |
| Version | `uint32` | 4 bytes | File format version (little-endian) |
| Reserved | `uint32` | 4 bytes | Reserved for future flags / alignment |

### 2. Record Structure (Sequential Append)
Every write operation (PUT, DELETE, or BATCH) writes exactly one self-contained record:

| Field | Type | Size | Description |
|---|---|---|---|
| Payload Length ($L$) | `uint32` | 4 bytes | Size of payload in bytes |
| CRC32 Checksum | `uint32` | 4 bytes | IEEE CRC32 over Payload Length (4 bytes) + Payload ($L$ bytes) |
| Payload | `[]byte` | $L$ bytes | Encoded operations |

*Note: The CRC32 checksum covers the 4 bytes of Payload Length and the subsequent Payload bytes. It does not cover the CRC field itself.*

### 3. Payload Format
| Field | Type | Size | Description |
|---|---|---|---|
| Operation Count ($N$) | `uint32` | 4 bytes | Number of operations in this batch record |
| Operations | Repeated $N$ times | Variable | Sequential operation entries |

Each operation entry:
- **Type:** `uint8` (`1 = PUT`, `2 = DELETE`)
- **Key Length ($K$):** `uint16` (1–1024 bytes)
- **Value Length ($V$):** `uint32` (0–1 MB; 0 for DELETE)
- **Key:** `[K]byte` (UTF-8 / arbitrary bytes)
- **Value:** `[V]byte` (arbitrary binary data)

---

## Technology Restrictions

- **Language:** Go (Standard library only).
- **Zero Third-Party Storage Dependencies:** No SQLite, LevelDB, RocksDB, BoltDB, bbolt, BadgerDB, or third-party B-tree/LSM/journaling packages.
- **No Copied Code:** All components are implemented from first principles.

---

## Engine Implementation Roadmap

- [x] **Step 0:** Inspect and prepare Go project, toolchain, git repository, and documentation.
- [x] **Step 1:** Binary constants, record header structures, serialization (`encodeRecord`) & CRC32 calculation.
- [x] **Step 2:** Binary record deserialization (`decodeRecord`), CRC validation, bounds checking & round-trip verification.
- [x] **Step 3:** In-memory hash index (`Index`, `entry`, ordered batch application).
- [x] **Step 4:** File header structure (16 bytes), file initialization, header validation, and open/close lifecycle.
- [x] **Step 5:** Record append, fsync durability, and rollback on failed write.
- [x] **Step 6:** Core operations (`Put`, `Get`, `Delete`) with global synchronization and safe value extraction.
- [x] **Step 7:** Atomic multi-operation batch write (`Batch`).
- [x] **Step 8:** Startup recovery scan & index rebuilding.
- [x] **Step 9:** Million-key recovery benchmark and performance optimisation.
- [x] **Step 10:** Final review and verification of database engine invariants.

---

## How to Run Tests

Run the full standard test suite:
```bash
go test -v ./...
```

Run race detector tests:
```bash
go test -race -v ./...
```

Format code:
```bash
gofmt -w .
```

