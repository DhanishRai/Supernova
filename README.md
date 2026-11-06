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

## Current Implementation Status

- **Status:** Step 8 complete
- **Engine Core:**
  - Record data structures (`OperationType`, `Operation`) and validation constants implemented.
  - Binary record encoder (`encodeRecord`) implemented with full boundary validation (1–1024 byte keys, 0–1 MB values, 1–10,000 batch operations, zero-value DELETE checks, integer overflow prevention).
  - IEEE CRC32 checksum generation implemented covering payload length and payload bytes.
  - Binary record decoder (`decodeRecord`) and offset-aware variant (`decodeRecordWithOffsets`) implemented with defensive bounds checking before reading/allocating; each decoded operation carries its value's offset relative to the start of its encoded record.
  - In-memory hash index (`Index` backed by `map[string]entry`) implemented with ordered batch application.
  - Database file structure and safe initialization (`Open`, `Close`, `DB` struct) implemented with 16-byte header.
  - Internal durable record append (`appendRecord`) with rollback and poison state handling.
  - Core operations (`Put`, `Delete`, `Get`) implemented with global synchronization.
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
  - Comprehensive unit test suite: **75 tests** covering record serialization, indexing, file lifecycle, fault injection, atomic batches, concurrency, and all recovery edge cases.

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
- [ ] **Step 9:** Crash safety verification, boundary torture testing, and recovery guarantees.

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

