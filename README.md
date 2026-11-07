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
  - **Torture Test Suite Validation:** Autonomous multi-iteration crash torture testing verified across all modes (`default`, `large-values`, `large-batches`, `compaction`, `disk-full`, `recovery-crash`) with golden template caching (1,000,000 ops), empirical startup/recovery delay calibration ($S = T - R$), multi-variant torn tails, deterministic C2 half-done repair fallback, chained F2 kills, engine-marker-proven phase telemetry, strict ACK recording during golden seeding, scoped F4 unACK relaxation strictly bounded to the cut trailing batch, deep-cloned oracle state isolation, explicit ERR verification in the Oracle asserting complete absence of errored writes, deletions, and partial batches from recovered state, hardened Windows kill detection distinguishing true SIGKILL from premature engine crashes, configurable key pools (`--key-pool-size 500..5000`), and high-performance oracle indexing ($O(1)$ batch key lookup, single-pass $O(N)$ state-before precomputation, rotating seed pool, and 5-key sample point lookups) enabling rapid 100-200 iteration stress testing without verification timeouts or bottlenecks.
  - **Disk-Full & Organic Capacity Lifecycle (Items 5 & 14):**
    - **Organic Cap Approach:** Replaced static scripts with dynamic, random-sized writes and operations (`randint(12, 35)`) that naturally reach and trigger disk-full errors, breaking dynamically on capacity limit.
    - **Midway-Crossing Batch Rejection (E2):** Multi-operation atomic batches that cross `--max-bytes` midway are cleanly rejected with `ERR` and asserted to never leak partial keys or intermediate state into storage or dump.
    - **Compaction at Cap (E4):** `COMPACT` command is executed when the database is at capacity, reclaiming storage from superseded and tombstoned records.
    - **Write Resumption (E5):** New write operations resume after compaction frees space, receiving `ACK` and durably verifying persistence upon crash recovery.
    - **Max-Bytes Invariant Enforcement:** Engine file size on disk is strictly asserted to not exceed `--max-bytes` both before kill and after post-crash recovery.
    - **Sabotage Verification:** Added sabotage tests proving immediate failure detection if an engine ignores `--max-bytes` or partially commits batches under capacity errors. All 105 tests in the torture harness pass cleanly.

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

## Judge Verification & Evaluation Guide

This guide provides the exact commands for judges to verify every crash-safety guarantee and torture-tool quality metric. All commands can be run directly from the repository root in **Git Bash** (or Windows PowerShell).

### 1. Database Survives Judge's Kill Runs

| Judge Verification Item | Command to Verify | Guarantee & Evidence |
|---|---|---|
| **`put()` preserves all acknowledged data after a crash** | `go test -v -run TestStorage_DurableAppend ./engine/...`<br>`PYTHONPATH="torture tool" python -m torture --iterations 50 --engine-bin ./kvdb.exe` | Calls `WriteAt` and `Sync()` (fsync) before returning. Un-synced data is never acknowledged. Recovery confirms all ACKed PUTs exist. |
| **`get()` always returns correct values** | `go test -v -run TestOperations_Get ./engine/...`<br>`./kvdb.exe get ./my_db key` | Reads directly from disk at `entry.valueOffset` using `ReadAt`. Returns exact stored bytes; non-existent keys return `ErrKeyNotFound` / `NOTFOUND`. |
| **`delete()` remains durable after a crash** | `go test -v -run TestOperations_Delete ./engine/...`<br>`go test -v -run TestDelete_TombstoneDurability ./engine/...` | Appends a durable tombstone record (`Type=2`) and syncs. Subsequent `get()` returns `NOTFOUND` across crashes and reboots. |
| **`batch()` provides all-or-nothing atomicity** | `go test -v -run TestBatch_Atomicity ./engine/...`<br>`PYTHONPATH="torture tool" python -m torture --mode large-batches --iterations 20 --engine-bin ./kvdb.exe` | Encodes entire batch into **exactly one record**. Upfront validation rejects corrupt batches with 0 writes; torn batches mid-write recover 0 keys. |
| **Recovery handles incomplete and torn writes** | `go test -v -run TestRecovery_TornTail ./engine/...`<br>`go test -v -run TestRecovery_Corrupted ./engine/...` | Scans sequential CRC32 checksums from byte 16. Stops at first torn/incomplete header or CRC mismatch, truncating file to last valid boundary. |
| **Recovery survives being interrupted** | `PYTHONPATH="torture tool" python -m torture --mode recovery-crash --iterations 10 --engine-bin ./kvdb.exe` | Tests chained $F2$ kills (`Kill 1 -> Reopen -> Kill 2 -> Reopen -> Verify`). Truncation is idempotent; recovery is safe to crash and rerun indefinitely. |
| **Compaction is crash-safe** | `go test -v -run TestCompaction_ShrinksFileAndPreservesLiveKeys ./engine/...`<br>`PYTHONPATH="torture tool" python -m torture --mode compaction --iterations 10 --engine-bin ./kvdb.exe` | Rewrites live keys to `<path>.tmp`, syncs, and atomically swaps via `os.Rename`. Interrupted compactions discard `.tmp` on startup with 0 data loss. |
| **Concurrent writers never corrupt data** | `go test -v -run TestInvariant10_ConcurrentGetAndWritesRaceFree ./engine/...`<br>`PYTHONPATH="torture tool" python -m torture --workers 2 --iterations 20 --engine-bin ./kvdb.exe` | Protected by a single global `sync.RWMutex`. Writers execute under `db.mu.Lock()`, serializing disk append and index updates with zero race conditions. |
| **Disk-full conditions never corrupt existing data** | `go test -v -run TestBatch_MaxSizeRejection ./engine/...`<br>`PYTHONPATH="torture tool" python -m torture --mode disk-full --iterations 10 --engine-bin ./kvdb.exe --max-bytes 50000` | Pre-checks capacity (`size + recLen > maxBytes`); midway-crossing batches (E2) rejected atomically; file size strictly asserted to never exceed `--max-bytes`. |
| **All database invariants remain valid** | `go test -v -run TestInvariant ./engine/...` | Comprehensive test suite validating all 10 core crash-safety and storage invariants defined in the specification. |

---

### 2. Torture Tool Quality & Compliance

| Torture Tool Requirement | Verification Command / Flag | Implementation Details |
|---|---|---|
| **Independent oracle that records every ACKed operation** | `PYTHONPATH="torture tool" python -m unittest "torture tool/torture/tests/test_oracle.py"` | Standalone `DatabaseOracle` tracking `record_put`, `record_del`, `record_batch`, `record_ack`, and `record_err`. Verifies recovered dump against definite state. |
| **Random process termination during actual operations** | `PYTHONPATH="torture tool" python -m torture --iterations 50 --engine-bin ./kvdb.exe` | Injects direct uncatchable `SIGKILL` (`kill -9` / `TerminateProcess`) drawn from `random.Random(seed)` while concurrent client operations are in flight. |
| **Concurrent writers** | `PYTHONPATH="torture tool" python -m torture --workers 2 --iterations 20 --engine-bin ./kvdb.exe` | Multiple worker threads generate concurrent PUT, DEL, and BATCH operations, verifying concurrent durability and conflict resolution. |
| **Batch testing** | `PYTHONPATH="torture tool" python -m unittest "torture tool/torture/tests/test_large_batches.py"` | Tests atomic multi-operation batches with interleaved puts, deletes, and repeated key modifications. |
| **Large-value testing (1 MB)** | `PYTHONPATH="torture tool" python -m torture --mode large-values --iterations 10 --engine-bin ./kvdb.exe` | Generates 1 MB payloads (`1024 * 1024` bytes) with content verification checksums to assert zero partial or corrupted value reads. |
| **Large-batch testing (10,000 operations)** | `python -c 'for i in range(10000): print(f"put key_{i:05d} val_{i:05d}")' | ./kvdb.exe batch ./test_batch.db`<br>`./kvdb.exe open ./test_batch.db` | Verifies full 10,000-operation single-record batches; asserts strict rejection of 10,001 operations with `ErrBatchTooLarge`. |
| **Recovery-crash testing** | `PYTHONPATH="torture tool" python -m torture --mode recovery-crash --iterations 10 --engine-bin ./kvdb.exe` | Seeds large multi-batch database, injects 6 torn-tail variants, calibrates startup/recovery delays, and kills recovery mid-scan and mid-truncate. |
| **Compaction-crash testing** | `PYTHONPATH="torture tool" python -m torture --mode compaction --iterations 10 --engine-bin ./kvdb.exe` | Injects randomized SIGKILL during active compaction (`COMPACT`), verifying atomicity of the `.tmp -> fsync -> rename` swap. |
| **Disk-full testing** | `PYTHONPATH="torture tool" python -m torture --mode disk-full --iterations 10 --engine-bin ./kvdb.exe --max-bytes 50000` | Tests organic random-sized writes approaching cap, E2 midway batch crossing rejection, E4 compaction at cap, E5 write resumption, and `--max-bytes` enforcement. |
| **Reproducible random seeds** | `PYTHONPATH="torture tool" python -m torture --seed 12345 --iterations 10 --engine-bin ./kvdb.exe` | Uses `random.Random(seed)` with derived per-iteration seeds; every run with `--seed <N>` executes the exact same schedule and workload. |
| **Default 500 iterations** | `PYTHONPATH="torture tool" python -m torture --engine-bin ./kvdb.exe` | Omitting `--iterations` defaults to **500 iterations** as mandated by hackathon rules. |

### 3. Hands-On Interactive CLI Walkthrough for Judges

Judges can test all database operations and performance guarantees directly from the command line in **Git Bash** (or Windows PowerShell):

#### A. Initialize / Open Database
```bash
./kvdb.exe open ./my_test.db
```
*(If `./my_test.db` does not exist, it creates a fresh crash-safe 16-byte authoritative file and reports `keys=0 recovery=<duration>` before cleanly closing).*

#### B. Insert or Update Key-Value Pairs (`put`)
```bash
# Syntax: ./kvdb.exe put <db_file> <key> <value>
./kvdb.exe put ./my_test.db name "Hrithik"
./kvdb.exe put ./my_test.db role "Lead Engineer"
./kvdb.exe put ./my_test.db score 100
```
*(Every write encodes a binary record with IEEE CRC32 checksum, flushes to disk with `fsync`, and returns `OK`).*

#### C. Read Key Values (`get`)
```bash
# Syntax: ./kvdb.exe get <db_file> <key>
./kvdb.exe get ./my_test.db name
# Output: Hrithik

./kvdb.exe get ./my_test.db role
# Output: Lead Engineer

./kvdb.exe get ./my_test.db non_existent
# Output: NOTFOUND
```
*(Performs direct O(1) in-memory index lookup and reads the exact bytes from disk at `valueOffset` using `ReadAt`).*

#### D. Dump All Live Key-Values (`dump`)
```bash
./kvdb.exe dump ./my_test.db
```
**Output:**
```
name Hrithik
role Lead Engineer
score 100
```
*(Dumps all live, un-deleted keys and their values sorted alphabetically).*

#### E. Delete a Key (`del`)
```bash
# Syntax: ./kvdb.exe del <db_file> <key>
./kvdb.exe del ./my_test.db score
# Output: OK

# Verify it is deleted:
./kvdb.exe get ./my_test.db score
# Output: NOTFOUND
```
*(Appends an authoritative durable tombstone record, syncs it, and removes the key from the live index).*

#### F. Atomic Multi-Operation Batch (`batch`)
```bash
cat <<EOF | ./kvdb.exe batch ./my_test.db
put city "Bangalore"
put hackathon "Kill It and Prove It"
del role
EOF
```
**Output:** `OK`

Verify the batch was applied atomically:
```bash
./kvdb.exe dump ./my_test.db
```
**Output:**
```
city Bangalore
hackathon Kill It and Prove It
name Hrithik
```

#### G. High-Volume Batch (10,000 Operations in Exactly One Record)
```bash
# Submit an atomic batch of 10,000 operations in one command:
python -c 'for i in range(10000): print(f"put key_{i:05d} value_{i:05d}")' | ./kvdb.exe batch ./test_batch.db

# Verify all 10,000 keys recovered in milliseconds:
./kvdb.exe open ./test_batch.db
# Output: keys=10000 recovery=2.8ms

# Verify 10,001 operations are strictly rejected upfront:
python -c 'for i in range(10001): print(f"put key_{i:05d} value_{i:05d}")' | ./kvdb.exe batch ./test_batch.db
# Output: batch error: batch exceeds maximum of 10,000 operations
```

#### H. Recover 1 Million Keys in Under 2 Seconds
```bash
go test -v -run TestRecovery_MillionKeys -timeout 120s ./engine/...
```
**Output:**
```
=== RUN   TestRecovery_MillionKeys
    perf_test.go:104: Phase 1: building database with 1,000,000 keys …
    perf_test.go:110: Build complete in 1.04s — file size: 53.4 MB
    perf_test.go:117: Phase 2: measuring Open() recovery …
    perf_test.go:133: Recovery elapsed:   627.07ms
    perf_test.go:134: Recovered keys:     1000000
    perf_test.go:191: PERFORMANCE PASS: recovery in 627.07ms (target: <2s)
--- PASS: TestRecovery_MillionKeys (1.70s)
PASS
```

#### I. Compaction (Reclaim Space and Shrink Database)
```bash
# 1. Insert large keys (~100 KB):
python -c 'import subprocess; [subprocess.run(["./kvdb.exe", "put", "./demo.db", f"k_{i:02d}", "X"*2048]) for i in range(50)]'
ls -lh ./demo.db # Size: ~104 KB

# 2. Delete 40 keys:
python -c 'import subprocess; [subprocess.run(["./kvdb.exe", "del", "./demo.db", f"k_{i:02d}"]) for i in range(40)]'
ls -lh ./demo.db # Size: ~105 KB (append-only log holds tombstones)

# 3. Compact file:
./kvdb.exe compact ./demo.db
ls -lh ./demo.db # Size: ~20 KB (SHRUNK by 80%!)

# Clean up demo files:
rm -f ./my_test.db* ./test_batch.db* ./demo.db*
```

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

