---
trigger: always_on
---

You are helping me implement the engine core of a custom embedded key-value database for a 24-hour hackathon called "Kill It and Prove It".

MY ROLE:
I own the database engine only. Another teammate owns the torture-testing tool. Another teammate owns the CLI, compaction and documentation. Do not implement their components unless explicitly requested.

TECHNICAL RESTRICTIONS:

- Language: Go.
- Standard library only.
- No SQLite, LevelDB, RocksDB, BoltDB, bbolt, BadgerDB or any existing database/storage engine.
- Do not use external libraries for WAL, journaling, B-trees, LSM trees or key-value storage.
- Do not copy existing database implementation code.

FIXED DESIGN:

1. One authoritative data file.
2. Header: 16 bytes:
   - Magic: "KVDB0001" (8 bytes)
   - Version: uint32
   - Reserved: uint32
3. Each record:
   - Payload length: uint32
   - CRC32: uint32
   - Payload
4. CRC32 covers the four payload-length bytes followed by the payload. It does not cover the CRC field.
5. Payload:
   - Operation count: uint32
   - Repeated operations:
     - Type: uint8 (1=PUT, 2=DELETE)
     - Key length: uint16
     - Value length: uint32
     - Key bytes
     - Value bytes
6. Every write is exactly one record. PUT and DELETE are batches containing one operation. A batch is one record containing all its operations.
7. Use an in-memory map[string]entry. Each entry stores valueOffset int64 and valueLen uint32. Values remain on disk and GET uses ReadAt.
8. One global sync.RWMutex. Writes take Lock. GET takes RLock.
9. Batch operations apply in their supplied order.
10. Keys: 1–1024 bytes. Values: 0–1 MB. Batches: maximum 10,000 operations.
11. Database API: Open(path string, maxBytes int64), Put, Get, Delete, Batch, Close.
12. All successful writes must be synced before success is returned.

CRASH-SAFETY RULES:

- A successful operation must always survive a process crash.
- An unsuccessful operation may be fully present or fully absent.
- A batch must never be partially visible after recovery.
- Never return a value that was not written for that key.
- Recovery must be repeatable if the process is killed during recovery.
- Validate all lengths and boundaries before reading or allocating based on untrusted file data.
- If a write or Sync fails, attempt rollback to the old file size and sync the rollback. If rollback cannot be confirmed, mark the DB as unusable and reject future operations until reopened. Never continue appending blindly.
- Never update the in-memory index before the record has been successfully synced.
- Keep the existing data file safe during all error paths.

IMPLEMENTATION RULES:

- Work ONLY on the current requested step.
- Do not implement future steps early.
- Inspect existing files before editing.
- Write focused unit tests for every function.
- Run gofmt and go test ./... after each step.
- Run go test -race ./... when appropriate.
- Fix all failures before proceeding.
- Update README.md after every step. Only update the engine implementation/status section; preserve content owned by teammates.
- After tests pass, create one meaningful Git commit for that step.
- Never commit code with failing tests.
- Never push to GitHub without my explicit permission.
- Never silently change the agreed storage format or public API.
- If a design flaw is discovered, explain it before changing the design.
- At the end of each step, report files changed, tests run and results, README changes, commit hash, remaining risks, and the answer to: "What is on disk if the process dies on this line?"

IMPORTANT:
Do not build the entire project in one response. Stop after completing the requested step and wait for my review and approval.
