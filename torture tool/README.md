# Crash-Consistency Torture Tool (`torture tool`)

Part of the **Kill It and Prove It** Hackathon Project (Supernova).

## Role and Responsibility (Person B)
The torture tool is an adversarial testing harness designed to break our Go key-value database engine (`src/kvdb`). It continuously executes concurrent transactions against the engine running in `serve` mode, issues unannounced `SIGKILL` signals at random microsecond offsets, and mathematically verifies durability, atomicity, and recovery safety against an independent external **Oracle**.

### Core Guarantees Verified:
1. **Durability**: Every acknowledged operation (`ACK`) survives a crash.
2. **Crash-Consistency / Atomicity**: Every batch operation is either fully applied or fully discarded—never partially visible.
3. **Integrity**: No key contains unwritten garbage or torn values.
4. **Recovery Resilience**: Recovery from crashes never corrupts the database or crashes the engine upon subsequent runs.
5. **Compaction Safety**: Compaction preserves all acknowledged data.

## Stages Completed
### CP1: Foundation, Configuration, and Protocol Wire Contract
- **`config.py`**: Strict argument parsing, deterministic random seed management, failure mode selection (`default`, `large-values`, `large-batches`, `compaction`, `recovery-crash`), and execution budgets.
- **`main.py`**: CLI entry point and validation.

### CP2: CLI Communication and Protocol Handling (`serve` mode)
- **`protocol.py`**: Production-grade client engine controller managing `./kvdb serve <path>`:
  - Wire format: `PUT <id> <hexkey> <hexvalue>`, `DEL <id> <hexkey>`, `BATCH <id> P:... D:...`, `COMPACT <id>`.
  - Responses: `ACK <id>`, `ERR <id> <message>`.
  - **Single Dedicated Reader**: Exactly one thread reading stdout to ensure sequential consumption without response loss.
  - **Stderr Drainer**: Dedicated thread draining stderr into a bounded circular buffer, preventing pipe-buffer deadlocks.
  - **Thread-Safe Submission**: Mutex-guarded `stdin` writes with explicit flush.
  - **Asynchronous Matching**: `threading.Event`-based pending operation table keyed by unique operation IDs.
  - **Resilience**: Full detection of unknown IDs, malformed responses, broken pipes, unexpected EOF on crash, timeouts, and orphan cleanup via `SIGKILL`.
- **`tests/fake_engine.py`**: Controllable mock engine process simulating `./kvdb serve` for deterministic edge-case testing.

### CP3: The Independent Oracle
- **`oracle.py`**: Thread-safe ground truth state model:
  - **Zero Trust**: Completely independent from the database file and engine dump.
  - **Full Provenance**: Tracks `op_id`, sequence number, writer thread, and exact state transitions for every key.
  - **Definite vs Permissible States**: Models both durable guaranteed state (all ACKed operations) and valid crash transition states (unACKed operations).
  - **Batch Atomicity Verification**: Collapses repeated sub-operations within batches to verify net all-or-nothing visibility.
  - **Integrity Validation**: Detects alien, corrupted, or unwritten values.

### CP4: Single-Writer Random-Time Crash Testing
- **`worker.py`**: Workload generator running in a dedicated thread, continuously issuing PUT, DEL, and BATCH operations with controlled pseudorandom seed.
- **`verifier.py`**: Post-crash verifier executing `open`, `check`, and `dump`, detecting malformed output, and performing deep value hash comparisons against the Oracle.
- **`reporting.py`**: Console iteration reporting and failure artifact archiving (copies corrupted DB, dumps, and Oracle JSON log into `/tmp/torture_failures/`).
- **`runner.py`**: Orchestrates the adversarial crash loop, injecting `SIGKILL` at microsecond/millisecond offsets while the writer is active, re-opening, and verifying recovery safety.

## Running Tests
From `Supernova/torture tool`:
```bash
python3 -m unittest discover -s torture/tests -p "test_*.py" -v
```
