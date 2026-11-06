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

### CP5: Concurrent Writer Testing
- **`worker.py` (`ConcurrentWriter`)**: Multiple concurrent writer threads, each with:
  - Isolated deterministic `random.Random` instances derived from the master seed.
  - Namespaced operation ID generation (`w1-op-...`, `w2-op-...`) guaranteeing zero cross-thread collisions.
  - Configurable workloads: mixed, put-only, del-only, batch-only across disjoint or shared keys.
- **`oracle.py` (Linearizability & Concurrency Modeling)**:
  - Models happened-before relationships ($O_1 \prec O_2$) versus true concurrency ($O_1 \parallel O_2$).
  - Never assumes Python thread submission order equals commit order.
  - Accepts any valid linearization of concurrent in-flight writes without weakening baseline durability or integrity.
- **`tests/test_concurrency.py`**: Exercises all 6 mandated concurrency stress scenarios.

### CP6: Shared-Key Testing and Large Values (`--mode large-values`)
- **`worker.py` (`generate_large_value`)**:
  - Deterministic ~1 MB ($1,048,576$ bytes) payloads structured with headers: `SUPERNOVA_LARGE_VAL:<op_id>:<writer_id>:<key>:len=1048576:` followed by repeating op-tagged chunks.
  - Defaults to small shared key pools (`shared_key_0` .. `shared_key_3`) when in `large-values` mode to force maximum concurrent contention, log head contention, and repeated overwrites.
- **Memory Efficiency & Bounded Failure Reports**:
  - `oracle.py`: Custom `format_val_summary()` truncates values $>64$ bytes to `<len=..., sha256=..., preview='...'>`.
  - `verifier.py`: Formats SHA-256 hex digests and byte lengths; never dumps 1 MB raw strings into console or error buffers.
  - `reporting.py`: Failure artifacts in `oracle_history.json` and `recovered_dump.json` store cryptographic hashes and lengths for values $>256$ bytes, preventing disk explosion.
- **`tests/test_large_values.py`**:
  - Exercises all 7 mandated scenarios: Large PUT, Large overwrite, Large values across multiple keys, Large value followed by DELETE, Repeated large writes on shared keys, Kill during transmission, Kill during processing.
  - Verifies defect detection: Truncated values, corrupted bytes, incorrect hex decoding, partial persistence / torn records, and incorrect overwrites.

### CP7: Large-Batch Testing (`--mode large-batches`)
- **`worker.py` (`generate_large_batch`)**:
  - High-performance generator creating batches up to 10,000 operations across diverse topologies: `all_put`, `all_del`, `mixed`, `repeated`, `put_then_del`, and `del_then_put`.
  - Evaluates batch order semantics: repeated operations on identical keys within the same batch collapse to the net final sub-operation.
- **Strict Atomicity Verification & Crash Semantics**:
  - A batch is treated as a single indivisible logical transaction ($A$ in ACID).
  - The Oracle mathematically enforces that a batch is either 100% applied or 0% applied. Torn/partial application is strictly flagged as an `ATOMICITY VIOLATION`.
  - Distinguishes crashes mid-processing from crashes after durability but before ACK delivery (where the batch is validly durable on disk and accepted as `unacked_committed`).
- **Memory & Wire Protocol Scaling**:
  - Adaptive socket/pipe write timeouts scaled for multi-thousand item batches.
  - Diagnostic mismatch reporting bounded to 50 entries with omission summaries to protect terminal buffers while preserving full details in artifacts.
- **`tests/test_large_batches.py`**:
  - Exercises all 12 large-batch scenarios and defect models: 10,000-op all-PUT, all-DEL, mixed, repeated keys, PUT-then-DEL, DEL-then-PUT, transmission crash, processing crash, post-durability pre-ACK crash, torn batch rejection, live/mock engine integration, and memory efficiency.

### CP8: Professional Failure Reporting and Reproducibility
- **Predictable Failure Artifact Preservation**:
  - Preserved in a predictable directory inside `torture tool/artifacts/` (customizable via `--artifacts-dir`).
  - Saved on any failure or crash:
    - `corrupted_target.db`: Exact snapshot of the target database file.
    - Sidecars (`*.wal`, `*.data`): Preserved alongside the main database file.
    - `failure_report.json`: Machine-readable JSON structured failure record.
    - `diagnostic_report.txt`: Human-readable formatted diagnostic investigation document.
    - `oracle_history.json`: Complete operation history with lean summaries for values > 256 bytes.
    - `recovered_dump.json`: Dump of recovered state with value lengths and SHA-256 digests.
- **Canonical Failure Classification Taxonomy**:
  - `Engine failure`: Unexpected non-zero process exits, engine segfaults, or unprovoked terminations.
  - `Protocol failure`: Malformed wire responses, unrecognized operation IDs, or codec violations.
  - `Oracle failure`: Durability violations (missing ACKed keys), atomicity violations (torn batches), or data corruption.
  - `Torture tool failure`: Unexpected exceptions in the testing harness itself (never masked or reported as PASS).
  - `Timeout`: Unresponsive operations or worker timeouts.
  - `Recovery failure`: Subcommand failures during `kvdb open`, `kvdb check`, or `kvdb dump`.
- **Reproducibility Guarantee & Disclaimer**:
  - Workload generation and randomized kill delays are deterministically derived from `--seed` using isolated `random.Random` instances. No use of the global `random` module.
  - Mandatory disclaimer documented and embedded in all failure artifacts:
    > *"A seed reproduces workload generation and requested random delays, but cannot guarantee identical operating-system scheduling, disk timing, or exact crash instructions."*
- **`tests/test_reporting.py`**:
  - Validates artifact preservation, structured JSON report fields, large-value memory bounding (hashes/lengths), cryptographic SHA-256 correctness, deterministic workload reproduction, classification taxonomy, and harness crash safety.

## Running Tests
From `Supernova/torture tool`:
```bash
python3 -m unittest discover -s torture/tests -p "test_*.py" -v
```
