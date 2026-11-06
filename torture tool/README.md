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

## Current Stage: CP1 (Foundation, Configuration, and Protocol Wire Contract)
In Checkpoint 1, we establish:
- **`config.py`**: Strict argument parsing, deterministic random seed management, failure mode selection (`default`, `large-values`, `large-batches`, `compaction`, `recovery-crash`), and execution budgets.
- **`protocol.py`**: The wire protocol contract between the torture harness and `./kvdb serve`. Models discrete database operations (`PUT`, `DEL`, `BATCH`), serialization to `stdin`, and parsing of asynchronous `ACK <op-id>` confirmations from `stdout`.
- **`tests/`**: Comprehensive test suite using Python 3 standard library `unittest` (no external dependencies).

## Running Tests
From `Supernova/torture tool`:
```bash
python3 -m unittest discover -s torture/tests -p "test_*.py" -v
```
