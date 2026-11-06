# PROMPTS Log - Kill It and Prove It (Person B: Torture Tool)

This file maintains an honest and unedited chronological log of all prompts and tasks submitted to AI tools during the development of the crash-consistency torture tool.

---

### Entry 1
- **AI Tool**: Antigravity (Powered by Gemini 3.8 Flash)
- **Approximate Date/Time**: 2026-11-06 14:38:56 IST
- **Development Stage**: CP1 (Project Foundation, Configuration & Protocol Wire Contract)
- **Prompt**:
```text
You are my senior Python systems engineer, storage-engine testing engineer, software architect, and technical mentor.

PROJECT: KILL IT AND PROVE IT

I am Person B in a three-person hackathon team.

TEAM RESPONSIBILITIES:

Person A:
Go database engine inside src/.

Person B (ME):
Python crash-consistency torture tool.

Person C:
CLI, compaction, disk-full handling, and documentation.

MY PROJECT LOCATION:

The existing project directory is named:

Supernova

Inside Supernova, I want a NEW folder named:

torture tool

ALL MY WORK MUST BE INSIDE:

Supernova/torture tool/

This is extremely important.

Do not create my Python files in the Supernova root directory.

Do not create another Supernova directory.

Do not modify Person A's or Person C's files.

EXPECTED STRUCTURE:

Supernova/
    torture tool/
        torture
        torture/
            __init__.py
            main.py
            config.py
            protocol.py
            oracle.py
            worker.py
            runner.py
            verifier.py
            reporting.py
            tests/
        PROMPTS.md
        README.md

Create files gradually as needed.

Do not create the complete structure immediately.

PROJECT OBJECTIVE:

Build a professional crash-consistency torture testing tool in Python.

The Go database must survive SIGKILL at any instant.

Guarantees being tested:

1. Every acknowledged operation survives a crash.
2. Every unacknowledged operation is fully present or fully absent, subject to the engine's documented ordering contract.
3. Every batch is atomic.
4. No key contains a value never written to that key.
5. Recovery remains safe when interrupted.
6. Compaction never loses acknowledged data.

TECHNOLOGY:

- Python 3.
- Standard library only.
- No external packages.
- No pytest.
- No SQLite.
- No storage-engine libraries.

PERMANENT DEVELOPMENT RULES:

1. Inspect before modifying.
2. Work on one stage at a time.
3. Never generate the entire project in one shot.
4. Explain engineering decisions.
5. Implement actual working code.
6. Run actual tests.
7. Review all changes.
8. Fix discovered problems.
9. Never weaken tests just to obtain PASS.
10. Never claim untested functionality works.

GIT RULES:

Inspect the existing Git repository.

Use the existing Supernova Git repository if present.

Do not initialize a nested Git repository inside torture tool.

After every successfully completed stage:

- Run git status.
- Inspect git diff.
- Stage only files belonging to torture tool/.
- Never stage teammates' changes.
- Commit with the exact message specified in the stage.
- Verify the commit.
- Display the commit hash.

Never amend commits.
Never rewrite Git history.
Never force-push.

Do not commit untested code.

PROMPT LOG:

Maintain an honest PROMPTS.md inside torture tool/.

Record:
- AI tool name when known.
- Approximate date/time.
- Actual prompt or task description.
- Development stage.

Never fabricate historical prompts.

TEACHING RULES:

I am a B.E. AIML student.

Explain systems concepts using simple practical examples.

For every important check, explain:

"What bug would this check catch, and what bug could slip past it?"

After completing a stage:
- Provide a report.
- Show tests.
- Show commit hash.
- Explain limitations.
- STOP and wait for my next prompt.

BEGIN:

Inspect Supernova.

Confirm the repository location, Git status, existing files, and engine availability.

Create torture tool/ inside Supernova.

Implement only CP1.

Do not proceed to CP2 automatically.
```


---

### Entry 2
- **AI Tool**: Antigravity (Powered by Gemini 3.8 Flash)
- **Approximate Date/Time**: 2026-11-06 14:51:21 IST
- **Development Stage**: CP2 (CLI Communication & Protocol Handling)
- **Prompt**:
```text
IMPORTANT LOCATION:

All my work must be inside:

Supernova/torture tool/

Implement CP2: CLI communication and protocol handling.

FIRST:

Inspect the current CP1 implementation and the actual Go CLI.

Agreed command:

./kvdb serve <path>

Protocol:

PUT <id> <hexkey> <hexvalue>

DEL <id> <hexkey>

BATCH <id> P:<hexkey>:<hexvalue> D:<hexkey>

COMPACT <id>

Responses:

ACK <id>

ERR <id> <message>

IMPORTANT:

Inspect actual engine behaviour before assuming protocol details.

IMPLEMENT:

Create torture/protocol.py.

Use subprocess.Popen.

Configure:

- stdin pipe.
- stdout pipe.
- stderr independently drained or safely captured.
- Explicit encoding.
- Proper process tracking.
- Timeouts.
- Reliable cleanup.

Implement:

1. Command encoding.
2. PUT.
3. DEL.
4. BATCH.
5. COMPACT.
6. Unique operation IDs.
7. One dedicated stdout reader thread.
8. Thread-safe stdin writing.
9. ACK parsing.
10. ERR parsing.
11. Unknown ID detection.
12. Malformed response detection.
13. Unexpected EOF handling.
14. Broken-pipe handling.
15. Child termination handling.

CRITICAL:

There must be exactly one stdout reader.

Never treat successful stdin transmission as an ACK.

Protect command writes using a lock.

Flush commands appropriately.

TEST:

Use a temporary fake CLI process if the real engine is unavailable.

Test:

- ACK.
- ERR.
- Unknown IDs.
- Malformed responses.
- Unexpected EOF.
- Broken pipes.
- Child process crash.
- Timeout.
- Multiple concurrent submissions.
- Command interleaving.

Clearly identify fake-engine tests.

REVIEW:

Look for deadlocks, race conditions, response loss, and orphan processes.

GIT COMMIT:

add serve protocol handling

Report the tests and commit hash.

Explain the protocol design.

STOP after CP2.
```


---

### Entry 3
- **AI Tool**: Antigravity (Powered by Gemini 3.8 Flash)
- **Approximate Date/Time**: 2026-11-06 15:01:25 IST
- **Development Stage**: CP1 (Minimum Python Project Skeleton & Argument Parsing)
- **Prompt**:
```text
IMPORTANT LOCATION:

All my work must be inside:

Supernova/torture tool/

Do not create Python files anywhere else.

We are implementing CP1 of the KILL IT AND PROVE IT torture tool.

OBJECTIVE:

Inspect the repository and create the minimum Python project skeleton.

STEP 1: INSPECT

Check:

1. Current working directory.
2. Supernova directory location.
3. Git repository status.
4. Existing torture tool directory.
5. Existing ./kvdb executable.
6. Python version.
7. Existing CLI implementation.
8. Existing executable permissions.

Never assume files or commands exist.

STEP 2: CREATE

Inside torture tool/, create only:

torture/
    __init__.py
    main.py
    config.py

Create an executable launcher named:

torture

STEP 3: ARGUMENT PARSING

Implement argparse.

Required arguments:

--iterations
--seed
--mode

Supported modes:

default
large-values
large-batches
recovery-crash
compaction
disk-full

Defaults:

iterations = 500
mode = default

If seed is omitted:
- Generate a seed.
- Print it clearly.
- Ensure future workload generation can use it.

Validate invalid arguments.

Do not implement database communication yet.

STEP 4: TEST

Test:

- Default arguments.
- Custom iterations.
- Custom seed.
- All supported modes.
- Invalid modes.
- Zero iterations.
- Negative iterations.
- Invalid seeds.

Use unittest or direct tests.

STEP 5: REVIEW

Check:
- Imports.
- Argument validation.
- Executable permissions.
- Folder structure.
- Accidental modifications outside torture tool/.

STEP 6: GIT COMMIT

Inspect git status and diff.

Stage only my files.

Commit:

add torture tool skeleton

Verify the commit and display its hash.

STEP 7: REPORT

Explain every created file.

Show commands executed and actual test results.

Ask:

"What bug would this check catch, and what bug could slip past it?"

STOP after CP1.
```


---

### Entry 4
- **AI Tool**: Antigravity (Powered by Gemini 3.8 Flash)
- **Approximate Date/Time**: 2026-11-06 15:06:45 IST
- **Development Stage**: CP3 (The Independent Oracle)
- **Prompt**:
```text
IMPORTANT LOCATION:

All my work must be inside:

Supernova/torture tool/

Implement CP3: the independent oracle.

This is one of the most important components of the entire project.

OBJECTIVE:

Build an independent model of the database state.

The oracle must never use the database dump to calculate expected state.

STORE:

- Operation ID.
- Operation type.
- Key.
- Value.
- Batch contents.
- Submission sequence.
- ACK status.
- ERR status.
- Writer information.

SUPPORT:

PUT
DEL
BATCH

RULES:

1. Every ACKed operation must be reflected in recovered state.
2. Unacknowledged operations may be fully present or absent.
3. Unacknowledged batches must be fully present or fully absent.
4. Batches must be atomic.
5. Repeated keys must be handled correctly.
6. Deletes must be represented correctly.

CRITICAL ORDERING:

Inspect the engine's operation-processing guarantees.

Do not assume submission order equals commit order.

For this initial stage, use a single writer.

Establish whether the CLI processes input sequentially.

If ordering guarantees cannot be verified, report the ambiguity.

Do not invent guarantees.

IMPLEMENT:

Create torture/oracle.py.

Use locks where required.

Maintain operation history.

Preserve enough information to identify which operation IDs produced each final value.

TEST:

1. ACKed PUT.
2. ACKed DEL.
3. ACKed BATCH.
4. Unacknowledged PUT.
5. Unacknowledged BATCH.
6. Repeated overwrites.
7. Delete followed by PUT.
8. Batch with repeated keys.
9. Multiple writes to the same key.

For each test explain:

"What bug would this check catch, and what bug could slip past it?"

GIT COMMIT:

track acknowledged writes

STOP after CP3.
```


---

### Entry 5
- **AI Tool**: Antigravity (Powered by Gemini 3.8 Flash)
- **Approximate Date/Time**: 2026-11-06 15:12:24 IST
- **Development Stage**: CP4 (Single-Writer Random-Time Crash Testing)
- **Prompt**:
```text
IMPORTANT LOCATION:

All my work must be inside:

Supernova/torture tool/

Implement CP4: single-writer random-time crash testing.

Use the existing protocol and oracle.

OBJECTIVE:

For each iteration:

1. Create a fresh database directory.
2. Start kvdb serve.
3. Start a writer.
4. Generate random PUT, DEL, and BATCH operations.
5. Continuously submit operations.
6. Record every actual ACK in the oracle.
7. Choose a random millisecond delay.
8. Kill the database using SIGKILL.
9. Stop and join the writer safely.
10. Preserve all received responses.
11. Run kvdb open.
12. Run kvdb check.
13. Run kvdb dump.
14. Parse the dump.
15. Compare recovered state against the independent oracle.

RANDOMNESS:

Use:

rng = random.Random(seed)

Never use the global random module.

All workload generation and random kill delays must use controlled RNG instances.

KILL TIMING:

Example:

delay_ms = rng.randint(1, 100)

The writer must remain active during the kill window.

Do not wait for an operation to finish before killing.

Use proc.kill() on Linux.

Verify the process actually exited because of SIGKILL.

ORACLE VERIFICATION:

- Missing acknowledged operations are failures.
- Incorrect values are failures.
- Unknown values are failures.
- Partial batches are failures.
- Fully applied unacknowledged operations may be valid.
- Lost ACKs must not cause false failures.

Respect the documented engine ordering contract.

DUMP VERIFICATION:

Detect malformed dump output.

For mismatches report:

- Key.
- Expected state.
- Actual state.
- Operation IDs.
- Value lengths.
- Value hashes.

REPORTING:

Print one line per iteration.

Stop at the first genuine failure.

Preserve failure artifacts.

TEST:

Use fake-process tests where necessary.

Never claim fake-engine tests prove crash consistency.

GIT COMMIT:

add random crash verification

Explain the design and limitations.

STOP after CP4.
```


---

### Entry 6
- **AI Tool**: Antigravity (Powered by Gemini 3.8 Flash)
- **Approximate Date/Time**: 2026-11-06 15:20:45 IST
- **Development Stage**: CP4 Sabotage Test (Fault Injection & Torture Tool Verification)
- **Prompt**:
```text
IMPORTANT LOCATION:

All torture-tool improvements must remain inside:

Supernova/torture tool/

We have completed CP4.

Now perform the CP4 SABOTAGE TEST.

OBJECTIVE:

Prove that our torture tool can detect deliberately introduced engine bugs.

Coordinate with Person A.

Ask Person A to create temporary sabotaged engine versions.

SABOTAGE 1:

Remove the engine's required fsync operation.

SABOTAGE 2:

Send ACK before the operation becomes durable.

IMPORTANT:

Never permanently modify or commit Person A's engine code.

Run the torture tool against each sabotaged version separately.

RECORD:

- Sabotage type.
- Seed.
- Iteration.
- Mode.
- Expected result.
- Actual result.
- Whether the bug was detected.
- Failure evidence.

IF NOT DETECTED:

Investigate:

1. Did the workload overlap the vulnerable operation?
2. Did SIGKILL occur during a useful window?
3. Is the oracle correct?
4. Is the workload sensitive enough?
5. Are we accidentally accepting invalid states?

Improve only the torture tool when necessary.

Repeat the experiment.

Do not weaken assertions.

Do not claim success without evidence.

Explain why random crash testing is probabilistic.

Ask Person A for temporary instrumentation around write and fsync operations if useful.

Instrumentation must not replace real SIGKILL testing.

Commit legitimate improvements separately.

STOP after the sabotage report.
```


---

### Entry 7
- **AI Tool**: Antigravity (Powered by Gemini 3.8 Flash)
- **Approximate Date/Time**: 2026-11-06 15:25:55 IST
- **Development Stage**: CP5 (Concurrent Writer Testing)
- **Prompt**:
```text
IMPORTANT LOCATION:

All my work must be inside:

Supernova/torture tool/

Implement CP5: concurrent writer testing.

OBJECTIVE:

Support at least two concurrent writer threads.

Each writer must have:

- Unique operation IDs.
- Its own deterministic random.Random instance.
- Independent operation generation.
- Proper exception handling.

SHARED RESOURCES:

- One database process.
- One stdout reader.
- One synchronized stdin writer.
- One thread-safe oracle.

TEST SCENARIOS:

1. Two writers modifying different keys.
2. Two writers modifying the same keys.
3. Concurrent PUT and DEL.
4. Concurrent PUT and BATCH.
5. Concurrent large batches where supported.
6. SIGKILL while both writers are active.

CRITICAL ORDERING:

Do not assume Python thread submission order equals database commit order.

Do not assume ACK arrival order establishes commit order unless guaranteed by the engine.

Preserve actual protocol submission ordering.

Model every legal outcome consistent with the engine's documented ordering.

Do not weaken oracle correctness.

TEST:

Check:
- Deadlocks.
- Race conditions.
- Missing responses.
- Duplicate IDs.
- Incorrect recovered states.
- Writer cleanup.

Explain the limitations of concurrent testing.

GIT COMMIT:

add concurrent writers

STOP after CP5.
```


---

### Entry 8
- **AI Tool**: Antigravity (Powered by Gemini 3.8 Flash)
- **Approximate Date/Time**: 2026-11-06 15:35:00 IST
- **Development Stage**: CP6 (Shared-Key Testing and Large Values)
- **Prompt**:
```text
IMPORTANT LOCATION:

All my work must be inside:

Supernova/torture tool/

Implement CP6: shared-key testing and large values.

Add:

--mode large-values

OBJECTIVE:

Generate values approximately 1 MB in size.

TEST:

1. Large PUT.
2. Large overwrite.
3. Large values across multiple keys.
4. Large value followed by DELETE.
5. Repeated large-value writes.
6. Kill during large-value transmission.
7. Kill during large-value processing.

SHARED-KEY TESTING:

Multiple writers must deliberately modify a small set of common keys.

Use distinguishable values associated with operation IDs.

MEMORY:

Avoid unnecessary duplication of large values.

Use hashes and lengths in failure reports.

Retain sufficient information for exact value verification.

TEST:

Detect:
- Truncated values.
- Corrupted values.
- Incorrect decoding.
- Partial persistence.
- Incorrect overwrites.

Do not print entire large values.

GIT COMMIT:

test large values

STOP after CP6.
```


---

### Entry 9
- **AI Tool**: Antigravity (Powered by Gemini 3.8 Flash)
- **Approximate Date/Time**: 2026-11-06 16:01:00 IST
- **Development Stage**: CP7 (Large-Batch Testing)
- **Prompt**:
```text
IMPORTANT LOCATION:

All my work must be inside:

Supernova/torture tool/

Implement CP7: large-batch testing.

Add:

--mode large-batches

OBJECTIVE:

Support batches containing up to 10,000 operations.

TEST:

1. All-PUT batch.
2. All-DELETE batch.
3. Mixed batch.
4. Repeated keys.
5. PUT followed by DELETE.
6. DELETE followed by PUT.
7. Crash during batch transmission.
8. Crash during batch processing.
9. Crash after durability but before ACK delivery.

CRITICAL:

A batch is one logical operation.

Never allow the oracle to accept partial batch application.

Handle repeated keys according to documented batch order.

MEMORY:

Keep memory usage reasonable.

Avoid unnecessary copies.

Do not print all 10,000 operations unless a failure occurs.

TEST:

Use fake-process tests for protocol and oracle correctness.

Use the real engine for actual batch atomicity testing.

GIT COMMIT:

test large batches

STOP after CP7.
```


---

### Entry 10
- **AI Tool**: Antigravity (Powered by Gemini 3.8 Flash)
- **Approximate Date/Time**: 2026-11-06 16:11:00 IST
- **Development Stage**: CP8 (Professional Failure Reporting and Reproducibility)
- **Prompt**:
```text
IMPORTANT LOCATION:

All my work must be inside:

Supernova/torture tool/

Implement CP8: professional failure reporting and reproducibility.

OBJECTIVE:

Every failure must report:

- Seed.
- Iteration.
- Mode.
- Database path.
- Operation IDs.
- Operation types.
- Expected state.
- Actual state.
- Expected value length.
- Actual value length.
- Expected value hash.
- Actual value hash.
- Relevant operation history.
- Child process exit information.
- Available stderr output.

SAVE:

Preserve useful failure artifacts in a predictable directory inside torture tool/.

REPRODUCIBILITY:

Use deterministic derived seeds for writer threads.

Never use the global random module.

Document:

A seed reproduces workload generation and requested random delays, but cannot guarantee identical operating-system scheduling, disk timing, or exact crash instructions.

CLASSIFY FAILURES:

- Engine failure.
- Protocol failure.
- Oracle failure.
- Torture tool failure.
- Timeout.
- Recovery failure.

A torture-tool crash must never be reported as PASS.

TEST:

Verify:
- Failure artifact creation.
- Large-value reporting.
- Hash correctness.
- Reproducible workload generation.
- Failure classification.

GIT COMMIT:

improve failure reports

STOP after CP8.
```

---

### Entry 11
- **AI Tool**: Antigravity (Powered by Gemini 3.8 Flash)
- **Approximate Date/Time**: 2026-11-06 16:25:18 IST
- **Development Stage**: CP9 (Recovery-Crash Mode Testing)
- **Prompt**:
```text
IMPORTANT LOCATION:

All my work must be inside:

Supernova/torture tool/

Implement CP9: recovery-crash mode.

Add:

--mode recovery-crash

WORKFLOW:

1. Create a database with acknowledged operations.
2. Kill the serve process.
3. Start kvdb open.
4. Select a random millisecond delay.
5. Kill the open process using SIGKILL.
6. Start kvdb open again.
7. Run kvdb check.
8. Run kvdb dump.
9. Verify recovered state against the independent oracle.

TEST:

Perform repeated recovery interruptions.

Use sufficiently large databases to increase the recovery window.

CRITICAL:

If open finishes before SIGKILL, record:

MISSED CRASH WINDOW

Do not count this as a successful recovery-crash test.

RECORD:

- Recovery start time.
- Requested kill delay.
- Actual process exit status.
- Whether SIGKILL occurred.
- Recovery completion time.

Verify the second recovery completes correctly.

GIT COMMIT:

test recovery crashes

STOP after CP9.
```




