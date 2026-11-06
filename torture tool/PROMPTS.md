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
