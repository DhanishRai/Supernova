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
