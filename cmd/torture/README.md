# Torture Testing Tool (`cmd/torture`)

**Owner:** Teammate (Torture-Testing Specialist)

## Purpose
This directory houses the autonomous torture testing tool (`torture` binary). It is designed to stress test the database engine under aggressive, non-deterministic conditions:
- Randomized operation sequences (PUT, DELETE, BATCH).
- Arbitrary unannounced process termination (`SIGKILL` / `kill -9`).
- Torn write detection and recovery verification.
- Compaction crash verification.

## How to Import the Database Engine
```go
package main

import (
    "killitdb/src" // Authoritative KillItDB engine core
)
```

## Build & Run Commands
From the repository root (`/c/projects/srinathon-3.0`):
```bash
# Build binary
go build -o torture ./cmd/torture

# Run torture tests with seeds
./torture --iterations 500 --seed 12345
./torture --iterations 100 --seed 999 --mode large-values
./torture --iterations 100 --seed 7   --mode compaction
./torture --iterations 100 --seed 42  --mode recovery-crash
```
