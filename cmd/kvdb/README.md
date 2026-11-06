# KillItDB CLI & Server (`cmd/kvdb`)

**Owner:** Teammate (CLI, Server & Compaction Specialist)

## Purpose
This directory houses the command-line interface and daemon server (`kvdb` binary):
- CLI operations: `put`, `get`, `del`, `batch`, `open`, `check`, `dump`.
- Server daemon: `serve` (plain-text or socket-based protocol with immediate line-by-line flushing).
- Compaction trigger: `compact` (safely rewrites valid keys to `<path>.tmp` and atomically renames).

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
go build -o kvdb ./cmd/kvdb

# Basic commands
./kvdb put db.kv name Mohith
./kvdb get db.kv name
./kvdb del db.kv name
./kvdb open db.kv
./kvdb check db.kv

# Daemon server
./kvdb serve db.kv
```
