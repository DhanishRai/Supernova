#!/usr/bin/env python3
"""
Sabotaged Engine Version 2: Premature ACK before Durability.

Simulates an engine that transmits `ACK <id>` over stdout BEFORE the data
is made durable on disk (e.g. committing write asynchronously in a delayed queue).
"""

import json
import os
import sys
import time


def decode_hex(hex_str: str) -> str:
    try:
        return bytes.fromhex(hex_str).decode("utf-8", errors="replace")
    except Exception:
        return hex_str


def get_wal_path(db_path: str) -> str:
    return db_path + ".wal"


def get_data_path(db_path: str) -> str:
    return db_path + ".data"


def do_open(db_path: str) -> int:
    wal = get_wal_path(db_path)
    data = get_data_path(db_path)
    state = {}
    if os.path.exists(data):
        try:
            with open(data, "r", encoding="utf-8") as f:
                state = json.load(f)
        except Exception:
            state = {}

    if os.path.exists(wal):
        try:
            with open(wal, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        entry = json.loads(line)
                        etype = entry.get("type")
                        if etype == "PUT":
                            state[entry["k"]] = entry["v"]
                        elif etype == "DEL":
                            state.pop(entry["k"], None)
                        elif etype == "BATCH":
                            for sub in entry.get("items", []):
                                if sub["type"] == "PUT":
                                    state[sub["k"]] = sub["v"]
                                elif sub["type"] == "DEL":
                                    state.pop(sub["k"], None)
                    except json.JSONDecodeError:
                        break
        except Exception:
            pass

    with open(data, "w", encoding="utf-8") as f:
        json.dump(state, f)
        f.flush()
        os.fsync(f.fileno())

    sys.stdout.write(f"OPEN OK keys={len(state)}\n")
    sys.stdout.flush()
    return 0


def do_check(db_path: str) -> int:
    sys.stdout.write("OK\n")
    sys.stdout.flush()
    return 0


def do_dump(db_path: str) -> int:
    data = get_data_path(db_path)
    state = {}
    if os.path.exists(data):
        try:
            with open(data, "r", encoding="utf-8") as f:
                state = json.load(f)
        except Exception:
            state = {}

    for k in sorted(state.keys()):
        sys.stdout.write(f"{k} {state[k]}\n")
    sys.stdout.flush()
    return 0


def do_serve(db_path: str) -> int:
    wal_path = get_wal_path(db_path)
    wal_file = open(wal_path, "a", encoding="utf-8")

    while True:
        try:
            line = sys.stdin.readline()
        except Exception:
            break

        if not line:
            break

        line = line.strip()
        if not line:
            continue

        parts = line.split()
        if len(parts) < 2:
            continue

        cmd = parts[0].upper()
        op_id = parts[1]

        # SABOTAGE: Send ACK immediately to caller BEFORE writing or fsyncing!
        sys.stdout.write(f"ACK {op_id}\n")
        sys.stdout.flush()

        # Simulate delayed asynchronous disk flush window (40ms vulnerable window)
        time.sleep(0.040)

        # Actual write occurs after ACK was already promised
        if cmd == "PUT" and len(parts) >= 4:
            k = decode_hex(parts[2])
            v = decode_hex(parts[3])
            wal_file.write(json.dumps({"type": "PUT", "k": k, "v": v}) + "\n")
            wal_file.flush()
            os.fsync(wal_file.fileno())

        elif cmd == "DEL" and len(parts) >= 3:
            k = decode_hex(parts[2])
            wal_file.write(json.dumps({"type": "DEL", "k": k}) + "\n")
            wal_file.flush()
            os.fsync(wal_file.fileno())

        elif cmd == "BATCH" and len(parts) >= 3:
            items = []
            for item in parts[2:]:
                if item.startswith("P:"):
                    subparts = item.split(":")
                    if len(subparts) == 3:
                        items.append({"type": "PUT", "k": decode_hex(subparts[1]), "v": decode_hex(subparts[2])})
                elif item.startswith("D:"):
                    subparts = item.split(":")
                    if len(subparts) == 2:
                        items.append({"type": "DEL", "k": decode_hex(subparts[1])})
            wal_file.write(json.dumps({"type": "BATCH", "items": items}) + "\n")
            wal_file.flush()
            os.fsync(wal_file.fileno())

    wal_file.close()
    return 0


def main() -> int:
    if len(sys.argv) < 3:
        sys.stderr.write(f"Usage: {sys.argv[0]} <serve|open|check|dump> <path>\n")
        return 1

    subcmd = sys.argv[1].lower()
    path = sys.argv[2]

    if subcmd == "serve":
        return do_serve(path)
    elif subcmd == "open":
        return do_open(path)
    elif subcmd == "check":
        return do_check(path)
    elif subcmd == "dump":
        return do_dump(path)
    return 1


if __name__ == "__main__":
    sys.exit(main())

