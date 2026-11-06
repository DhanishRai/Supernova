#!/usr/bin/env python3
"""
Fake Storage Engine Process for Unit and Integration Testing of Torture Harness.

Implements all required CLI subcommands:
    fake_engine.py serve <path>
    fake_engine.py open <path>
    fake_engine.py check <path>
    fake_engine.py dump <path>

In serve mode, maintains an append-only WAL on disk with fsync before ACK,
replaying upon `open` so it faithfully exhibits crash-consistency semantics
for test verification.
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
    """Recovers database by replaying WAL into data file."""
    # Support simulated recovery window duration for recovery-crash testing
    delay_ms_str = os.environ.get("SUPERNOVA_RECOVERY_DELAY_MS")
    if delay_ms_str:
        try:
            delay_sec = float(delay_ms_str) / 1000.0
            time.sleep(delay_sec)
        except ValueError:
            pass

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
                        # Torn tail at crash point! Stop replay cleanly.
                        break
        except Exception:
            pass

    with open(data, "w", encoding="utf-8") as f:
        json.dump(state, f)
        f.flush()
        os.fsync(f.fileno())

    sys.stdout.write(f"OPEN OK keys={len(state)} recovery_time=0.001s\n")
    sys.stdout.flush()
    return 0


def do_check(db_path: str) -> int:
    """Verifies file integrity."""
    sys.stdout.write("OK\n")
    sys.stdout.flush()
    return 0


def do_dump(db_path: str) -> int:
    """Dumps all key-value pairs sorted by key."""
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
            raw_line = sys.stdin.readline()
        except Exception:
            break

        if not raw_line:
            break

        if not raw_line.endswith("\n"):
            # Severed mid-line by SIGKILL! Drop incomplete wire fragment
            break

        line = raw_line.strip()
        if not line:
            continue

        parts = line.split()
        if len(parts) < 2:
            sys.stdout.write("MALFORMED_NO_OP_ID\n")
            sys.stdout.flush()
            continue

        cmd = parts[0].upper()
        op_id = parts[1]

        # Decode command arguments
        k, v = "", ""
        if cmd == "PUT" and len(parts) >= 4:
            k = decode_hex(parts[2])
            v = decode_hex(parts[3])
            decoded_payload = f"{op_id} {k} {v}"
        elif cmd == "DEL" and len(parts) >= 3:
            k = decode_hex(parts[2])
            decoded_payload = f"{op_id} {k}"
        elif cmd == "BATCH" and len(parts) >= 3:
            decoded_payload = f"{op_id} " + " ".join(parts[2:20])
        else:
            decoded_payload = op_id

        # Triggers for unit testing
        if "TRIGGER_" in decoded_payload:
            if "TRIGGER_STDERR:" in decoded_payload:
                idx = decoded_payload.find("TRIGGER_STDERR:")
                msg = decoded_payload[idx + len("TRIGGER_STDERR:"):].split()[0]
                sys.stderr.write(f"DEBUG_LOG: {msg}\n")
                sys.stderr.flush()

            if "TRIGGER_UNKNOWN_ID:" in decoded_payload:
                idx = decoded_payload.find("TRIGGER_UNKNOWN_ID:")
                ghost_id = decoded_payload[idx + len("TRIGGER_UNKNOWN_ID:"):].split()[0]
                sys.stdout.write(f"ACK {ghost_id}\n")
                sys.stdout.flush()

            if "TRIGGER_MALFORMED" in decoded_payload:
                sys.stdout.write("CORRUPTED_LINE_NOT_A_VALID_ACK_OR_ERR\n")
                sys.stdout.flush()

            if "TRIGGER_DELAY:" in decoded_payload:
                idx = decoded_payload.find("TRIGGER_DELAY:")
                sec = float(decoded_payload[idx + len("TRIGGER_DELAY:"):].split()[0])
                time.sleep(sec)

            if "TRIGGER_CRASH" in decoded_payload and "TRIGGER_CRASH_AFTER_SYNC" not in decoded_payload:
                sys.stdout.flush()
                sys.stderr.flush()
                os._exit(137)

            if "TRIGGER_ERR:" in decoded_payload:
                idx = decoded_payload.find("TRIGGER_ERR:")
                err_msg = decoded_payload[idx + len("TRIGGER_ERR:"):].split()[0]
                sys.stdout.write(f"ERR {op_id} {err_msg}\n")
                sys.stdout.flush()
                continue

        # Normal execution
        if cmd == "PUT":
            wal_file.write(json.dumps({"type": "PUT", "k": k, "v": v}) + "\n")
            wal_file.flush()
            os.fsync(wal_file.fileno())
        elif cmd == "DEL":
            wal_file.write(json.dumps({"type": "DEL", "k": k}) + "\n")
            wal_file.flush()
            os.fsync(wal_file.fileno())
        elif cmd == "BATCH":
            items = []
            for item in parts[2:]:
                if item.startswith("P:"):
                    subparts = item.split(":", 2)
                    if len(subparts) == 3:
                        items.append({"type": "PUT", "k": decode_hex(subparts[1]), "v": decode_hex(subparts[2])})
                elif item.startswith("D:"):
                    subparts = item.split(":", 1)
                    if len(subparts) == 2:
                        items.append({"type": "DEL", "k": decode_hex(subparts[1])})

            if "TRIGGER_TORN_BATCH" in raw_line:
                # Deliberate sabotage: persist only the first half of batch items
                items = items[: len(items) // 2]

            wal_file.write(json.dumps({"type": "BATCH", "items": items}) + "\n")
            wal_file.flush()
            os.fsync(wal_file.fileno())

            if "TRIGGER_CRASH_AFTER_SYNC" in raw_line:
                wal_file.close()
                sys.stdout.flush()
                sys.stderr.flush()
                os._exit(137)

        # Send ACK
        sys.stdout.write(f"ACK {op_id}\n")
        sys.stdout.flush()

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
    else:
        sys.stderr.write(f"Unknown subcommand '{subcmd}'\n")
        return 1


if __name__ == "__main__":
    sys.exit(main())
