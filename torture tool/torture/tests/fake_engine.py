#!/usr/bin/env python3
"""
Fake Storage Engine Process for Unit and Integration Testing of Protocol Client.

Implements the agreed CLI interface:
    fake_engine.py serve <path>

Reads commands from stdin:
    PUT <id> <hexkey> <hexvalue>
    DEL <id> <hexkey>
    BATCH <id> P:<hexkey>:<hexvalue> D:<hexkey> ...
    COMPACT <id>

Responds on stdout:
    ACK <id>
    ERR <id> <message>

Decodes hex keys and values to detect scripted test triggers.
"""

import os
import sys
import time


def decode_hex(hex_str: str) -> str:
    try:
        return bytes.fromhex(hex_str).decode("utf-8", errors="replace")
    except Exception:
        return hex_str


def main() -> int:
    if len(sys.argv) < 3 or sys.argv[1] != "serve":
        sys.stderr.write(f"Usage: {sys.argv[0]} serve <path>\n")
        return 1

    while True:
        try:
            line = sys.stdin.readline()
        except Exception:
            break

        if not line:
            # Stdin EOF
            break

        line = line.strip()
        if not line:
            continue

        parts = line.split()
        if len(parts) < 2:
            sys.stdout.write("MALFORMED_NO_OP_ID\n")
            sys.stdout.flush()
            continue

        cmd = parts[0].upper()
        op_id = parts[1]

        # Decode payload for inspecting triggers
        decoded_payload = ""
        if cmd == "PUT" and len(parts) >= 4:
            decoded_payload = f"{decode_hex(parts[2])} {decode_hex(parts[3])}"
        elif cmd == "DEL" and len(parts) >= 3:
            decoded_payload = decode_hex(parts[2])
        elif cmd == "BATCH" and len(parts) >= 3:
            items = []
            for item in parts[2:]:
                if item.startswith("P:"):
                    subparts = item.split(":")
                    if len(subparts) == 3:
                        items.append(f"{decode_hex(subparts[1])}:{decode_hex(subparts[2])}")
                elif item.startswith("D:"):
                    subparts = item.split(":")
                    if len(subparts) == 2:
                        items.append(decode_hex(subparts[1]))
            decoded_payload = " ".join(items)
        else:
            decoded_payload = " ".join(parts[2:])

        # 1. Trigger stderr output (testing stderr drain)
        if "TRIGGER_STDERR:" in decoded_payload:
            idx = decoded_payload.find("TRIGGER_STDERR:")
            msg = decoded_payload[idx + len("TRIGGER_STDERR:"):].split()[0]
            sys.stderr.write(f"DEBUG_LOG: {msg}\n")
            sys.stderr.flush()

        # 2. Trigger unknown ID injection
        if "TRIGGER_UNKNOWN_ID:" in decoded_payload:
            idx = decoded_payload.find("TRIGGER_UNKNOWN_ID:")
            ghost_id = decoded_payload[idx + len("TRIGGER_UNKNOWN_ID:"):].split()[0]
            sys.stdout.write(f"ACK {ghost_id}\n")
            sys.stdout.flush()

        # 3. Trigger malformed response
        if "TRIGGER_MALFORMED" in decoded_payload:
            sys.stdout.write("CORRUPTED_LINE_NOT_A_VALID_ACK_OR_ERR\n")
            sys.stdout.flush()

        # 4. Trigger delay (testing timeout)
        if "TRIGGER_DELAY:" in decoded_payload:
            idx = decoded_payload.find("TRIGGER_DELAY:")
            sec = float(decoded_payload[idx + len("TRIGGER_DELAY:"):].split()[0])
            time.sleep(sec)

        # 5. Trigger abrupt crash (testing child crash / unexpected EOF)
        if "TRIGGER_CRASH" in decoded_payload:
            sys.stdout.flush()
            sys.stderr.flush()
            os._exit(137)

        # 6. Trigger explicit ERR response
        if "TRIGGER_ERR:" in decoded_payload:
            idx = decoded_payload.find("TRIGGER_ERR:")
            err_msg = decoded_payload[idx + len("TRIGGER_ERR:"):].split()[0]
            sys.stdout.write(f"ERR {op_id} {err_msg}\n")
            sys.stdout.flush()
            continue

        # Standard successful ACK
        sys.stdout.write(f"ACK {op_id}\n")
        sys.stdout.flush()

    return 0


if __name__ == "__main__":
    sys.exit(main())
