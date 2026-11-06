#!/usr/bin/env python3
"""
CLI entry point for the Supernova Crash-Consistency Torture Tool.

Usage:
    python3 -m torture.main --iterations 500 --seed 12345
    ./torture/main.py --iterations 100 --seed 999 --mode large-values
    ./torture/main.py --mode disk-full
"""

import os
import sys

# Ensure package root is in sys.path
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PARENT_DIR = os.path.dirname(CURRENT_DIR)
if PARENT_DIR not in sys.path:
    sys.path.insert(0, PARENT_DIR)

from torture.config import parse_args, TortureMode


def main(args=None) -> int:
    try:
        config = parse_args(args)
    except SystemExit as exc:
        return exc.code if isinstance(exc.code, int) else 1
    except Exception as exc:
        sys.stderr.write(f"Configuration Error: {exc}\n")
        return 1

    print("=" * 60)
    print("SUPERNOVA CRASH-CONSISTENCY TORTURE TOOL")
    print("Stage: CP1 (Project Skeleton & Argument Parsing)")
    print(f"Mode: {config.mode.value}")
    print(f"Iterations: {config.iterations}")
    print(f"Seed: {config.seed} (generated={config.seed_generated})")
    print(f"Engine Executable: {config.engine_bin}")
    print(f"Target Database: {config.db_path}")
    print(f"Independent Oracle: {config.oracle_path}")
    print(f"Concurrent Writers: {config.workers}")
    if config.max_bytes:
        print(f"Simulated Max Disk Bytes: {config.max_bytes}")
    print("=" * 60)
    print("CP1 validation successful: Project skeleton and argument parsing operational.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
