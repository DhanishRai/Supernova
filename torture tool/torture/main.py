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
from torture.runner import CrashTestRunner


def main(args=None) -> int:
    try:
        config = parse_args(args)
    except SystemExit as exc:
        return exc.code if isinstance(exc.code, int) else 1
    except Exception as exc:
        sys.stderr.write(f"Configuration Error: {exc}\n")
        return 1

    # Check if target engine binary is available
    if not os.path.exists(config.engine_bin):
        print("=" * 65)
        print("SUPERNOVA CRASH-CONSISTENCY TORTURE TOOL")
        print("Stage: CP8 (Failure Reporting & Reproducibility)")
        print(f"Mode: {config.mode.value} | Iterations: {config.iterations} | Seed: {config.seed}")
        print("=" * 65)
        print(f"NOTICE: Engine executable '{config.engine_bin}' not found.")
        print("Person A's Go storage engine must be compiled before running live testing.")
        print("Harness verification is available via unit test suite: python3 -m unittest discover")
        return 0

    runner = CrashTestRunner(config)
    return runner.run()


if __name__ == "__main__":
    sys.exit(main())
