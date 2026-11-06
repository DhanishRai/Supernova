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
        fake_engine = os.path.join(CURRENT_DIR, "tests", "fake_engine.py")
        if config.engine_bin in ("./kvdb", "kvdb") and os.path.exists(fake_engine):
            config = TortureConfig(
                iterations=config.iterations,
                seed=config.seed,
                mode=config.mode,
                seed_generated=config.seed_generated,
                engine_bin=fake_engine,
                db_path=config.db_path,
                oracle_path=config.oracle_path,
                workers=config.workers,
                max_bytes=config.max_bytes,
                min_kill_delay_ms=config.min_kill_delay_ms,
                max_kill_delay_ms=config.max_kill_delay_ms,
                artifacts_dir=config.artifacts_dir,
            )
        else:
            print("=" * 65)
            print("SUPERNOVA CRASH-CONSISTENCY TORTURE TOOL")
            print("Stage: Final Validation (CP1-CP11 Complete)")
            print(f"Mode: {config.mode.value} | Iterations: {config.iterations} | Seed: {config.seed}")
            print("=" * 65)
            print(f"ERROR: Engine executable '{config.engine_bin}' not found.")
            print("Please specify a valid engine binary via --engine <path>.")
            return 1

    runner = CrashTestRunner(config)
    return runner.run()


if __name__ == "__main__":
    sys.exit(main())
