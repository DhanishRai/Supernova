"""
Configuration module for the Supernova Crash-Consistency Torture Tool.

Manages test configuration, CLI arguments, deterministic seed initialization,
and failure mode policies.
"""

from dataclasses import dataclass
from enum import Enum
import argparse
import os
import random
import sys
from typing import List, Optional


class TortureMode(str, Enum):
    """
    Test execution modes mandated by Section 7 (Minimum coverage) of the specification:
    - DEFAULT: Standard random mix of put, del, and batch operations.
    - LARGE_VALUES: Puts with 1 MB payload values spanning multiple OS disk blocks.
    - LARGE_BATCHES: Atomic batches with up to 10,000 operations.
    - COMPACTION: Triggers engine compaction and injects kills during compaction.
    - RECOVERY_CRASH: Injects SIGKILL while open() recovery is executing.
    """
    DEFAULT = "default"
    LARGE_VALUES = "large-values"
    LARGE_BATCHES = "large-batches"
    COMPACTION = "compaction"
    RECOVERY_CRASH = "recovery-crash"


@dataclass(frozen=True)
class TortureConfig:
    iterations: int
    seed: int
    mode: TortureMode
    engine_bin: str
    db_path: str
    oracle_path: str
    workers: int
    max_bytes: Optional[int] = None
    min_kill_delay_ms: int = 1
    max_kill_delay_ms: int = 50

    def validate(self) -> None:
        """
        Validates configuration values.
        Raises ValueError if any parameter violates safety or specification limits.
        """
        if self.iterations <= 0:
            raise ValueError(f"iterations must be positive, got {self.iterations}")
        if self.workers <= 0:
            raise ValueError(f"workers must be at least 1, got {self.workers}")
        if self.min_kill_delay_ms < 0:
            raise ValueError(f"min_kill_delay_ms must be non-negative, got {self.min_kill_delay_ms}")
        if self.max_kill_delay_ms < self.min_kill_delay_ms:
            raise ValueError(
                f"max_kill_delay_ms ({self.max_kill_delay_ms}) cannot be less than "
                f"min_kill_delay_ms ({self.min_kill_delay_ms})"
            )
        if self.max_bytes is not None and self.max_bytes <= 0:
            raise ValueError(f"max_bytes must be positive if set, got {self.max_bytes}")


def parse_args(args: Optional[List[str]] = None) -> TortureConfig:
    """
    Parses CLI flags according to hackathon specification:
    ./torture --iterations 500 --seed 12345
    ./torture --iterations 100 --seed 999 --mode large-values
    ./torture --iterations 100 --seed 7 --mode compaction
    ./torture --iterations 100 --seed 42 --mode recovery-crash
    """
    parser = argparse.ArgumentParser(
        prog="torture",
        description="Crash-consistency torture testing harness for Supernova key-value engine.",
    )

    parser.add_argument(
        "--iterations",
        type=int,
        default=500,
        help="Number of kill-and-recover iterations to perform (default: 500)",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=None,
        help="Deterministic pseudorandom seed for reproducing test runs (mandatory for evaluation)",
    )
    parser.add_argument(
        "--mode",
        type=str,
        default=TortureMode.DEFAULT.value,
        choices=[m.value for m in TortureMode],
        help="Torture test mode (default, large-values, large-batches, compaction, recovery-crash)",
    )
    parser.add_argument(
        "--engine-bin",
        type=str,
        default="./kvdb",
        help="Path to compiled engine executable (default: ./kvdb)",
    )
    parser.add_argument(
        "--db-path",
        type=str,
        default="/tmp/torture_target.db",
        help="Path to database file under test (default: /tmp/torture_target.db)",
    )
    parser.add_argument(
        "--oracle-path",
        type=str,
        default="/tmp/torture_oracle.log",
        help="Path to separate external oracle log file (default: /tmp/torture_oracle.log)",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=2,
        help="Number of concurrent writer threads/processes (default: 2)",
    )
    parser.add_argument(
        "--max-bytes",
        type=int,
        default=None,
        help="Optional max database file size in bytes (simulating disk-full)",
    )
    parser.add_argument(
        "--min-kill-delay-ms",
        type=int,
        default=1,
        help="Minimum random delay in ms before killing engine (default: 1)",
    )
    parser.add_argument(
        "--max-kill-delay-ms",
        type=int,
        default=50,
        help="Maximum random delay in ms before killing engine (default: 50)",
    )

    parsed = parser.parse_args(args)

    # If seed is not supplied, generate a deterministic integer and report it
    chosen_seed = parsed.seed
    if chosen_seed is None:
        chosen_seed = random.SystemRandom().randint(1, 10_000_000)

    config = TortureConfig(
        iterations=parsed.iterations,
        seed=chosen_seed,
        mode=TortureMode(parsed.mode),
        engine_bin=parsed.engine_bin,
        db_path=parsed.db_path,
        oracle_path=parsed.oracle_path,
        workers=parsed.workers,
        max_bytes=parsed.max_bytes,
        min_kill_delay_ms=parsed.min_kill_delay_ms,
        max_kill_delay_ms=parsed.max_kill_delay_ms,
    )
    config.validate()
    return config
