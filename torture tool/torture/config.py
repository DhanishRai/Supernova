"""
Configuration module for the Supernova Crash-Consistency Torture Tool.

Manages test configuration, CLI arguments, deterministic seed initialization,
and failure mode policies.
"""

from dataclasses import dataclass
from enum import Enum
import argparse
import random
import sys
from typing import List, Optional


class TortureMode(str, Enum):
    """
    Supported test execution modes:
    - default: Standard random mix of put, del, and batch operations.
    - large-values: Puts with 1 MB payload values spanning multiple OS disk blocks.
    - large-batches: Atomic batches with up to 10,000 operations.
    - recovery-crash: Injects SIGKILL while open() recovery is executing.
    - compaction: Triggers engine compaction and injects kills during compaction.
    - disk-full: Simulates disk space exhaustion with --max-bytes.
    """
    DEFAULT = "default"
    LARGE_VALUES = "large-values"
    LARGE_BATCHES = "large-batches"
    RECOVERY_CRASH = "recovery-crash"
    COMPACTION = "compaction"
    DISK_FULL = "disk-full"


def positive_int(value: str) -> int:
    """Argparse type validator ensuring value is an integer > 0."""
    try:
        val = int(value)
    except ValueError:
        raise argparse.ArgumentTypeError(f"Invalid integer value: '{value}'")
    if val <= 0:
        raise argparse.ArgumentTypeError(f"iterations must be a positive integer > 0, got {val}")
    return val


def valid_seed(value: str) -> int:
    """Argparse type validator ensuring seed is a valid non-negative integer."""
    try:
        val = int(value)
    except ValueError:
        raise argparse.ArgumentTypeError(f"Invalid integer seed: '{value}'")
    if val < 0:
        raise argparse.ArgumentTypeError(f"seed must be a non-negative integer, got {val}")
    return val


@dataclass(frozen=True)
class TortureConfig:
    iterations: int
    seed: int
    mode: TortureMode
    seed_generated: bool = False
    engine_bin: str = "./kvdb"
    db_path: str = "/tmp/torture_target.db"
    oracle_path: str = "/tmp/torture_oracle.log"
    workers: int = 2
    max_bytes: Optional[int] = None
    min_kill_delay_ms: int = 1
    max_kill_delay_ms: int = 50

    def validate(self) -> None:
        """
        Validates configuration invariants.
        Raises ValueError if any parameter violates safety or specification limits.
        """
        if self.iterations <= 0:
            raise ValueError(f"iterations must be positive, got {self.iterations}")
        if self.seed < 0:
            raise ValueError(f"seed must be non-negative, got {self.seed}")
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


def create_argument_parser() -> argparse.ArgumentParser:
    """Creates the standard argparse instance for the torture tool."""
    parser = argparse.ArgumentParser(
        prog="torture",
        description="Crash-consistency torture testing harness for Supernova key-value engine.",
    )

    parser.add_argument(
        "--iterations",
        type=positive_int,
        default=500,
        help="Number of kill-and-recover iterations to perform (default: 500)",
    )
    parser.add_argument(
        "--seed",
        type=valid_seed,
        default=None,
        help="Deterministic pseudorandom seed for reproducing test runs",
    )
    parser.add_argument(
        "--mode",
        type=str,
        default=TortureMode.DEFAULT.value,
        choices=[m.value for m in TortureMode],
        help="Torture test mode (default, large-values, large-batches, recovery-crash, compaction, disk-full)",
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

    return parser


def parse_args(args: Optional[List[str]] = None, print_generated_seed: bool = True) -> TortureConfig:
    """
    Parses CLI flags according to the hackathon specification.
    If seed is omitted, generates a random seed and prints it clearly.
    """
    parser = create_argument_parser()
    parsed = parser.parse_args(args)

    # If seed is not supplied, generate a deterministic integer and report it
    seed_generated = False
    chosen_seed = parsed.seed
    if chosen_seed is None:
        chosen_seed = random.SystemRandom().randint(1, 10_000_000)
        seed_generated = True
        if print_generated_seed:
            sys.stdout.write(f"[torture] No seed provided. Generated deterministic seed: {chosen_seed}\n")
            sys.stdout.flush()

    config = TortureConfig(
        iterations=parsed.iterations,
        seed=chosen_seed,
        mode=TortureMode(parsed.mode),
        seed_generated=seed_generated,
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
