"""
Reporting and artifact preservation module for the Supernova Torture Tool.

Prints concise one-line summaries per iteration and preserves diagnostic artifacts
(corrupted database file, Oracle history, recovered dump, diff) upon failure.
"""

from dataclasses import dataclass
import json
import os
import shutil
import sys
import time
from typing import Dict, List, Optional

from torture.oracle import DatabaseOracle


@dataclass
class IterationReport:
    iteration: int
    total_iterations: int
    passed: bool
    delay_ms: int
    submitted: int
    acked: int
    recovered_keys: int
    duration_s: float
    violations: List[str]


class Reporter:
    """Manages console reporting and failure artifact archiving."""

    def __init__(self, failure_dir: str = "/tmp/torture_failures") -> None:
        self.failure_dir = failure_dir

    def print_iteration_summary(self, rep: IterationReport) -> None:
        """Prints a single clean progress line per iteration."""
        status_tag = "\033[92m[PASS]\033[0m" if rep.passed else "\033[91m[FAIL]\033[0m"
        line = (
            f"[{rep.iteration:03d}/{rep.total_iterations:03d}] {status_tag} "
            f"delay={rep.delay_ms:02d}ms "
            f"ops={rep.submitted:03d} "
            f"ack={rep.acked:03d} "
            f"keys={rep.recovered_keys:03d} "
            f"({rep.duration_s:.3f}s)"
        )
        sys.stdout.write(line + "\n")
        sys.stdout.flush()

    def preserve_failure_artifacts(
        self,
        iteration: int,
        seed: int,
        db_path: str,
        oracle: DatabaseOracle,
        actual_dump: Dict[str, str],
        errors: List[str],
    ) -> str:
        """
        Preserves complete diagnostic state into a dedicated directory when a crash test fails.
        Returns the artifact directory path.
        """
        timestamp = int(time.time())
        dest_dir = os.path.join(self.failure_dir, f"fail_iter_{iteration}_seed_{seed}_{timestamp}")
        os.makedirs(dest_dir, exist_ok=True)

        # 1. Copy the database file if it exists
        if os.path.exists(db_path):
            shutil.copy2(db_path, os.path.join(dest_dir, "corrupted_target.db"))

        # 2. Write the Oracle history
        history_file = os.path.join(dest_dir, "oracle_history.json")
        history_records = [
            {
                "op_id": op.op_id,
                "op_type": op.op_type.value,
                "seq": op.seq,
                "writer_id": op.writer_id,
                "key": op.key,
                "value": op.value,
                "acked": op.acked,
                "errored": op.errored,
                "error_message": op.error_message,
                "batch_items": [
                    {"op_type": b.op_type.value, "key": b.key, "value": b.value}
                    for b in op.batch_items
                ],
            }
            for op in oracle.get_history()
        ]
        with open(history_file, "w", encoding="utf-8") as f:
            json.dump(history_records, f, indent=2)

        # 3. Write actual recovered dump
        dump_file = os.path.join(dest_dir, "recovered_dump.json")
        with open(dump_file, "w", encoding="utf-8") as f:
            json.dump(actual_dump, f, indent=2)

        # 4. Write full error report
        report_file = os.path.join(dest_dir, "diagnostic_report.txt")
        with open(report_file, "w", encoding="utf-8") as f:
            f.write(f"=== FAILURE IN ITERATION {iteration} (SEED: {seed}) ===\n\n")
            for err in errors:
                f.write(f"{err}\n\n")

        sys.stderr.write(f"\n[CRITICAL FAILURE] Artifacts preserved in: {dest_dir}\n")
        sys.stderr.flush()
        return dest_dir
