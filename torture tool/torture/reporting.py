"""
Reporting and artifact preservation module for the Supernova Torture Tool.

CP8 Implementation:
- Professional failure reporting and reproducibility.
- Canonical failure classification:
    - Engine failure
    - Protocol failure
    - Oracle failure
    - Torture tool failure
    - Timeout
    - Recovery failure
- Predictable failure artifacts stored inside torture tool/artifacts/
- Complete diagnostic report capturing:
    - Seed (master and derived)
    - Iteration and mode
    - Target database path
    - Operation IDs and operation types
    - Expected vs actual state
    - Expected and actual value lengths and SHA-256 hashes
    - Relevant operation history
    - Child process exit information (PID, exit code, signal)
    - Available engine stderr logs
"""

from dataclasses import dataclass, field
from enum import Enum
import json
import os
import shutil
import signal
import sys
import time
from typing import Any, Dict, List, Optional, Set, Tuple, Union

from torture.oracle import DatabaseOracle, hash_val, OpType, format_val_summary


# Predictable failure artifacts directory inside torture tool/
DEFAULT_ARTIFACTS_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "artifacts"
)

REPRODUCIBILITY_NOTICE = (
    "A seed reproduces workload generation and requested random delays, "
    "but cannot guarantee identical operating-system scheduling, disk timing, "
    "or exact crash instructions."
)


class FailureCategory(str, Enum):
    """
    Standard failure taxonomy for Supernova storage engine torture testing.
    """
    ENGINE_FAILURE = "Engine failure"
    PROTOCOL_FAILURE = "Protocol failure"
    ORACLE_FAILURE = "Oracle failure"
    TORTURE_TOOL_FAILURE = "Torture tool failure"
    TIMEOUT = "Timeout"
    RECOVERY_FAILURE = "Recovery failure"


@dataclass
class ChildProcessExitInfo:
    """Diagnostic details of the engine child process termination."""
    pid: Optional[int] = None
    returncode: Optional[int] = None
    is_alive: bool = False
    killed_by_sigkill: bool = False
    termination_cause: str = "Unknown"

    @classmethod
    def from_client(cls, client: Any, kill_ret: Optional[int] = None) -> "ChildProcessExitInfo":
        pid = getattr(client, "pid", None)
        proc = getattr(client, "_proc", None)
        returncode = proc.returncode if proc else None
        is_alive = getattr(client, "is_alive", False)

        effective_code = kill_ret if kill_ret is not None else returncode
        valid_kill_codes = (-signal.SIGKILL, 137, 9, 15) if sys.platform == "win32" else (-signal.SIGKILL, 137)
        killed_by_sigkill = (effective_code in valid_kill_codes)

        cause = "Still running" if is_alive else f"Exit code {effective_code}"
        if killed_by_sigkill:
            cause = "SIGKILL (signal 9)"
        elif effective_code is not None and effective_code < 0:
            cause = f"Terminated by signal {-effective_code}"

        return cls(
            pid=pid,
            returncode=returncode,
            is_alive=is_alive,
            killed_by_sigkill=killed_by_sigkill,
            termination_cause=cause,
        )


@dataclass
class RecoveryCrashInfo:
    """Detailed telemetry for recovery-crash executions (CP9)."""
    recovery_start_time: float
    requested_kill_delay_ms: float
    actual_exit_status: Optional[int]
    sigkill_occurred: bool
    recovery_completion_time: Optional[float]
    missed_crash_window: bool
    second_recovery_success: bool
    second_recovery_time_s: Optional[float] = None


@dataclass
class CompactionCrashInfo:
    """Detailed telemetry for compaction-crash executions (CP10)."""
    compact_op_id: str
    compact_acked: bool
    requested_kill_delay_ms: float
    actual_exit_status: Optional[int]
    temp_files_found: List[str] = field(default_factory=list)


@dataclass
class DiskFullCrashInfo:
    """Detailed telemetry for disk-full crash executions (CP11)."""
    max_bytes: int
    successful_writes: int
    failed_puts: int
    failed_batches: int
    clean_err_count: int
    data_readable_before_kill: bool
    recovery_succeeded: bool
    recovered_keys: int


@dataclass
class FailureDetails:
    """Comprehensive failure context required for professional reporting."""
    category: FailureCategory
    iteration: int
    total_iterations: int
    seed: int
    derived_seed: int
    mode: str
    db_path: str
    errors: List[str]
    child_exit: Optional[ChildProcessExitInfo] = None
    stderr_lines: List[str] = field(default_factory=list)
    actual_dump: Dict[str, str] = field(default_factory=dict)
    operation_ids: List[str] = field(default_factory=list)
    operation_types: List[str] = field(default_factory=list)
    recovery_info: Optional[RecoveryCrashInfo] = None
    compaction_info: Optional[CompactionCrashInfo] = None
    disk_full_info: Optional[DiskFullCrashInfo] = None


def classify_failure(
    errors: List[str],
    child_exit: Optional[ChildProcessExitInfo] = None,
    client_malformed: Optional[List[str]] = None,
    client_unknown: Optional[List[str]] = None,
    worker_exceptions: Optional[List[Exception]] = None,
    tool_crashed: bool = False,
) -> FailureCategory:
    """
    Classifies a failure into one of the 6 canonical categories:
    - Engine failure
    - Protocol failure
    - Oracle failure
    - Torture tool failure
    - Timeout
    - Recovery failure
    """
    if tool_crashed:
        return FailureCategory.TORTURE_TOOL_FAILURE

    # 1. Timeout detection
    if worker_exceptions:
        for exc in worker_exceptions:
            if "Timeout" in type(exc).__name__ or "timed out" in str(exc).lower():
                return FailureCategory.TIMEOUT
    for err in errors:
        if "timed out" in err.lower() or "timeout" in err.lower():
            return FailureCategory.TIMEOUT

    # 2. Protocol failure detection
    if (client_malformed and len(client_malformed) > 0) or (client_unknown and len(client_unknown) > 0):
        return FailureCategory.PROTOCOL_FAILURE
    for err in errors:
        if "malformed" in err.lower() or "unknown op_id" in err.lower() or "protocol" in err.lower():
            return FailureCategory.PROTOCOL_FAILURE

    # 3. Engine failure (unexpected exit code, panic, or dying without SIGKILL)
    if child_exit and not child_exit.killed_by_sigkill:
        return FailureCategory.ENGINE_FAILURE

    for err in errors:
        err_l = err.lower()
        if "engine crash" in err_l or "engine failure" in err_l or "engine process" in err_l or "engine died" in err_l or "did not exit with sigkill" in err_l:
            return FailureCategory.ENGINE_FAILURE

    # 4. Recovery failure detection (open, check, dump)
    for err in errors:
        err_lower = err.lower()
        if (
            "command 'open' failed" in err_lower
            or "command 'check' reported corruption" in err_lower
            or "command 'dump' failed" in err_lower
            or "dump contained malformed" in err_lower
            or "failed to execute 'open'" in err_lower
            or "failed to execute 'check'" in err_lower
            or "failed to execute 'dump'" in err_lower
        ):
            return FailureCategory.RECOVERY_FAILURE

    # 5. Oracle failure detection (durability, atomicity, integrity, errored ops)
    for err in errors:
        if any(v in err for v in [
            "DURABILITY VIOLATION",
            "ATOMICITY VIOLATION",
            "INTEGRITY VIOLATION",
            "DELETION VIOLATION",
            "ERRORED OP VIOLATION",
        ]):
            return FailureCategory.ORACLE_FAILURE

    return FailureCategory.ORACLE_FAILURE if errors else FailureCategory.ENGINE_FAILURE


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
    mode: str = "default"
    missed_crash_window: bool = False
    recovery_info: Optional[RecoveryCrashInfo] = None
    compaction_info: Optional[CompactionCrashInfo] = None
    disk_full_info: Optional[DiskFullCrashInfo] = None


class Reporter:
    """Manages console reporting and failure artifact archiving."""

    def __init__(self, failure_dir: Optional[str] = None) -> None:
        self.failure_dir = failure_dir or DEFAULT_ARTIFACTS_DIR

    def print_iteration_summary(self, rep: IterationReport) -> None:
        """Prints a single clean progress line per iteration."""
        if isinstance(rep.delay_ms, float):
            delay_str = f"delay={rep.delay_ms:05.2f}ms"
        else:
            delay_str = f"delay={rep.delay_ms:02d}ms"

        if rep.missed_crash_window:
            status_tag = "\033[93m[MISSED CRASH WINDOW]\033[0m"
            exit_code = rep.recovery_info.actual_exit_status if rep.recovery_info else "unknown"
            line = (
                f"[{rep.iteration:03d}/{rep.total_iterations:03d}] {status_tag} "
                f"{delay_str} "
                f"(open finished before SIGKILL; not counted) "
                f"exit_status={exit_code} "
                f"({rep.duration_s:.3f}s)"
            )
        else:
            status_tag = "\033[92m[PASS]\033[0m" if rep.passed else "\033[91m[FAIL]\033[0m"
            if rep.mode == "recovery-crash":
                line = (
                    f"[{rep.iteration:03d}/{rep.total_iterations:03d}] {status_tag} "
                    f"{delay_str} "
                    f"(recovery interrupted by SIGKILL; 2nd open OK) "
                    f"keys={rep.recovered_keys:03d} "
                    f"({rep.duration_s:.3f}s)"
                )
            elif rep.mode == "compaction":
                compact_str = "ACKED" if (rep.compaction_info and rep.compaction_info.compact_acked) else "INTERRUPTED"
                line = (
                    f"[{rep.iteration:03d}/{rep.total_iterations:03d}] {status_tag} "
                    f"delay={rep.delay_ms:02d}ms "
                    f"compact={compact_str} "
                    f"keys={rep.recovered_keys:03d} "
                    f"({rep.duration_s:.3f}s)"
                )
            elif rep.mode == "disk-full":
                errs_str = f"errs={rep.disk_full_info.clean_err_count}" if rep.disk_full_info else "errs=0"
                limit_str = f"limit={rep.disk_full_info.max_bytes}B" if rep.disk_full_info else ""
                line = (
                    f"[{rep.iteration:03d}/{rep.total_iterations:03d}] {status_tag} "
                    f"delay={rep.delay_ms:02d}ms "
                    f"writes={rep.acked:03d} "
                    f"{errs_str} "
                    f"{limit_str} "
                    f"keys={rep.recovered_keys:03d} "
                    f"({rep.duration_s:.3f}s)"
                )
            else:
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
        details_or_iter: Union[FailureDetails, int],
        seed: Optional[int] = None,
        db_path: Optional[str] = None,
        oracle: Optional[DatabaseOracle] = None,
        actual_dump: Optional[Dict[str, str]] = None,
        errors: Optional[List[str]] = None,
        mode: str = "default",
        child_exit: Optional[ChildProcessExitInfo] = None,
        stderr_lines: Optional[List[str]] = None,
        category: Optional[FailureCategory] = None,
        derived_seed: Optional[int] = None,
    ) -> str:
        """
        Preserves complete diagnostic state into a predictable directory inside torture tool/artifacts.
        Supports both modern FailureDetails and backward-compatible positional signatures.
        """
        # Normalize into FailureDetails
        if isinstance(details_or_iter, FailureDetails):
            details = details_or_iter
        else:
            iter_num = details_or_iter
            actual_errors = errors or []
            dump_data = actual_dump or {}
            cat = category or classify_failure(actual_errors, child_exit=child_exit)
            details = FailureDetails(
                category=cat,
                iteration=iter_num,
                total_iterations=iter_num,
                seed=seed or 0,
                derived_seed=derived_seed or seed or 0,
                mode=mode,
                db_path=db_path or "",
                errors=actual_errors,
                child_exit=child_exit,
                stderr_lines=stderr_lines or [],
                actual_dump=dump_data,
            )

        timestamp = int(time.time())
        dest_dir = os.path.join(
            self.failure_dir,
            f"fail_iter_{details.iteration}_seed_{details.seed}_{timestamp}",
        )
        os.makedirs(dest_dir, exist_ok=True)

        # 1. Copy database file and WAL/data sidecars
        if details.db_path and os.path.exists(details.db_path):
            shutil.copy2(details.db_path, os.path.join(dest_dir, "corrupted_target.db"))
            # Sidecars
            parent_dir = os.path.dirname(os.path.abspath(details.db_path))
            base_name = os.path.basename(details.db_path)
            for fname in os.listdir(parent_dir):
                if fname.startswith(base_name + ".") and fname != base_name:
                    src_sidecar = os.path.join(parent_dir, fname)
                    if os.path.isfile(src_sidecar):
                        shutil.copy2(src_sidecar, os.path.join(dest_dir, fname))

        def summarize_val(v: Optional[str]) -> Any:
            if v is None:
                return None
            if len(v) <= 256:
                return v
            return {
                "length": len(v),
                "sha256": hash_val(v),
                "preview": v[:32],
            }

        # 2. Extract Oracle state, provenance, and history
        history_records = []
        definite_state: Dict[str, str] = {}
        definite_prov: Dict[str, Any] = {}
        if oracle is not None:
            definite_state, definite_prov = oracle.compute_definite_state()
            history = oracle.get_history()
            if not details.operation_ids:
                details.operation_ids = [op.op_id for op in history]
            if not details.operation_types:
                details.operation_types = sorted(list(set(op.op_type.value for op in history)))

            history_records = [
                {
                    "op_id": op.op_id,
                    "op_type": op.op_type.value,
                    "seq": op.seq,
                    "writer_id": op.writer_id,
                    "key": op.key,
                    "value": summarize_val(op.value),
                    "acked": op.acked,
                    "errored": op.errored,
                    "error_message": op.error_message,
                    "batch_items": [
                        {
                            "op_type": b.op_type.value,
                            "key": b.key,
                            "value": summarize_val(b.value),
                        }
                        for b in op.batch_items
                    ],
                }
                for op in history
            ]

        history_file = os.path.join(dest_dir, "oracle_history.json")
        with open(history_file, "w", encoding="utf-8") as f:
            json.dump(history_records, f, indent=2)

        # 3. Write recovered dump summary
        dump_file = os.path.join(dest_dir, "recovered_dump.json")
        dump_summary = {k: summarize_val(v) for k, v in details.actual_dump.items()}
        with open(dump_file, "w", encoding="utf-8") as f:
            json.dump(dump_summary, f, indent=2)

        # 4. Compile key-by-key comparison with lengths and SHA-256 hashes
        all_keys = sorted(list(set(definite_state.keys()) | set(details.actual_dump.keys())))
        mismatched_keys_data = {}
        relevant_history_lines = []

        for k in all_keys:
            exp_val = definite_state.get(k)
            act_val = details.actual_dump.get(k)
            if exp_val != act_val:
                prov = definite_prov.get(k)
                last_op_id = prov.op_id if prov else "unknown"
                last_op_type = "UNKNOWN"
                if oracle:
                    rec = oracle.get_record(last_op_id)
                    if rec:
                        last_op_type = rec.op_type.value

                mismatched_keys_data[k] = {
                    "key": k,
                    "expected_length": len(exp_val) if exp_val is not None else 0,
                    "actual_length": len(act_val) if act_val is not None else 0,
                    "expected_sha256": hash_val(exp_val),
                    "actual_sha256": hash_val(act_val),
                    "expected_preview": exp_val[:32] if exp_val else "<none>",
                    "actual_preview": act_val[:32] if act_val else "<none>",
                    "responsible_op_id": last_op_id,
                    "responsible_op_type": last_op_type,
                }

                # Find relevant historical operations for this key
                if oracle:
                    for op in oracle.get_history():
                        touches_key = (
                            op.key == k
                            or any(b.key == k for b in op.batch_items)
                        )
                        if touches_key:
                            relevant_history_lines.append(
                                f"  [seq={op.seq:03d}] {op.op_type.value:<5} "
                                f"id='{op.op_id}' writer='{op.writer_id}' "
                                f"acked={op.acked} key='{k}'"
                            )

        # 5. Write structured machine-readable JSON report
        machine_report = {
            "failure_category": details.category.value,
            "iteration": details.iteration,
            "total_iterations": details.total_iterations,
            "seed": details.seed,
            "derived_seed": details.derived_seed,
            "mode": details.mode,
            "database_path": details.db_path,
            "operation_ids": details.operation_ids,
            "operation_types": details.operation_types,
            "expected_state": {k: summarize_val(v) for k, v in definite_state.items()},
            "actual_state": dump_summary,
            "expected_value_lengths": {k: len(v) for k, v in definite_state.items() if v is not None},
            "actual_value_lengths": {k: len(v) for k, v in details.actual_dump.items() if v is not None},
            "expected_value_hashes": {k: hash_val(v) for k, v in definite_state.items() if v is not None},
            "actual_value_hashes": {k: hash_val(v) for k, v in details.actual_dump.items() if v is not None},
            "relevant_operation_history": relevant_history_lines,
            "child_process_exit_info": {
                "pid": details.child_exit.pid if details.child_exit else None,
                "returncode": details.child_exit.returncode if details.child_exit else None,
                "is_alive": details.child_exit.is_alive if details.child_exit else False,
                "killed_by_sigkill": details.child_exit.killed_by_sigkill if details.child_exit else False,
                "termination_cause": details.child_exit.termination_cause if details.child_exit else "Unknown",
            },
            "available_stderr": list(details.stderr_lines) if details.stderr_lines is not None else [],
            "mismatched_keys": mismatched_keys_data,
            "errors": details.errors,
            "recovery_crash_info": {
                "recovery_start_time": details.recovery_info.recovery_start_time,
                "requested_kill_delay_ms": details.recovery_info.requested_kill_delay_ms,
                "actual_process_exit_status": details.recovery_info.actual_exit_status,
                "sigkill_occurred": details.recovery_info.sigkill_occurred,
                "recovery_completion_time": details.recovery_info.recovery_completion_time,
                "missed_crash_window": details.recovery_info.missed_crash_window,
                "second_recovery_success": details.recovery_info.second_recovery_success,
                "second_recovery_time_s": details.recovery_info.second_recovery_time_s,
            } if details.recovery_info else None,
            "compaction_crash_info": {
                "compact_op_id": details.compaction_info.compact_op_id,
                "compact_acked": details.compaction_info.compact_acked,
                "requested_kill_delay_ms": details.compaction_info.requested_kill_delay_ms,
                "actual_process_exit_status": details.compaction_info.actual_exit_status,
                "temp_files_found": details.compaction_info.temp_files_found,
            } if details.compaction_info else None,
            "disk_full_crash_info": {
                "max_bytes": details.disk_full_info.max_bytes,
                "successful_writes": details.disk_full_info.successful_writes,
                "failed_puts": details.disk_full_info.failed_puts,
                "failed_batches": details.disk_full_info.failed_batches,
                "clean_err_count": details.disk_full_info.clean_err_count,
                "data_readable_before_kill": details.disk_full_info.data_readable_before_kill,
                "recovery_succeeded": details.disk_full_info.recovery_succeeded,
                "recovered_keys": details.disk_full_info.recovered_keys,
            } if details.disk_full_info else None,
            "reproducibility_notice": REPRODUCIBILITY_NOTICE,
        }
        report_json_file = os.path.join(dest_dir, "failure_report.json")
        with open(report_json_file, "w", encoding="utf-8") as f:
            json.dump(machine_report, f, indent=2)

        # 6. Write human-readable diagnostic_report.txt
        report_file = os.path.join(dest_dir, "diagnostic_report.txt")
        with open(report_file, "w", encoding="utf-8") as f:
            f.write("=" * 80 + "\n")
            f.write("SUPERNOVA TORTURE TOOL - FAILURE DIAGNOSTIC REPORT\n")
            f.write("=" * 80 + "\n")
            f.write(f"Classification     : {details.category.value}\n")
            f.write(f"Iteration          : {details.iteration} / {details.total_iterations}\n")
            f.write(f"Master Seed        : {details.seed}\n")
            f.write(f"Derived Seed       : {details.derived_seed}\n")
            f.write(f"Execution Mode     : {details.mode}\n")
            f.write(f"Database Path      : {details.db_path}\n")
            f.write(f"Total Op IDs       : {len(details.operation_ids)}\n")
            f.write(f"Operation Types    : {', '.join(details.operation_types) if details.operation_types else 'None'}\n\n")

            if details.recovery_info:
                f.write("--- RECOVERY-CRASH INTERRUPT TELEMETRY ---\n")
                f.write(f"Recovery Start Time        : {details.recovery_info.recovery_start_time:.6f}\n")
                f.write(f"Requested Kill Delay       : {details.recovery_info.requested_kill_delay_ms:.2f} ms\n")
                f.write(f"Actual Process Exit Status : {details.recovery_info.actual_exit_status}\n")
                f.write(f"Whether SIGKILL Occurred   : {details.recovery_info.sigkill_occurred}\n")
                f.write(f"Recovery Completion Time   : {details.recovery_info.recovery_completion_time}\n")
                f.write(f"Missed Crash Window        : {details.recovery_info.missed_crash_window}\n")
                f.write(f"Second Recovery Succeeded  : {details.recovery_info.second_recovery_success}\n")
                if details.recovery_info.second_recovery_time_s is not None:
                    f.write(f"Second Recovery Duration   : {details.recovery_info.second_recovery_time_s:.4f} s\n")
                f.write("\n")

            if details.compaction_info:
                f.write("--- COMPACTION-CRASH INTERRUPT TELEMETRY ---\n")
                f.write(f"Compact Op ID              : {details.compaction_info.compact_op_id}\n")
                f.write(f"Compaction Acknowledged    : {details.compaction_info.compact_acked}\n")
                f.write(f"Requested Kill Delay       : {details.compaction_info.requested_kill_delay_ms:.2f} ms\n")
                f.write(f"Actual Process Exit Status : {details.compaction_info.actual_exit_status}\n")
                f.write(f"Temp Files Found on Disk   : {details.compaction_info.temp_files_found}\n\n")

            if details.disk_full_info:
                f.write("--- DISK-FULL LIMIT TELEMETRY ---\n")
                f.write(f"Configured Max Bytes       : {details.disk_full_info.max_bytes}\n")
                f.write(f"Successful Pre-Limit Writes: {details.disk_full_info.successful_writes}\n")
                f.write(f"Failed Over-Limit PUTs     : {details.disk_full_info.failed_puts}\n")
                f.write(f"Failed Over-Limit BATCHes  : {details.disk_full_info.failed_batches}\n")
                f.write(f"Clean ERR Responses        : {details.disk_full_info.clean_err_count}\n")
                f.write(f"Data Readable Before Kill  : {details.disk_full_info.data_readable_before_kill}\n")
                f.write(f"Post-Crash Recovery OK     : {details.disk_full_info.recovery_succeeded}\n")
                f.write(f"Recovered Keys Count       : {details.disk_full_info.recovered_keys}\n\n")

            f.write("--- CHILD PROCESS EXIT INFORMATION ---\n")
            if details.child_exit:
                f.write(f"PID                : {details.child_exit.pid}\n")
                f.write(f"Is Alive           : {details.child_exit.is_alive}\n")
                f.write(f"Exit Code          : {details.child_exit.returncode}\n")
                f.write(f"Termination Cause  : {details.child_exit.termination_cause}\n")
            else:
                f.write("(Child exit information unavailable)\n")
            f.write("\n")

            f.write("--- AVAILABLE STDERR OUTPUT ---\n")
            if details.stderr_lines:
                for line in list(details.stderr_lines)[-30:]:
                    f.write(f"  {line.rstrip()}\n")
            else:
                f.write("  (No stderr output captured)\n")
            f.write("\n")

            f.write("--- DETECTED VIOLATIONS & ERRORS ---\n")
            for err in details.errors:
                f.write(f"{err}\n\n")

            f.write("--- DETAILED STATE COMPARISON (EXPECTED vs ACTUAL) ---\n")
            if mismatched_keys_data:
                shown = 0
                for k, info in mismatched_keys_data.items():
                    shown += 1
                    if shown > 50:
                        f.write(f"... and {len(mismatched_keys_data) - 50} additional mismatched keys omitted for brevity.\n")
                        break
                    f.write(f"Key: '{k}'\n")
                    f.write(f"  Expected Value Length : {info['expected_length']} bytes\n")
                    f.write(f"  Actual Value Length   : {info['actual_length']} bytes\n")
                    f.write(f"  Expected SHA-256 Hash : {info['expected_sha256']}\n")
                    f.write(f"  Actual SHA-256 Hash   : {info['actual_sha256']}\n")
                    f.write(f"  Expected Preview      : {info['expected_preview']}\n")
                    f.write(f"  Actual Preview        : {info['actual_preview']}\n")
                    f.write(f"  Responsible Op ID     : {info['responsible_op_id']}\n")
                    f.write(f"  Responsible Op Type   : {info['responsible_op_type']}\n\n")
            else:
                f.write("  (No key mismatches detected in dump)\n\n")

            f.write("--- RELEVANT OPERATION HISTORY ---\n")
            if relevant_history_lines:
                for h_line in relevant_history_lines[-50:]:
                    f.write(h_line + "\n")
            else:
                f.write("  (No relevant operation history)\n")
            f.write("\n")

            f.write("=" * 80 + "\n")
            f.write("REPRODUCIBILITY NOTICE:\n")
            f.write(f"{REPRODUCIBILITY_NOTICE}\n")
            f.write("=" * 80 + "\n")

        sys.stderr.write(f"\n[CRITICAL FAILURE - {details.category.value}] Artifacts preserved in:\n  {dest_dir}\n")
        sys.stderr.flush()
        return dest_dir
