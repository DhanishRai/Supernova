"""
Verification module for recovered database state.

Executes post-crash engine commands:
    ./kvdb open <path>
    ./kvdb check <path>
    ./kvdb dump <path>

Parses the database dump, checks for malformed records, and compares recovered state
against the Oracle's mathematical expectations, reporting exact value hashes on mismatch.
"""

from dataclasses import dataclass, field
import hashlib
import os
import sys
import subprocess
from typing import Dict, List, Optional, Tuple

from torture.oracle import DatabaseOracle, VerificationResult


def hash_val(val: Optional[str]) -> str:
    """Returns a short SHA-256 hex digest for reporting."""
    if val is None:
        return "<none>"
    return hashlib.sha256(val.encode("utf-8")).hexdigest()[:16]


@dataclass
class DumpParseResult:
    """Result of parsing `./kvdb dump` output."""
    records: Dict[str, str] = field(default_factory=dict)
    malformed_lines: List[str] = field(default_factory=list)
    raw_output: str = ""


class DumpParser:
    """
    Parses `./kvdb dump <path>` output:
    Expected line format: `<key> <value>` or `<key>=<value>`
    Detects unparseable or corrupted lines.
    """

    @staticmethod
    def parse(dump_text: str) -> DumpParseResult:
        records: Dict[str, str] = {}
        malformed: List[str] = []

        lines = dump_text.splitlines()
        for idx, line in enumerate(lines):
            stripped = line.strip()
            if not stripped:
                continue

            # Support "key value" (standard) or "key=value" (alternative)
            space_parts = stripped.split(maxsplit=1)
            if len(space_parts) == 2:
                k, v = space_parts[0].strip(), space_parts[1].strip()
            elif len(space_parts) == 1 and "=" in space_parts[0]:
                eq_parts = space_parts[0].split("=", 1)
                k, v = eq_parts[0].strip(), eq_parts[1].strip()
            elif len(space_parts) == 1:
                k, v = space_parts[0].strip(), ""
            else:
                malformed.append(f"Line {idx+1}: {line}")
                continue

            if not k:
                malformed.append(f"Line {idx+1}: missing key in '{line}'")
                continue

            records[k] = v

        return DumpParseResult(
            records=records,
            malformed_lines=malformed,
            raw_output=dump_text,
        )


class StateVerifier:
    """
    Coordinates recovery verification:
    1. Runs open
    2. Runs check
    3. Runs dump
    4. Evaluates dump against Oracle
    """

    def __init__(self, engine_bin: str, db_path: str, timeout: float = 5.0) -> None:
        self.engine_bin = engine_bin
        self.db_path = db_path
        self.timeout = timeout

    def _run_cmd(self, subcmd: str, extra_args: Optional[List[str]] = None) -> Tuple[int, str, str]:
        """Runs `./kvdb <subcmd> <db_path> [extra_args]` synchronously."""
        real_bin = self.engine_bin
        if os.path.isfile(real_bin) and not real_bin.endswith(".exe"):
            try:
                with open(real_bin, "r", encoding="utf-8", errors="ignore") as f:
                    first_line = f.readline().strip()
                if first_line.endswith(".py"):
                    real_bin = os.path.normpath(os.path.join(os.path.dirname(real_bin), first_line))
            except Exception:
                pass

        if real_bin.endswith(".py"):
            cmd = [sys.executable, real_bin, subcmd, self.db_path]
        elif sys.platform == "win32" and not real_bin.endswith(".exe") and os.path.isfile(real_bin + ".exe"):
            cmd = [real_bin + ".exe", subcmd, self.db_path]
        else:
            cmd = [real_bin, subcmd, self.db_path]
        if extra_args:
            cmd.extend(extra_args)
        proc = subprocess.run(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=self.timeout,
        )
        return proc.returncode, proc.stdout, proc.stderr

    def verify(self, oracle: DatabaseOracle) -> Tuple[bool, List[str], Dict[str, str]]:
        """
        Executes open, check, and dump, and verifies recovered state against the Oracle.
        Returns:
            Tuple of (passed: bool, errors: List[str], recovered_dump: Dict[str, str])
        """
        errors: List[str] = []

        # 1. Run open (recovery)
        try:
            ret_open, out_open, err_open = self._run_cmd("open")
            if ret_open != 0:
                errors.append(f"Command 'open' failed with exit code {ret_open}: {err_open.strip()}")
                return False, errors, {}
        except Exception as exc:
            errors.append(f"Failed to execute 'open': {exc}")
            return False, errors, {}

        # 2. Run check (integrity verification)
        try:
            ret_check, out_check, err_check = self._run_cmd("check")
            if ret_check != 0 or "OK" not in out_check.upper():
                errors.append(
                    f"Command 'check' reported corruption (exit {ret_check}): "
                    f"{out_check.strip()} {err_check.strip()}"
                )
        except Exception as exc:
            errors.append(f"Failed to execute 'check': {exc}")

        # 3. Run dump (extract recovered state)
        dump_records: Dict[str, str] = {}
        try:
            ret_dump, out_dump, err_dump = self._run_cmd("dump")
            if ret_dump != 0:
                errors.append(f"Command 'dump' failed with exit code {ret_dump}: {err_dump.strip()}")
                return False, errors, {}

            parsed = DumpParser.parse(out_dump)
            if parsed.malformed_lines:
                errors.append(f"Dump contained malformed records: {parsed.malformed_lines}")

            dump_records = parsed.records
        except Exception as exc:
            errors.append(f"Failed to execute 'dump': {exc}")
            return False, errors, {}

        # 4. Compare dump against Oracle
        oracle_res: VerificationResult = oracle.verify_recovered_dump(dump_records)
        if not oracle_res.passed:
            definite_state, definite_prov = oracle.compute_definite_state()
            for v in oracle_res.violations:
                errors.append(v)

            # Generate detailed diagnostic diff with value hashes and lengths
            diagnostic_lines = ["--- DETAILED RECOVERY MISMATCH REPORT ---"]
            mismatch_count = 0
            for key, expected_val in definite_state.items():
                actual_val = dump_records.get(key)
                if actual_val != expected_val:
                    mismatch_count += 1
                    if mismatch_count <= 50:
                        prov = definite_prov.get(key)
                        op_id = prov.op_id if prov else "unknown"
                        exp_len = len(expected_val) if expected_val else 0
                        act_len = len(actual_val) if actual_val else 0
                        exp_hash = hash_val(expected_val)
                        act_hash = hash_val(actual_val)

                        diagnostic_lines.append(
                            f"Key: '{key}'\n"
                            f"  Expected: length={exp_len}, sha256={exp_hash}, op_id='{op_id}'\n"
                            f"  Actual:   length={act_len}, sha256={act_hash}"
                        )
            if mismatch_count > 50:
                diagnostic_lines.append(f"... and {mismatch_count - 50} additional mismatched keys omitted for brevity.")
            errors.append("\n".join(diagnostic_lines))

        # 5. Direct Point-Lookup verification (GET):
        # Verify that ReadAt on disk returns exact value for recovered keys
        sample_keys = list(dump_records.keys())[:25]
        for k in sample_keys:
            try:
                ret_get, out_get, err_get = self._run_cmd("get", extra_args=[k])
                if ret_get != 0:
                    if "Unknown subcommand" in err_get or "unknown command" in err_get:
                        break
                    errors.append(f"Direct 'get {k}' failed with code {ret_get}: {err_get.strip()}")
                elif out_get.strip() != dump_records[k]:
                    errors.append(
                        f"Direct 'get {k}' value mismatch: dump had '{dump_records[k]}', "
                        f"but 'get' returned '{out_get.strip()}'"
                    )
            except Exception as exc:
                errors.append(f"Failed to execute 'get {k}': {exc}")

        passed = (len(errors) == 0)
        return passed, errors, dump_records

