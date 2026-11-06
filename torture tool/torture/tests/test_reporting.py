"""
Unit tests for Failure Reporting, Classification, Artifact Preservation,
and Reproducibility (CP8).
"""

import hashlib
import json
import os
import random
import shutil
import tempfile
import unittest
from unittest.mock import MagicMock, patch

from torture.config import TortureConfig, TortureMode
from torture.oracle import DatabaseOracle, OpType, hash_val
from torture.protocol import EngineClient, ProtocolError
from torture.reporting import (
    DEFAULT_ARTIFACTS_DIR,
    REPRODUCIBILITY_NOTICE,
    ChildProcessExitInfo,
    FailureCategory,
    FailureDetails,
    Reporter,
    classify_failure,
)
from torture.runner import CrashTestRunner
from torture.worker import ConcurrentWriter, generate_large_value


class TestFailureReporting(unittest.TestCase):
    """Verifies CP8 failure reporting, structured JSON, artifact retention, and classification."""

    def setUp(self) -> None:
        self.temp_dir = tempfile.mkdtemp(prefix="torture_test_cp8_")
        self.artifacts_dir = os.path.join(self.temp_dir, "artifacts")
        self.db_path = os.path.join(self.temp_dir, "test.db")
        with open(self.db_path, "w") as f:
            f.write("dummy db content")
        with open(self.db_path + ".wal", "w") as f:
            f.write("dummy wal content")

    def tearDown(self) -> None:
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_failure_artifact_creation(self) -> None:
        """Verifies that all required diagnostic artifacts are preserved in the artifacts directory."""
        reporter = Reporter(failure_dir=self.artifacts_dir)
        oracle = DatabaseOracle()
        oracle.record_submission("op-1", OpType.PUT, key="alpha", value="val1", writer_id="w1")
        oracle.record_ack("op-1")

        child_exit = ChildProcessExitInfo(
            pid=1234,
            returncode=-9,
            is_alive=False,
            killed_by_sigkill=True,
            termination_cause="SIGKILL (signal 9)",
        )
        details = FailureDetails(
            category=FailureCategory.ORACLE_FAILURE,
            iteration=1,
            total_iterations=10,
            seed=42,
            derived_seed=999,
            mode=TortureMode.DEFAULT.value,
            db_path=self.db_path,
            errors=["DURABILITY VIOLATION: ACKed key 'alpha' missing from recovered dump."],
            child_exit=child_exit,
            stderr_lines=["stderr line 1", "stderr line 2"],
            actual_dump={},
            operation_ids=["op-1"],
            operation_types=["PUT"],
        )

        saved_path = reporter.preserve_failure_artifacts(details, oracle=oracle)
        self.assertTrue(os.path.isdir(saved_path))

        # Check preserved files
        expected_files = [
            "corrupted_target.db",
            "test.db.wal",
            "failure_report.json",
            "diagnostic_report.txt",
            "oracle_history.json",
            "recovered_dump.json",
        ]
        for ef in expected_files:
            full_p = os.path.join(saved_path, ef)
            self.assertTrue(os.path.exists(full_p), f"Missing artifact: {ef}")

    def test_failure_report_fields(self) -> None:
        """Verifies all mandatory metadata fields in failure_report.json."""
        reporter = Reporter(failure_dir=self.artifacts_dir)
        oracle = DatabaseOracle()
        oracle.record_submission("op-1", OpType.PUT, key="k1", value="v1", writer_id="w1")
        oracle.record_ack("op-1")

        child_exit = ChildProcessExitInfo(
            pid=5555,
            returncode=-9,
            is_alive=False,
            killed_by_sigkill=True,
            termination_cause="SIGKILL (signal 9)",
        )
        details = FailureDetails(
            category=FailureCategory.ORACLE_FAILURE,
            iteration=3,
            total_iterations=50,
            seed=12345,
            derived_seed=67890,
            mode=TortureMode.DEFAULT.value,
            db_path=self.db_path,
            errors=["INTEGRITY VIOLATION: key 'k1' value mismatch"],
            child_exit=child_exit,
            stderr_lines=["server listening on pipe", "checkpoint written"],
            actual_dump={"k1": "corrupted_val"},
            operation_ids=["op-1"],
            operation_types=["PUT"],
        )

        saved_path = reporter.preserve_failure_artifacts(details, oracle=oracle)
        report_json = os.path.join(saved_path, "failure_report.json")

        with open(report_json, "r", encoding="utf-8") as f:
            data = json.load(f)

        # Validate mandatory fields
        self.assertEqual(data["failure_category"], "Oracle failure")
        self.assertEqual(data["iteration"], 3)
        self.assertEqual(data["total_iterations"], 50)
        self.assertEqual(data["seed"], 12345)
        self.assertEqual(data["derived_seed"], 67890)
        self.assertEqual(data["mode"], "default")
        self.assertEqual(data["database_path"], self.db_path)
        self.assertIn("op-1", data["operation_ids"])
        self.assertIn("PUT", data["operation_types"])

        # State, lengths, hashes
        self.assertIn("k1", data["expected_state"])
        self.assertIn("k1", data["actual_state"])
        self.assertEqual(data["expected_value_lengths"]["k1"], len("v1"))
        self.assertEqual(data["actual_value_lengths"]["k1"], len("corrupted_val"))
        self.assertEqual(data["expected_value_hashes"]["k1"], hash_val("v1"))
        self.assertEqual(data["actual_value_hashes"]["k1"], hash_val("corrupted_val"))

        # Child exit info & stderr
        self.assertEqual(data["child_process_exit_info"]["pid"], 5555)
        self.assertEqual(data["child_process_exit_info"]["returncode"], -9)
        self.assertTrue(data["child_process_exit_info"]["killed_by_sigkill"])
        self.assertEqual(data["available_stderr"], ["server listening on pipe", "checkpoint written"])

        # History and Reproducibility notice
        self.assertIsInstance(data["relevant_operation_history"], list)
        self.assertIn("A seed reproduces workload generation", data["reproducibility_notice"])

    def test_large_value_reporting(self) -> None:
        """Verifies that large values (~1 MB) are summarized via length and SHA-256 without memory blowout."""
        reporter = Reporter(failure_dir=self.artifacts_dir)
        oracle = DatabaseOracle()

        # 1 MB value
        large_val_expected = generate_large_value("op-lg1", "w1", "large_key", size=1024 * 1024)
        large_val_corrupted = large_val_expected[: 1024 * 512]  # Truncated half

        oracle.record_submission("op-lg1", OpType.PUT, key="large_key", value=large_val_expected, writer_id="w1")
        oracle.record_ack("op-lg1")

        child_exit = ChildProcessExitInfo(
            pid=777,
            returncode=-9,
            is_alive=False,
            killed_by_sigkill=True,
            termination_cause="SIGKILL (signal 9)",
        )
        details = FailureDetails(
            category=FailureCategory.ORACLE_FAILURE,
            iteration=1,
            total_iterations=1,
            seed=42,
            derived_seed=42,
            mode=TortureMode.LARGE_VALUES.value,
            db_path=self.db_path,
            errors=["INTEGRITY VIOLATION: key 'large_key' value length mismatch"],
            child_exit=child_exit,
            stderr_lines=[],
            actual_dump={"large_key": large_val_corrupted},
            operation_ids=["op-lg1"],
            operation_types=["PUT"],
        )

        saved_path = reporter.preserve_failure_artifacts(details, oracle=oracle)

        # 1. Check failure_report.json
        report_json = os.path.join(saved_path, "failure_report.json")
        with open(report_json, "r", encoding="utf-8") as f:
            data = json.load(f)

        mismatched = data["mismatched_keys"]["large_key"]
        self.assertEqual(mismatched["expected_length"], 1024 * 1024)
        self.assertEqual(mismatched["actual_length"], 1024 * 512)
        self.assertEqual(mismatched["expected_sha256"], hash_val(large_val_expected))
        self.assertEqual(mismatched["actual_sha256"], hash_val(large_val_corrupted))

        # Size of json report must be modest (< 50 KB), NOT multiple megabytes
        report_size = os.path.getsize(report_json)
        self.assertLess(report_size, 50 * 1024, f"failure_report.json is too large: {report_size} bytes")

        # 2. Check diagnostic_report.txt
        diag_txt = os.path.join(saved_path, "diagnostic_report.txt")
        with open(diag_txt, "r", encoding="utf-8") as f:
            text = f.read()

        # Must report lengths and hashes, not full 1 MB payload
        self.assertIn("1048576 bytes", text)
        self.assertIn("524288 bytes", text)
        self.assertIn(hash_val(large_val_expected), text)
        self.assertNotIn(large_val_expected[100:10000], text)

    def test_hash_correctness(self) -> None:
        """Verifies that hash_val produces exact cryptographic SHA-256 digests."""
        test_strings = [
            "",
            "hello world",
            "Supernova Key-Value Engine CP8",
            "a" * 100000,
        ]
        for s in test_strings:
            expected_hash = hashlib.sha256(s.encode("utf-8")).hexdigest()
            self.assertEqual(hash_val(s), expected_hash)

        self.assertIsNone(hash_val(None))

    def test_reproducible_workload_generation(self) -> None:
        """Verifies that runs with the same master seed yield identical derived seeds and operations."""
        master_seed = 884422

        def simulate_run_workload(seed: int):
            master_rng = random.Random(seed)
            iter_seed = master_rng.randint(1, 100_000_000)
            iter_rng = random.Random(iter_seed)

            # Writer 1 and Writer 2 seeds
            w1_seed = iter_rng.randint(1, 100_000_000)
            w2_seed = iter_rng.randint(1, 100_000_000)
            kill_delay = iter_rng.randint(1, 50)

            # Generate sample operations for w1
            w1_rng = random.Random(w1_seed)
            w1_ops = [
                (w1_rng.choice(["PUT", "DEL", "BATCH"]), w1_rng.choice(["k1", "k2", "k3"]), w1_rng.randint(1, 1000))
                for _ in range(20)
            ]
            return iter_seed, w1_seed, w2_seed, kill_delay, w1_ops

        run1 = simulate_run_workload(master_seed)
        run2 = simulate_run_workload(master_seed)
        self.assertEqual(run1, run2, "Identical master seed must produce perfectly deterministic workload parameters.")

    def test_failure_classification_categories(self) -> None:
        """Verifies canonical classification across all 6 failure categories."""
        # 1. Torture tool failure
        cat = classify_failure([], tool_crashed=True)
        self.assertEqual(cat, FailureCategory.TORTURE_TOOL_FAILURE)

        # 2. Timeout
        cat = classify_failure(["Operation op-1 timed out after 2.0s"])
        self.assertEqual(cat, FailureCategory.TIMEOUT)
        cat = classify_failure([], worker_exceptions=[TimeoutError("Timed out")])
        self.assertEqual(cat, FailureCategory.TIMEOUT)

        # 3. Protocol failure
        cat = classify_failure([], client_malformed=["Garbage response from server"])
        self.assertEqual(cat, FailureCategory.PROTOCOL_FAILURE)
        cat = classify_failure([], client_unknown=["ACK for unknown op_id: op-99"])
        self.assertEqual(cat, FailureCategory.PROTOCOL_FAILURE)
        cat = classify_failure(["Malformed protocol line received"])
        self.assertEqual(cat, FailureCategory.PROTOCOL_FAILURE)

        # 4. Recovery failure
        cat = classify_failure(["Command 'open' failed with exit code 1"])
        self.assertEqual(cat, FailureCategory.RECOVERY_FAILURE)
        cat = classify_failure(["Command 'check' reported corruption: checksum mismatch"])
        self.assertEqual(cat, FailureCategory.RECOVERY_FAILURE)
        cat = classify_failure(["Command 'dump' failed to execute"])
        self.assertEqual(cat, FailureCategory.RECOVERY_FAILURE)

        # 5. Oracle failure
        cat = classify_failure(["DURABILITY VIOLATION: ACKed key missing"])
        self.assertEqual(cat, FailureCategory.ORACLE_FAILURE)
        cat = classify_failure(["ATOMICITY VIOLATION: Partial batch applied"])
        self.assertEqual(cat, FailureCategory.ORACLE_FAILURE)
        cat = classify_failure(["INTEGRITY VIOLATION: Value mismatch"])
        self.assertEqual(cat, FailureCategory.ORACLE_FAILURE)
        cat = classify_failure(["DELETION VIOLATION: Deleted key still present"])
        self.assertEqual(cat, FailureCategory.ORACLE_FAILURE)

        # 6. Engine failure
        exit_info = ChildProcessExitInfo(returncode=1, killed_by_sigkill=False)
        cat = classify_failure(["Engine process exited prematurely"], child_exit=exit_info)
        self.assertEqual(cat, FailureCategory.ENGINE_FAILURE)

    def test_torture_tool_crash_never_passes(self) -> None:
        """Ensures that any internal exception in CrashTestRunner returns exit code 1 and never passes."""
        config = TortureConfig(
            iterations=1,
            seed=42,
            mode=TortureMode.DEFAULT,
            artifacts_dir=self.artifacts_dir,
            db_path=self.db_path,
        )
        runner = CrashTestRunner(config)

        # Simulate unexpected crash in runner loop (e.g. during client.start)
        with patch.object(EngineClient, "start", side_effect=RuntimeError("Simulated internal harness bug")):
            ret = runner.run()
            self.assertEqual(ret, 1, "Harness internal failure must return exit code 1 (NEVER PASS)")

        # Verify failure artifacts were preserved under artifacts directory
        subdirs = [d for d in os.listdir(self.artifacts_dir) if os.path.isdir(os.path.join(self.artifacts_dir, d))]
        self.assertGreaterEqual(len(subdirs), 1)

        art_dir = os.path.join(self.artifacts_dir, subdirs[0])
        report_file = os.path.join(art_dir, "failure_report.json")
        self.assertTrue(os.path.exists(report_file))

        with open(report_file, "r", encoding="utf-8") as f:
            data = json.load(f)
        self.assertIn(data["failure_category"], [FailureCategory.TORTURE_TOOL_FAILURE.value, FailureCategory.ENGINE_FAILURE.value])


if __name__ == "__main__":
    unittest.main()
