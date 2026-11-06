"""
Unit and integration tests for Compaction-Crash Mode (CP10).

Tests:
1. Compaction interruption before ACK (crash mid-compaction).
2. Compaction completion and crash after ACK.
3. Temporary file safety and isolation (temporary files never become authoritative).
4. Full Oracle verification of state recovery after compaction crash.
5. Multi-iteration repeated compaction crash resilience.
6. Detection of sabotaged compaction bugs (e.g. data loss during swap).
"""

import json
import os
import signal
import sys
import tempfile
import unittest
from unittest.mock import patch

from torture.config import TortureConfig, TortureMode
from torture.oracle import DatabaseOracle
from torture.reporting import CompactionCrashInfo, Reporter
from torture.runner import CrashTestRunner


FAKE_ENGINE = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "fake_engine.py"
)


class TestCompactionCrash(unittest.TestCase):
    """Exhaustive test suite for CP10 compaction-crash mode."""

    def setUp(self) -> None:
        self.temp_dir = tempfile.mkdtemp(prefix="torture_compact_")
        self.db_path = os.path.join(self.temp_dir, "compact_test.db")
        self.artifacts_dir = os.path.join(self.temp_dir, "artifacts")
        os.chmod(FAKE_ENGINE, 0o755)

    def tearDown(self) -> None:
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)
        os.environ.pop("SUPERNOVA_COMPACTION_DELAY_MS", None)

    def test_01_compaction_interrupted_before_ack(self) -> None:
        """
        [WINDOW 1] Injects SIGKILL while compaction is actively writing temp file before ACK.
        Verifies:
        - Compaction was unacknowledged (interrupted).
        - Database recovers cleanly via open, check, and dump.
        - All ACKed data is 100% preserved.
        """
        # Compaction takes 50ms, kill strikes within 2-8ms
        os.environ["SUPERNOVA_COMPACTION_DELAY_MS"] = "50"

        config = TortureConfig(
            iterations=1,
            seed=10101,
            mode=TortureMode.COMPACTION,
            engine_bin=FAKE_ENGINE,
            db_path=self.db_path,
            min_kill_delay_ms=2,
            max_kill_delay_ms=8,
            artifacts_dir=self.artifacts_dir,
        )

        reporter = Reporter(failure_dir=self.artifacts_dir)
        reports = []
        original_print = reporter.print_iteration_summary
        reporter.print_iteration_summary = lambda rep: (reports.append(rep), original_print(rep))

        runner = CrashTestRunner(config, reporter=reporter)
        ret = runner.run()
        self.assertEqual(ret, 0, "Compaction interruption run must succeed.")
        self.assertEqual(len(reports), 1)
        self.assertTrue(reports[0].passed)
        self.assertIsNotNone(reports[0].compaction_info)
        # Compaction was killed before ACK
        self.assertFalse(reports[0].compaction_info.compact_acked)

    def test_02_compaction_crash_after_ack(self) -> None:
        """
        [WINDOW 2] Compaction finishes and ACKs, then SIGKILL strikes.
        Verifies:
        - Compaction was ACKed and durable on disk.
        - Database recovers cleanly with compacted state.
        - All data is preserved according to Oracle.
        """
        # Compaction takes 5ms, kill arrives at 25-35ms
        os.environ["SUPERNOVA_COMPACTION_DELAY_MS"] = "5"

        config = TortureConfig(
            iterations=1,
            seed=20202,
            mode=TortureMode.COMPACTION,
            engine_bin=FAKE_ENGINE,
            db_path=self.db_path,
            min_kill_delay_ms=25,
            max_kill_delay_ms=35,
            artifacts_dir=self.artifacts_dir,
        )

        reporter = Reporter(failure_dir=self.artifacts_dir)
        reports = []
        original_print = reporter.print_iteration_summary
        reporter.print_iteration_summary = lambda rep: (reports.append(rep), original_print(rep))

        runner = CrashTestRunner(config, reporter=reporter)
        ret = runner.run()
        self.assertEqual(ret, 0, "Post-compaction crash run must succeed.")
        self.assertEqual(len(reports), 1)
        self.assertTrue(reports[0].passed)
        self.assertIsNotNone(reports[0].compaction_info)
        # Compaction was successfully ACKed before kill
        self.assertTrue(reports[0].compaction_info.compact_acked)

    def test_03_temporary_files_safety_and_isolation(self) -> None:
        """
        [SAFETY INVARIANT] Verifies that orphan temporary compaction files (e.g. *.compact.tmp)
        created before SIGKILL do NOT corrupt recovered state or become authoritative incorrectly.
        """
        os.environ["SUPERNOVA_COMPACTION_DELAY_MS"] = "40"

        config = TortureConfig(
            iterations=1,
            seed=30303,
            mode=TortureMode.COMPACTION,
            engine_bin=FAKE_ENGINE,
            db_path=self.db_path,
            min_kill_delay_ms=3,
            max_kill_delay_ms=7,
            artifacts_dir=self.artifacts_dir,
        )

        runner = CrashTestRunner(config)
        ret = runner.run()
        self.assertEqual(ret, 0)

        # Confirm that no orphan temporary files linger as authoritative files
        parent_dir = os.path.dirname(os.path.abspath(self.db_path))
        base_name = os.path.basename(self.db_path)
        for fname in os.listdir(parent_dir):
            self.assertFalse(
                fname.endswith(".compact.tmp"),
                f"Temporary compaction file '{fname}' was not cleaned up during recovery."
            )

    def test_04_repeated_compaction_crash_iterations(self) -> None:
        """
        [MULTI-ITERATION] Runs 3 consecutive compaction crash iterations, ensuring that repeated
        interrupted or completed compactions maintain full database integrity.
        """
        os.environ["SUPERNOVA_COMPACTION_DELAY_MS"] = "25"

        config = TortureConfig(
            iterations=3,
            seed=40404,
            mode=TortureMode.COMPACTION,
            engine_bin=FAKE_ENGINE,
            db_path=self.db_path,
            min_kill_delay_ms=2,
            max_kill_delay_ms=15,
            artifacts_dir=self.artifacts_dir,
        )

        runner = CrashTestRunner(config)
        ret = runner.run()
        self.assertEqual(ret, 0, "All 3 compaction crash iterations must pass cleanly.")

    def test_05_sabotaged_compaction_detected_and_reported(self) -> None:
        """
        [DEFECT DETECTION] Proves that if an engine bug corrupts or drops data during compaction,
        the harness detects the durability/integrity violation and preserves failure artifacts.
        """
        os.environ["SUPERNOVA_COMPACTION_DELAY_MS"] = "20"

        config = TortureConfig(
            iterations=1,
            seed=50505,
            mode=TortureMode.COMPACTION,
            engine_bin=FAKE_ENGINE,
            db_path=self.db_path,
            min_kill_delay_ms=2,
            max_kill_delay_ms=10,
            artifacts_dir=self.artifacts_dir,
        )

        runner = CrashTestRunner(config)

        # Sabotage: inject a mock verification failure (e.g. data loss during compaction)
        from torture.verifier import StateVerifier
        real_verify = StateVerifier.verify

        def sabotaged_verify(self_v, oracle):
            passed, violations, dump = real_verify(self_v, oracle)
            violations.append("DURABILITY VIOLATION: key 'key_0001' missing after compaction crash!")
            return False, violations, dump

        with patch.object(StateVerifier, "verify", side_effect=sabotaged_verify, autospec=True):
            ret = runner.run()
            self.assertEqual(ret, 1, "Sabotaged compaction must return exit code 1 (FAIL).")

        # Verify failure artifacts preserved with compaction_crash_info
        subdirs = [d for d in os.listdir(self.artifacts_dir) if os.path.isdir(os.path.join(self.artifacts_dir, d))]
        self.assertGreaterEqual(len(subdirs), 1)

        art_dir = os.path.join(self.artifacts_dir, subdirs[0])
        report_file = os.path.join(art_dir, "failure_report.json")
        self.assertTrue(os.path.exists(report_file))

        with open(report_file, "r", encoding="utf-8") as f:
            data = json.load(f)

        self.assertEqual(data["failure_category"], "Oracle failure")
        self.assertIn("compaction_crash_info", data)
        self.assertIsNotNone(data["compaction_crash_info"])


if __name__ == "__main__":
    unittest.main()
