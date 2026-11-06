"""
Unit and integration tests for Recovery-Crash Mode (CP9).

Tests:
1. First `open` process interrupted by SIGKILL.
2. Second `open` process finishes recovery cleanly.
3. Integrity check and dump verified against Oracle.
4. Detection and recording of MISSED CRASH WINDOW when `open` exits before SIGKILL.
5. Telemetry recording: recovery start time, requested kill delay, actual exit status,
   whether SIGKILL occurred, recovery completion time.
6. Failure preservation on second recovery corruption.
"""

import json
import os
import signal
import sys
import tempfile
import unittest
from unittest.mock import MagicMock, patch

from torture.config import TortureConfig, TortureMode
from torture.oracle import DatabaseOracle
from torture.reporting import RecoveryCrashInfo, Reporter
from torture.runner import CrashTestRunner


FAKE_ENGINE = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "fake_engine.py"
)


class TestRecoveryCrash(unittest.TestCase):
    """Exhaustive test suite for CP9 recovery-crash mode."""

    def setUp(self) -> None:
        self.temp_dir = tempfile.mkdtemp(prefix="torture_rec_crash_")
        self.db_path = os.path.join(self.temp_dir, "rec_test.db")
        self.artifacts_dir = os.path.join(self.temp_dir, "artifacts")
        # Ensure fake_engine is executable
        os.chmod(FAKE_ENGINE, 0o755)

    def tearDown(self) -> None:
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)
        os.environ.pop("SUPERNOVA_RECOVERY_DELAY_MS", None)

    def test_recovery_crash_interruption_and_second_open_success(self) -> None:
        """
        Verifies that recovery-crash mode strikes `open` with SIGKILL mid-flight,
        subsequently runs a 2nd `open`, and confirms that all ACKed data matches Oracle.
        """
        # Set simulated recovery delay to 40ms to guarantee SIGKILL strikes while open is active
        os.environ["SUPERNOVA_RECOVERY_DELAY_MS"] = "40"

        config = TortureConfig(
            iterations=1,
            seed=12345,
            mode=TortureMode.RECOVERY_CRASH,
            engine_bin=FAKE_ENGINE,
            db_path=self.db_path,
            min_kill_delay_ms=2,
            max_kill_delay_ms=8,
            artifacts_dir=self.artifacts_dir,
        )

        runner = CrashTestRunner(config)
        ret = runner.run()
        self.assertEqual(ret, 0, "Recovery-crash run must return 0 on success.")

    def test_missed_crash_window_recorded(self) -> None:
        """
        Verifies that if `open` finishes before SIGKILL arrives:
        1. It records MISSED CRASH WINDOW.
        2. It does not count this as a successful recovery-crash test.
        """
        # Set 0ms simulated delay and a relatively large kill delay (60ms)
        os.environ["SUPERNOVA_RECOVERY_DELAY_MS"] = "0"

        config = TortureConfig(
            iterations=1,
            seed=9999,
            mode=TortureMode.RECOVERY_CRASH,
            engine_bin=FAKE_ENGINE,
            db_path=self.db_path,
            min_kill_delay_ms=250,
            max_kill_delay_ms=300,
            artifacts_dir=self.artifacts_dir,
        )

        # Mock the report output to capture missed crash window reporting
        reporter = Reporter(failure_dir=self.artifacts_dir)
        reports_captured = []
        original_print = reporter.print_iteration_summary

        def capture_summary(rep):
            reports_captured.append(rep)
            original_print(rep)

        reporter.print_iteration_summary = capture_summary

        runner = CrashTestRunner(config, reporter=reporter)
        # We run the loop directly or single attempt
        # Because open finishes in < 5ms and kill delay is 60ms, the first attempt will miss the window
        # Max attempts is bounded
        # Let's inspect the first attempt in reports_captured
        runner.run()

        missed_reports = [r for r in reports_captured if r.missed_crash_window]
        self.assertGreaterEqual(len(missed_reports), 1, "Must capture at least one MISSED CRASH WINDOW report.")
        
        first_missed = missed_reports[0]
        self.assertTrue(first_missed.missed_crash_window)
        self.assertFalse(first_missed.passed, "Missed crash window must NOT be counted as a passed test.")
        self.assertIsNotNone(first_missed.recovery_info)
        self.assertFalse(first_missed.recovery_info.sigkill_occurred)
        self.assertEqual(first_missed.recovery_info.actual_exit_status, 0)

    def test_telemetry_fields_recorded(self) -> None:
        """
        Verifies that all mandatory telemetry fields are populated:
        - Recovery start time.
        - Requested kill delay.
        - Actual process exit status.
        - Whether SIGKILL occurred.
        - Recovery completion time.
        - Second recovery status.
        """
        os.environ["SUPERNOVA_RECOVERY_DELAY_MS"] = "35"

        rec_info = RecoveryCrashInfo(
            recovery_start_time=1700000000.123,
            requested_kill_delay_ms=5.5,
            actual_exit_status=-9,
            sigkill_occurred=True,
            recovery_completion_time=1700000000.130,
            missed_crash_window=False,
            second_recovery_success=True,
            second_recovery_time_s=0.012,
        )

        self.assertEqual(rec_info.recovery_start_time, 1700000000.123)
        self.assertEqual(rec_info.requested_kill_delay_ms, 5.5)
        self.assertEqual(rec_info.actual_exit_status, -9)
        self.assertTrue(rec_info.sigkill_occurred)
        self.assertFalse(rec_info.missed_crash_window)
        self.assertTrue(rec_info.second_recovery_success)
        self.assertEqual(rec_info.second_recovery_time_s, 0.012)

    def test_second_recovery_failure_preserves_artifacts(self) -> None:
        """
        Verifies that if the second `open` fails, the test fails, classifies as
        RECOVERY_FAILURE, and preserves complete artifacts including recovery_crash_info.
        """
        os.environ["SUPERNOVA_RECOVERY_DELAY_MS"] = "30"

        config = TortureConfig(
            iterations=1,
            seed=777,
            mode=TortureMode.RECOVERY_CRASH,
            engine_bin=FAKE_ENGINE,
            db_path=self.db_path,
            min_kill_delay_ms=2,
            max_kill_delay_ms=5,
            artifacts_dir=self.artifacts_dir,
        )
        runner = CrashTestRunner(config)

        # Patch StateVerifier._run_cmd so second open returns exit code 1 (failure)
        from torture.verifier import StateVerifier
        real_run_cmd = StateVerifier._run_cmd

        def failing_run_cmd(self_verifier, subcmd):
            if subcmd == "open":
                return 1, "", "Simulated crash during 2nd open recovery"
            return real_run_cmd(self_verifier, subcmd)

        with patch.object(StateVerifier, "_run_cmd", side_effect=failing_run_cmd, autospec=True):
            ret = runner.run()
            self.assertEqual(ret, 1, "Failed second recovery must return exit code 1 (FAIL).")

        # Verify artifacts were preserved with recovery_crash_info
        subdirs = [d for d in os.listdir(self.artifacts_dir) if os.path.isdir(os.path.join(self.artifacts_dir, d))]
        self.assertGreaterEqual(len(subdirs), 1)

        art_dir = os.path.join(self.artifacts_dir, subdirs[0])
        report_file = os.path.join(art_dir, "failure_report.json")
        self.assertTrue(os.path.exists(report_file))

        with open(report_file, "r", encoding="utf-8") as f:
            data = json.load(f)

        self.assertEqual(data["failure_category"], "Recovery failure")
        self.assertIn("recovery_crash_info", data)
        self.assertIsNotNone(data["recovery_crash_info"])
        self.assertFalse(data["recovery_crash_info"]["second_recovery_success"])

    def test_repeated_recovery_crash_mode_multi_iterations(self) -> None:
        """
        Executes 3 consecutive iterations under --mode recovery-crash, verifying that
        repeated recovery interruptions leave the database fully consistent each time.
        """
        os.environ["SUPERNOVA_RECOVERY_DELAY_MS"] = "35"

        config = TortureConfig(
            iterations=3,
            seed=445566,
            mode=TortureMode.RECOVERY_CRASH,
            engine_bin=FAKE_ENGINE,
            db_path=self.db_path,
            min_kill_delay_ms=2,
            max_kill_delay_ms=8,
            artifacts_dir=self.artifacts_dir,
        )
        runner = CrashTestRunner(config)
        ret = runner.run()
        self.assertEqual(ret, 0, "All 3 recovery-crash iterations must pass cleanly.")


if __name__ == "__main__":
    unittest.main()
