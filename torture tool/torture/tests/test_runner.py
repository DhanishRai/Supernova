"""
Integration and Unit Tests for CP4 Crash Testing Pipeline.

Tests:
1. SingleWriterWorker operation generation and graceful exit on SIGKILL.
2. DumpParser parsing and malformed line detection.
3. StateVerifier post-crash recovery verification.
4. End-to-end CrashTestRunner multi-iteration torture loop with randomized SIGKILL.
   (Clearly identified as [FAKE-ENGINE TEST]).
"""

import os
import random
import shutil
import sys
import time
import unittest

from torture.config import TortureConfig, TortureMode
from torture.oracle import DatabaseOracle
from torture.protocol import EngineClient
from torture.runner import CrashTestRunner
from torture.verifier import DumpParser, StateVerifier
from torture.worker import SingleWriterWorker


class TestVerifier(unittest.TestCase):
    """Unit tests for dump parsing and verification logic."""

    def test_dump_parser_valid_lines(self) -> None:
        raw_dump = (
            "alpha 100\n"
            "beta 200\n"
            "gamma=300\n"
        )
        res = DumpParser.parse(raw_dump)
        self.assertEqual(len(res.malformed_lines), 0)
        self.assertEqual(res.records, {"alpha": "100", "beta": "200", "gamma": "300"})

    def test_dump_parser_empty_and_spaces(self) -> None:
        raw_dump = "  \n\nkey1 val1\n\n"
        res = DumpParser.parse(raw_dump)
        self.assertEqual(res.records, {"key1": "val1"})
        self.assertEqual(len(res.malformed_lines), 0)


class TestCrashRunnerWithFakeEngine(unittest.TestCase):
    """
    [FAKE-ENGINE TEST] Integration tests for the full CP4 crash testing harness.
    NOTE: These tests use a mock Python process to verify harness mechanics;
    they do not prove crash consistency of Person A's Go database engine.
    """

    def setUp(self) -> None:
        self.test_dir = os.path.dirname(os.path.abspath(__file__))
        self.fake_engine = os.path.join(self.test_dir, "fake_engine.py")
        self.test_db = f"/tmp/torture_test_run_{int(time.time()*1000)}.db"

    def tearDown(self) -> None:
        for suffix in ["", ".wal", ".data", ".lock"]:
            p = self.test_db + suffix
            if os.path.exists(p):
                try:
                    os.remove(p)
                except OSError:
                    pass

    def test_fake_engine_single_iteration_crash(self) -> None:
        """
        [FAKE-ENGINE TEST] Runs a single iteration: starts worker, kills with SIGKILL,
        recovers via open/check/dump, and verifies Oracle invariants hold.
        """
        config = TortureConfig(
            iterations=1,
            seed=42,
            mode=TortureMode.DEFAULT,
            engine_bin=self.fake_engine,
            db_path=self.test_db,
            oracle_path="/tmp/test_oracle.log",
            workers=1,
            min_kill_delay_ms=20,
            max_kill_delay_ms=40,
        )
        runner = CrashTestRunner(config)
        ret = runner.run()
        self.assertEqual(ret, 0, "CrashTestRunner single iteration failed")

    def test_fake_engine_multi_iteration_torture(self) -> None:
        """
        [FAKE-ENGINE TEST] Runs 5 consecutive kill-and-recover iterations with randomized delays.
        """
        config = TortureConfig(
            iterations=5,
            seed=12345,
            mode=TortureMode.DEFAULT,
            engine_bin=self.fake_engine,
            db_path=self.test_db,
            oracle_path="/tmp/test_oracle.log",
            workers=1,
            min_kill_delay_ms=10,
            max_kill_delay_ms=30,
        )
        runner = CrashTestRunner(config)
        ret = runner.run()
        self.assertEqual(ret, 0, "Multi-iteration torture run failed")

    def test_engine_panic_crash_detected_as_engine_failure(self) -> None:
        """
        [SABOTAGE TEST - Item 4]
        Proves that if the engine crashes/panics with a non-zero exit code (e.g. Go panic exit code 2),
        the torture tool does NOT mask it as SIGKILL.
        It must fail the run, classify as ENGINE_FAILURE, and record exact exit code 2.
        """
        import json
        import tempfile
        temp_dir = tempfile.mkdtemp(prefix="torture_panic_test_")
        artifacts_dir = os.path.join(temp_dir, "artifacts")
        panic_engine = os.path.join(temp_dir, "panic_engine.py")

        # Create an engine script that immediately panics and exits with code 2
        with open(panic_engine, "w", encoding="utf-8") as f:
            f.write(
                "import sys\n"
                "if len(sys.argv) > 1 and sys.argv[1] == 'serve':\n"
                "    sys.stderr.write('panic: runtime error: invalid memory address\\n')\n"
                "    sys.exit(2)\n"
                "sys.exit(0)\n"
            )

        try:
            config = TortureConfig(
                iterations=1,
                seed=999,
                mode=TortureMode.DEFAULT,
                engine_bin=panic_engine,
                db_path=os.path.join(temp_dir, "panic.db"),
                artifacts_dir=artifacts_dir,
                workers=1,
                min_kill_delay_ms=100,
                max_kill_delay_ms=150,
            )
            runner = CrashTestRunner(config)
            ret = runner.run()

            # Must FAIL
            self.assertEqual(ret, 1, "Spontaneous engine panic/crash must cause runner to return exit code 1!")

            # Verify artifact classification
            subdirs = [d for d in os.listdir(artifacts_dir) if os.path.isdir(os.path.join(artifacts_dir, d))]
            self.assertGreaterEqual(len(subdirs), 1, "Artifacts must be preserved for engine crash")
            report_file = os.path.join(artifacts_dir, subdirs[0], "failure_report.json")
            self.assertTrue(os.path.exists(report_file))

            with open(report_file, "r", encoding="utf-8") as f:
                data = json.load(f)

            self.assertEqual(data["failure_category"], "Engine failure", "Crash must be categorized as Engine failure!")
            self.assertFalse(data["child_process_exit_info"]["killed_by_sigkill"], "Engine panic must NOT be marked killed_by_sigkill!")
            self.assertEqual(data["child_process_exit_info"]["returncode"], 2, "Exact exit code 2 must be recorded!")
        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()

