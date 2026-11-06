"""
Sabotage Verification Suite for CP4 Crash Consistency Torture Tool.

Executes the torture tool against deliberately sabotaged engine versions to verify
that our test harness and independent Oracle detect real engine durability bugs.

Sabotage 1: Missing fsync with user-space memory buffering.
Sabotage 2: Sending ACK before the operation is made durable.
"""

import os
import time
import unittest

from torture.config import TortureConfig, TortureMode
from torture.runner import CrashTestRunner


class TestSabotageDetection(unittest.TestCase):
    """
    Proves that the torture tool detects engine durability bugs.
    Both tests expect runner.run() to return 1 (failure detected by Oracle).
    """

    def setUp(self) -> None:
        self.test_dir = os.path.dirname(os.path.abspath(__file__))
        self.sabotage_dir = os.path.join(self.test_dir, "sabotage")
        self.no_fsync_engine = os.path.join(self.sabotage_dir, "sabotage_no_fsync.py")
        self.early_ack_engine = os.path.join(self.sabotage_dir, "sabotage_early_ack.py")
        self.test_db = f"/tmp/torture_sabotage_{int(time.time()*1000)}.db"

    def tearDown(self) -> None:
        for suffix in ["", ".wal", ".data", ".lock"]:
            p = self.test_db + suffix
            if os.path.exists(p):
                try:
                    os.remove(p)
                except OSError:
                    pass

    def test_sabotage_1_detects_missing_fsync(self) -> None:
        """
        [SABOTAGE 1 TEST] Proves torture tool catches engine missing fsync.
        Expected: Tool catches DURABILITY VIOLATION and halts.
        """
        seed = 444
        config = TortureConfig(
            iterations=5,
            seed=seed,
            mode=TortureMode.DEFAULT,
            engine_bin=self.no_fsync_engine,
            db_path=self.test_db,
            oracle_path="/tmp/sabotage1_oracle.log",
            workers=1,
            min_kill_delay_ms=20,
            max_kill_delay_ms=40,
        )
        runner = CrashTestRunner(config)
        ret = runner.run()
        # Must return 1 indicating failure was detected!
        self.assertEqual(ret, 1, "Torture tool failed to catch missing fsync sabotage!")

    def test_sabotage_2_detects_premature_ack(self) -> None:
        """
        [SABOTAGE 2 TEST] Proves torture tool catches engine sending ACK before durability.
        Expected: Tool catches DURABILITY VIOLATION and halts.
        """
        seed = 555
        config = TortureConfig(
            iterations=5,
            seed=seed,
            mode=TortureMode.DEFAULT,
            engine_bin=self.early_ack_engine,
            db_path=self.test_db,
            oracle_path="/tmp/sabotage2_oracle.log",
            workers=1,
            min_kill_delay_ms=10,
            max_kill_delay_ms=30,
        )
        runner = CrashTestRunner(config)
        ret = runner.run()
        # Must return 1 indicating failure was detected!
        self.assertEqual(ret, 1, "Torture tool failed to catch premature ACK sabotage!")


if __name__ == "__main__":
    unittest.main()
