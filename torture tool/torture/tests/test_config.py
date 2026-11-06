"""
Unit tests for torture tool configuration and CLI parsing.
Standard library unittest only.
"""

import unittest
from torture.config import parse_args, TortureConfig, TortureMode


class TestConfig(unittest.TestCase):

    def test_default_config(self) -> None:
        config = parse_args([])
        self.assertEqual(config.iterations, 500)
        self.assertEqual(config.mode, TortureMode.DEFAULT)
        self.assertEqual(config.engine_bin, "./kvdb")
        self.assertEqual(config.workers, 2)
        self.assertIsInstance(config.seed, int)
        self.assertGreater(config.seed, 0)

    def test_judge_cli_variants(self) -> None:
        # ./torture --iterations 500 --seed 12345
        c1 = parse_args(["--iterations", "500", "--seed", "12345"])
        self.assertEqual(c1.iterations, 500)
        self.assertEqual(c1.seed, 12345)
        self.assertEqual(c1.mode, TortureMode.DEFAULT)

        # ./torture --iterations 100 --seed 999 --mode large-values
        c2 = parse_args(["--iterations", "100", "--seed", "999", "--mode", "large-values"])
        self.assertEqual(c2.iterations, 100)
        self.assertEqual(c2.seed, 999)
        self.assertEqual(c2.mode, TortureMode.LARGE_VALUES)

        # ./torture --iterations 100 --seed 7 --mode compaction
        c3 = parse_args(["--iterations", "100", "--seed", "7", "--mode", "compaction"])
        self.assertEqual(c3.iterations, 100)
        self.assertEqual(c3.seed, 7)
        self.assertEqual(c3.mode, TortureMode.COMPACTION)

        # ./torture --iterations 100 --seed 42 --mode recovery-crash
        c4 = parse_args(["--iterations", "100", "--seed", "42", "--mode", "recovery-crash"])
        self.assertEqual(c4.iterations, 100)
        self.assertEqual(c4.seed, 42)
        self.assertEqual(c4.mode, TortureMode.RECOVERY_CRASH)

    def test_invalid_iterations(self) -> None:
        with self.assertRaises(ValueError):
            config = TortureConfig(
                iterations=0,
                seed=42,
                mode=TortureMode.DEFAULT,
                engine_bin="./kvdb",
                db_path="/tmp/test.db",
                oracle_path="/tmp/oracle.log",
                workers=2,
            )
            config.validate()

    def test_invalid_workers(self) -> None:
        with self.assertRaises(ValueError):
            config = TortureConfig(
                iterations=10,
                seed=42,
                mode=TortureMode.DEFAULT,
                engine_bin="./kvdb",
                db_path="/tmp/test.db",
                oracle_path="/tmp/oracle.log",
                workers=0,
            )
            config.validate()

    def test_invalid_kill_delay_range(self) -> None:
        with self.assertRaises(ValueError):
            config = TortureConfig(
                iterations=10,
                seed=42,
                mode=TortureMode.DEFAULT,
                engine_bin="./kvdb",
                db_path="/tmp/test.db",
                oracle_path="/tmp/oracle.log",
                workers=2,
                min_kill_delay_ms=100,
                max_kill_delay_ms=50,  # max < min
            )
            config.validate()

    def test_immutability(self) -> None:
        config = parse_args(["--iterations", "10", "--seed", "123"])
        with self.assertRaises(Exception):  # FrozenInstanceError
            config.iterations = 20  # type: ignore


if __name__ == "__main__":
    unittest.main()
