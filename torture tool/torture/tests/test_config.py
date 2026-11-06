"""
Unit tests for torture tool configuration and CLI parsing.
Standard library unittest only.
"""

import io
import sys
import unittest
from torture.config import (
    create_argument_parser,
    parse_args,
    TortureConfig,
    TortureMode,
)


class TestConfig(unittest.TestCase):
    """Verifies all CLI argument parsing, defaults, mode selections, and validations."""

    def test_default_arguments(self) -> None:
        """Verifies default values when no flags are passed."""
        # Capture stdout to verify seed is announced
        captured_stdout = io.StringIO()
        old_stdout = sys.stdout
        try:
            sys.stdout = captured_stdout
            config = parse_args([], print_generated_seed=True)
        finally:
            sys.stdout = old_stdout

        self.assertEqual(config.iterations, 500)
        self.assertEqual(config.mode, TortureMode.DEFAULT)
        self.assertTrue(config.seed_generated)
        self.assertIsInstance(config.seed, int)
        self.assertGreater(config.seed, 0)
        self.assertIn("Generated deterministic seed", captured_stdout.getvalue())

    def test_custom_iterations(self) -> None:
        """Verifies user-supplied positive iteration count."""
        config = parse_args(["--iterations", "125", "--seed", "42"], print_generated_seed=False)
        self.assertEqual(config.iterations, 125)

    def test_custom_seed(self) -> None:
        """Verifies user-supplied seed is preserved exactly without auto-generation."""
        config = parse_args(["--seed", "987654"], print_generated_seed=False)
        self.assertEqual(config.seed, 987654)
        self.assertFalse(config.seed_generated)

    def test_all_supported_modes(self) -> None:
        """
        Verifies all 6 required modes:
        default, large-values, large-batches, recovery-crash, compaction, disk-full.
        """
        expected_modes = [
            ("default", TortureMode.DEFAULT),
            ("large-values", TortureMode.LARGE_VALUES),
            ("large-batches", TortureMode.LARGE_BATCHES),
            ("recovery-crash", TortureMode.RECOVERY_CRASH),
            ("compaction", TortureMode.COMPACTION),
            ("disk-full", TortureMode.DISK_FULL),
        ]
        for mode_str, mode_enum in expected_modes:
            config = parse_args(["--mode", mode_str, "--seed", "1"], print_generated_seed=False)
            self.assertEqual(config.mode, mode_enum)
            self.assertEqual(config.mode.value, mode_str)

    def test_invalid_mode(self) -> None:
        """Verifies that an unrecognized mode is rejected by argparse."""
        parser = create_argument_parser()
        with self.assertRaises(SystemExit):
            # Suppress stderr during expected failure
            old_stderr = sys.stderr
            try:
                sys.stderr = io.StringIO()
                parser.parse_args(["--mode", "ultra-warp-speed"])
            finally:
                sys.stderr = old_stderr

    def test_zero_iterations(self) -> None:
        """Verifies that 0 iterations is rejected."""
        parser = create_argument_parser()
        with self.assertRaises(SystemExit):
            old_stderr = sys.stderr
            try:
                sys.stderr = io.StringIO()
                parser.parse_args(["--iterations", "0"])
            finally:
                sys.stderr = old_stderr

    def test_negative_iterations(self) -> None:
        """Verifies that negative iterations are rejected."""
        parser = create_argument_parser()
        with self.assertRaises(SystemExit):
            old_stderr = sys.stderr
            try:
                sys.stderr = io.StringIO()
                parser.parse_args(["--iterations", "-50"])
            finally:
                sys.stderr = old_stderr

    def test_invalid_seeds(self) -> None:
        """Verifies rejection of non-integer or negative seeds."""
        parser = create_argument_parser()
        for bad_seed in ["-1", "not_a_number", "4.5"]:
            with self.assertRaises(SystemExit):
                old_stderr = sys.stderr
                try:
                    sys.stderr = io.StringIO()
                    parser.parse_args(["--seed", bad_seed])
                finally:
                    sys.stderr = old_stderr

    def test_immutability(self) -> None:
        """Verifies config is frozen and cannot be mutated at runtime."""
        config = parse_args(["--iterations", "10", "--seed", "123"], print_generated_seed=False)
        with self.assertRaises(Exception):  # FrozenInstanceError
            config.iterations = 20  # type: ignore


if __name__ == "__main__":
    unittest.main()
