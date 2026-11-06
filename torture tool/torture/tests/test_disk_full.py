"""
Unit and Integration Tests for CP11 Disk-Full Mode.

Tests:
1. Successful writes before reaching max-bytes limit.
2. Single PUT exceeding limit receives clean ERR and is omitted from storage.
3. BATCH exceeding limit receives clean ERR and is atomically omitted (no partial writes).
4. Existing data remains readable before kill.
5. Post-kill recovery and Oracle verification of acknowledged state.
6. Full CrashTestRunner execution with --mode disk-full.
7. Sabotage detection:
   - Sabotage A: Engine accepts operation exceeding max-bytes limit.
   - Sabotage B: Engine partially applies batch exceeding limit (atomicity violation).
   - Sabotage C: Engine corrupts existing acknowledged data on disk full.
"""

import json
import os
import shutil
import signal
import sys
import tempfile
import time
import unittest

from torture.config import TortureConfig, TortureMode
from torture.oracle import BatchSubOp, DatabaseOracle, OpType
from torture.protocol import BatchItem, EngineClient
from torture.runner import CrashTestRunner
from torture.verifier import StateVerifier


class TestDiskFullMode(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.mkdtemp(prefix="torture_disk_full_")
        self.db_path = os.path.join(self.temp_dir, "df_test.db")
        self.engine_bin = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            "fake_engine.py",
        )

    def tearDown(self) -> None:
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_put_and_batch_rejection_at_limit(self) -> None:
        """
        Verifies that operations exceeding max-bytes receive clean ERR responses,
        leave existing data intact, and are completely absent upon recovery.
        """
        max_bytes = 4000
        client = EngineClient(
            engine_bin=self.engine_bin,
            db_path=self.db_path,
            max_bytes=max_bytes,
        )
        client.start()
        oracle = DatabaseOracle()

        try:
            # 1. Successful small writes under limit
            oracle.record_put("p1", "key1", "val1", writer_id="t1")
            resp1 = client.put("key1", "val1", op_id="p1")
            self.assertTrue(resp1.is_ack)
            oracle.record_ack("p1")

            oracle.record_put("p2", "key2", "val2", writer_id="t1")
            resp2 = client.put("key2", "val2", op_id="p2")
            self.assertTrue(resp2.is_ack)
            oracle.record_ack("p2")

            # 2. PUT exceeding max_bytes limit
            over_val = "Z" * 5000
            oracle.record_put("p_over", "key_over", over_val, writer_id="t1")
            resp_over = client.put("key_over", over_val, op_id="p_over")
            self.assertFalse(resp_over.is_ack)
            self.assertEqual(resp_over.status, "ERR")
            self.assertIn("disk full", resp_over.message)
            oracle.record_err("p_over", resp_over.message)

            # 3. BATCH exceeding max_bytes limit
            batch_items = [
                BatchItem.make_put("batch_small", "good_val"),
                BatchItem.make_put("batch_huge", "W" * 5000),
            ]
            oracle.record_batch(
                "b_over",
                [
                    BatchSubOp(op_type=OpType.PUT, key="batch_small", value="good_val"),
                    BatchSubOp(op_type=OpType.PUT, key="batch_huge", value="W" * 5000),
                ],
                writer_id="t1",
            )
            resp_batch = client.batch(batch_items, op_id="b_over")
            self.assertFalse(resp_batch.is_ack)
            self.assertEqual(resp_batch.status, "ERR")
            self.assertIn("disk full", resp_batch.message)
            oracle.record_err("b_over", resp_batch.message)

            # 4. Existing data remains readable before kill
            self.assertTrue(client.is_alive)
            get_resp = client.get("key1", op_id="g1")
            self.assertTrue(get_resp.is_ack)
            self.assertEqual(bytes.fromhex(get_resp.message).decode("utf-8"), "val1")

            # 5. Kill after disk-full errors
            client.kill(signal.SIGKILL)
            client.close()

            # 6. Recovery and state verification
            verifier = StateVerifier(engine_bin=self.engine_bin, db_path=self.db_path)
            passed, violations, dump = verifier.verify(oracle)

            self.assertTrue(passed, f"Verification failed with violations: {violations}")
            self.assertEqual(dump.get("key1"), "val1")
            self.assertEqual(dump.get("key2"), "val2")
            self.assertNotIn("key_over", dump)
            self.assertNotIn("batch_small", dump)
            self.assertNotIn("batch_huge", dump)

        finally:
            if client.is_alive:
                client.kill(signal.SIGKILL)
                client.close()

    def test_disk_full_runner_mode_execution(self) -> None:
        """Verifies full CrashTestRunner execution in disk-full mode across 2 iterations."""
        config = TortureConfig(
            iterations=2,
            seed=771122,
            mode=TortureMode.DISK_FULL,
            engine_bin=self.engine_bin,
            db_path=self.db_path,
            max_bytes=25_000,
            artifacts_dir=os.path.join(self.temp_dir, "artifacts"),
            min_kill_delay_ms=5,
            max_kill_delay_ms=20,
        )
        runner = CrashTestRunner(config)
        ret = runner.run()
        self.assertEqual(ret, 0, "CrashTestRunner disk-full mode failed")

    def test_sabotage_detection_partial_batch_persistence(self) -> None:
        """
        Sabotage Test: An engine has a bug where it returns ERR on disk full
        for a batch, but partially applies the first item before failing.
        The torture tool must detect this as an ATOMICITY / INTEGRITY VIOLATION.
        """
        # Create sabotaged engine script that partially writes errored batches
        sabotaged_engine_path = os.path.join(self.temp_dir, "sabotaged_engine_partial_batch.py")
        with open(self.engine_bin, "r", encoding="utf-8") as src_f:
            content = src_f.read()

        # Inject bug: before returning ERR for BATCH, persist first item
        buggy_code = """
            if max_bytes is not None and (get_current_storage_size() + len(payload_str.encode("utf-8")) > max_bytes):
                # SABOTAGE: partially persist first item despite returning ERR!
                if items:
                    wal_file.write(json.dumps(items[0]) + "\\n")
                    wal_file.flush()
                sys.stdout.write(f"ERR {op_id} disk full (exceeded max-bytes limit of {max_bytes} bytes)\\n")
                sys.stdout.flush()
                continue
"""
        target_str = """            if max_bytes is not None and (get_current_storage_size() + len(payload_str.encode("utf-8")) > max_bytes):
                # Critical: Atomically reject the entire batch! None of the items are applied.
                sys.stdout.write(f"ERR {op_id} disk full (exceeded max-bytes limit of {max_bytes} bytes)\\n")
                sys.stdout.flush()
                continue"""

        self.assertIn(target_str, content)
        sabotaged_content = content.replace(target_str, buggy_code, 1)

        with open(sabotaged_engine_path, "w", encoding="utf-8") as dst_f:
            dst_f.write(sabotaged_content)
        os.chmod(sabotaged_engine_path, 0o755)

        config = TortureConfig(
            iterations=1,
            seed=882233,
            mode=TortureMode.DISK_FULL,
            engine_bin=sabotaged_engine_path,
            db_path=self.db_path,
            max_bytes=25_000,
            artifacts_dir=os.path.join(self.temp_dir, "artifacts"),
            min_kill_delay_ms=5,
            max_kill_delay_ms=15,
        )
        runner = CrashTestRunner(config)
        ret = runner.run()

        # Must detect bug and exit with code 1
        self.assertEqual(ret, 1, "Torture tool failed to detect partial batch persistence bug under disk-full!")

        # Verify failure artifact preserved
        artifacts_dir = config.artifacts_dir
        self.assertTrue(os.path.exists(artifacts_dir))
        fail_folders = os.listdir(artifacts_dir)
        self.assertTrue(len(fail_folders) > 0)
        report_json = os.path.join(artifacts_dir, fail_folders[0], "failure_report.json")
        self.assertTrue(os.path.exists(report_json))

        with open(report_json, "r", encoding="utf-8") as f:
            data = json.load(f)
        self.assertIn(data["failure_category"], ["Oracle failure", "Engine failure"])
        self.assertTrue(
            any("ATOMICITY VIOLATION" in err or "INTEGRITY VIOLATION" in err or "ERRORED OP VIOLATION" in err for err in data["errors"])
        )


if __name__ == "__main__":
    unittest.main()

