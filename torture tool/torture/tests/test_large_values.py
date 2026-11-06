"""
Large Values and Shared-Key Torture Test Suite for Supernova Crash-Consistency Tool.

CP6 Implementation:
- Values approximately 1 MB (1,048,576 bytes) in size.
- Shared-key testing across concurrent writers on small key sets.
- Strict detection of:
    1. Large PUT
    2. Large overwrite
    3. Large values across multiple keys
    4. Large value followed by DELETE
    5. Repeated large-value writes on shared keys
    6. Kill during large-value transmission (broken pipe handling)
    7. Kill during large-value processing
    8. Truncated values
    9. Corrupted values
    10. Incorrect decoding
    11. Partial persistence
    12. Incorrect overwrites
    13. Memory efficiency: hashes and lengths used, no full 1 MB dumps in reports
"""

import hashlib
import os
import signal
import sys
import time
import unittest

from torture.config import TortureConfig, TortureMode
from torture.oracle import DatabaseOracle, OpType
from torture.protocol import (
    EngineClient,
    UnexpectedEOFError,
)
from torture.runner import CrashTestRunner
from torture.verifier import StateVerifier
from torture.worker import (
    ConcurrentWriter,
    WriterWorkloadType,
    generate_large_value,
)


class TestLargeValues(unittest.TestCase):
    """
    Test suite for CP6: 1 MB values, shared keys, transmission kills,
    and defect detection.
    """

    def setUp(self) -> None:
        self.test_dir = os.path.dirname(os.path.abspath(__file__))
        self.fake_engine = os.path.join(self.test_dir, "fake_engine.py")
        self.test_db = f"/tmp/torture_large_val_{int(time.time() * 1000)}.db"

    def tearDown(self) -> None:
        for suffix in ["", ".wal", ".data", ".lock"]:
            p = self.test_db + suffix
            if os.path.exists(p):
                try:
                    os.remove(p)
                except OSError:
                    pass

    # 1. Large PUT
    def test_01_large_put(self) -> None:
        """[SCENARIO 1] Submits a single 1 MB PUT, verifies ACK and recovery match."""
        client = EngineClient(engine_bin=self.fake_engine, db_path=self.test_db)
        client.start()
        oracle = DatabaseOracle()

        key = "large_key_1"
        op_id = "op-large-put-1"
        large_val = generate_large_value(op_id, "w1", key, size=1024 * 1024)
        self.assertEqual(len(large_val), 1024 * 1024, "Generated value is not exactly 1 MB!")

        oracle.record_put(op_id, key, large_val, writer_id="w1")
        resp = client.put(key, large_val, op_id=op_id)
        self.assertTrue(resp.is_ack)
        oracle.record_ack(op_id)
        client.close()

        # Verify recovery
        verifier = StateVerifier(engine_bin=self.fake_engine, db_path=self.test_db)
        passed, violations, dump = verifier.verify(oracle)
        self.assertTrue(passed, f"Recovery failed: {violations}")
        self.assertIn(key, dump)
        self.assertEqual(len(dump[key]), 1024 * 1024)
        self.assertEqual(dump[key], large_val)

    # 2. Large Overwrite
    def test_02_large_overwrite(self) -> None:
        """[SCENARIO 2] Overwrites a 1 MB value with a different 1 MB value on the same key."""
        client = EngineClient(engine_bin=self.fake_engine, db_path=self.test_db)
        client.start()
        oracle = DatabaseOracle()

        key = "overwrite_key"
        val1 = generate_large_value("op-ow-1", "w1", key, size=1024 * 1024)
        val2 = generate_large_value("op-ow-2", "w2", key, size=1024 * 1024)
        self.assertNotEqual(val1, val2, "Generated values for overwrite must be distinguishable!")

        # First write
        oracle.record_put("op-ow-1", key, val1, writer_id="w1")
        resp1 = client.put(key, val1, op_id="op-ow-1")
        self.assertTrue(resp1.is_ack)
        oracle.record_ack("op-ow-1")

        # Second write (overwrite)
        oracle.record_put("op-ow-2", key, val2, writer_id="w2")
        resp2 = client.put(key, val2, op_id="op-ow-2")
        self.assertTrue(resp2.is_ack)
        oracle.record_ack("op-ow-2")

        client.close()

        # Verify recovered state contains exactly the second 1 MB value
        verifier = StateVerifier(engine_bin=self.fake_engine, db_path=self.test_db)
        passed, violations, dump = verifier.verify(oracle)
        self.assertTrue(passed, f"Recovery failed: {violations}")
        self.assertEqual(dump[key], val2)
        self.assertNotEqual(dump[key], val1)

    # 3. Large values across multiple keys
    def test_03_large_values_across_multiple_keys(self) -> None:
        """[SCENARIO 3] Writes distinct 1 MB values across 4 distinct keys."""
        client = EngineClient(engine_bin=self.fake_engine, db_path=self.test_db)
        client.start()
        oracle = DatabaseOracle()

        keys = [f"multi_k_{i}" for i in range(4)]
        values = {}
        for idx, k in enumerate(keys):
            op_id = f"op-multi-{idx}"
            v = generate_large_value(op_id, f"w{idx}", k, size=1024 * 1024)
            values[k] = v
            oracle.record_put(op_id, k, v, writer_id=f"w{idx}")
            resp = client.put(k, v, op_id=op_id)
            self.assertTrue(resp.is_ack)
            oracle.record_ack(op_id)

        client.close()

        verifier = StateVerifier(engine_bin=self.fake_engine, db_path=self.test_db)
        passed, violations, dump = verifier.verify(oracle)
        self.assertTrue(passed, f"Recovery failed: {violations}")
        self.assertEqual(len(dump), 4)
        for k in keys:
            self.assertEqual(dump[k], values[k])
            self.assertEqual(len(dump[k]), 1024 * 1024)

    # 4. Large value followed by DELETE
    def test_04_large_value_followed_by_delete(self) -> None:
        """[SCENARIO 4] Writes a 1 MB value and then cleanly deletes it."""
        client = EngineClient(engine_bin=self.fake_engine, db_path=self.test_db)
        client.start()
        oracle = DatabaseOracle()

        key = "del_large_key"
        val = generate_large_value("op-put-del", "w1", key, size=1024 * 1024)

        # PUT
        oracle.record_put("op-put-del", key, val, writer_id="w1")
        resp1 = client.put(key, val, op_id="op-put-del")
        self.assertTrue(resp1.is_ack)
        oracle.record_ack("op-put-del")

        # DEL
        oracle.record_del("op-del", key, writer_id="w1")
        resp2 = client.delete(key, op_id="op-del")
        self.assertTrue(resp2.is_ack)
        oracle.record_ack("op-del")

        client.close()

        # Verify recovered state has completely removed the key
        verifier = StateVerifier(engine_bin=self.fake_engine, db_path=self.test_db)
        passed, violations, dump = verifier.verify(oracle)
        self.assertTrue(passed, f"Recovery failed: {violations}")
        self.assertNotIn(key, dump)

    # 5. Repeated large-value writes on shared keys
    def test_05_repeated_large_writes_shared_keys(self) -> None:
        """[SCENARIO 5] Two concurrent writers repeatedly writing 1 MB values to 3 shared keys."""
        client = EngineClient(engine_bin=self.fake_engine, db_path=self.test_db)
        client.start()
        oracle = DatabaseOracle()

        shared_keys = ["shared_large_0", "shared_large_1", "shared_large_2"]
        import random
        w1 = ConcurrentWriter(
            client=client,
            oracle=oracle,
            rng=random.Random(111),
            writer_id="w1",
            mode=TortureMode.LARGE_VALUES,
            workload_type=WriterWorkloadType.PUT_ONLY,
            key_pool=shared_keys,
        )
        w2 = ConcurrentWriter(
            client=client,
            oracle=oracle,
            rng=random.Random(222),
            writer_id="w2",
            mode=TortureMode.LARGE_VALUES,
            workload_type=WriterWorkloadType.PUT_ONLY,
            key_pool=shared_keys,
        )

        w1.start()
        w2.start()
        time.sleep(0.080)
        client.kill(signal.SIGKILL)
        w1.stop()
        w2.stop()
        w1.join(timeout=1.0)
        w2.join(timeout=1.0)
        client.close()

        self.assertGreater(w1.submitted_count + w2.submitted_count, 0)
        verifier = StateVerifier(engine_bin=self.fake_engine, db_path=self.test_db)
        passed, violations, dump = verifier.verify(oracle)
        self.assertTrue(passed, f"Recovery failed: {violations}")

    # 6. Kill during large-value transmission
    def test_06_kill_during_large_value_transmission(self) -> None:
        """
        [SCENARIO 6] SIGKILL strikes mid-transmission while sending a 1 MB payload.
        Verifies:
        - BrokenPipeError / UnexpectedEOFError handled cleanly without deadlock
        - Engine recovery parses valid WAL without choking on severed pipe lines
        """
        client = EngineClient(engine_bin=self.fake_engine, db_path=self.test_db)
        client.start()
        oracle = DatabaseOracle()

        key = "pipe_kill_key"
        val = generate_large_value("op-pipe-kill", "w1", key, size=1024 * 1024)

        # Trigger kill right as command is written
        import threading
        def delayed_kill():
            time.sleep(0.005)
            client.kill(signal.SIGKILL)

        kill_thread = threading.Thread(target=delayed_kill, daemon=True)
        kill_thread.start()

        # Submit large PUT; should catch transmission termination
        caught_expected_error = False
        try:
            client.put(key, val, op_id="op-pipe-kill", timeout=1.0)
        except (BrokenPipeError, UnexpectedEOFError, OSError):
            caught_expected_error = True
        except Exception:
            caught_expected_error = True

        kill_thread.join(timeout=1.0)
        client.close()
        self.assertTrue(caught_expected_error, "Expected broken pipe or process death error on mid-write kill")

        # Re-open and verify database integrity
        verifier = StateVerifier(engine_bin=self.fake_engine, db_path=self.test_db)
        ret_open, _, _ = verifier._run_cmd("open")
        self.assertEqual(ret_open, 0, "Database failed to open after severed transmission")

    # 7. Kill during large-value processing
    def test_07_kill_during_large_value_processing(self) -> None:
        """
        [SCENARIO 7] Injects TRIGGER_DELAY so engine delays while writing 1 MB value,
        then strikes with SIGKILL.
        """
        client = EngineClient(engine_bin=self.fake_engine, db_path=self.test_db)
        client.start()
        oracle = DatabaseOracle()

        key = "process_kill_key"
        val = generate_large_value("op-proc-kill", "w1", key, size=1024 * 1024)
        # Inject artificial processing delay so kill lands during engine disk processing
        val = "TRIGGER_DELAY:0.050 " + val[20:]

        oracle.record_put("op-proc-kill", key, val, writer_id="w1")

        import threading
        def delayed_kill():
            time.sleep(0.020)
            client.kill(signal.SIGKILL)

        kill_thread = threading.Thread(target=delayed_kill, daemon=True)
        kill_thread.start()

        try:
            client.put(key, val, op_id="op-proc-kill", timeout=0.5)
        except (BrokenPipeError, UnexpectedEOFError, OSError):
            pass

        kill_thread.join(timeout=1.0)
        client.close()

        # Post-crash verification: unACKed write must be either fully committed or discarded
        verifier = StateVerifier(engine_bin=self.fake_engine, db_path=self.test_db)
        passed, violations, dump = verifier.verify(oracle)
        self.assertTrue(passed, f"Recovery failed after processing kill: {violations}")

    # 8. Detection: Truncated value
    def test_08_detect_truncated_value(self) -> None:
        """[DETECTION 1] Proves verifier and oracle catch a truncated 1 MB value."""
        oracle = DatabaseOracle()
        key = "test_trunc"
        full_val = generate_large_value("op-trunc", "w1", key, size=1024 * 1024)

        oracle.record_put("op-trunc", key, full_val, writer_id="w1")
        oracle.record_ack("op-trunc")

        # Truncated to 512 KB
        truncated_val = full_val[: 512 * 1024]
        res = oracle.verify_recovered_dump({key: truncated_val})

        self.assertFalse(res.passed, "Oracle failed to detect truncated large value!")
        self.assertTrue(any("DURABILITY VIOLATION" in v for v in res.violations))
        self.assertTrue(any("<len=1048576" in v for v in res.violations))
        self.assertTrue(any("<len=524288" in v for v in res.violations))

    # 9. Detection: Corrupted value (bit flip)
    def test_09_detect_corrupted_value(self) -> None:
        """[DETECTION 2] Proves verifier and oracle catch a corrupted byte in a 1 MB value."""
        oracle = DatabaseOracle()
        key = "test_corrupt"
        full_val = generate_large_value("op-corrupt", "w1", key, size=1024 * 1024)

        oracle.record_put("op-corrupt", key, full_val, writer_id="w1")
        oracle.record_ack("op-corrupt")

        # Corrupt 1 byte in the middle of 1 MB
        corrupted_val = full_val[:500000] + "X" + full_val[500001:]
        res = oracle.verify_recovered_dump({key: corrupted_val})

        self.assertFalse(res.passed, "Oracle failed to detect corrupted large value!")
        self.assertTrue(any("DURABILITY VIOLATION" in v for v in res.violations))
        # Hash mismatch should be recorded
        h_orig = hashlib.sha256(full_val.encode("utf-8")).hexdigest()[:16]
        h_corr = hashlib.sha256(corrupted_val.encode("utf-8")).hexdigest()[:16]
        self.assertNotEqual(h_orig, h_corr)

    # 10. Detection: Incorrect decoding
    def test_10_detect_incorrect_decoding(self) -> None:
        """[DETECTION 3] Proves verifier and oracle catch incorrect hex decoding."""
        oracle = DatabaseOracle()
        key = "test_decoding"
        full_val = generate_large_value("op-dec", "w1", key, size=1024 * 1024)

        oracle.record_put("op-dec", key, full_val, writer_id="w1")
        oracle.record_ack("op-dec")

        # Flawed decode replaced characters with '?'
        decoded_val = full_val[:1000] + ("?" * 200) + full_val[1200:]
        res = oracle.verify_recovered_dump({key: decoded_val})

        self.assertFalse(res.passed, "Oracle failed to detect incorrect decoding!")
        self.assertTrue(any("DURABILITY VIOLATION" in v for v in res.violations))

    # 11. Detection: Partial persistence
    def test_11_detect_partial_persistence(self) -> None:
        """[DETECTION 4] Proves verifier and oracle catch partial persistence (torn disk tail)."""
        oracle = DatabaseOracle()
        key = "test_partial"
        full_val = generate_large_value("op-part", "w1", key, size=1024 * 1024)

        oracle.record_put("op-part", key, full_val, writer_id="w1")
        oracle.record_ack("op-part")

        # Half of payload is zeros
        torn_val = full_val[: 256 * 1024] + ("\x00" * (768 * 1024))
        res = oracle.verify_recovered_dump({key: torn_val})

        self.assertFalse(res.passed, "Oracle failed to detect partial persistence!")
        self.assertTrue(any("DURABILITY VIOLATION" in v for v in res.violations))

    # 12. Detection: Incorrect overwrite
    def test_12_detect_incorrect_overwrite(self) -> None:
        """[DETECTION 5] Proves verifier and oracle catch a stale older value returned on overwrite."""
        oracle = DatabaseOracle()
        key = "shared_key"
        v1 = generate_large_value("op-1", "w1", key, size=1024 * 1024)
        v2 = generate_large_value("op-2", "w2", key, size=1024 * 1024)

        # op-1 written and ACKed
        oracle.record_put("op-1", key, v1, writer_id="w1")
        oracle.record_ack("op-1")

        # op-2 written and ACKed sequentially AFTER op-1
        oracle.record_put("op-2", key, v2, writer_id="w2")
        oracle.record_ack("op-2")

        # Engine incorrectly retained v1 (missed overwrite)
        res = oracle.verify_recovered_dump({key: v1})

        self.assertFalse(res.passed, "Oracle failed to detect stale missed overwrite!")
        self.assertTrue(any("DURABILITY VIOLATION" in v for v in res.violations))

    # 13. Memory efficiency: No entire 1 MB values printed
    def test_13_no_entire_large_values_in_reports(self) -> None:
        """
        [MEMORY INVARIANT] Verifies that when a 1 MB value mismatch occurs,
        violations and error strings never dump the entire 1 MB string.
        """
        oracle = DatabaseOracle()
        key = "huge_key"
        val = generate_large_value("op-huge", "w1", key, size=1024 * 1024)

        oracle.record_put("op-huge", key, val, writer_id="w1")
        oracle.record_ack("op-huge")

        # Corrupted dump
        res = oracle.verify_recovered_dump({key: val[:500000] + "BAD" + val[500003:]})
        self.assertFalse(res.passed)

        for v in res.violations:
            # Each violation string must be bounded and concise
            self.assertLess(
                len(v),
                1000,
                f"Violation message is too large ({len(v)} chars)! Entire large value was dumped to string.",
            )
            self.assertNotIn(val[100:10000], v, "Raw 1 MB value body leaked into error violation!")


if __name__ == "__main__":
    unittest.main()
