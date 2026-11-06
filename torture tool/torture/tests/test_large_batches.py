"""
Large Batch Torture Test Suite for Supernova Crash-Consistency Tool.

CP7 Implementation:
- Batches containing up to 10,000 operations.
- Atomicity verification (all-or-nothing execution).
- Batch order semantics for repeated keys.
- Scenarios:
    1. All-PUT batch (10,000 ops)
    2. All-DELETE batch (5,000 ops)
    3. Mixed batch (interleaved PUT and DEL)
    4. Repeated keys within a batch
    5. PUT followed by DELETE on same key
    6. DELETE followed by PUT on same key
    7. Crash during batch transmission (broken pipe)
    8. Crash during batch processing
    9. Crash after durability but before ACK delivery
    10. Oracle strictly rejects torn/partial batch application
    11. Live engine fallback/mock testing
    12. Memory efficiency: no full dump of 10,000 ops during normal runs
"""

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
from torture.verifier import StateVerifier
from torture.worker import generate_large_batch


class TestLargeBatches(unittest.TestCase):
    """
    Test suite for CP7: High-volume atomic batches up to 10,000 operations.
    """

    def setUp(self) -> None:
        self.test_dir = os.path.dirname(os.path.abspath(__file__))
        self.fake_engine = os.path.join(self.test_dir, "fake_engine.py")
        self.real_engine = os.path.abspath("./kvdb")
        self.test_db = f"/tmp/torture_large_batch_{int(time.time() * 1000)}.db"

    def tearDown(self) -> None:
        for suffix in ["", ".wal", ".data", ".lock"]:
            p = self.test_db + suffix
            if os.path.exists(p):
                try:
                    os.remove(p)
                except OSError:
                    pass

    # 1. All-PUT batch (up to 10,000 operations)
    def test_01_all_put_batch(self) -> None:
        """[SCENARIO 1] Submits an atomic batch of 10,000 PUT operations."""
        client = EngineClient(engine_bin=self.fake_engine, db_path=self.test_db)
        client.start()
        oracle = DatabaseOracle()

        batch_size = 10000
        op_id = "batch-all-put-1"
        oracle_items, proto_items = generate_large_batch(
            size=batch_size,
            mix_type="all_put",
            op_id=op_id,
            writer_id="w1",
        )
        self.assertEqual(len(oracle_items), batch_size)
        self.assertEqual(len(proto_items), batch_size)

        # Record submission in Oracle
        oracle.record_batch(op_id, oracle_items, writer_id="w1")

        # Submit to engine
        resp = client.batch(proto_items, op_id=op_id, timeout=15.0)
        self.assertTrue(resp.is_ack)
        oracle.record_ack(op_id)
        client.close()

        # Verify post-recovery
        verifier = StateVerifier(engine_bin=self.fake_engine, db_path=self.test_db)
        passed, violations, dump = verifier.verify(oracle)
        self.assertTrue(passed, f"Recovery verification failed: {violations}")
        self.assertEqual(len(dump), batch_size, f"Expected {batch_size} keys, got {len(dump)}")

    # 2. All-DELETE batch
    def test_02_all_delete_batch(self) -> None:
        """[SCENARIO 2] Pre-seeds 3,000 keys and submits an all-DEL batch deleting them."""
        client = EngineClient(engine_bin=self.fake_engine, db_path=self.test_db)
        client.start()
        oracle = DatabaseOracle()

        keys = [f"seeded_k_{i}" for i in range(3000)]
        seed_op, seed_proto = generate_large_batch(
            size=3000,
            key_pool=keys,
            mix_type="all_put",
            op_id="seed-batch",
            writer_id="w1",
        )
        oracle.record_batch("seed-batch", seed_op, writer_id="w1")
        resp1 = client.batch(seed_proto, op_id="seed-batch", timeout=10.0)
        self.assertTrue(resp1.is_ack)
        oracle.record_ack("seed-batch")

        # Now delete all 3,000 keys in a single all-DEL batch
        del_op, del_proto = generate_large_batch(
            size=3000,
            key_pool=keys,
            mix_type="all_del",
            op_id="del-batch",
            writer_id="w1",
        )
        oracle.record_batch("del-batch", del_op, writer_id="w1")
        resp2 = client.batch(del_proto, op_id="del-batch", timeout=10.0)
        self.assertTrue(resp2.is_ack)
        oracle.record_ack("del-batch")
        client.close()

        # Verify all keys are deleted
        verifier = StateVerifier(engine_bin=self.fake_engine, db_path=self.test_db)
        passed, violations, dump = verifier.verify(oracle)
        self.assertTrue(passed, f"Recovery verification failed: {violations}")
        self.assertEqual(len(dump), 0, f"Expected 0 keys after deletion, found {len(dump)}")

    # 3. Mixed batch (5,000 PUTs and 5,000 DELs)
    def test_03_mixed_batch(self) -> None:
        """[SCENARIO 3] Submits a mixed batch of 10,000 interleaved operations."""
        client = EngineClient(engine_bin=self.fake_engine, db_path=self.test_db)
        client.start()
        oracle = DatabaseOracle()

        op_id = "batch-mixed-1"
        oracle_items, proto_items = generate_large_batch(
            size=10000,
            mix_type="mixed",
            op_id=op_id,
            writer_id="w1",
        )
        oracle.record_batch(op_id, oracle_items, writer_id="w1")

        resp = client.batch(proto_items, op_id=op_id, timeout=15.0)
        self.assertTrue(resp.is_ack)
        oracle.record_ack(op_id)
        client.close()

        verifier = StateVerifier(engine_bin=self.fake_engine, db_path=self.test_db)
        passed, violations, dump = verifier.verify(oracle)
        self.assertTrue(passed, f"Recovery verification failed: {violations}")

    # 4. Repeated keys within a batch
    def test_04_repeated_keys_in_batch(self) -> None:
        """[SCENARIO 4] Repeatedly modifies 5 keys across 2,000 batch operations."""
        client = EngineClient(engine_bin=self.fake_engine, db_path=self.test_db)
        client.start()
        oracle = DatabaseOracle()

        op_id = "batch-rep-1"
        oracle_items, proto_items = generate_large_batch(
            size=2000,
            mix_type="repeated",
            op_id=op_id,
            writer_id="w1",
        )
        oracle.record_batch(op_id, oracle_items, writer_id="w1")

        resp = client.batch(proto_items, op_id=op_id, timeout=10.0)
        self.assertTrue(resp.is_ack)
        oracle.record_ack(op_id)
        client.close()

        # Oracle and engine must agree on the exact sequence order
        verifier = StateVerifier(engine_bin=self.fake_engine, db_path=self.test_db)
        passed, violations, dump = verifier.verify(oracle)
        self.assertTrue(passed, f"Recovery verification failed: {violations}")

    # 5. PUT followed by DELETE on same key
    def test_05_put_followed_by_delete(self) -> None:
        """[SCENARIO 5] Tests 1,000 sequential (PUT k v, DEL k) pairs in a single batch."""
        client = EngineClient(engine_bin=self.fake_engine, db_path=self.test_db)
        client.start()
        oracle = DatabaseOracle()

        op_id = "batch-ptd-1"
        oracle_items, proto_items = generate_large_batch(
            size=2000,  # 1,000 pairs = 2,000 ops
            mix_type="put_then_del",
            op_id=op_id,
            writer_id="w1",
        )
        oracle.record_batch(op_id, oracle_items, writer_id="w1")

        resp = client.batch(proto_items, op_id=op_id, timeout=10.0)
        self.assertTrue(resp.is_ack)
        oracle.record_ack(op_id)
        client.close()

        # All keys must be absent!
        verifier = StateVerifier(engine_bin=self.fake_engine, db_path=self.test_db)
        passed, violations, dump = verifier.verify(oracle)
        self.assertTrue(passed, f"Recovery verification failed: {violations}")
        self.assertEqual(len(dump), 0, "Keys were not deleted by PUT-then-DEL batch!")

    # 6. DELETE followed by PUT on same key
    def test_06_delete_followed_by_put(self) -> None:
        """[SCENARIO 6] Tests 1,000 sequential (DEL k, PUT k v) pairs in a single batch."""
        client = EngineClient(engine_bin=self.fake_engine, db_path=self.test_db)
        client.start()
        oracle = DatabaseOracle()

        op_id = "batch-dtp-1"
        oracle_items, proto_items = generate_large_batch(
            size=2000,  # 1,000 pairs = 2,000 ops
            mix_type="del_then_put",
            op_id=op_id,
            writer_id="w1",
        )
        oracle.record_batch(op_id, oracle_items, writer_id="w1")

        resp = client.batch(proto_items, op_id=op_id, timeout=10.0)
        self.assertTrue(resp.is_ack)
        oracle.record_ack(op_id)
        client.close()

        # All 1,000 keys must be present with their final values!
        verifier = StateVerifier(engine_bin=self.fake_engine, db_path=self.test_db)
        passed, violations, dump = verifier.verify(oracle)
        self.assertTrue(passed, f"Recovery verification failed: {violations}")
        self.assertEqual(len(dump), 1000, f"Expected 1,000 keys, found {len(dump)}")

    # 7. Crash during batch transmission
    def test_07_crash_during_batch_transmission(self) -> None:
        """
        [SCENARIO 7] SIGKILL strikes while transmitting a 10,000-op batch line into stdin.
        Verifies client handles broken pipe cleanly and recovery succeeds.
        """
        client = EngineClient(engine_bin=self.fake_engine, db_path=self.test_db)
        client.start()
        oracle = DatabaseOracle()

        op_id = "batch-trans-kill"
        oracle_items, proto_items = generate_large_batch(
            size=10000,
            mix_type="all_put",
            op_id=op_id,
            writer_id="w1",
        )
        oracle.record_batch(op_id, oracle_items, writer_id="w1")

        import threading
        def delayed_kill():
            time.sleep(0.005)
            client.kill(signal.SIGKILL)

        kill_thread = threading.Thread(target=delayed_kill, daemon=True)
        kill_thread.start()

        caught_error = False
        try:
            client.batch(proto_items, op_id=op_id, timeout=2.0)
        except (BrokenPipeError, UnexpectedEOFError, OSError):
            caught_error = True
        except Exception:
            caught_error = True

        kill_thread.join(timeout=1.0)
        client.close()
        self.assertTrue(caught_error, "Expected transmission pipe break or EOF")

        # Post-crash recovery
        verifier = StateVerifier(engine_bin=self.fake_engine, db_path=self.test_db)
        passed, violations, dump = verifier.verify(oracle)
        self.assertTrue(passed, f"Recovery verification failed after transmission kill: {violations}")

    # 8. Crash during batch processing
    def test_08_crash_during_batch_processing(self) -> None:
        """
        [SCENARIO 8] Transmits 5,000-op batch with TRIGGER_DELAY, then kills during processing.
        """
        client = EngineClient(engine_bin=self.fake_engine, db_path=self.test_db)
        client.start()
        oracle = DatabaseOracle()

        op_id = "batch-proc-kill-TRIGGER_DELAY:0.060"
        oracle_items, proto_items = generate_large_batch(
            size=5000,
            mix_type="all_put",
            op_id=op_id,
            writer_id="w1",
        )
        oracle.record_batch(op_id, oracle_items, writer_id="w1")

        import threading
        def delayed_kill():
            time.sleep(0.020)
            client.kill(signal.SIGKILL)

        kill_thread = threading.Thread(target=delayed_kill, daemon=True)
        kill_thread.start()

        try:
            client.batch(proto_items, op_id=op_id, timeout=0.5)
        except (BrokenPipeError, UnexpectedEOFError, OSError):
            pass

        kill_thread.join(timeout=1.0)
        client.close()

        # Must be all-or-nothing!
        verifier = StateVerifier(engine_bin=self.fake_engine, db_path=self.test_db)
        passed, violations, dump = verifier.verify(oracle)
        self.assertTrue(passed, f"Recovery verification failed: {violations}")

    # 9. Crash after durability but before ACK delivery
    def test_09_crash_after_durability_before_ack(self) -> None:
        """
        [SCENARIO 9] Engine writes batch to WAL and fsyncs, but crashes before sending ACK.
        Verifies Oracle recognizes unACKed batch committed cleanly without atomicity violations.
        """
        client = EngineClient(engine_bin=self.fake_engine, db_path=self.test_db)
        client.start()
        oracle = DatabaseOracle()

        op_id = "batch-sync-crash-TRIGGER_CRASH_AFTER_SYNC"
        oracle_items, proto_items = generate_large_batch(
            size=3000,
            mix_type="all_put",
            op_id=op_id,
            writer_id="w1",
        )
        oracle.record_batch(op_id, oracle_items, writer_id="w1")

        # Client sends batch; engine dies before ACK arrives
        try:
            client.batch(proto_items, op_id=op_id, timeout=2.0)
        except UnexpectedEOFError:
            pass
        except Exception:
            pass

        client.close()

        # The batch WAS made durable before crash.
        # Oracle must accept this atomic application as unacked_committed without errors!
        verifier = StateVerifier(engine_bin=self.fake_engine, db_path=self.test_db)
        passed, violations, dump = verifier.verify(oracle)
        self.assertTrue(passed, f"Oracle failed to accept atomic unACKed durable batch: {violations}")
        self.assertEqual(len(dump), 3000)

    # 10. Oracle strictly rejects torn batch
    def test_10_oracle_rejects_torn_batch(self) -> None:
        """
        [CRITICAL ATOMICITY] Engine sabotaged to persist only half a batch.
        Verifies Oracle strictly detects and flags ATOMICITY VIOLATION.
        """
        client = EngineClient(engine_bin=self.fake_engine, db_path=self.test_db)
        client.start()
        oracle = DatabaseOracle()

        op_id = "batch-torn-TRIGGER_TORN_BATCH"
        oracle_items, proto_items = generate_large_batch(
            size=2000,
            mix_type="all_put",
            op_id=op_id,
            writer_id="w1",
        )
        oracle.record_batch(op_id, oracle_items, writer_id="w1")

        try:
            client.batch(proto_items, op_id=op_id, timeout=2.0)
        except Exception:
            pass
        client.close()

        # Post-crash verification MUST FAIL due to atomicity violation
        verifier = StateVerifier(engine_bin=self.fake_engine, db_path=self.test_db)
        passed, violations, dump = verifier.verify(oracle)
        self.assertFalse(passed, "Oracle accepted a torn batch! Atomicity check failed.")
        self.assertTrue(
            any("ATOMICITY VIOLATION" in v for v in violations),
            f"Expected ATOMICITY VIOLATION in: {violations}",
        )

    # 11. Live engine fallback or actual batch atomicity testing
    def test_11_live_engine_or_mock_testing(self) -> None:
        """
        [LIVE/MOCK TESTING] If real `./kvdb` exists, executes live 1,000-op batch;
        otherwise validates mock engine.
        """
        engine_path = self.real_engine if os.path.exists(self.real_engine) else self.fake_engine
        client = EngineClient(engine_bin=engine_path, db_path=self.test_db)
        client.start()
        oracle = DatabaseOracle()

        op_id = "batch-live-1"
        oracle_items, proto_items = generate_large_batch(
            size=1000,
            mix_type="mixed",
            op_id=op_id,
            writer_id="w1",
        )
        oracle.record_batch(op_id, oracle_items, writer_id="w1")
        resp = client.batch(proto_items, op_id=op_id, timeout=10.0)
        self.assertTrue(resp.is_ack)
        oracle.record_ack(op_id)
        client.close()

        verifier = StateVerifier(engine_bin=engine_path, db_path=self.test_db)
        passed, violations, dump = verifier.verify(oracle)
        self.assertTrue(passed, f"Batch test failed on {engine_path}: {violations}")

    # 12. Memory efficiency: no full dump of 10,000 ops
    def test_12_memory_efficiency_no_print_10000_ops(self) -> None:
        """
        [MEMORY INVARIANT] Verifies that OperationRecord repr and reporting
        never serialize all 10,000 sub-operations into log/console strings.
        """
        op_id = "mem-test"
        oracle_items, _ = generate_large_batch(size=10000, mix_type="all_put", op_id=op_id)
        oracle = DatabaseOracle()
        rec = oracle.record_batch(op_id, oracle_items, writer_id="w1")

        rec_repr = repr(rec)
        self.assertLess(len(rec_repr), 500, f"repr(OperationRecord) is too large ({len(rec_repr)} chars)!")
        self.assertNotIn("batch_k_5000", rec_repr, "Individual batch sub-items leaked into OperationRecord repr!")


if __name__ == "__main__":
    unittest.main()
