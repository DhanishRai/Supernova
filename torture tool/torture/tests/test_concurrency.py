"""
Comprehensive Test Suite for CP5: Concurrent Writer Testing.

Tests all 6 mandated scenarios:
1. Two writers modifying different keys (disjoint keys).
2. Two writers modifying the same keys (concurrent overwrites).
3. Concurrent PUT and DEL operations.
4. Concurrent PUT and BATCH operations.
5. Concurrent large batches.
6. SIGKILL while both writers are active.
"""

import os
import random
import signal
import time
import unittest

from torture.config import TortureConfig, TortureMode
from torture.oracle import DatabaseOracle
from torture.protocol import EngineClient
from torture.verifier import StateVerifier
from torture.worker import ConcurrentWriter, WriterWorkloadType


class TestConcurrencyScenarios(unittest.TestCase):
    """
    [CONCURRENCY TEST SUITE]
    Tests multi-threaded client submission against `./kvdb serve` with randomized SIGKILL crashes.
    """

    def setUp(self) -> None:
        self.test_dir = os.path.dirname(os.path.abspath(__file__))
        self.fake_engine = os.path.join(self.test_dir, "fake_engine.py")
        self.test_db = f"/tmp/torture_concurrency_{int(time.time()*1000)}.db"

    def tearDown(self) -> None:
        parent_dir = os.path.dirname(os.path.abspath(self.test_db))
        base_name = os.path.basename(self.test_db)
        if os.path.exists(parent_dir):
            for fname in os.listdir(parent_dir):
                if fname == base_name or fname.startswith(base_name + "."):
                    try:
                        os.remove(os.path.join(parent_dir, fname))
                    except OSError:
                        pass

    def _run_concurrent_test(
        self,
        writer1_config,
        writer2_config,
        kill_delay_s: float = 0.040,
        mode: TortureMode = TortureMode.DEFAULT,
    ):
        """Helper to orchestrate concurrent writers, kill, and verify recovery."""
        oracle = DatabaseOracle()
        client = EngineClient(engine_bin=self.fake_engine, db_path=self.test_db, default_timeout=2.0)
        client.start()

        rng1 = random.Random(111)
        rng2 = random.Random(222)

        w1 = ConcurrentWriter(
            client=client,
            oracle=oracle,
            rng=rng1,
            writer_id="w1",
            mode=mode,
            workload_type=writer1_config.get("type", WriterWorkloadType.MIXED),
            key_pool=writer1_config.get("keys"),
        )
        w2 = ConcurrentWriter(
            client=client,
            oracle=oracle,
            rng=rng2,
            writer_id="w2",
            mode=mode,
            workload_type=writer2_config.get("type", WriterWorkloadType.MIXED),
            key_pool=writer2_config.get("keys"),
        )

        # Start both writers simultaneously
        w1.start()
        w2.start()

        # Let both writers generate active concurrent traffic
        time.sleep(kill_delay_s)

        # Inject SIGKILL while both writers are active
        client.kill(signal.SIGKILL)

        # Stop and join both writers
        w1.stop()
        w2.stop()
        w1.join(timeout=1.0)
        w2.join(timeout=1.0)
        client.close()

        # Verify no duplicate operation IDs were generated
        history = oracle.get_history()
        op_ids = [op.op_id for op in history]
        self.assertEqual(len(op_ids), len(set(op_ids)), "Duplicate operation IDs detected across concurrent writers!")

        # Verify post-crash recovery
        verifier = StateVerifier(engine_bin=self.fake_engine, db_path=self.test_db)
        passed, violations, dump = verifier.verify(oracle)
        self.assertTrue(passed, f"Recovery verification failed: {violations}")
        return w1, w2, dump

    # Scenario 1: Two writers modifying different keys
    def test_scenario_1_disjoint_keys(self) -> None:
        """[SCENARIO 1] Two concurrent writers modifying completely disjoint key partitions."""
        keys_w1 = [f"part1_k{i}" for i in range(20)]
        keys_w2 = [f"part2_k{i}" for i in range(20)]

        w1, w2, dump = self._run_concurrent_test(
            {"keys": keys_w1, "type": WriterWorkloadType.MIXED},
            {"keys": keys_w2, "type": WriterWorkloadType.MIXED},
            kill_delay_s=0.035,
        )
        self.assertGreater(w1.submitted_count + w2.submitted_count, 0)

    # Scenario 2: Two writers modifying the same keys
    def test_scenario_2_overlapping_keys(self) -> None:
        """[SCENARIO 2] Two concurrent writers heavily contending on the exact same keys."""
        shared_keys = [f"contended_key_{i}" for i in range(5)]

        w1, w2, dump = self._run_concurrent_test(
            {"keys": shared_keys, "type": WriterWorkloadType.MIXED},
            {"keys": shared_keys, "type": WriterWorkloadType.MIXED},
            kill_delay_s=0.040,
        )
        self.assertGreater(w1.submitted_count + w2.submitted_count, 0)

    # Scenario 3: Concurrent PUT and DEL
    def test_scenario_3_concurrent_put_and_del(self) -> None:
        """[SCENARIO 3] Writer 1 continuously PUTs while Writer 2 continuously DELs on shared keys."""
        shared_keys = [f"race_key_{i}" for i in range(10)]

        w1, w2, dump = self._run_concurrent_test(
            {"keys": shared_keys, "type": WriterWorkloadType.PUT_ONLY},
            {"keys": shared_keys, "type": WriterWorkloadType.DEL_ONLY},
            kill_delay_s=0.035,
        )
        self.assertGreater(w1.submitted_count, 0)
        self.assertGreater(w2.submitted_count, 0)

    # Scenario 4: Concurrent PUT and BATCH
    def test_scenario_4_concurrent_put_and_batch(self) -> None:
        """[SCENARIO 4] Writer 1 issues single PUTs while Writer 2 issues atomic multi-op batches."""
        shared_keys = [f"batch_race_key_{i}" for i in range(15)]

        w1, w2, dump = self._run_concurrent_test(
            {"keys": shared_keys, "type": WriterWorkloadType.PUT_ONLY},
            {"keys": shared_keys, "type": WriterWorkloadType.BATCH_ONLY},
            kill_delay_s=0.045,
        )
        self.assertGreater(w1.submitted_count, 0)
        self.assertGreater(w2.submitted_count, 0)

    # Scenario 5: Concurrent large batches
    def test_scenario_5_concurrent_large_batches(self) -> None:
        """[SCENARIO 5] Two writers issuing large multi-key batches concurrently."""
        shared_keys = [f"large_batch_k{i}" for i in range(50)]

        w1, w2, dump = self._run_concurrent_test(
            {"keys": shared_keys, "type": WriterWorkloadType.BATCH_ONLY},
            {"keys": shared_keys, "type": WriterWorkloadType.BATCH_ONLY},
            kill_delay_s=0.050,
            mode=TortureMode.LARGE_BATCHES,
        )
        self.assertGreater(w1.submitted_count + w2.submitted_count, 0)

    # Scenario 6: SIGKILL while both writers are actively writing
    def test_scenario_6_sigkill_while_both_writers_active(self) -> None:
        """[SCENARIO 6] Verifies clean worker teardown when SIGKILL strikes during concurrent writes."""
        shared_keys = [f"kill_active_{i}" for i in range(20)]

        w1, w2, dump = self._run_concurrent_test(
            {"keys": shared_keys, "type": WriterWorkloadType.MIXED},
            {"keys": shared_keys, "type": WriterWorkloadType.MIXED},
            kill_delay_s=0.025,
        )
        # Both workers should have caught process termination without hanging
        self.assertFalse(w1._thread.is_alive())
        self.assertFalse(w2._thread.is_alive())


if __name__ == "__main__":
    unittest.main()
