"""
Unit tests for the Independent Storage Engine Oracle.
Standard library unittest only.

Tests all 9 mandated conditions:
1. ACKed PUT
2. ACKed DEL
3. ACKed BATCH
4. Unacknowledged PUT
5. Unacknowledged BATCH
6. Repeated overwrites
7. Delete followed by PUT
8. Batch with repeated keys
9. Multiple writes to the same key
"""

import unittest
from torture.oracle import (
    BatchSubOp,
    DatabaseOracle,
    OpType,
    VerificationResult,
)


class TestDatabaseOracle(unittest.TestCase):

    def setUp(self) -> None:
        self.oracle = DatabaseOracle()

    # 1. ACKed PUT
    def test_acked_put(self) -> None:
        """Verifies that an acknowledged PUT is mandatory in recovered state."""
        self.oracle.record_put("op-1", "user:1", "alice")
        self.oracle.record_ack("op-1")

        state, prov = self.oracle.compute_definite_state()
        self.assertEqual(state, {"user:1": "alice"})
        self.assertEqual(prov["user:1"].op_id, "op-1")
        self.assertFalse(prov["user:1"].is_deleted)

        # Dump matches -> Pass
        res_pass = self.oracle.verify_recovered_dump({"user:1": "alice"})
        self.assertTrue(res_pass.passed)

        # Dump missing key -> Durability violation
        res_missing = self.oracle.verify_recovered_dump({})
        self.assertFalse(res_missing.passed)
        self.assertTrue(any("DURABILITY VIOLATION" in v for v in res_missing.violations))

    # 2. ACKed DEL
    def test_acked_del(self) -> None:
        """Verifies that an acknowledged DEL properly deletes key in recovered state."""
        self.oracle.record_put("op-1", "user:1", "alice")
        self.oracle.record_ack("op-1")
        self.oracle.record_del("op-2", "user:1")
        self.oracle.record_ack("op-2")

        state, prov = self.oracle.compute_definite_state()
        self.assertNotIn("user:1", state)
        self.assertTrue(prov["user:1"].is_deleted)
        self.assertEqual(prov["user:1"].op_id, "op-2")

        # Dump without key -> Pass
        res_pass = self.oracle.verify_recovered_dump({})
        self.assertTrue(res_pass.passed)

        # Dump with key still present -> Deletion violation
        res_fail = self.oracle.verify_recovered_dump({"user:1": "alice"})
        self.assertFalse(res_fail.passed)
        self.assertTrue(any("DELETION VIOLATION" in v for v in res_fail.violations))

    # 3. ACKed BATCH
    def test_acked_batch(self) -> None:
        """Verifies that an acknowledged atomic batch is completely reflected."""
        items = [
            BatchSubOp(OpType.PUT, "account:A", "100"),
            BatchSubOp(OpType.PUT, "account:B", "200"),
            BatchSubOp(OpType.DEL, "account:C"),
        ]
        self.oracle.record_batch("batch-1", items)
        self.oracle.record_ack("batch-1")

        state, _ = self.oracle.compute_definite_state()
        self.assertEqual(state, {"account:A": "100", "account:B": "200"})

        # Dump matches -> Pass
        res_pass = self.oracle.verify_recovered_dump({"account:A": "100", "account:B": "200"})
        self.assertTrue(res_pass.passed)

        # Torn or missing item -> Durability / atomicity violation
        res_fail = self.oracle.verify_recovered_dump({"account:A": "100"})
        self.assertFalse(res_fail.passed)

    # 4. Unacknowledged PUT
    def test_unacknowledged_put(self) -> None:
        """
        Verifies that an unacknowledged PUT is allowed to be either:
        - 100% present (committed right before crash), OR
        - 100% absent (discarded before commit).
        Never holding random corrupted garbage.
        """
        self.oracle.record_put("op-in-flight", "session:token", "xyz-777")
        # Notice: record_ack is NOT called!

        state, _ = self.oracle.compute_definite_state()
        self.assertEqual(state, {})  # Definite minimum state is empty

        # Case A: Fully present (valid)
        res_present = self.oracle.verify_recovered_dump({"session:token": "xyz-777"})
        self.assertTrue(res_present.passed)

        # Case B: Fully absent (valid)
        res_absent = self.oracle.verify_recovered_dump({})
        self.assertTrue(res_absent.passed)

        # Case C: Unknown garbage value -> Integrity violation
        res_garbage = self.oracle.verify_recovered_dump({"session:token": "corrupted_garbage"})
        self.assertFalse(res_garbage.passed)
        self.assertTrue(any("INTEGRITY VIOLATION" in v for v in res_garbage.violations))

    # 5. Unacknowledged BATCH
    def test_unacknowledged_batch(self) -> None:
        """
        Verifies that an unacknowledged batch must be strictly all-or-nothing (atomic):
        - All items present -> Pass
        - No items present -> Pass
        - Half items present -> Atomicity Violation!
        """
        items = [
            BatchSubOp(OpType.PUT, "tx:from", "debit_50"),
            BatchSubOp(OpType.PUT, "tx:to", "credit_50"),
        ]
        self.oracle.record_batch("batch-tx", items)
        # Not ACKed

        # Case A: All items present -> Pass
        res_all = self.oracle.verify_recovered_dump({"tx:from": "debit_50", "tx:to": "credit_50"})
        self.assertTrue(res_all.passed)

        # Case B: No items present -> Pass
        res_none = self.oracle.verify_recovered_dump({})
        self.assertTrue(res_none.passed)

        # Case C: Half items present -> Atomicity Violation!
        res_half = self.oracle.verify_recovered_dump({"tx:from": "debit_50"})
        self.assertFalse(res_half.passed)
        self.assertTrue(any("ATOMICITY VIOLATION" in v for v in res_half.violations))

    # 6. Repeated Overwrites
    def test_repeated_overwrites(self) -> None:
        """Verifies monotonic overwrite provenance tracking."""
        self.oracle.record_put("op-1", "counter", "1")
        self.oracle.record_ack("op-1")
        self.oracle.record_put("op-2", "counter", "2")
        self.oracle.record_ack("op-2")
        self.oracle.record_put("op-3", "counter", "3")
        self.oracle.record_ack("op-3")

        state, prov = self.oracle.compute_definite_state()
        self.assertEqual(state["counter"], "3")
        self.assertEqual(prov["counter"].op_id, "op-3")

        # Stale value from op-2 in dump -> Durability violation
        res_stale = self.oracle.verify_recovered_dump({"counter": "2"})
        self.assertFalse(res_stale.passed)
        self.assertTrue(any("DURABILITY VIOLATION" in v for v in res_stale.violations))

        # Latest value -> Pass
        res_correct = self.oracle.verify_recovered_dump({"counter": "3"})
        self.assertTrue(res_correct.passed)

    # 7. Delete Followed by PUT
    def test_delete_followed_by_put(self) -> None:
        """Verifies that a subsequent PUT resurrects a deleted key correctly."""
        self.oracle.record_put("op-1", "flag", "true")
        self.oracle.record_ack("op-1")
        self.oracle.record_del("op-2", "flag")
        self.oracle.record_ack("op-2")
        self.oracle.record_put("op-3", "flag", "reborn")
        self.oracle.record_ack("op-3")

        state, prov = self.oracle.compute_definite_state()
        self.assertEqual(state["flag"], "reborn")
        self.assertEqual(prov["flag"].op_id, "op-3")
        self.assertFalse(prov["flag"].is_deleted)

        res = self.oracle.verify_recovered_dump({"flag": "reborn"})
        self.assertTrue(res.passed)

    # 8. Batch with Repeated Keys
    def test_batch_with_repeated_keys(self) -> None:
        """
        Verifies internal sequential ordering within an atomic batch:
        Later sub-operations on the same key override earlier ones.
        """
        items = [
            BatchSubOp(OpType.PUT, "shared_key", "initial"),
            BatchSubOp(OpType.DEL, "shared_key"),
            BatchSubOp(OpType.PUT, "shared_key", "final"),
        ]
        self.oracle.record_batch("batch-multi", items)
        self.oracle.record_ack("batch-multi")

        state, prov = self.oracle.compute_definite_state()
        self.assertEqual(state["shared_key"], "final")
        self.assertEqual(prov["shared_key"].op_id, "batch-multi")

        res_pass = self.oracle.verify_recovered_dump({"shared_key": "final"})
        self.assertTrue(res_pass.passed)

        # Intermediate value should fail
        res_fail = self.oracle.verify_recovered_dump({"shared_key": "initial"})
        self.assertFalse(res_fail.passed)

    # 9. Multiple Writes to Same Key (ACKed + UnACKed)
    def test_multiple_writes_with_unacked(self) -> None:
        """
        Verifies key with ACKed write followed by in-flight unACKed overwrite:
        - Value may be the ACKed value (unACKed discard) -> Pass
        - Value may be the unACKed value (unACKed commit) -> Pass
        - Missing or random garbage -> Fail
        """
        self.oracle.record_put("op-base", "balance", "100")
        self.oracle.record_ack("op-base")

        self.oracle.record_put("op-unacked", "balance", "150")
        # op-unacked is NOT ACKed

        # Case 1: Database holds ACKed 100 -> Pass
        self.assertTrue(self.oracle.verify_recovered_dump({"balance": "100"}).passed)

        # Case 2: Database holds unACKed 150 -> Pass
        self.assertTrue(self.oracle.verify_recovered_dump({"balance": "150"}).passed)

        # Case 3: Database missing balance entirely -> Durability violation!
        res_missing = self.oracle.verify_recovered_dump({})
        self.assertFalse(res_missing.passed)

        # Case 4: Database holds unwritten value 200 -> Integrity violation!
        res_corrupt = self.oracle.verify_recovered_dump({"balance": "200"})
        self.assertFalse(res_corrupt.passed)


    def test_oracle_clone_isolation(self) -> None:
        """Verifies that clone() creates an independent copy of history and records."""
        self.oracle.record_put("op-1", "k1", "v1")
        self.oracle.record_ack("op-1")

        cloned = self.oracle.clone()
        rec_cloned = cloned.get_record("op-1")
        self.assertIsNotNone(rec_cloned)
        self.assertTrue(rec_cloned.acked)

        # Un-ACK the cloned record (like cut_by_1_byte_f4 does for the last batch)
        rec_cloned.acked = False

        # Cloned record is unACKed, but original MUST remain ACKed
        self.assertFalse(cloned.get_record("op-1").acked)
        self.assertTrue(self.oracle.get_record("op-1").acked)

    def test_f4_scoping_durability_enforcement(self) -> None:
        """
        [Item 10 Proof]
        Verifies that seed batches marked ACKed are strictly required by the oracle.
        In F4, only the final truncated batch is allowed to be discarded;
        dropping any earlier batch results in DURABILITY VIOLATION.
        """
        # Batch 0 and Batch 1 are ACKed seed batches
        self.oracle.record_batch(
            "rc-golden-batch-0",
            [BatchSubOp(OpType.PUT, "k0", "v0"), BatchSubOp(OpType.PUT, "k1", "v1")]
        )
        self.oracle.record_ack("rc-golden-batch-0")

        self.oracle.record_batch(
            "rc-golden-batch-1",
            [BatchSubOp(OpType.PUT, "k2", "v2"), BatchSubOp(OpType.PUT, "k3", "v3")]
        )
        self.oracle.record_ack("rc-golden-batch-1")

        # In F4, the last batch is truncated and unACKed in the clone
        f4_oracle = self.oracle.clone()
        last_rec = f4_oracle.get_record("rc-golden-batch-1")
        last_rec.acked = False

        # Valid F4 recovery: batch 0 present, batch 1 discarded
        res_valid = f4_oracle.verify_recovered_dump({"k0": "v0", "k1": "v1"})
        self.assertTrue(res_valid.passed, f"Valid F4 recovery failed: {res_valid.violations}")

        # Invalid F4 recovery (sabotage): batch 0 dropped as well
        res_sabotage_b0 = f4_oracle.verify_recovered_dump({})
        self.assertFalse(res_sabotage_b0.passed, "Dropping ACKed batch 0 in F4 must fail!")
        self.assertTrue(any("DURABILITY VIOLATION" in v for v in res_sabotage_b0.violations))

        # Non-F4 recovery: both batch 0 and batch 1 must be present
        res_non_f4_missing_b1 = self.oracle.verify_recovered_dump({"k0": "v0", "k1": "v1"})
        self.assertFalse(res_non_f4_missing_b1.passed, "Non-F4 must strictly require all seed batches!")
        self.assertTrue(any("DURABILITY VIOLATION" in v for v in res_non_f4_missing_b1.violations))

    def test_errored_put_absent_from_recovered_state(self) -> None:
        """
        [Item 9 Proof - Errored PUT]
        Verifies that an operation that errored (e.g. disk-full) must NOT
        be present in the recovered database. If an engine applies it anyway,
        the Oracle must detect ERRORED OP VIOLATION.
        """
        self.oracle.record_put("op-base", "account:1", "100")
        self.oracle.record_ack("op-base")

        self.oracle.record_put("op-fail", "account:1", "999")
        self.oracle.record_err("op-fail", "disk full (exceeded max-bytes)")

        # Case 1: Database holds pre-error ACKed state (100) -> PASS
        res_ok = self.oracle.verify_recovered_dump({"account:1": "100"})
        self.assertTrue(res_ok.passed, f"Valid recovery failed: {res_ok.violations}")

        # Case 2: Database holds errored write (999) -> FAIL with ERRORED OP VIOLATION
        res_leaked = self.oracle.verify_recovered_dump({"account:1": "999"})
        self.assertFalse(res_leaked.passed, "Leaked errored PUT must fail verification!")
        self.assertTrue(
            any("ERRORED OP VIOLATION" in v for v in res_leaked.violations),
            f"Expected ERRORED OP VIOLATION, got: {res_leaked.violations}"
        )

    def test_errored_del_absent_from_recovered_state(self) -> None:
        """
        [Item 9 Proof - Errored DEL]
        Verifies that an errored deletion does NOT delete the key.
        If an engine deletes the key despite returning an error,
        the Oracle must detect ERRORED OP VIOLATION.
        """
        self.oracle.record_put("op-base", "account:2", "500")
        self.oracle.record_ack("op-base")

        self.oracle.record_del("op-fail-del", "account:2")
        self.oracle.record_err("op-fail-del", "disk full")

        # Case 1: Key remains present with pre-error value -> PASS
        res_ok = self.oracle.verify_recovered_dump({"account:2": "500"})
        self.assertTrue(res_ok.passed, f"Valid recovery failed: {res_ok.violations}")

        # Case 2: Key was deleted by errored op -> FAIL with ERRORED OP VIOLATION
        res_leaked = self.oracle.verify_recovered_dump({})
        self.assertFalse(res_leaked.passed, "Leaked errored DEL must fail verification!")
        self.assertTrue(
            any("ERRORED OP VIOLATION" in v or "DURABILITY VIOLATION" in v for v in res_leaked.violations),
            f"Expected ERRORED OP VIOLATION, got: {res_leaked.violations}"
        )

    def test_errored_batch_absent_from_recovered_state(self) -> None:
        """
        [Item 9 Proof - Errored BATCH]
        Verifies that an errored batch (partial or full) is completely absent.
        """
        self.oracle.record_batch(
            "b-fail",
            [
                BatchSubOp(OpType.PUT, "b_key_1", "b_val_1"),
                BatchSubOp(OpType.PUT, "b_key_2", "b_val_2"),
            ]
        )
        self.oracle.record_err("b-fail", "disk full")

        # Case 1: Completely absent -> PASS
        res_ok = self.oracle.verify_recovered_dump({})
        self.assertTrue(res_ok.passed, f"Valid recovery failed: {res_ok.violations}")

        # Case 2: Partially applied (first item leaked) -> FAIL
        res_partial = self.oracle.verify_recovered_dump({"b_key_1": "b_val_1"})
        self.assertFalse(res_partial.passed, "Partially applied errored batch must fail!")
        self.assertTrue(
            any("ERRORED OP VIOLATION" in v for v in res_partial.violations),
            f"Expected ERRORED OP VIOLATION, got: {res_partial.violations}"
        )

        # Case 3: Fully applied (both items leaked) -> FAIL
        res_full = self.oracle.verify_recovered_dump({"b_key_1": "b_val_1", "b_key_2": "b_val_2"})
        self.assertFalse(res_full.passed, "Fully applied errored batch must fail!")
        self.assertTrue(
            any("ERRORED OP VIOLATION" in v for v in res_full.violations),
            f"Expected ERRORED OP VIOLATION, got: {res_full.violations}"
        )



if __name__ == "__main__":
    unittest.main()

