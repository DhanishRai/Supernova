"""
Tests for Protocol, CLI Communication, and Process Management.

Includes:
1. Pure unit tests for codec, hex utilities, and thread-safe ID generator.
2. Comprehensive integration tests against a controllable fake engine process.
   (Clearly identified with prefix [FAKE-ENGINE TEST]).
"""

import os
import signal
import sys
import threading
import time
import unittest
from typing import Dict, List

from torture.protocol import (
    BatchItem,
    CommandType,
    EngineClient,
    EngineError,
    EngineProcessError,
    EngineTimeoutError,
    IdGenerator,
    ProtocolCodec,
    ProtocolError,
    ProtocolResponse,
    UnexpectedEOFError,
    from_hex,
    from_hex_str,
    to_hex,
)


class TestProtocolUnit(unittest.TestCase):
    """Unit tests for wire encoding, decoding, and parsing logic."""

    def test_hex_conversion(self) -> None:
        raw_str = "hello:world"
        raw_bytes = b"\x00\xff\xfe\x01data"

        hex_str = to_hex(raw_str)
        self.assertEqual(from_hex_str(hex_str), raw_str)

        hex_bytes = to_hex(raw_bytes)
        self.assertEqual(from_hex(hex_bytes), raw_bytes)

    def test_encode_put(self) -> None:
        cmd = ProtocolCodec.encode_put("op-1", "user:key", "alice")
        expected_key = to_hex("user:key")
        expected_val = to_hex("alice")
        self.assertEqual(cmd, f"PUT op-1 {expected_key} {expected_val}\n")

    def test_encode_del(self) -> None:
        cmd = ProtocolCodec.encode_del("op-2", "user:key")
        expected_key = to_hex("user:key")
        self.assertEqual(cmd, f"DEL op-2 {expected_key}\n")

    def test_encode_batch(self) -> None:
        items = [
            BatchItem.make_put("k1", "v1"),
            BatchItem.make_del("k2"),
            BatchItem.make_put("k3", "v3"),
        ]
        cmd = ProtocolCodec.encode_batch("op-3", items)
        expected = f"BATCH op-3 P:{to_hex('k1')}:{to_hex('v1')} D:{to_hex('k2')} P:{to_hex('k3')}:{to_hex('v3')}\n"
        self.assertEqual(cmd, expected)

    def test_encode_batch_empty_fails(self) -> None:
        with self.assertRaises(ValueError):
            ProtocolCodec.encode_batch("op-bad", [])

    def test_encode_compact(self) -> None:
        cmd = ProtocolCodec.encode_compact("op-compact-1")
        self.assertEqual(cmd, "COMPACT op-compact-1\n")

    def test_parse_ack(self) -> None:
        resp, malformed = ProtocolCodec.parse_response_line("ACK op-100\n")
        self.assertFalse(malformed)
        self.assertIsNotNone(resp)
        assert resp is not None
        self.assertEqual(resp.status, "ACK")
        self.assertEqual(resp.op_id, "op-100")
        self.assertTrue(resp.is_ack)
        self.assertIsNone(resp.message)

    def test_parse_err(self) -> None:
        resp, malformed = ProtocolCodec.parse_response_line("ERR op-101 disk full: out of space\n")
        self.assertFalse(malformed)
        self.assertIsNotNone(resp)
        assert resp is not None
        self.assertEqual(resp.status, "ERR")
        self.assertEqual(resp.op_id, "op-101")
        self.assertFalse(resp.is_ack)
        self.assertEqual(resp.message, "disk full: out of space")

    def test_parse_malformed(self) -> None:
        resp, malformed = ProtocolCodec.parse_response_line("INVALID_COMMAND op-102\n")
        self.assertTrue(malformed)
        self.assertIsNone(resp)

        # Empty line should neither be response nor malformed
        resp_empty, malformed_empty = ProtocolCodec.parse_response_line("   \n")
        self.assertFalse(malformed_empty)
        self.assertIsNone(resp_empty)

    def test_id_generator_thread_safety(self) -> None:
        gen = IdGenerator(prefix="test")
        collected: List[str] = []
        lock = threading.Lock()

        def worker() -> None:
            local = [gen.next_id() for _ in range(100)]
            with lock:
                collected.extend(local)

        threads = [threading.Thread(target=worker) for _ in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        self.assertEqual(len(collected), 500)
        self.assertEqual(len(set(collected)), 500)  # All 500 must be unique


class TestProtocolClientWithFakeEngine(unittest.TestCase):
    """
    Integration tests using the temporary fake CLI process.
    All tests in this suite are [FAKE-ENGINE TEST] verifying process management,
    error recovery, concurrency, and crash resilience.
    """

    def setUp(self) -> None:
        self.test_dir = os.path.dirname(os.path.abspath(__file__))
        self.fake_engine_script = os.path.join(self.test_dir, "fake_engine.py")
        self.db_path = "/tmp/test_torture_mock.db"
        self.engine_cmd = sys.executable

    def _create_client(self, timeout: float = 2.0) -> EngineClient:
        return EngineClient(
            engine_bin=self.fake_engine_script,
            db_path=self.db_path,
            default_timeout=timeout,
        )

    def test_fake_engine_ack_put_del_batch_compact(self) -> None:
        """[FAKE-ENGINE TEST] Verifies standard ACK responses for all operation types."""
        client = self._create_client()
        with client:
            # 1. PUT
            r_put = client.put("user:alice", "developer")
            self.assertTrue(r_put.is_ack)
            self.assertEqual(r_put.status, "ACK")

            # 2. DEL
            r_del = client.delete("user:alice")
            self.assertTrue(r_del.is_ack)

            # 3. BATCH
            items = [
                BatchItem.make_put("k1", "v1"),
                BatchItem.make_del("k2"),
            ]
            r_batch = client.batch(items)
            self.assertTrue(r_batch.is_ack)

            # 4. COMPACT
            r_compact = client.compact()
            self.assertTrue(r_compact.is_ack)

    def test_fake_engine_err_response(self) -> None:
        """[FAKE-ENGINE TEST] Verifies parsing of explicit engine ERR responses."""
        client = self._create_client()
        with client:
            resp = client.put("key", "val TRIGGER_ERR:disk_full")
            self.assertFalse(resp.is_ack)
            self.assertEqual(resp.status, "ERR")
            self.assertEqual(resp.message, "disk_full")

    def test_fake_engine_unknown_id_detection(self) -> None:
        """[FAKE-ENGINE TEST] Verifies detection of unsolicited/ghost ACK IDs."""
        client = self._create_client()
        with client:
            resp = client.put("key TRIGGER_UNKNOWN_ID:ghost-op-999", "val")
            self.assertTrue(resp.is_ack)
            time.sleep(0.05)
            self.assertIn("ghost-op-999", client.unknown_ids)

    def test_fake_engine_malformed_response_detection(self) -> None:
        """[FAKE-ENGINE TEST] Verifies handling and tracking of unparseable response lines."""
        client = self._create_client()
        with client:
            resp = client.put("key TRIGGER_MALFORMED", "val")
            self.assertTrue(resp.is_ack)
            time.sleep(0.05)
            self.assertTrue(len(client.malformed_responses) > 0)
            self.assertIn("CORRUPTED_LINE_NOT_A_VALID_ACK_OR_ERR", client.malformed_responses)

    def test_fake_engine_unexpected_eof(self) -> None:
        """[FAKE-ENGINE TEST] Verifies that abrupt engine crash mid-request raises UnexpectedEOFError."""
        client = self._create_client()
        with client:
            with self.assertRaises(UnexpectedEOFError):
                client.put("key TRIGGER_CRASH", "val")

            # Engine is now dead
            self.assertFalse(client.is_alive)

    def test_fake_engine_broken_pipe(self) -> None:
        """[FAKE-ENGINE TEST] Verifies attempting to write to a crashed engine raises BrokenPipeError or EngineProcessError."""
        client = self._create_client()
        with client:
            # Crash the child process
            try:
                client.put("key TRIGGER_CRASH", "val")
            except UnexpectedEOFError:
                pass

            # Wait briefly for process cleanup
            time.sleep(0.1)

            # Next write must fail immediately
            with self.assertRaises((BrokenPipeError, EngineProcessError)):
                client.put("new_key", "val")

    def test_fake_engine_timeout(self) -> None:
        """[FAKE-ENGINE TEST] Verifies EngineTimeoutError when engine fails to respond within deadline."""
        client = self._create_client(timeout=0.3)
        with client:
            with self.assertRaises(EngineTimeoutError):
                # Trigger a 1.0 second delay with a 0.3 second timeout
                client.put("key TRIGGER_DELAY:1.0", "val")

    def test_fake_engine_sigkill_termination(self) -> None:
        """[FAKE-ENGINE TEST] Verifies explicit adversarial kill() terminates process cleanly."""
        client = self._create_client()
        client.start()
        self.assertTrue(client.is_alive)
        pid = client.pid
        self.assertIsNotNone(pid)

        # Adversarially kill with SIGKILL
        ret = client.kill(signal.SIGKILL)
        self.assertFalse(client.is_alive)
        self.assertEqual(client.close(), ret)

    def test_fake_engine_concurrent_submissions(self) -> None:
        """
        [FAKE-ENGINE TEST] Verifies multiple concurrent worker threads submitting operations
        simultaneously without race condition, data corruption, or deadlock.
        """
        client = self._create_client(timeout=5.0)
        num_threads = 5
        ops_per_thread = 20
        errors: List[Exception] = []
        lock = threading.Lock()

        def worker(thread_idx: int) -> None:
            for i in range(ops_per_thread):
                key = f"thread-{thread_idx}:key-{i}"
                val = f"val-{i}"
                try:
                    resp = client.put(key, val)
                    if not resp.is_ack:
                        with lock:
                            errors.append(RuntimeError(f"Expected ACK, got {resp.status}"))
                except Exception as exc:
                    with lock:
                        errors.append(exc)

        with client:
            threads = [threading.Thread(target=worker, args=(i,)) for i in range(num_threads)]
            for t in threads:
                t.start()
            for t in threads:
                t.join()

        self.assertEqual(len(errors), 0, f"Encountered concurrency errors: {errors}")

    def test_fake_engine_command_interleaving(self) -> None:
        """
        [FAKE-ENGINE TEST] Verifies that out-of-order response arrivals (command interleaving)
        correctly match their respective caller threads without cross-talk.
        """
        client = self._create_client(timeout=3.0)
        results: Dict[str, float] = {}
        lock = threading.Lock()

        def slow_worker() -> None:
            # Op 1 will be delayed by 0.3s
            t0 = time.time()
            resp = client.put("slow_key TRIGGER_DELAY:0.3", "slow_val", op_id="slow-op-1")
            t1 = time.time()
            with lock:
                results[resp.op_id] = t1 - t0

        def fast_worker() -> None:
            # Op 2 will return immediately
            time.sleep(0.05)  # Ensure slow_worker submitted first
            t0 = time.time()
            resp = client.put("fast_key", "fast_val", op_id="fast-op-2")
            t1 = time.time()
            with lock:
                results[resp.op_id] = t1 - t0

        with client:
            t_slow = threading.Thread(target=slow_worker)
            t_fast = threading.Thread(target=fast_worker)
            t_slow.start()
            t_fast.start()
            t_slow.join()
            t_fast.join()

        self.assertIn("slow-op-1", results)
        self.assertIn("fast-op-2", results)
        # fast-op-2 completed much quicker despite being submitted after slow-op-1
        self.assertLess(results["fast-op-2"], results["slow-op-1"])

    def test_fake_engine_stderr_drain(self) -> None:
        """[FAKE-ENGINE TEST] Verifies stderr lines are safely drained without blocking the process."""
        client = self._create_client()
        with client:
            resp = client.put("key TRIGGER_STDERR:test_warning_message", "val")
            self.assertTrue(resp.is_ack)
            time.sleep(0.05)
            self.assertTrue(any("test_warning_message" in line for line in client.stderr_lines))


if __name__ == "__main__":
    unittest.main()
