"""
Single-Writer Workload Generator for Supernova Torture Tool.

Continuously generates and submits pseudorandom PUT, DEL, and BATCH operations
to the database engine client while an independent crash timer counts down.
"""

from enum import Enum
import random
import threading
import time
from typing import List, Optional

from torture.config import TortureMode
from torture.oracle import BatchSubOp, DatabaseOracle, OpType
from torture.protocol import (
    BatchItem,
    EngineClient,
    EngineProcessError,
    ProtocolResponse,
    UnexpectedEOFError,
)


class SingleWriterWorker:
    """
    Spawns a dedicated worker thread that continuously generates operations
    and submits them to the EngineClient while recording submissions and ACKs in the Oracle.
    """

    def __init__(
        self,
        client: EngineClient,
        oracle: DatabaseOracle,
        rng: random.Random,
        mode: TortureMode = TortureMode.DEFAULT,
        key_pool_size: int = 50,
        writer_id: str = "writer-1",
    ) -> None:
        self.client = client
        self.oracle = oracle
        self.rng = rng
        self.mode = mode
        self.key_pool_size = key_pool_size
        self.writer_id = writer_id

        self._keys = [f"key_{i:04d}" for i in range(self.key_pool_size)]
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self.submitted_count = 0
        self.acked_count = 0
        self.errors: List[Exception] = []

    def start(self) -> None:
        """Starts the worker thread."""
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._run_loop,
            name=f"Worker-{self.writer_id}",
            daemon=True,
        )
        self._thread.start()

    def stop(self) -> None:
        """Signals the worker thread to stop."""
        self._stop_event.set()

    def join(self, timeout: float = 2.0) -> None:
        """Waits for the worker thread to terminate."""
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=timeout)

    def _generate_value(self) -> str:
        """Generates a value string based on the active test mode."""
        if self.mode == TortureMode.LARGE_VALUES:
            # 1 MB value for large-values mode
            chunk = "V" * 1024
            return chunk * 1024
        # Standard value: sequence-tagged payload
        rand_suffix = self.rng.randint(1000, 9999)
        return f"val_{self.submitted_count}_{rand_suffix}"

    def _run_loop(self) -> None:
        """Continuous execution loop."""
        while not self._stop_event.is_set():
            if not self.client.is_alive:
                break

            op_choice = self.rng.random()

            try:
                if op_choice < 0.60:
                    # 60% PUT operations
                    key = self.rng.choice(self._keys)
                    val = self._generate_value()
                    op_id = self.client.id_gen.next_id()

                    self.oracle.record_put(op_id, key, val, writer_id=self.writer_id)
                    self.submitted_count += 1

                    resp = self.client.put(key, val, op_id=op_id)
                    if resp.is_ack:
                        self.oracle.record_ack(op_id)
                        self.acked_count += 1
                    else:
                        self.oracle.record_err(op_id, resp.message or "unknown error")

                elif op_choice < 0.80:
                    # 20% DEL operations
                    key = self.rng.choice(self._keys)
                    op_id = self.client.id_gen.next_id()

                    self.oracle.record_del(op_id, key, writer_id=self.writer_id)
                    self.submitted_count += 1

                    resp = self.client.delete(key, op_id=op_id)
                    if resp.is_ack:
                        self.oracle.record_ack(op_id)
                        self.acked_count += 1
                    else:
                        self.oracle.record_err(op_id, resp.message or "unknown error")

                else:
                    # 20% BATCH operations (atomic mix of 2 to 6 operations)
                    batch_size = (
                        self.rng.randint(100, 500)
                        if self.mode == TortureMode.LARGE_BATCHES
                        else self.rng.randint(2, 6)
                    )
                    batch_oracle_items: List[BatchSubOp] = []
                    batch_proto_items: List[BatchItem] = []

                    for _ in range(batch_size):
                        sub_key = self.rng.choice(self._keys)
                        if self.rng.random() < 0.70:
                            sub_val = self._generate_value()
                            batch_oracle_items.append(BatchSubOp(OpType.PUT, sub_key, sub_val))
                            batch_proto_items.append(BatchItem.make_put(sub_key, sub_val))
                        else:
                            batch_oracle_items.append(BatchSubOp(OpType.DEL, sub_key))
                            batch_proto_items.append(BatchItem.make_del(sub_key))

                    op_id = self.client.id_gen.next_id()
                    self.oracle.record_batch(op_id, batch_oracle_items, writer_id=self.writer_id)
                    self.submitted_count += 1

                    resp = self.client.batch(batch_proto_items, op_id=op_id)
                    if resp.is_ack:
                        self.oracle.record_ack(op_id)
                        self.acked_count += 1
                    else:
                        self.oracle.record_err(op_id, resp.message or "unknown error")

            except (BrokenPipeError, UnexpectedEOFError, EngineProcessError) as exc:
                # Expected when SIGKILL lands concurrently!
                self.errors.append(exc)
                break
            except Exception as exc:
                self.errors.append(exc)
                break
