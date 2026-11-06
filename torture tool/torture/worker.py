"""
Workload Generator for Supernova Torture Tool.

Supports multiple concurrent writer threads, each operating with:
- Its own isolated, deterministic `random.Random` instance.
- Unique namespaced operation IDs (e.g. `w1-op-101`, `w2-op-205`).
- Configurable key pool (allowing isolated or overlapping key spaces).
- Robust exception handling when SIGKILL strikes the engine mid-flight.
"""

from enum import Enum
import random
import threading
import time
from typing import List, Optional, Set, Tuple

from torture.config import TortureMode
from torture.oracle import BatchSubOp, DatabaseOracle, OpType
from torture.protocol import (
    BatchItem,
    EngineClient,
    EngineProcessError,
    IdGenerator,
    ProtocolResponse,
    UnexpectedEOFError,
)


class WriterWorkloadType(str, Enum):
    MIXED = "mixed"       # 60% PUT, 20% DEL, 20% BATCH
    PUT_ONLY = "put"      # 100% PUT
    DEL_ONLY = "del"      # 100% DEL
    BATCH_ONLY = "batch"  # 100% BATCH


def generate_large_value(op_id: str, writer_id: str, key: str, size: int = 1024 * 1024) -> str:
    """
    Generates a deterministic, distinguishable large value payload (~1 MB).
    
    Structure:
    - Header: SUPERNOVA_LARGE_VAL:<op_id>:<writer_id>:<key>:len=<size>:
    - Body: Deterministic pattern repeated to exact target size
    
    Guarantees:
    - Exact byte length matching `size`
    - Distinguishable across writers, operations, and keys
    - Instantly detects truncation, corruption, partial persistence, or missed overwrites
    """
    prefix = f"SUPERNOVA_LARGE_VAL:{op_id}:{writer_id}:{key}:len={size}:"
    pattern = f"[{op_id}#{writer_id}]"
    remaining = size - len(prefix)
    if remaining <= 0:
        return prefix[:size]
    repeats = remaining // len(pattern)
    tail = remaining % len(pattern)
    return prefix + (pattern * repeats) + pattern[:tail]


def generate_large_batch(
    size: int,
    key_pool: Optional[List[str]] = None,
    rng: Optional[random.Random] = None,
    mix_type: str = "mixed",
    op_id: str = "batch",
    writer_id: str = "w1",
) -> Tuple[List[BatchSubOp], List[BatchItem]]:
    """
    Constructs high-volume batches containing up to 10,000 sub-operations with zero redundant memory overhead.
    
    Supported Workload Topologies:
    - 'all_put': Pure batch of PUT operations on unique or pooled keys.
    - 'all_del': Pure batch of DELETE operations on existing keys.
    - 'mixed': Balanced interleaved sequence of PUTs and DELs.
    - 'repeated': Dense repetitions and updates over a tiny key set (e.g. 5 keys).
    - 'put_then_del': Sequential pairs of PUT followed immediately by DEL on same key (net: absent).
    - 'del_then_put': Sequential pairs of DEL followed immediately by PUT on same key (net: present).
    """
    r = rng or random.Random(42)
    oracle_items: List[BatchSubOp] = []
    proto_items: List[BatchItem] = []

    if mix_type == "all_put":
        for i in range(size):
            k = key_pool[i % len(key_pool)] if key_pool else f"batch_k_{i}"
            v = f"val_{writer_id}_{op_id}_{i}"
            oracle_items.append(BatchSubOp(OpType.PUT, k, v))
            proto_items.append(BatchItem.make_put(k, v))

    elif mix_type == "all_del":
        for i in range(size):
            k = key_pool[i % len(key_pool)] if key_pool else f"batch_k_{i}"
            oracle_items.append(BatchSubOp(OpType.DEL, k))
            proto_items.append(BatchItem.make_del(k))

    elif mix_type == "put_then_del":
        num_pairs = max(1, size // 2)
        for i in range(num_pairs):
            k = key_pool[i % len(key_pool)] if key_pool else f"ptd_k_{i}"
            v = f"val_ptd_{i}"
            oracle_items.append(BatchSubOp(OpType.PUT, k, v))
            proto_items.append(BatchItem.make_put(k, v))
            oracle_items.append(BatchSubOp(OpType.DEL, k))
            proto_items.append(BatchItem.make_del(k))

    elif mix_type == "del_then_put":
        num_pairs = max(1, size // 2)
        for i in range(num_pairs):
            k = key_pool[i % len(key_pool)] if key_pool else f"dtp_k_{i}"
            v = f"val_dtp_{i}"
            oracle_items.append(BatchSubOp(OpType.DEL, k))
            proto_items.append(BatchItem.make_del(k))
            oracle_items.append(BatchSubOp(OpType.PUT, k, v))
            proto_items.append(BatchItem.make_put(k, v))

    elif mix_type == "repeated":
        small_pool = [f"rep_k_{j}" for j in range(5)]
        for i in range(size):
            k = small_pool[i % 5]
            if r.random() < 0.75:
                v = f"rep_v_{writer_id}_{i}"
                oracle_items.append(BatchSubOp(OpType.PUT, k, v))
                proto_items.append(BatchItem.make_put(k, v))
            else:
                oracle_items.append(BatchSubOp(OpType.DEL, k))
                proto_items.append(BatchItem.make_del(k))

    else:  # "mixed"
        for i in range(size):
            k = key_pool[i % len(key_pool)] if key_pool else f"mix_k_{i}"
            if r.random() < 0.60:
                v = f"val_mix_{writer_id}_{i}"
                oracle_items.append(BatchSubOp(OpType.PUT, k, v))
                proto_items.append(BatchItem.make_put(k, v))
            else:
                oracle_items.append(BatchSubOp(OpType.DEL, k))
                proto_items.append(BatchItem.make_del(k))

    return oracle_items, proto_items


class ConcurrentWriter:
    """
    Independent worker thread for concurrent stress testing.
    """

    def __init__(
        self,
        client: EngineClient,
        oracle: DatabaseOracle,
        rng: random.Random,
        writer_id: str,
        mode: TortureMode = TortureMode.DEFAULT,
        workload_type: WriterWorkloadType = WriterWorkloadType.MIXED,
        key_pool: Optional[List[str]] = None,
    ) -> None:
        self.client = client
        self.oracle = oracle
        self.rng = rng
        self.writer_id = writer_id
        self.mode = mode
        self.workload_type = workload_type

        # Unique namespaced ID generator prevents cross-thread ID collisions
        self.id_gen = IdGenerator(prefix=f"{writer_id}-op")

        # Key pool (shared or isolated). In large-values mode, default to a small shared pool
        if key_pool is not None:
            self._keys = list(key_pool)
        elif self.mode == TortureMode.LARGE_VALUES:
            self._keys = [f"shared_key_{i}" for i in range(4)]
        else:
            self._keys = [f"key_{i:04d}" for i in range(50)]

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
        """Waits for the worker thread to cleanly stop."""
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=timeout)

    def _generate_value(self, key: str = "", op_id: str = "") -> str:
        """Generates payload value based on mode."""
        if self.mode == TortureMode.LARGE_VALUES:
            return generate_large_value(op_id=op_id, writer_id=self.writer_id, key=key, size=1024 * 1024)
        rand_suffix = self.rng.randint(1000, 9999)
        return f"{self.writer_id}_val_{self.submitted_count}_{rand_suffix}"

    def _run_loop(self) -> None:
        """Main submission loop."""
        while not self._stop_event.is_set():
            if not self.client.is_alive:
                break

            # Determine operation type
            if self.workload_type == WriterWorkloadType.PUT_ONLY:
                chosen_type = "PUT"
            elif self.workload_type == WriterWorkloadType.DEL_ONLY:
                chosen_type = "DEL"
            elif self.workload_type == WriterWorkloadType.BATCH_ONLY:
                chosen_type = "BATCH"
            else:
                rand_val = self.rng.random()
                if rand_val < 0.60:
                    chosen_type = "PUT"
                elif rand_val < 0.80:
                    chosen_type = "DEL"
                else:
                    chosen_type = "BATCH"

            try:
                op_id = self.id_gen.next_id()

                if chosen_type == "PUT":
                    key = self.rng.choice(self._keys)
                    val = self._generate_value(key=key, op_id=op_id)

                    self.oracle.record_put(op_id, key, val, writer_id=self.writer_id)
                    self.submitted_count += 1

                    resp = self.client.put(key, val, op_id=op_id)
                    if resp.is_ack:
                        self.oracle.record_ack(op_id)
                        self.acked_count += 1
                    else:
                        self.oracle.record_err(op_id, resp.message or "error")

                elif chosen_type == "DEL":
                    key = self.rng.choice(self._keys)

                    self.oracle.record_del(op_id, key, writer_id=self.writer_id)
                    self.submitted_count += 1

                    resp = self.client.delete(key, op_id=op_id)
                    if resp.is_ack:
                        self.oracle.record_ack(op_id)
                        self.acked_count += 1
                    else:
                        self.oracle.record_err(op_id, resp.message or "error")

                elif chosen_type == "BATCH":
                    if self.mode == TortureMode.LARGE_BATCHES:
                        batch_size = self.rng.randint(50, 200)
                    elif self.mode == TortureMode.LARGE_VALUES:
                        batch_size = self.rng.randint(2, 3)
                    else:
                        batch_size = self.rng.randint(2, 6)

                    batch_oracle_items: List[BatchSubOp] = []
                    batch_proto_items: List[BatchItem] = []

                    for idx in range(batch_size):
                        sub_key = self.rng.choice(self._keys)
                        if self.rng.random() < 0.70:
                            sub_op_id = f"{op_id}-s{idx}"
                            sub_val = self._generate_value(key=sub_key, op_id=sub_op_id)
                            batch_oracle_items.append(BatchSubOp(OpType.PUT, sub_key, sub_val))
                            batch_proto_items.append(BatchItem.make_put(sub_key, sub_val))
                        else:
                            batch_oracle_items.append(BatchSubOp(OpType.DEL, sub_key))
                            batch_proto_items.append(BatchItem.make_del(sub_key))

                    self.oracle.record_batch(op_id, batch_oracle_items, writer_id=self.writer_id)
                    self.submitted_count += 1

                    resp = self.client.batch(batch_proto_items, op_id=op_id)
                    if resp.is_ack:
                        self.oracle.record_ack(op_id)
                        self.acked_count += 1
                    else:
                        self.oracle.record_err(op_id, resp.message or "error")

            except (BrokenPipeError, UnexpectedEOFError, EngineProcessError, OSError) as exc:
                # Normal when SIGKILL strikes engine process while writing to pipe
                self.errors.append(exc)
                break
            except Exception as exc:
                self.errors.append(exc)
                break


# Alias for backward compatibility with CP4
SingleWriterWorker = ConcurrentWriter
