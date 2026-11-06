"""
Independent Storage Engine Oracle for Supernova Crash-Consistency Torture Tool.

The Oracle maintains an independent ground-truth model of the database state.
It NEVER reads or relies on the database dump to calculate expectations.

Core Responsibilities:
1. Records every operation submitted to the engine with full provenance:
   - Operation ID
   - Operation type (PUT, DEL, BATCH)
   - Key and Value
   - Batch contents
   - Monotonic submission sequence
   - ACK status and timestamp
   - ERR status and message
   - Writer/thread identification
2. Calculates the definitive minimum state guaranteed by durability (all ACKed operations).
3. Evaluates unacknowledged in-flight operations:
   - Unacknowledged operations may be fully present or fully absent.
   - Unacknowledged batches MUST be atomic: 100% applied or 0% applied.
   - Repeated overwrites and deletes are tracked with exact sequence provenance.
4. Verifies a recovered database dump against the oracle's invariants.
"""

from dataclasses import dataclass, field
from enum import Enum
import threading
import time
from typing import Dict, List, Optional, Set, Tuple, Union


class OpType(str, Enum):
    PUT = "PUT"
    DEL = "DEL"
    BATCH = "BATCH"


@dataclass(frozen=True)
class BatchSubOp:
    """Sub-operation within an atomic batch."""
    op_type: OpType  # PUT or DEL
    key: str
    value: Optional[str] = None

    def __post_init__(self) -> None:
        if self.op_type not in (OpType.PUT, OpType.DEL):
            raise ValueError(f"Batch sub-op must be PUT or DEL, got {self.op_type}")
        if self.op_type == OpType.PUT and self.value is None:
            raise ValueError("PUT sub-op requires a value")
        if self.op_type == OpType.DEL and self.value is not None:
            raise ValueError("DEL sub-op cannot have a value")


@dataclass
class OperationRecord:
    """Full historical record of an operation submitted to the storage engine."""
    op_id: str
    op_type: OpType
    seq: int
    writer_id: str
    submitted_at: float
    key: Optional[str] = None
    value: Optional[str] = None
    batch_items: Tuple[BatchSubOp, ...] = field(default_factory=tuple)
    acked: bool = False
    errored: bool = False
    error_message: Optional[str] = None
    resolved_at: Optional[float] = None


@dataclass(frozen=True)
class KeyProvenance:
    """Captures which operation ID and sequence produced a key's state."""
    key: str
    value: Optional[str]
    op_id: str
    seq: int
    is_deleted: bool


@dataclass
class VerificationResult:
    """Detailed result of verifying an actual database dump against the Oracle."""
    passed: bool
    violations: List[str] = field(default_factory=list)
    acknowledged_keys_count: int = 0
    actual_keys_count: int = 0
    unacked_committed_ops: List[str] = field(default_factory=list)
    unacked_discarded_ops: List[str] = field(default_factory=list)


class DatabaseOracle:
    """
    Thread-safe independent state oracle.
    Tracks operations, maintains submission history, and computes expected recovery invariants.
    """

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._seq_counter = 0
        self._history: List[OperationRecord] = []
        self._history_by_id: Dict[str, OperationRecord] = {}

    @property
    def history_length(self) -> int:
        with self._lock:
            return len(self._history)

    def record_put(
        self,
        op_id: str,
        key: str,
        value: str,
        writer_id: str = "main",
    ) -> OperationRecord:
        """Records the submission of a PUT operation."""
        with self._lock:
            if op_id in self._history_by_id:
                raise ValueError(f"Duplicate op_id '{op_id}' in oracle history")
            self._seq_counter += 1
            rec = OperationRecord(
                op_id=op_id,
                op_type=OpType.PUT,
                seq=self._seq_counter,
                writer_id=writer_id,
                submitted_at=time.time(),
                key=key,
                value=value,
            )
            self._history.append(rec)
            self._history_by_id[op_id] = rec
            return rec

    def record_del(
        self,
        op_id: str,
        key: str,
        writer_id: str = "main",
    ) -> OperationRecord:
        """Records the submission of a DEL operation."""
        with self._lock:
            if op_id in self._history_by_id:
                raise ValueError(f"Duplicate op_id '{op_id}' in oracle history")
            self._seq_counter += 1
            rec = OperationRecord(
                op_id=op_id,
                op_type=OpType.DEL,
                seq=self._seq_counter,
                writer_id=writer_id,
                submitted_at=time.time(),
                key=key,
            )
            self._history.append(rec)
            self._history_by_id[op_id] = rec
            return rec

    def record_batch(
        self,
        op_id: str,
        items: List[BatchSubOp],
        writer_id: str = "main",
    ) -> OperationRecord:
        """Records the submission of a BATCH operation."""
        with self._lock:
            if op_id in self._history_by_id:
                raise ValueError(f"Duplicate op_id '{op_id}' in oracle history")
            if not items:
                raise ValueError("Batch items cannot be empty")
            self._seq_counter += 1
            rec = OperationRecord(
                op_id=op_id,
                op_type=OpType.BATCH,
                seq=self._seq_counter,
                writer_id=writer_id,
                submitted_at=time.time(),
                batch_items=tuple(items),
            )
            self._history.append(rec)
            self._history_by_id[op_id] = rec
            return rec

    def record_ack(self, op_id: str) -> None:
        """Marks an operation as successfully acknowledged (fsync durable)."""
        with self._lock:
            rec = self._history_by_id.get(op_id)
            if rec is None:
                raise KeyError(f"Cannot ACK unknown op_id '{op_id}'")
            if rec.acked:
                raise ValueError(f"op_id '{op_id}' is already ACKed")
            rec.acked = True
            rec.resolved_at = time.time()

    def record_err(self, op_id: str, message: str) -> None:
        """Marks an operation as rejected with an explicit error (e.g. disk full)."""
        with self._lock:
            rec = self._history_by_id.get(op_id)
            if rec is None:
                raise KeyError(f"Cannot ERR unknown op_id '{op_id}'")
            rec.errored = True
            rec.error_message = message
            rec.resolved_at = time.time()

    def get_record(self, op_id: str) -> Optional[OperationRecord]:
        with self._lock:
            return self._history_by_id.get(op_id)

    def get_history(self) -> List[OperationRecord]:
        """Returns a copy of all recorded operations in monotonic sequence order."""
        with self._lock:
            return list(self._history)

    # --- Authoritative State Calculation ---

    def compute_definite_state(self) -> Tuple[Dict[str, str], Dict[str, KeyProvenance]]:
        """
        Computes the DEFINITIVE ground truth state:
        Replays ONLY acknowledged operations in sequential order.
        Any acknowledged write MUST be reflected in this state.
        
        Returns:
            Tuple of (definite_state_dict, provenance_dict)
            - definite_state_dict: maps key -> value (keys deleted are absent)
            - provenance_dict: maps key -> KeyProvenance (including tombstone deletions)
        """
        with self._lock:
            state: Dict[str, str] = {}
            provenance: Dict[str, KeyProvenance] = {}

            for op in self._history:
                if not op.acked:
                    continue  # Skip unacknowledged or errored ops

                if op.op_type == OpType.PUT:
                    assert op.key is not None and op.value is not None
                    state[op.key] = op.value
                    provenance[op.key] = KeyProvenance(
                        key=op.key,
                        value=op.value,
                        op_id=op.op_id,
                        seq=op.seq,
                        is_deleted=False,
                    )
                elif op.op_type == OpType.DEL:
                    assert op.key is not None
                    state.pop(op.key, None)
                    provenance[op.key] = KeyProvenance(
                        key=op.key,
                        value=None,
                        op_id=op.op_id,
                        seq=op.seq,
                        is_deleted=True,
                    )
                elif op.op_type == OpType.BATCH:
                    # In a batch, sub-operations execute atomically in their listed order
                    for sub in op.batch_items:
                        if sub.op_type == OpType.PUT:
                            assert sub.value is not None
                            state[sub.key] = sub.value
                            provenance[sub.key] = KeyProvenance(
                                key=sub.key,
                                value=sub.value,
                                op_id=op.op_id,
                                seq=op.seq,
                                is_deleted=False,
                            )
                        elif sub.op_type == OpType.DEL:
                            state.pop(sub.key, None)
                            provenance[sub.key] = KeyProvenance(
                                key=sub.key,
                                value=None,
                                op_id=op.op_id,
                                seq=op.seq,
                                is_deleted=True,
                            )

            return state, provenance

    # --- Verification Engine ---

    def verify_recovered_dump(self, actual_dump: Dict[str, str]) -> VerificationResult:
        """
        Verifies actual database key-value state (from `./kvdb dump`) against the Oracle.

        Invariant Guarantees:
        1. Durability: Every ACKed operation must be reflected unless legally superseded
           by a later valid operation.
        2. Clean Unacknowledged Transitions: UnACKed operations are either 100% present or 100% absent.
        3. Batch Atomicity: A batch is never half-present.
        4. Data Integrity: No key contains unknown garbage or values never written to it.
        """
        with self._lock:
            definite_state, definite_prov = self.compute_definite_state()
            violations: List[str] = []
            unacked_committed: List[str] = []
            unacked_discarded: List[str] = []

            # 1. Collect all valid historical values ever written per key
            historical_values_per_key: Dict[str, Set[str]] = {}
            for op in self._history:
                if op.op_type == OpType.PUT and op.key and op.value:
                    historical_values_per_key.setdefault(op.key, set()).add(op.value)
                elif op.op_type == OpType.BATCH:
                    for sub in op.batch_items:
                        if sub.op_type == OpType.PUT and sub.value:
                            historical_values_per_key.setdefault(sub.key, set()).add(sub.value)

            # 2. Check Data Integrity: No key may hold a value never written to that key
            for key, val in actual_dump.items():
                valid_values = historical_values_per_key.get(key, set())
                if val not in valid_values:
                    violations.append(
                        f"INTEGRITY VIOLATION: Key '{key}' holds value '{val}' which was NEVER written to it! "
                        f"(Known historical values for this key: {valid_values})"
                    )

            # 3. Check Durability: Every ACKed key must be present with the ACKed value
            # (or legitimately modified by a later unACKed operation that committed before crash)
            unacked_ops = [op for op in self._history if not op.acked and not op.errored]
            unacked_keys = set()
            for op in unacked_ops:
                if op.key:
                    unacked_keys.add(op.key)
                if op.op_type == OpType.BATCH:
                    for sub in op.batch_items:
                        unacked_keys.add(sub.key)

            for key, expected_val in definite_state.items():
                if key not in actual_dump:
                    # Missing an ACKed key! Allowed only if a later unACKed DEL deleted it.
                    can_be_deleted_by_unacked = any(
                        (op.op_type == OpType.DEL and op.key == key) or
                        (op.op_type == OpType.BATCH and any(s.op_type == OpType.DEL and s.key == key for s in op.batch_items))
                        for op in unacked_ops
                    )
                    if not can_be_deleted_by_unacked:
                        violations.append(
                            f"DURABILITY VIOLATION: ACKed key '{key}' with value '{expected_val}' is MISSING from recovered database!"
                        )
                else:
                    actual_val = actual_dump[key]
                    if actual_val != expected_val:
                        # Mismatched value: Allowed only if a later unACKed PUT overwrote it
                        can_be_overwritten_by_unacked = any(
                            (op.op_type == OpType.PUT and op.key == key and op.value == actual_val) or
                            (op.op_type == OpType.BATCH and any(s.op_type == OpType.PUT and s.key == key and s.value == actual_val for s in op.batch_items))
                            for op in unacked_ops
                        )
                        if not can_be_overwritten_by_unacked:
                            violations.append(
                                f"DURABILITY VIOLATION: ACKed key '{key}' expected '{expected_val}', but found '{actual_val}'!"
                            )

            # 4. Check Deleted Keys: Keys ACKed as deleted must NOT be present
            # (unless re-created by a later unACKed PUT)
            for key, prov in definite_prov.items():
                if prov.is_deleted and key in actual_dump:
                    can_be_recreated_by_unacked = any(
                        (op.op_type == OpType.PUT and op.key == key) or
                        (op.op_type == OpType.BATCH and any(s.op_type == OpType.PUT and s.key == key for s in op.batch_items))
                        for op in unacked_ops
                    )
                    if not can_be_recreated_by_unacked:
                        violations.append(
                            f"DELETION VIOLATION: Key '{key}' was ACKed as deleted by op '{prov.op_id}', but is still present with value '{actual_dump[key]}'!"
                        )

            # Helper to compute definite state prior to a given operation sequence
            def get_state_before(target_seq: int) -> Dict[str, str]:
                st: Dict[str, str] = {}
                for o in self._history:
                    if o.seq >= target_seq:
                        break
                    if not o.acked:
                        continue
                    if o.op_type == OpType.PUT and o.key and o.value:
                        st[o.key] = o.value
                    elif o.op_type == OpType.DEL and o.key:
                        st.pop(o.key, None)
                    elif o.op_type == OpType.BATCH:
                        for s in o.batch_items:
                            if s.op_type == OpType.PUT and s.value:
                                st[s.key] = s.value
                            elif s.op_type == OpType.DEL:
                                st.pop(s.key, None)
                return st

            # 5. Check Batch Atomicity for all batches (ACKed and unACKed)
            for op in self._history:
                if op.op_type != OpType.BATCH:
                    continue

                state_before = get_state_before(op.seq)

                # Collapse repeated operations within the same batch to the net final sub-op per key
                net_batch_ops: Dict[str, BatchSubOp] = {}
                for sub in op.batch_items:
                    net_batch_ops[sub.key] = sub

                # Determine visibility for informative operations
                informative_effects: List[Tuple[str, bool]] = []
                for sub_key, sub in net_batch_ops.items():
                    if sub.op_type == OpType.PUT:
                        is_vis = (actual_dump.get(sub_key) == sub.value)
                        informative_effects.append((sub_key, is_vis))
                    elif sub.op_type == OpType.DEL:
                        # Deleting an already non-existent key is a no-op; only active deletions are informative
                        if sub_key in state_before:
                            is_vis = (sub_key not in actual_dump)
                            informative_effects.append((sub_key, is_vis))

                if not informative_effects:
                    # Batch consisted solely of deleting non-existent keys (all no-ops)
                    continue

                visible_count = sum(1 for _, vis in informative_effects if vis)
                total_items = len(informative_effects)

                if 0 < visible_count < total_items:
                    # Potential torn batch! Check if the non-visible keys were overwritten by later ops
                    is_torn = False
                    for sub_k, is_vis in informative_effects:
                        if not is_vis:
                            # If no subsequent op touched this key, then this batch was torn!
                            subsequent_writes = [
                                o for o in self._history
                                if o.seq > op.seq and (
                                    o.key == sub_k or
                                    (o.op_type == OpType.BATCH and any(s.key == sub_k for s in o.batch_items))
                                )
                            ]
                            if not subsequent_writes:
                                is_torn = True
                                break

                    if is_torn:
                        violations.append(
                            f"ATOMICITY VIOLATION: Batch '{op.op_id}' was torn! "
                            f"{visible_count} of {total_items} distinct key effects visible in dump."
                        )

                # Classify unACKed batch status
                if not op.acked and not op.errored:
                    if visible_count == total_items:
                        unacked_committed.append(op.op_id)
                    elif visible_count == 0:
                        unacked_discarded.append(op.op_id)
                        unacked_committed.append(op.op_id)
                    elif visible_count == 0:
                        unacked_discarded.append(op.op_id)

            passed = (len(violations) == 0)
            return VerificationResult(
                passed=passed,
                violations=violations,
                acknowledged_keys_count=len(definite_state),
                actual_keys_count=len(actual_dump),
                unacked_committed_ops=unacked_committed,
                unacked_discarded_ops=unacked_discarded,
            )
