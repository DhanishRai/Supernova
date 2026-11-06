"""
Wire protocol definition and serialization for the Supernova storage engine.

Handles formatting operations sent to the engine's `serve` mode via stdin,
and parsing incoming responses (such as `ACK <op-id>`) from stdout.
"""

from dataclasses import dataclass, field
from enum import Enum
import re
from typing import List, Optional, Tuple, Union


class OpType(str, Enum):
    PUT = "put"
    DEL = "del"
    BATCH = "batch"


@dataclass(frozen=True)
class SubOp:
    """
    Sub-operation within an atomic batch.
    """
    op_type: OpType  # PUT or DEL
    key: str
    value: Optional[str] = None

    def __post_init__(self) -> None:
        if self.op_type not in (OpType.PUT, OpType.DEL):
            raise ValueError(f"Batch sub-op must be put or del, got {self.op_type}")
        if not self.key:
            raise ValueError("Batch sub-op key cannot be empty")
        if len(self.key.encode("utf-8")) > 1024:
            raise ValueError("Key exceeds maximum length of 1024 bytes")
        if self.op_type == OpType.PUT and self.value is None:
            raise ValueError("PUT sub-op requires a value")
        if self.op_type == OpType.DEL and self.value is not None:
            raise ValueError("DEL sub-op cannot have a value")


@dataclass(frozen=True)
class Operation:
    """
    A top-level client operation submitted to the engine's serve mode.
    Every operation carries a unique op_id used to match asynchronous ACKs.
    """
    op_id: str
    op_type: OpType
    key: Optional[str] = None
    value: Optional[str] = None
    sub_ops: Tuple[SubOp, ...] = field(default_factory=tuple)

    def __post_init__(self) -> None:
        if not self.op_id or not self.op_id.strip():
            raise ValueError("op_id cannot be empty")
        if self.op_type == OpType.PUT:
            if not self.key:
                raise ValueError("PUT operation requires a key")
            if self.value is None:
                raise ValueError("PUT operation requires a value")
            if len(self.key.encode("utf-8")) > 1024:
                raise ValueError("Key exceeds 1024 bytes limit")
            if len(self.value.encode("utf-8")) > 1_048_576:
                raise ValueError("Value exceeds 1 MB limit")
        elif self.op_type == OpType.DEL:
            if not self.key:
                raise ValueError("DEL operation requires a key")
            if len(self.key.encode("utf-8")) > 1024:
                raise ValueError("Key exceeds 1024 bytes limit")
            if self.value is not None:
                raise ValueError("DEL operation cannot specify a value")
        elif self.op_type == OpType.BATCH:
            if not self.sub_ops:
                raise ValueError("BATCH operation requires at least one sub-op")
            if len(self.sub_ops) > 10_000:
                raise ValueError("BATCH exceeds 10,000 operations limit")

    @classmethod
    def make_put(cls, op_id: str, key: str, value: str) -> "Operation":
        return cls(op_id=str(op_id), op_type=OpType.PUT, key=key, value=value)

    @classmethod
    def make_del(cls, op_id: str, key: str) -> "Operation":
        return cls(op_id=str(op_id), op_type=OpType.DEL, key=key)

    @classmethod
    def make_batch(cls, op_id: str, sub_ops: List[SubOp]) -> "Operation":
        return cls(op_id=str(op_id), op_type=OpType.BATCH, sub_ops=tuple(sub_ops))


class ProtocolCodec:
    """
    Encodes operations for transmission over stdin and decodes engine responses from stdout.
    
    Standard Line Protocol:
      PUT:   `put <op_id> <key> <value>\n`
      DEL:   `del <op_id> <key>\n`
      BATCH: `batch <op_id> <count>\n` followed by `<put|del> <key> [value]\n` for each sub-op.

    Engine Acknowledgements:
      `ACK <op_id>\n`  -> Confirms operation is durable on disk (fsync complete).
      `ERR <op_id> <reason>\n` -> Operation rejected (e.g. disk full / max-bytes reached).
    """

    ACK_PATTERN = re.compile(r"^ACK\s+(\S+)")
    ERR_PATTERN = re.compile(r"^ERR\s+(\S+)(?:\s+(.*))?")

    @staticmethod
    def encode_operation(op: Operation) -> str:
        """
        Serializes an Operation into wire format string to be written to stdin.
        """
        if op.op_type == OpType.PUT:
            return f"put {op.op_id} {op.key} {op.value}\n"
        elif op.op_type == OpType.DEL:
            return f"del {op.op_id} {op.key}\n"
        elif op.op_type == OpType.BATCH:
            lines = [f"batch {op.op_id} {len(op.sub_ops)}"]
            for sub in op.sub_ops:
                if sub.op_type == OpType.PUT:
                    lines.append(f"put {sub.key} {sub.value}")
                else:
                    lines.append(f"del {sub.key}")
            return "\n".join(lines) + "\n"
        raise ValueError(f"Unknown op_type: {op.op_type}")

    @classmethod
    def parse_response_line(cls, line: str) -> Optional[Tuple[str, str, Optional[str]]]:
        """
        Parses a single line received from engine stdout.
        Returns:
            Tuple of (status, op_id, optional_message)
            e.g. ("ACK", "op_123", None)
                 ("ERR", "op_124", "disk full")
            or None if the line is not a recognized protocol response (e.g. debug logging, empty line, torn fragment).
        """
        cleaned = line.strip()
        if not cleaned:
            return None

        ack_match = cls.ACK_PATTERN.match(cleaned)
        if ack_match:
            return ("ACK", ack_match.group(1), None)

        err_match = cls.ERR_PATTERN.match(cleaned)
        if err_match:
            op_id = err_match.group(1)
            reason = err_match.group(2) if err_match.group(2) else "unspecified error"
            return ("ERR", op_id, reason)

        return None


class StreamDecoder:
    """
    Handles streaming stdout bytes from the engine process, buffering fragments
    across reads, and safely handling torn lines when SIGKILL terminates the process.
    """

    def __init__(self) -> None:
        self._buffer = ""

    def feed(self, chunk: str) -> List[Tuple[str, str, Optional[str]]]:
        """
        Feeds new chunk of text from stdout, returning list of complete parsed responses.
        Remaining incomplete line stays in the internal buffer.
        """
        self._buffer += chunk
        lines = self._buffer.split("\n")
        # The last element after split is either an incomplete line or empty string (if chunk ended with \n)
        self._buffer = lines[-1]

        responses = []
        for line in lines[:-1]:
            parsed = ProtocolCodec.parse_response_line(line)
            if parsed is not None:
                responses.append(parsed)
        return responses

    def get_remaining_unparsed(self) -> str:
        """
        Returns any leftover fragment in the buffer (useful for diagnostics after SIGKILL).
        """
        return self._buffer
