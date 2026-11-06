"""
Wire protocol implementation and engine communication client for Supernova.

Implements the agreed protocol:
    Commands (sent to ./kvdb serve <path> via stdin):
        PUT <id> <hexkey> <hexvalue>
        DEL <id> <hexkey>
        BATCH <id> P:<hexkey>:<hexvalue> D:<hexkey> ...
        COMPACT <id>

    Responses (received from stdout):
        ACK <id>
        ERR <id> <message>

Concurrency & Safety Guarantees:
- Exactly ONE dedicated stdout reader thread.
- One dedicated stderr drain thread (prevents child process pipe-buffer deadlocks).
- Thread-safe command serialization & stdin writes guarded by a write lock.
- Immediate flushing of stdin after every command.
- Asynchronous response matching by unique operation ID using threading.Event.
- Detection of unknown IDs, malformed responses, unexpected EOF, broken pipes, and timeouts.
- Never treats successful stdin write as an ACK.
- Robust process tracking and resource cleanup (no orphan processes).
"""

from collections import deque
from dataclasses import dataclass, field
from enum import Enum
import itertools
import os
import sys
import re
import signal
import subprocess
import threading
import time
from typing import Deque, Dict, Iterable, List, Optional, Tuple, Union


# --- Utility Functions for Hex Encoding ---

def to_hex(data: Union[str, bytes]) -> str:
    """Encodes string or bytes into lowercase hexadecimal string."""
    if isinstance(data, str):
        return data.encode("utf-8").hex()
    elif isinstance(data, (bytes, bytearray)):
        return bytes(data).hex()
    raise TypeError(f"Expected str or bytes, got {type(data).__name__}")


def from_hex(hex_str: str) -> bytes:
    """Decodes hexadecimal string back to raw bytes."""
    return bytes.fromhex(hex_str.strip())


def from_hex_str(hex_str: str) -> str:
    """Decodes hexadecimal string back to UTF-8 string."""
    return from_hex(hex_str).decode("utf-8", errors="replace")


# --- Custom Protocol Exceptions ---

class ProtocolError(Exception):
    """Base exception for protocol-level errors."""
    pass


class EngineError(ProtocolError):
    """Raised when engine returns an explicit ERR response."""
    def __init__(self, op_id: str, message: str) -> None:
        super().__init__(f"Engine rejected op '{op_id}': {message}")
        self.op_id = op_id
        self.message = message


class EngineTimeoutError(ProtocolError):
    """Raised when an operation does not receive an ACK/ERR within timeout."""
    pass


class UnexpectedEOFError(ProtocolError):
    """Raised when engine process closes stdout unexpectedly while operations are pending."""
    pass


class EngineProcessError(ProtocolError):
    """Raised when engine process fails to start or dies unexpectedly."""
    pass


# --- Data Structures ---

class CommandType(str, Enum):
    PUT = "PUT"
    DEL = "DEL"
    BATCH = "BATCH"
    COMPACT = "COMPACT"


@dataclass(frozen=True)
class BatchItem:
    """
    Sub-operation in a BATCH command:
    - PUT: P:<hexkey>:<hexval>
    - DEL: D:<hexkey>
    """
    is_put: bool
    key_hex: str
    value_hex: Optional[str] = None

    def __post_init__(self) -> None:
        if self.is_put and self.value_hex is None:
            raise ValueError("PUT batch item requires value_hex")
        if not self.is_put and self.value_hex is not None:
            raise ValueError("DEL batch item cannot have value_hex")

    @classmethod
    def make_put(cls, key: Union[str, bytes], value: Union[str, bytes]) -> "BatchItem":
        return cls(is_put=True, key_hex=to_hex(key), value_hex=to_hex(value))

    @classmethod
    def make_del(cls, key: Union[str, bytes]) -> "BatchItem":
        return cls(is_put=False, key_hex=to_hex(key))

    def encode(self) -> str:
        if self.is_put:
            return f"P:{self.key_hex}:{self.value_hex}"
        return f"D:{self.key_hex}"


@dataclass(frozen=True)
class ProtocolResponse:
    """Parsed response from the engine stdout."""
    status: str  # "ACK" or "ERR"
    op_id: str
    message: Optional[str] = None
    is_ack: bool = field(init=False)

    def __post_init__(self) -> None:
        object.__setattr__(self, "is_ack", self.status == "ACK")


class IdGenerator:
    """Thread-safe monotonic operation ID generator."""

    def __init__(self, prefix: str = "op") -> None:
        self._prefix = prefix
        self._counter = itertools.count(1)
        self._lock = threading.Lock()

    def next_id(self) -> str:
        with self._lock:
            val = next(self._counter)
        return f"{self._prefix}-{val}"


@dataclass
class _PendingOp:
    """Internal tracker for an in-flight operation awaiting ACK or ERR."""
    op_id: str
    event: threading.Event = field(default_factory=threading.Event)
    response: Optional[ProtocolResponse] = None
    exception: Optional[Exception] = None


# --- Wire Formatter and Parser ---

class ProtocolCodec:
    """Encodes commands and parses responses according to the agreed wire protocol."""

    # Regex for responses
    ACK_RE = re.compile(r"^ACK\s+(\S+)(?:\s+(.*))?$")
    ERR_RE = re.compile(r"^ERR\s+(\S+)(?:\s+(.*))?$")

    @staticmethod
    def encode_put(op_id: str, key: Union[str, bytes], value: Union[str, bytes]) -> str:
        return f"PUT {op_id} {to_hex(key)} {to_hex(value)}\n"

    @staticmethod
    def encode_del(op_id: str, key: Union[str, bytes]) -> str:
        return f"DEL {op_id} {to_hex(key)}\n"

    @staticmethod
    def encode_get(op_id: str, key: Union[str, bytes]) -> str:
        return f"GET {op_id} {to_hex(key)}\n"

    @staticmethod
    def encode_batch(op_id: str, items: Iterable[BatchItem]) -> str:
        items_str = " ".join(item.encode() for item in items)
        if not items_str:
            raise ValueError("BATCH must contain at least one operation")
        return f"BATCH {op_id} {items_str}\n"

    @staticmethod
    def encode_compact(op_id: str) -> str:
        return f"COMPACT {op_id}\n"

    @classmethod
    def parse_response_line(cls, line: str) -> Tuple[Optional[ProtocolResponse], bool]:
        """
        Parses a line received from stdout.
        Returns:
            (ProtocolResponse, is_malformed=False) if valid ACK or ERR.
            (None, is_malformed=False) if empty or ignorable whitespace.
            (None, is_malformed=True) if non-empty but does not match protocol.
        """
        cleaned = line.strip()
        if not cleaned:
            return None, False

        ack_match = cls.ACK_RE.match(cleaned)
        if ack_match:
            op_id = ack_match.group(1)
            msg = ack_match.group(2) if ack_match.group(2) else None
            return ProtocolResponse(status="ACK", op_id=op_id, message=msg), False

        err_match = cls.ERR_RE.match(cleaned)
        if err_match:
            op_id = err_match.group(1)
            msg = err_match.group(2) if err_match.group(2) else ""
            return ProtocolResponse(status="ERR", op_id=op_id, message=msg), False

        return None, True


# --- Engine Client Process Controller ---

class EngineClient:
    """
    Manages the lifecycle of `./kvdb serve <path>` child process.
    Provides thread-safe command execution, dedicated single-thread stdout reading,
    and asynchronous ACK matching.
    """

    def __init__(
        self,
        engine_bin: str,
        db_path: str,
        id_generator: Optional[IdGenerator] = None,
        default_timeout: float = 5.0,
        max_bytes: Optional[int] = None,
    ) -> None:
        self.engine_bin = engine_bin
        self.db_path = db_path
        self.default_timeout = default_timeout
        self.max_bytes = max_bytes
        self.id_gen = id_generator or IdGenerator()

        self._proc: Optional[subprocess.Popen] = None
        self._write_lock = threading.Lock()
        self._pending_lock = threading.Lock()
        self._pending_ops: Dict[str, _PendingOp] = {}

        # Tracking anomaly counters / logs
        self.unknown_ids: List[str] = []
        self.malformed_responses: List[str] = []
        self.stderr_lines: Deque[str] = deque(maxlen=200)

        # Worker threads
        self._stdout_thread: Optional[threading.Thread] = None
        self._stderr_thread: Optional[threading.Thread] = None
        self._is_running = threading.Event()
        self._eof_reached = threading.Event()

    @property
    def pid(self) -> Optional[int]:
        return self._proc.pid if self._proc else None

    @property
    def is_alive(self) -> bool:
        if self._proc is None:
            return False
        if self._proc.poll() is not None:
            return False
        if self._eof_reached.is_set():
            try:
                self._proc.wait(timeout=0.5)
            except Exception:
                pass
            return self._proc.poll() is None
        return True

    def start(self) -> None:
        """
        Starts the engine in `serve` mode: `./kvdb serve <path>`.
        Configures stdin, stdout, and stderr pipes.
        Spawns the single stdout reader thread and stderr drain thread.
        """
        if self.is_alive:
            raise EngineProcessError("Engine process is already running")

        real_bin = self.engine_bin
        if os.path.isfile(real_bin) and not real_bin.endswith(".exe"):
            try:
                with open(real_bin, "r", encoding="utf-8", errors="ignore") as f:
                    first_line = f.readline().strip()
                if first_line.endswith(".py"):
                    real_bin = os.path.normpath(os.path.join(os.path.dirname(real_bin), first_line))
            except Exception:
                pass

        if real_bin.endswith(".py"):
            cmd = [sys.executable, real_bin, "serve", self.db_path]
        elif sys.platform == "win32" and not real_bin.endswith(".exe") and os.path.isfile(real_bin + ".exe"):
            cmd = [real_bin + ".exe", "serve", self.db_path]
        else:
            cmd = [real_bin, "serve", self.db_path]
        if self.max_bytes is not None:
            cmd.extend(["--max-bytes", str(self.max_bytes)])
        try:
            self._proc = subprocess.Popen(
                cmd,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                bufsize=0,  # Unbuffered binary streams
            )
        except Exception as exc:
            raise EngineProcessError(f"Failed to spawn engine '{self.engine_bin}': {exc}") from exc

        self._is_running.set()
        self._eof_reached.clear()

        # Dedicated single stdout reader thread
        self._stdout_thread = threading.Thread(
            target=self._stdout_reader_loop,
            name="EngineStdoutReader",
            daemon=True,
        )
        self._stdout_thread.start()

        # Dedicated stderr drain thread
        self._stderr_thread = threading.Thread(
            target=self._stderr_drain_loop,
            name="EngineStderrDrain",
            daemon=True,
        )
        self._stderr_thread.start()

    def _stdout_reader_loop(self) -> None:
        """
        CRITICAL: Exactly ONE dedicated thread reading stdout line-by-line.
        Never blocks writers; matches responses asynchronously.
        """
        proc = self._proc
        if proc is None or proc.stdout is None:
            return

        while self._is_running.is_set():
            try:
                line_bytes = proc.stdout.readline()
            except Exception:
                break

            if not line_bytes:
                # EOF reached: pipe closed or process terminated
                break

            line_str = line_bytes.decode("utf-8", errors="replace")
            response, is_malformed = ProtocolCodec.parse_response_line(line_str)

            if is_malformed:
                with self._pending_lock:
                    self.malformed_responses.append(line_str.strip())
                continue

            if response is None:
                continue

            # Look up pending operation
            op_id = response.op_id
            with self._pending_lock:
                pending = self._pending_ops.pop(op_id, None)
                if pending is None:
                    self.unknown_ids.append(op_id)
                else:
                    pending.response = response
                    pending.event.set()

        # EOF or loop termination: fail all remaining pending ops
        self._eof_reached.set()
        with self._pending_lock:
            remaining = list(self._pending_ops.values())
            self._pending_ops.clear()
            for pending in remaining:
                pending.exception = UnexpectedEOFError(
                    f"Child process terminated or stdout EOF before op '{pending.op_id}' was acknowledged"
                )
                pending.event.set()

    def _stderr_drain_loop(self) -> None:
        """Continuously drains stderr to avoid child process pipe-buffer deadlock."""
        proc = self._proc
        if proc is None or proc.stderr is None:
            return

        while self._is_running.is_set():
            try:
                line_bytes = proc.stderr.readline()
            except Exception:
                break
            if not line_bytes:
                break
            line_str = line_bytes.decode("utf-8", errors="replace").strip()
            if line_str:
                self.stderr_lines.append(line_str)

    def _send_command(self, op_id: str, wire_cmd: str, timeout: Optional[float] = None) -> ProtocolResponse:
        """
        Core dispatch routine:
        1. Registers pending op.
        2. Acquires write lock, writes to stdin, and flushes.
        3. Waits for ACK/ERR on pending op's event.
        4. Validates timeouts and errors.
        """
        if not self.is_alive or self._proc is None or self._proc.stdin is None or self._eof_reached.is_set():
            raise EngineProcessError("Cannot send command: engine process is not running or has terminated")

        effective_timeout = timeout if timeout is not None else self.default_timeout
        pending = _PendingOp(op_id=op_id)

        # 1. Register pending op under lock
        with self._pending_lock:
            if self._eof_reached.is_set():
                raise UnexpectedEOFError("Cannot send command: engine stdout reached EOF")
            self._pending_ops[op_id] = pending

        # 2. Thread-safe write to stdin and flush
        raw_bytes = wire_cmd.encode("utf-8")
        try:
            with self._write_lock:
                if self._proc.stdin.closed:
                    raise BrokenPipeError("stdin is closed")
                self._proc.stdin.write(raw_bytes)
                self._proc.stdin.flush()
        except (BrokenPipeError, OSError) as exc:
            with self._pending_lock:
                self._pending_ops.pop(op_id, None)
            raise BrokenPipeError(f"Failed to write op '{op_id}' to engine stdin: {exc}") from exc

        # 3. Wait for response from dedicated stdout reader thread
        signaled = pending.event.wait(effective_timeout)
        if not signaled:
            # Operation timed out
            with self._pending_lock:
                self._pending_ops.pop(op_id, None)
            raise EngineTimeoutError(f"Op '{op_id}' timed out after {effective_timeout}s waiting for response")

        # 4. Check for exception or response
        if pending.exception is not None:
            raise pending.exception

        if pending.response is None:
            raise ProtocolError(f"Op '{op_id}' signaled with neither response nor exception")

        return pending.response

    # --- High-level Public Operations ---

    def put(
        self,
        key: Union[str, bytes],
        value: Union[str, bytes],
        op_id: Optional[str] = None,
        timeout: Optional[float] = None,
    ) -> ProtocolResponse:
        """Submits a PUT operation."""
        chosen_id = op_id or self.id_gen.next_id()
        cmd = ProtocolCodec.encode_put(chosen_id, key, value)
        return self._send_command(chosen_id, cmd, timeout=timeout)

    def delete(
        self,
        key: Union[str, bytes],
        op_id: Optional[str] = None,
        timeout: Optional[float] = None,
    ) -> ProtocolResponse:
        """Submits a DEL operation."""
        chosen_id = op_id or self.id_gen.next_id()
        cmd = ProtocolCodec.encode_del(chosen_id, key)
        return self._send_command(chosen_id, cmd, timeout=timeout)

    def get(
        self,
        key: Union[str, bytes],
        op_id: Optional[str] = None,
        timeout: Optional[float] = None,
    ) -> ProtocolResponse:
        """Submits a GET operation."""
        chosen_id = op_id or self.id_gen.next_id()
        cmd = ProtocolCodec.encode_get(chosen_id, key)
        return self._send_command(chosen_id, cmd, timeout=timeout)

    def batch(
        self,
        items: Iterable[BatchItem],
        op_id: Optional[str] = None,
        timeout: Optional[float] = None,
    ) -> ProtocolResponse:
        """Submits an atomic BATCH operation."""
        chosen_id = op_id or self.id_gen.next_id()
        cmd = ProtocolCodec.encode_batch(chosen_id, items)
        if timeout is None and hasattr(items, "__len__") and len(items) > 1000:
            timeout = max(self.default_timeout, 10.0)
        return self._send_command(chosen_id, cmd, timeout=timeout)

    def compact(
        self,
        op_id: Optional[str] = None,
        timeout: Optional[float] = None,
    ) -> ProtocolResponse:
        """Submits a COMPACT command."""
        chosen_id = op_id or self.id_gen.next_id()
        cmd = ProtocolCodec.encode_compact(chosen_id)
        return self._send_command(chosen_id, cmd, timeout=timeout)

    # --- Lifecycle and Process Termination ---

    def close(self, wait_timeout: float = 1.0) -> int:
        """
        Gracefully closes engine process by closing stdin, waiting for clean exit,
        and terminating if unresponsive.
        """
        self._is_running.clear()
        if self._proc is None:
            return 0

        # Close stdin to signal EOF to engine
        with self._write_lock:
            if self._proc.stdin and not self._proc.stdin.closed:
                try:
                    self._proc.stdin.close()
                except Exception:
                    pass

        # Wait for clean termination
        try:
            ret = self._proc.wait(timeout=wait_timeout)
        except subprocess.TimeoutExpired:
            self._proc.terminate()
            try:
                ret = self._proc.wait(timeout=1.0)
            except subprocess.TimeoutExpired:
                self._proc.kill()
                ret = self._proc.wait()

        self._cleanup_threads()
        return ret

    def kill(self, sig: int = signal.SIGKILL) -> int:
        """
        Adversarially terminates the engine process with SIGKILL (or specified signal).
        Immediately wakes up all in-flight caller threads, verifies termination, and joins workers.
        """
        self._is_running.clear()
        self._eof_reached.set()

        # Immediately fail and wake up all in-flight callers so they never hang
        with self._pending_lock:
            remaining = list(self._pending_ops.values())
            self._pending_ops.clear()
            for pending in remaining:
                pending.exception = UnexpectedEOFError(
                    f"Process killed with signal {sig} before op '{pending.op_id}' was acknowledged"
                )
                pending.event.set()

        if self._proc is None:
            return 0

        try:
            os.kill(self._proc.pid, sig)
        except (ProcessLookupError, PermissionError):
            pass  # Already terminated

        try:
            ret = self._proc.wait(timeout=2.0)
        except subprocess.TimeoutExpired:
            # Force kill if still hung
            self._proc.kill()
            ret = self._proc.wait()

        self._cleanup_threads()
        return ret

    def _cleanup_threads(self) -> None:
        """Ensures reader threads are joined cleanly and stream file descriptors are closed."""
        if self._stdout_thread and self._stdout_thread.is_alive():
            self._stdout_thread.join(timeout=1.0)
        if self._stderr_thread and self._stderr_thread.is_alive():
            self._stderr_thread.join(timeout=1.0)

        if self._proc:
            if self._proc.stdout and not self._proc.stdout.closed:
                try:
                    self._proc.stdout.close()
                except Exception:
                    pass
            if self._proc.stderr and not self._proc.stderr.closed:
                try:
                    self._proc.stderr.close()
                except Exception:
                    pass

    def __enter__(self) -> "EngineClient":
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()
