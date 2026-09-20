from __future__ import annotations

import io
import sys
import threading
import traceback as traceback_module
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, Iterator, Literal, TextIO

LogStream = Literal["STDOUT", "STDERR"]


@dataclass(frozen=True, slots=True)
class CapturedLogChunk:
    sequence: int
    stream: LogStream
    content: str
    emitted_at: datetime = field(default_factory=lambda: datetime.now(UTC))


@dataclass(frozen=True, slots=True)
class ExecutionOutcome:
    succeeded: bool
    value: Any = None
    logs: tuple[CapturedLogChunk, ...] = ()
    log_bytes: int = 0
    logs_truncated: bool = False
    error_type: str | None = None
    error_message: str | None = None
    traceback: str | None = None

    @classmethod
    def success(cls, value: Any, capture: CaptureBuffer) -> ExecutionOutcome:
        logs, log_bytes, truncated = capture.snapshot()
        return cls(
            succeeded=True,
            value=value,
            logs=logs,
            log_bytes=log_bytes,
            logs_truncated=truncated,
        )

    @classmethod
    def failure(
        cls,
        error: BaseException,
        capture: CaptureBuffer,
    ) -> ExecutionOutcome:
        logs, log_bytes, truncated = capture.snapshot()
        return cls(
            succeeded=False,
            logs=logs,
            log_bytes=log_bytes,
            logs_truncated=truncated,
            error_type=type(error).__name__,
            error_message=str(error)[:4000],
            traceback=traceback_module.format_exc(limit=50)[-16000:],
        )

    def with_value(self, value: Any) -> ExecutionOutcome:
        return ExecutionOutcome(
            succeeded=self.succeeded,
            value=value,
            logs=self.logs,
            log_bytes=self.log_bytes,
            logs_truncated=self.logs_truncated,
            error_type=self.error_type,
            error_message=self.error_message,
            traceback=self.traceback,
        )


@dataclass(slots=True)
class CaptureBuffer:
    max_bytes: int
    chunk_bytes: int
    capture_stderr: bool = True
    _chunks: list[tuple[LogStream, str, datetime]] = field(default_factory=list)
    _pending_stream: LogStream | None = None
    _pending_content: str = ""
    _pending_at: datetime | None = None
    _bytes: int = 0
    _truncated: bool = False
    _closed: bool = False
    _lock: threading.RLock = field(default_factory=threading.RLock)

    def write(self, stream: LogStream, value: str) -> int:
        if not value:
            return 0
        original_length = len(value)
        if stream == "STDERR" and not self.capture_stderr:
            return original_length
        with self._lock:
            if self._closed:
                return original_length
            encoded = value.encode("utf-8", errors="replace")
            remaining = self.max_bytes - self._bytes
            if remaining <= 0:
                self._truncated = True
                return len(value)
            if len(encoded) > remaining:
                encoded = encoded[:remaining]
                value = encoded.decode("utf-8", errors="ignore")
                encoded = value.encode("utf-8")
                self._truncated = True
            self._append(stream, value, datetime.now(UTC))
            self._bytes += len(encoded)
            return original_length

    def _append(self, stream: LogStream, value: str, emitted_at: datetime) -> None:
        if self._pending_stream is not None and self._pending_stream != stream:
            self._flush_pending()
        if self._pending_stream is None:
            self._pending_stream = stream
        self._pending_content += value
        self._pending_at = emitted_at

        while "\n" in self._pending_content:
            line, remainder = self._pending_content.split("\n", 1)
            self._pending_content = f"{line}\n"
            self._flush_pending()
            self._pending_stream = stream
            self._pending_content = remainder
            self._pending_at = emitted_at if remainder else None

        while len(self._pending_content.encode("utf-8")) >= self.chunk_bytes:
            part, remainder = _split_utf8(self._pending_content, self.chunk_bytes)
            self._pending_content = part
            self._flush_pending()
            self._pending_stream = stream
            self._pending_content = remainder
            self._pending_at = emitted_at if remainder else None

    def _flush_pending(self) -> None:
        if not self._pending_content or self._pending_stream is None:
            self._pending_stream = None
            self._pending_content = ""
            self._pending_at = None
            return
        self._chunks.append(
            (
                self._pending_stream,
                self._pending_content,
                self._pending_at or datetime.now(UTC),
            )
        )
        self._pending_stream = None
        self._pending_content = ""
        self._pending_at = None

    def close(self) -> None:
        with self._lock:
            self._flush_pending()
            self._closed = True

    def snapshot(self) -> tuple[tuple[CapturedLogChunk, ...], int, bool]:
        with self._lock:
            self._flush_pending()
            chunks = tuple(
                CapturedLogChunk(index, stream, content, emitted_at)
                for index, (stream, content, emitted_at) in enumerate(self._chunks)
            )
            return chunks, self._bytes, self._truncated


def _split_utf8(value: str, max_bytes: int) -> tuple[str, str]:
    if max_bytes <= 0:
        return "", value
    encoded = value.encode("utf-8")
    if len(encoded) <= max_bytes:
        return value, ""
    part = encoded[:max_bytes].decode("utf-8", errors="ignore")
    if not part:
        first = value[0]
        return first, value[1:]
    return part, value[len(part) :]


_active_capture: ContextVar[CaptureBuffer | None] = ContextVar(
    "pyscripts_active_output_capture",
    default=None,
)
_install_lock = threading.Lock()


class ContextAwareStream(io.TextIOBase):
    def __init__(self, original: TextIO, stream: LogStream):
        self.original = original
        self.stream = stream

    def write(self, value: str) -> int:
        capture = _active_capture.get()
        if capture is not None and not (
            self.stream == "STDERR" and not capture.capture_stderr
        ):
            return capture.write(self.stream, value)
        return self.original.write(value)

    def flush(self) -> None:
        capture = _active_capture.get()
        if capture is None or (
            self.stream == "STDERR" and not capture.capture_stderr
        ):
            self.original.flush()

    @property
    def encoding(self) -> str | None:
        return self.original.encoding

    @property
    def errors(self) -> str | None:
        return self.original.errors

    def isatty(self) -> bool:
        return self.original.isatty()

    def fileno(self) -> int:
        return self.original.fileno()

    def __getattr__(self, name: str) -> Any:
        return getattr(self.original, name)


def install_output_router() -> None:
    with _install_lock:
        if not isinstance(sys.stdout, ContextAwareStream):
            sys.stdout = ContextAwareStream(sys.stdout, "STDOUT")
        if not isinstance(sys.stderr, ContextAwareStream):
            sys.stderr = ContextAwareStream(sys.stderr, "STDERR")


@contextmanager
def capture_output(
    *,
    max_bytes: int,
    chunk_bytes: int,
    capture_stderr: bool,
) -> Iterator[CaptureBuffer]:
    install_output_router()
    capture = CaptureBuffer(max_bytes, chunk_bytes, capture_stderr)
    token = _active_capture.set(capture)
    try:
        yield capture
    finally:
        capture.close()
        _active_capture.reset(token)
