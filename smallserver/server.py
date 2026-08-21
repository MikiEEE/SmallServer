"""SmallOS-driven, non-blocking HTTP/1.1 server primitives."""

from __future__ import annotations

from dataclasses import dataclass
import socket
from typing import Any

from .http import Headers, Request, Response


class HTTPParseError(Exception):
    """A request rejected before it can be dispatched to application code."""

    def __init__(self, status: int, detail: str) -> None:
        self.status = status
        self.detail = detail
        super().__init__(detail)


class HTTPRequestParser:
    """Incrementally parse one bounded HTTP/1.1 request with Content-Length."""

    def __init__(self, max_header_bytes: int, max_header_count: int, max_body_bytes: int) -> None:
        self._max_header_bytes = max_header_bytes
        self._max_header_count = max_header_count
        self._max_body_bytes = max_body_bytes
        self._buffer = bytearray()
        self._request_head: tuple[str, str, Headers, int] | None = None

    def feed(self, data: bytes) -> Request | None:
        self._buffer.extend(data)
        if self._request_head is None:
            marker = self._buffer.find(b"\r\n\r\n")
            if marker < 0:
                if len(self._buffer) > self._max_header_bytes:
                    raise HTTPParseError(413, "request headers are too large")
                return None
            header_length = marker + 4
            if header_length > self._max_header_bytes:
                raise HTTPParseError(413, "request headers are too large")
            self._request_head = self._parse_head(bytes(self._buffer[:marker]))
            del self._buffer[:header_length]

        method, path, headers, content_length = self._request_head
        if len(self._buffer) > content_length:
            raise HTTPParseError(400, "pipelined requests are not supported")
        if len(self._buffer) < content_length:
            return None
        try:
            return Request(method, path, headers, bytes(self._buffer), "HTTP/1.1")
        except ValueError as exc:
            raise HTTPParseError(400, str(exc)) from exc

    def _parse_head(self, raw: bytes) -> tuple[str, str, Headers, int]:
        try:
            lines = raw.decode("iso-8859-1").split("\r\n")
        except UnicodeDecodeError as exc:  # pragma: no cover - ISO-8859-1 decodes all bytes
            raise HTTPParseError(400, "request headers are not valid bytes") from exc
        if not lines or len(lines[0].split(" ")) != 3:
            raise HTTPParseError(400, "malformed request line")
        method, path, version = lines[0].split(" ")
        if version != "HTTP/1.1" or not path.startswith("/"):
            raise HTTPParseError(400, "only origin-form HTTP/1.1 requests are supported")
        if "#" in path or any(not 0x21 <= ord(character) <= 0x7E for character in path):
            raise HTTPParseError(400, "request target is not valid origin-form")
        if len(lines) - 1 > self._max_header_count:
            raise HTTPParseError(413, "too many request headers")
        pairs: list[tuple[str, str]] = []
        content_lengths: list[str] = []
        for line in lines[1:]:
            if ":" not in line:
                raise HTTPParseError(400, "malformed request header")
            name, value = line.split(":", 1)
            value = value.strip(" \t")
            if name.lower() == "transfer-encoding":
                raise HTTPParseError(400, "transfer-encoding is not supported")
            if name.lower() == "content-length":
                content_lengths.append(value)
            pairs.append((name, value))
        if len(content_lengths) > 1:
            raise HTTPParseError(400, "multiple content-length headers are not supported")
        try:
            headers = Headers(pairs)
        except ValueError as exc:
            raise HTTPParseError(400, str(exc)) from exc
        length = 0
        if content_lengths:
            value = content_lengths[0]
            if not value.isascii() or not value.isdecimal():
                raise HTTPParseError(400, "invalid content-length")
            length = int(value)
            if length > self._max_body_bytes:
                raise HTTPParseError(413, "request body is too large")
        if not headers.get("host"):
            raise HTTPParseError(400, "HTTP/1.1 requests require a Host header")
        return method, path, headers, length


@dataclass(frozen=True)
class ServerConfig:
    """Finite resource limits for one SmallServer listener."""

    max_connections: int = 100
    max_header_bytes: int = 16 * 1024
    max_header_count: int = 100
    max_body_bytes: int = 1024 * 1024
    receive_chunk_bytes: int = 8 * 1024
    listener_priority: int = 1
    connection_priority: int = 2

    def __post_init__(self) -> None:
        for name, value in self.__dict__.items():
            if type(value) is not int or value <= 0:
                raise ValueError("{} must be a positive integer".format(name))


class ServerHandle:
    """A bound listener and its cooperative shutdown signal."""

    def __init__(self, runtime: Any, listener: socket.socket, config: ServerConfig) -> None:
        self._runtime = runtime
        self._listener = listener
        self._config = config
        self._wake_read, self._wake_write = socket.socketpair()
        self._wake_read.setblocking(False)
        self._wake_write.setblocking(False)
        self._closed = False
        self._listener_task: Any = None
        self._connections: dict[socket.socket, Any] = {}

    @property
    def address(self) -> tuple[str, int]:
        host, port = self._listener.getsockname()[:2]
        return str(host), int(port)

    @property
    def port(self) -> int:
        return self.address[1]

    @property
    def closed(self) -> bool:
        return self._closed

    def close(self) -> None:
        """Request shutdown safely from any thread without closing live FDs there."""
        if self._closed:
            return
        self._closed = True
        try:
            self._wake_write.send(b"x")
        except (BlockingIOError, OSError):
            pass

    def _finish_close(self) -> None:
        for task in list(self._connections.values()):
            self._runtime.resume_task(task)
        self._connections.clear()
        if self._listener_task is not None:
            self._runtime.resume_task(self._listener_task)
        for sock in (self._listener, self._wake_read, self._wake_write):
            try:
                sock.close()
            except OSError:
                pass

    def _abort_startup(self, tasks: tuple[Any, ...]) -> None:
        """Release bound resources after task registration fails."""
        self._closed = True
        cancel_task = getattr(self._runtime, "cancel_task", None)
        if callable(cancel_task):
            for task in tasks:
                try:
                    cancel_task(task)
                except BaseException:
                    pass
        for sock in (self._listener, self._wake_read, self._wake_write):
            try:
                sock.close()
            except OSError:
                pass


async def _wait_readable(task: Any, sock: socket.socket) -> None:
    await task.wait_readable(sock)


async def _wait_writable(task: Any, sock: socket.socket) -> None:
    await task.wait_writable(sock)
