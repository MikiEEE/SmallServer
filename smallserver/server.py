"""SmallOS-driven, non-blocking HTTP/1.1 server primitives."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from ._transport import KernelTransport, TransportHandle, WakeupChannel
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
    accept_batch_size: int = 16

    def __post_init__(self) -> None:
        for name, value in self.__dict__.items():
            if type(value) is not int or value <= 0:
                raise ValueError("{} must be a positive integer".format(name))


class ServerHandle:
    """A bound listener and its cooperative shutdown signal."""

    def __init__(
        self,
        runtime: Any,
        transport: KernelTransport,
        listener: TransportHandle,
        wakeup: WakeupChannel | None,
        config: ServerConfig,
    ) -> None:
        self._runtime = runtime
        self._transport = transport
        self._listener = listener
        self._wakeup = wakeup
        self._config = config
        self._close_requested = False
        self._notification_sent = False
        self._finalization_attempted = False
        self._finished = False
        self._failure: BaseException | None = None
        self._cleanup_errors: dict[str, BaseException] = {}
        self._listener_task: Any = None
        self._listener_resumed = False
        self._connections: dict[int, tuple[TransportHandle, Any]] = {}
        self._closing_connections: dict[int, TransportHandle] = {}

    @property
    def address(self) -> tuple[str, int]:
        return self._transport.local_address(self._listener)

    @property
    def port(self) -> int:
        return self.address[1]

    @property
    def closed(self) -> bool:
        return self._close_requested

    @property
    def failure(self) -> BaseException | None:
        """Return the fatal listener failure that initiated shutdown, if any."""
        return self._failure

    @property
    def finished(self) -> bool:
        """Whether every kernel-owned server resource closed successfully."""
        return self._finished

    @property
    def cleanup_errors(self) -> tuple[BaseException, ...]:
        """Latest close failures for resources still owned by this server."""
        return tuple(self._cleanup_errors.values())

    @property
    def owned_connection_count(self) -> int:
        """Connections still owned, including streams awaiting close retry."""
        return len(self._connections) + len(self._closing_connections)

    def close(self) -> None:
        """Request external shutdown through a kernel wakeup channel."""
        if self._finished:
            return
        if self._wakeup is None:
            raise RuntimeError(
                "this kernel cannot close a server from outside its scheduler; "
                "use await server.close_from_task(task)"
            )
        self._close_requested = True
        if self._notification_sent:
            if self._finalization_attempted and not self._finished:
                raise RuntimeError(
                    "shutdown cleanup is incomplete; retry it from the scheduler "
                    "with await server.close_from_task(task)"
                )
            return
        # A nonconforming channel may raise. Keep the server unfinished so the
        # caller can retry notification instead of turning close() into a no-op.
        self._wakeup.notify()
        self._notification_sent = True

    async def close_from_task(self, task: Any) -> None:
        """Close or retry incomplete cleanup on the SmallOS scheduler thread."""
        if getattr(self._runtime, "cursor", None) is not task:
            raise RuntimeError("close_from_task() requires the currently running SmallOS task")
        if self._finished:
            return
        self._close_requested = True
        self._finalization_attempted = True
        self._finish_close(current_task=task)

    def _listener_failed(self, exc: BaseException, task: Any) -> None:
        """Record a fatal accept failure and make shutdown observable."""
        if self._failure is None:
            self._failure = exc
        self._close_requested = True
        if self._wakeup is not None:
            try:
                self._wakeup.notify()
                self._notification_sent = True
                return
            except BaseException:
                pass
        self._finish_close(current_task=task)

    def _finish_close(self, current_task: Any = None) -> None:
        if self._finished:
            return
        self._close_requested = True
        self._finalization_attempted = True
        if self._wakeup is not None and not self._wakeup.closed:
            try:
                self._wakeup.close()
            except BaseException as exc:
                self._cleanup_errors["wakeup"] = exc
            else:
                self._cleanup_errors.pop("wakeup", None)

        # Retry resources retained by an earlier close failure once per
        # finalization attempt. Newly failed active connections remain owned
        # for the next attempt rather than being retried in a tight loop.
        for identity, connection in list(self._closing_connections.items()):
            if self._transport.close_safely(connection):
                self._closing_connections.pop(identity, None)
                self._cleanup_errors.pop("connection:{}".format(identity), None)
            else:
                error = connection.close_error or RuntimeError(
                    "kernel connection close failed"
                )
                self._cleanup_errors["connection:{}".format(identity)] = error

        for identity, (connection, task) in list(self._connections.items()):
            if task is not current_task:
                try:
                    self._runtime.resume_task(task)
                except BaseException:
                    pass
                self._connections.pop(identity, None)
                self._close_or_retain(connection, current_task)
        if (
            self._listener_task is not None
            and self._listener_task is not current_task
            and not self._listener_resumed
        ):
            try:
                self._runtime.resume_task(self._listener_task)
            except BaseException:
                pass
            self._listener_resumed = True
        if not self._listener.closed:
            try:
                self._transport.close(self._listener)
            except BaseException as exc:
                self._cleanup_errors["listener"] = exc
            else:
                self._cleanup_errors.pop("listener", None)
        self._update_finished()

    def _close_or_retain(
        self,
        connection: TransportHandle,
        task: Any = None,
        primary_error: BaseException | None = None,
    ) -> bool:
        """Close a connection or retain it and make the close failure fatal."""
        identity = id(connection)
        if self._transport.close_safely(connection):
            self._closing_connections.pop(identity, None)
            self._cleanup_errors.pop("connection:{}".format(identity), None)
            return True
        self._closing_connections[identity] = connection
        error = connection.close_error or RuntimeError("kernel connection close failed")
        self._cleanup_errors["connection:{}".format(identity)] = error
        self._connection_close_failed(error, task, primary_error)
        return False

    def _connection_close_failed(
        self,
        error: BaseException,
        task: Any = None,
        primary_error: BaseException | None = None,
    ) -> None:
        if self._failure is None:
            self._failure = primary_error or error
        self._close_requested = True
        if self._finalization_attempted:
            return
        if self._wakeup is not None and not self._notification_sent:
            try:
                self._wakeup.notify()
            except BaseException:
                self._finish_close(current_task=task)
                return
            self._notification_sent = True
            return
        if self._wakeup is None:
            self._finish_close(current_task=task)

    def _accepted_setup_failed(
        self, primary_error: BaseException, connection: TransportHandle, task: Any
    ) -> None:
        """Take ownership of an accepted stream whose configuration rollback failed."""
        identity = id(connection)
        self._closing_connections[identity] = connection
        error = connection.close_error or RuntimeError("kernel connection close failed")
        self._cleanup_errors["connection:{}".format(identity)] = error
        if self._failure is None:
            self._failure = primary_error
        self._connection_close_failed(error, task, primary_error)

    def _connection_finished(
        self,
        task: Any,
        connection: TransportHandle,
        primary_error: BaseException | None = None,
    ) -> None:
        """Release a completed connection without losing failed-close ownership."""
        self._connections.pop(id(connection), None)
        self._close_or_retain(connection, task, primary_error)
        self._update_finished()

    def _update_finished(self) -> None:
        wakeup_closed = self._wakeup is None or self._wakeup.closed
        self._finished = bool(
            self._close_requested
            and wakeup_closed
            and self._listener.closed
            and not self._connections
            and not self._closing_connections
        )
        if self._finished:
            self._cleanup_errors.clear()

    def _abort_startup(
        self, tasks: tuple[Any, ...]
    ) -> tuple[tuple[Any, BaseException], ...]:
        """Release bound resources after task registration fails."""
        self._close_requested = True
        failures: list[tuple[Any, BaseException]] = []
        cancel_task = getattr(self._runtime, "cancel_task", None)
        for task in reversed(tasks):
            try:
                if callable(cancel_task):
                    cancel_task(task)
                else:
                    task_cancel = getattr(task, "cancel", None)
                    if not callable(task_cancel):
                        raise RuntimeError(
                            "runtime cannot cancel a startup task"
                        )
                    task_cancel()
            except BaseException as exc:
                failures.append((task, exc))
        self._finish_close()
        return tuple(failures)
