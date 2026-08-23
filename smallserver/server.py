"""SmallOS-driven, non-blocking HTTP/1.1 server primitives."""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from typing import Any, Callable

from ._transport import KernelTransport, TransportHandle, WakeupChannel
from .http import Headers, Request, Response
from .routing import RouteErrorEvent
from .runtime import ManagedRuntimeConfig

_ROUTE_OBSERVER_SIGNAL = 31


class HTTPParseError(Exception):
    """A request rejected before it can be dispatched to application code."""

    def __init__(self, status: int, detail: str) -> None:
        self.status = status
        self.detail = detail
        super().__init__(detail)


class HTTPRequestParser:
    """Incrementally parse one bounded HTTP/1.1 request with Content-Length."""

    def __init__(
        self,
        max_header_bytes: int,
        max_header_count: int,
        max_body_bytes: int,
        max_request_target_bytes: int = 8 * 1024,
        preserve_trailing_data: bool = False,
    ) -> None:
        self._max_header_bytes = max_header_bytes
        self._max_header_count = max_header_count
        self._max_body_bytes = max_body_bytes
        self._max_request_target_bytes = max_request_target_bytes
        self._preserve_trailing_data = preserve_trailing_data
        self._buffer = bytearray()
        self._request_head: tuple[str, str, Headers, int] | None = None
        self._trailing_data = b""

    @property
    def trailing_data(self) -> bytes:
        """Bytes received after the request body for an explicit protocol handoff."""
        return self._trailing_data

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

        method, raw_target, headers, content_length = self._request_head
        if len(self._buffer) > content_length and not self._preserve_trailing_data:
            raise HTTPParseError(400, "pipelined requests are not supported")
        if len(self._buffer) < content_length:
            return None
        try:
            path, separator, query_string = raw_target.partition("?")
            body = bytes(self._buffer[:content_length])
            self._trailing_data = bytes(self._buffer[content_length:])
            return Request(
                method,
                path,
                headers,
                body,
                "HTTP/1.1",
                raw_target=raw_target,
                query_string=query_string if separator else "",
            )
        except ValueError as exc:
            raise HTTPParseError(400, str(exc)) from exc

    def _parse_head(self, raw: bytes) -> tuple[str, str, Headers, int]:
        try:
            lines = raw.decode("iso-8859-1").split("\r\n")
        except UnicodeDecodeError as exc:  # pragma: no cover - ISO-8859-1 decodes all bytes
            raise HTTPParseError(400, "request headers are not valid bytes") from exc
        if not lines or len(lines[0].split(" ")) != 3:
            raise HTTPParseError(400, "malformed request line")
        method, raw_target, version = lines[0].split(" ")
        if version != "HTTP/1.1" or not raw_target.startswith("/"):
            raise HTTPParseError(400, "only origin-form HTTP/1.1 requests are supported")
        if len(raw_target.encode("iso-8859-1")) > self._max_request_target_bytes:
            raise HTTPParseError(414, "request target is too large")
        if "#" in raw_target or any(not 0x21 <= ord(character) <= 0x7E for character in raw_target):
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
        return method, raw_target, headers, length


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
    max_request_target_bytes: int = 8 * 1024
    max_route_error_events: int = 16
    managed_runtime: ManagedRuntimeConfig | None = None

    def __post_init__(self) -> None:
        for name in (
            "max_connections",
            "max_header_bytes",
            "max_header_count",
            "max_body_bytes",
            "receive_chunk_bytes",
            "listener_priority",
            "connection_priority",
            "accept_batch_size",
            "max_request_target_bytes",
            "max_route_error_events",
        ):
            value = getattr(self, name)
            if type(value) is not int or value <= 0:
                raise ValueError("{} must be a positive integer".format(name))
        if self.managed_runtime is not None and not isinstance(
            self.managed_runtime, ManagedRuntimeConfig
        ):
            raise TypeError("managed_runtime must be a ManagedRuntimeConfig or None")


class RouteObserverChannel:
    """Bounded scheduler-local delivery state for one server invocation."""

    def __init__(self, observer: Any, max_events: int) -> None:
        self.observer = observer
        self.max_events = max_events
        self.events: deque[RouteErrorEvent] = deque()
        self.task: Any = None
        self.accepting = True
        self.dropped = 0
        self.failures = 0

    def bind(self, task: Any) -> None:
        self.task = task

    def enqueue(self, event: RouteErrorEvent, source_task: Any) -> bool:
        if not self.accepting or len(self.events) >= self.max_events:
            self.dropped += 1
            return False
        self.events.append(event)
        try:
            signalled = (
                self.task is not None
                and source_task.sendSignal(self.task.getID(), _ROUTE_OBSERVER_SIGNAL) == 0
            )
        except BaseException:
            signalled = False
        if not signalled:
            self.events.pop()
            self.dropped += 1
            return False
        return True

    def stop(self) -> None:
        self.accepting = False
        self.dropped += len(self.events)
        self.events.clear()
        task = self.task
        if task is not None and not getattr(task, "done", False):
            try:
                task.acceptSignal(_ROUTE_OBSERVER_SIGNAL)
            except BaseException:
                pass


async def run_route_observer(task: Any, channel: RouteObserverChannel) -> None:
    """Drain sanitized events on a dedicated SmallOS task."""
    try:
        while channel.accepting:
            while channel.events:
                event = channel.events.popleft()
                try:
                    channel.observer(event)
                except BaseException:
                    channel.failures += 1
            if channel.accepting:
                await task.wait_signal(_ROUTE_OBSERVER_SIGNAL)
    finally:
        channel.task = None


class ServerHandle:
    """A bound listener and its cooperative shutdown signal."""

    _CAPACITY_SIGNAL = 31

    def __init__(
        self,
        runtime: Any,
        transport: KernelTransport,
        listener: TransportHandle,
        wakeup: WakeupChannel | None,
        config: ServerConfig,
        on_finalized: Callable[[ServerHandle], None] | None = None,
        protocol: str = "http1",
        protocol_config: Any = None,
        route_observer_channel: RouteObserverChannel | None = None,
    ) -> None:
        self._runtime = runtime
        self._transport = transport
        self._listener = listener
        self._wakeup = wakeup
        self._config = config
        self._protocol = protocol
        self._protocol_config = protocol_config
        self._route_observer_channel = route_observer_channel
        self._address = transport.local_address(listener)
        self._on_finalized = on_finalized
        self._close_requested = False
        self._notification_sent = False
        self._finalization_attempted = False
        self._finished = False
        self._failure: BaseException | None = None
        self._cleanup_errors: dict[str, BaseException] = {}
        self._listener_task: Any = None
        self._listener_resumed = False
        self._close_task: Any = None
        self._owned_tasks: list[Any] = []
        self._cancelled_task_ids: set[int] = set()
        self._connections: dict[int, tuple[TransportHandle, Any]] = {}
        self._closing_connections: dict[int, TransportHandle] = {}
        self._pending_task_cancellations: dict[int, Any] = {}
        self._websocket_states: dict[int, Any] = {}
        self._capacity_waiting = False
        self._graceful_connections: set[int] = set()
        self._graceful_closers: dict[int, Callable[[], None]] = {}

    @property
    def address(self) -> tuple[str, int]:
        """Return the bound address, including after the handle is closed."""
        return self._address

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

    async def _wait_for_capacity(self, task: Any) -> None:
        """Block the listener on a scheduler-native signal until capacity changes."""
        self._capacity_waiting = True
        try:
            await task.wait_signal(self._CAPACITY_SIGNAL)
        finally:
            self._capacity_waiting = False

    def _notify_capacity_released(self, previous_count: int) -> None:
        if (
            not self._capacity_waiting
            or previous_count < self._config.max_connections
            or self.owned_connection_count >= self._config.max_connections
            or self._listener_task is None
        ):
            return
        accept_signal = getattr(self._listener_task, "acceptSignal", None)
        try:
            if not callable(accept_signal) or accept_signal(self._CAPACITY_SIGNAL) != 0:
                raise RuntimeError("listener capacity signal failed")
        except BaseException as error:
            self._listener_failed(error, getattr(self._runtime, "cursor", None))

    @property
    def dropped_route_error_events(self) -> int:
        channel = self._route_observer_channel
        return 0 if channel is None else int(channel.dropped)

    @property
    def route_observer_failures(self) -> int:
        channel = self._route_observer_channel
        return 0 if channel is None else int(channel.failures)

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

    def finalize(self) -> None:
        """Release server resources after the caller-owned runtime has stopped.

        Use :meth:`close` while the scheduler is running. This method is the
        manual-runtime escape hatch for a scheduler startup or exit failure.
        """
        self._finish_close(owner_thread=True)

    async def close_from_task(self, task: Any) -> None:
        """Close or retry incomplete cleanup on the SmallOS scheduler thread."""
        if getattr(self._runtime, "cursor", None) is not task:
            raise RuntimeError("close_from_task() requires the currently running SmallOS task")
        if self._finished:
            return
        self._close_requested = True
        self._finalization_attempted = True
        for state in tuple(self._websocket_states.values()):
            request_shutdown = getattr(state, "request_shutdown", None)
            if callable(request_shutdown):
                request_shutdown()
        self._finish_close(current_task=task)

    def _listener_failed(self, exc: BaseException, task: Any) -> None:
        """Record a fatal accept failure and make shutdown observable."""
        if self._failure is None:
            self._failure = exc
        self._close_requested = True
        if self._wakeup is not None and not self._notification_sent:
            try:
                self._wakeup.notify()
                self._notification_sent = True
                return
            except BaseException:
                pass
        elif self._notification_sent:
            return
        self._finish_close(current_task=task)

    def _cancel_task(self, task: Any) -> None:
        cancel_task = getattr(self._runtime, "cancel_task", None)
        if callable(cancel_task):
            try:
                cancel_task(task)
            except BaseException:
                pass

    def _finish_close(
        self, current_task: Any = None, *, owner_thread: bool = False
    ) -> None:
        """Idempotently release every resource owned by this invocation."""
        if self._finished:
            return
        self._close_requested = True
        self._finalization_attempted = True
        for state in tuple(self._websocket_states.values()):
            request_shutdown = getattr(state, "request_shutdown", None)
            if callable(request_shutdown):
                request_shutdown()
        channel = self._route_observer_channel
        if channel is not None:
            channel.stop()
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

        attempted_task_ids: set[int] = set()
        for identity, task in list(self._pending_task_cancellations.items()):
            attempted_task_ids.add(identity)
            if self._cancel_or_retain_task(task):
                self._pending_task_cancellations.pop(identity, None)
                self._cleanup_errors.pop("task:{}".format(identity), None)
                self._cancelled_task_ids.add(identity)

        for identity, (connection, task) in list(self._connections.items()):
            graceful_requested = False
            websocket_owned = identity in self._websocket_states
            if owner_thread:
                if (
                    task is not current_task
                    and id(task) not in self._cancelled_task_ids
                    and id(task) not in attempted_task_ids
                ):
                    attempted_task_ids.add(id(task))
                    if self._cancel_or_retain_task(task):
                        self._cancelled_task_ids.add(id(task))
            elif task is not current_task:
                closer = self._graceful_closers.get(identity)
                if closer is not None:
                    try:
                        closer()
                        graceful_requested = True
                    except BaseException:
                        try:
                            self._runtime.resume_task(task)
                        except BaseException:
                            pass
                else:
                    try:
                        self._runtime.resume_task(task)
                    except BaseException:
                        pass
                if websocket_owned:
                    # The live coordinator owns its bounded Close handshake
                    # and releases the stream through _connection_finished().
                    graceful_requested = True
            if (
                task is not current_task
                and not (not owner_thread and graceful_requested)
            ):
                self._connections.pop(identity, None)
                self._close_or_retain(connection, current_task)

        if owner_thread:
            for task in list(self._owned_tasks):
                if (
                    task is current_task
                    or id(task) in self._cancelled_task_ids
                    or id(task) in attempted_task_ids
                ):
                    continue
                attempted_task_ids.add(id(task))
                if self._cancel_or_retain_task(task):
                    self._cancelled_task_ids.add(id(task))
            # Owner-thread finalization has cancelled the listener task; it
            # must never be resumed by a later scheduler-side cleanup retry.
            self._listener_resumed = True
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
        if identity not in self._connections:
            self._closing_connections[identity] = connection
        error = connection.close_error or RuntimeError("kernel connection close failed")
        self._cleanup_errors["connection:{}".format(identity)] = error
        self._connection_close_failed(error, task, primary_error)
        return False

    def _force_connection_close(
        self,
        connection: TransportHandle,
        task: Any = None,
        primary_error: BaseException | None = None,
    ) -> bool:
        """Stop graceful handling and close through retryable ownership."""
        identity = id(connection)
        self._graceful_connections.discard(identity)
        self._graceful_closers.pop(identity, None)
        return self._close_or_retain(connection, task, primary_error)

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

    def _cancel_or_retain_task(self, task: Any) -> bool:
        identity = id(task)
        cancel_task = getattr(self._runtime, "cancel_task", None)
        try:
            if not callable(cancel_task):
                raise RuntimeError("runtime cannot unregister a server task")
            cancel_task(task)
        except BaseException as exc:
            self._pending_task_cancellations[identity] = task
            self._cleanup_errors["task:{}".format(identity)] = exc
            return False
        self._pending_task_cancellations.pop(identity, None)
        self._cleanup_errors.pop("task:{}".format(identity), None)
        return True

    def _connection_finished(
        self,
        task: Any,
        connection: TransportHandle,
        primary_error: BaseException | None = None,
    ) -> None:
        """Release a completed connection without losing failed-close ownership."""
        previous_count = self.owned_connection_count
        entry = self._connections.pop(id(connection), None)
        self._graceful_connections.discard(id(connection))
        self._graceful_closers.pop(id(connection), None)
        owned_task = entry[1] if entry is not None else task
        if owned_task in self._owned_tasks:
            self._owned_tasks.remove(owned_task)
        self._close_or_retain(connection, task, primary_error)
        self._notify_capacity_released(previous_count)
        self._update_finished()

    def _update_finished(self) -> None:
        wakeup_closed = self._wakeup is None or self._wakeup.closed
        was_finished = self._finished
        self._finished = bool(
            self._close_requested
            and wakeup_closed
            and self._listener.closed
            and not self._connections
            and not self._closing_connections
            and not self._pending_task_cancellations
            and not self._websocket_states
        )
        if self._finished:
            self._cleanup_errors.clear()
            if not was_finished:
                callback = self._on_finalized
                self._on_finalized = None
                if callback is not None:
                    callback(self)

    def _abort_startup(self, tasks: tuple[Any, ...]) -> None:
        """Release bound resources after task registration fails."""
        self._close_requested = True
        self._owned_tasks = list(tasks)
        self._finish_close(owner_thread=True)
