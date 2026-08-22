"""Bounded RFC 6455 WebSocket support driven by SmallOS tasks."""

from __future__ import annotations

import base64
from collections import deque
from dataclasses import dataclass
import hashlib
import importlib
import inspect
import math
import time
from typing import Any, AsyncIterator, Callable, Mapping

from .http import Headers, Request, Response

_GUID = b"258EAFA5-E914-47DA-95CA-C5AB0DC85B11"
_DECISION_SIGNAL = 24
_INBOX_SIGNAL = 25
_OUTBOX_SIGNAL = 26
_ACK_SIGNAL = 27
_HTTP_TOKEN_CHARACTERS = frozenset(
    "!#$%&'*+-.^_`|~0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
)


class WebSocketUnavailable(RuntimeError):
    """The optional WebSocket protocol dependency is unavailable."""


class WebSocketStateError(RuntimeError):
    """A WebSocket operation is invalid in the current lifecycle state."""


class WebSocketCapacityError(RuntimeError):
    """A bounded WebSocket mailbox cannot accept another item."""


class WebSocketDisconnect(Exception):
    """The peer or server closed a WebSocket connection."""

    def __init__(self, code: int = 1006, reason: str = "") -> None:
        self.code = code
        self.reason = reason
        super().__init__("WebSocket disconnected ({})".format(code))


@dataclass(frozen=True)
class WebSocketMessage:
    """One complete text or binary WebSocket message."""

    data: str | bytes

    @property
    def is_text(self) -> bool:
        return isinstance(self.data, str)

    @property
    def is_binary(self) -> bool:
        return isinstance(self.data, bytes)

    @property
    def text(self) -> str:
        if not isinstance(self.data, str):
            raise TypeError("WebSocket message is binary")
        return self.data

    @property
    def bytes(self) -> bytes:
        if not isinstance(self.data, bytes):
            raise TypeError("WebSocket message is text")
        return self.data


@dataclass(frozen=True)
class WebSocketConfig:
    """Finite resource and lifetime limits for WebSocket connections."""

    max_frame_payload_bytes: int = 1024 * 1024
    max_message_bytes: int = 1024 * 1024
    max_inbound_messages: int = 16
    max_inbound_bytes: int = 2 * 1024 * 1024
    max_outbound_commands: int = 16
    max_outbound_bytes: int = 2 * 1024 * 1024
    receive_chunk_bytes: int = 16 * 1024
    write_chunk_bytes: int = 16 * 1024
    max_connections: int = 100
    handshake_timeout: float = 10.0
    idle_timeout: float = 300.0
    pong_timeout: float = 10.0
    write_timeout: float = 30.0
    close_timeout: float = 5.0
    deadline_resolution: float = 0.05

    def __post_init__(self) -> None:
        integer_fields = (
            "max_frame_payload_bytes",
            "max_message_bytes",
            "max_inbound_messages",
            "max_inbound_bytes",
            "max_outbound_commands",
            "max_outbound_bytes",
            "receive_chunk_bytes",
            "write_chunk_bytes",
            "max_connections",
        )
        for name in integer_fields:
            value = getattr(self, name)
            if type(value) is not int or value <= 0:
                raise ValueError("{} must be a positive integer".format(name))
        for name in (
            "handshake_timeout",
            "idle_timeout",
            "pong_timeout",
            "write_timeout",
            "close_timeout",
            "deadline_resolution",
        ):
            value = getattr(self, name)
            if type(value) not in (int, float) or not math.isfinite(value) or value <= 0:
                raise ValueError("{} must be a finite positive number".format(name))
        if self.max_frame_payload_bytes > self.max_message_bytes:
            raise ValueError("max_frame_payload_bytes must not exceed max_message_bytes")


@dataclass(frozen=True)
class _WebSocketRoute:
    handler: Callable[[WebSocket], Any]
    origins: frozenset[str] | None
    subprotocols: tuple[str, ...]


@dataclass
class _OutboundCommand:
    event: Any
    size: int
    waiter: Any = None
    done: bool = False
    error: BaseException | None = None


@dataclass(frozen=True)
class _WSProtoAPI:
    Connection: Any
    ConnectionType: Any
    TextMessage: Any
    BytesMessage: Any
    Ping: Any
    Pong: Any
    CloseConnection: Any


def _load_wsproto() -> _WSProtoAPI:
    """Import the optional protocol engine only when an upgrade is served."""
    try:
        connection = importlib.import_module("wsproto.connection")
        events = importlib.import_module("wsproto.events")
    except ImportError as exc:
        raise WebSocketUnavailable(
            "WebSocket routes require the 'smallserver[websocket]' extra"
        ) from exc
    return _WSProtoAPI(
        connection.Connection,
        connection.ConnectionType,
        events.TextMessage,
        events.BytesMessage,
        events.Ping,
        events.Pong,
        events.CloseConnection,
    )


class _FrameGuard:
    """Validate declared client frame sizes before forwarding payload bytes."""

    def __init__(self, max_payload_bytes: int) -> None:
        self._max_payload_bytes = max_payload_bytes
        self._header = bytearray()
        self._header_length: int | None = None
        self._payload_remaining = 0

    def feed(self, data: bytes) -> tuple[bytes, ...]:
        chunks: list[bytes] = []
        offset = 0
        while offset < len(data):
            if self._payload_remaining:
                length = min(self._payload_remaining, len(data) - offset)
                chunks.append(data[offset : offset + length])
                offset += length
                self._payload_remaining -= length
                if self._payload_remaining == 0:
                    self._header_length = None
                continue

            if self._header_length is None:
                needed = 2 - len(self._header)
                if needed:
                    length = min(needed, len(data) - offset)
                    self._header.extend(data[offset : offset + length])
                    offset += length
                    if len(self._header) < 2:
                        continue
                second = self._header[1]
                marker = second & 0x7F
                extension = 2 if marker == 126 else 8 if marker == 127 else 0
                self._header_length = 2 + extension + (4 if second & 0x80 else 0)

            needed = self._header_length - len(self._header)
            if needed:
                length = min(needed, len(data) - offset)
                self._header.extend(data[offset : offset + length])
                offset += length
                if len(self._header) < self._header_length:
                    continue

            first, second = self._header[:2]
            if not second & 0x80:
                raise ValueError("client WebSocket frames must be masked")
            marker = second & 0x7F
            index = 2
            if marker == 126:
                payload_length = int.from_bytes(self._header[index : index + 2], "big")
                index += 2
                if payload_length < 126:
                    raise ValueError("non-minimal WebSocket frame length")
            elif marker == 127:
                payload_length = int.from_bytes(self._header[index : index + 8], "big")
                index += 8
                if payload_length < 65536 or payload_length >> 63:
                    raise ValueError("invalid WebSocket frame length")
            else:
                payload_length = marker
            opcode = first & 0x0F
            if opcode >= 8 and (not first & 0x80 or payload_length > 125):
                raise ValueError("invalid WebSocket control frame")
            if payload_length > self._max_payload_bytes:
                raise WebSocketCapacityError("WebSocket frame payload is too large")
            chunks.append(bytes(self._header))
            self._header.clear()
            self._payload_remaining = payload_length
            if payload_length == 0:
                self._header_length = None
        return tuple(chunks)


class WebSocket:
    """Application-facing WebSocket connection."""

    def __init__(self, state: _WebSocketState) -> None:
        self._state = state

    @property
    def request(self) -> Request:
        return self._state.request

    @property
    def subprotocol(self) -> str | None:
        return self._state.subprotocol

    async def accept(
        self,
        subprotocol: str | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> None:
        await self._state.accept(subprotocol, headers)

    async def reject(self, response: Response) -> None:
        await self._state.reject(response)

    async def receive(self) -> WebSocketMessage:
        return await self._state.receive()

    async def receive_text(self) -> str:
        return (await self.receive()).text

    async def receive_bytes(self) -> bytes:
        return (await self.receive()).bytes

    async def send_text(self, value: str) -> None:
        if not isinstance(value, str):
            raise TypeError("WebSocket text payload must be a string")
        await self._state.send_message(value)

    async def send_bytes(self, value: bytes) -> None:
        if not isinstance(value, bytes):
            raise TypeError("WebSocket binary payload must be bytes")
        await self._state.send_message(value)

    async def ping(self, payload: bytes = b"") -> None:
        if not isinstance(payload, bytes) or len(payload) > 125:
            raise ValueError("WebSocket Ping payload must be at most 125 bytes")
        await self._state.ping(payload)

    async def close(self, code: int = 1000, reason: str = "") -> None:
        await self._state.close(code, reason)

    def __aiter__(self) -> AsyncIterator[WebSocketMessage]:
        return self

    async def __anext__(self) -> WebSocketMessage:
        try:
            return await self.receive()
        except WebSocketDisconnect as exc:
            raise StopAsyncIteration from exc


class _WebSocketState:
    """One coordinator-owned WebSocket protocol and mailbox state."""

    def __init__(
        self,
        runtime: Any,
        transport: Any,
        client: Any,
        request: Request,
        route: _WebSocketRoute,
        config: WebSocketConfig,
        trailing_data: bytes,
    ) -> None:
        self.runtime = runtime
        self.transport = transport
        self.client = client
        self.request = request
        self.route = route
        self.config = config
        self.trailing_data = trailing_data
        self.api = _load_wsproto()
        self.protocol: Any = None
        self.coordinator_task: Any = None
        self.handler_task: Any = None
        self.reader_task: Any = None
        self.writer_task: Any = None
        self.deadline_task: Any = None
        self.accepted = False
        self.rejected = False
        self.handshake_state = "pending"
        self.handshake_error: BaseException | None = None
        self.shutdown = False
        self.peer_closed = False
        self.close_sent = False
        self.subprotocol: str | None = None
        self.disconnect: WebSocketDisconnect | None = None
        self.handler_error: BaseException | None = None
        self.fatal_error: BaseException | None = None
        self.inbox: deque[WebSocketMessage] = deque()
        self.inbox_bytes = 0
        self.outbox: deque[_OutboundCommand] = deque()
        self.outbox_bytes = 0
        self.writer_busy = False
        self.active_command: _OutboundCommand | None = None
        self._message_kind: type | None = None
        self._message_buffer = bytearray()
        self._message_bytes = 0
        self._guard = _FrameGuard(config.max_frame_payload_bytes)
        self.created_at = time.monotonic()
        self.last_activity = self.created_at
        self.pong_deadline: float | None = None
        self._ping_generation = 0
        self._pending_ping_generation: int | None = None
        self._pending_ping_payload: bytes | None = None
        self.close_deadline: float | None = None
        self.write_deadline: float | None = None
        self._children: list[Any] = []

    def _current_task(self) -> Any:
        task = getattr(self.runtime, "cursor", None)
        if task is None:
            raise WebSocketStateError("WebSocket operations require a running SmallOS task")
        return task

    @staticmethod
    def _signal(task: Any, signal: int) -> None:
        if task is not None:
            accept = getattr(task, "acceptSignal", None)
            if callable(accept):
                accept(signal)

    async def _send_http(self, task: Any, response: Response) -> None:
        headers = {
            name: value
            for name, value in response.headers.items()
            if name.lower() != "connection"
        }
        headers["Connection"] = "close"
        payload = Response(response.status, response.body, headers).to_http1()
        await self.transport.send_all(task, self.client, payload)

    async def accept(
        self, subprotocol: str | None, headers: Mapping[str, str] | None
    ) -> None:
        task = self._current_task()
        if self.handshake_state != "pending":
            raise WebSocketStateError("WebSocket handshake is already decided")
        if subprotocol is not None:
            if subprotocol not in self.route.subprotocols:
                raise WebSocketStateError("selected subprotocol is not allowed by the route")
            if subprotocol not in _token_list(self.request.headers.get("sec-websocket-protocol")):
                raise WebSocketStateError("selected subprotocol was not offered by the client")
        extra = Headers(headers or {})
        forbidden = {
            "connection",
            "upgrade",
            "sec-websocket-accept",
            "sec-websocket-protocol",
            "sec-websocket-extensions",
            "content-length",
        }
        if any(name.lower() in forbidden for name in extra):
            raise ValueError("handshake headers contain a reserved field")
        key = self.request.headers["sec-websocket-key"].encode("ascii")
        accept_value = base64.b64encode(hashlib.sha1(key + _GUID).digest()).decode("ascii")
        lines = [
            "HTTP/1.1 101 Switching Protocols",
            "Upgrade: websocket",
            "Connection: Upgrade",
            "Sec-WebSocket-Accept: {}".format(accept_value),
        ]
        if subprotocol is not None:
            lines.append("Sec-WebSocket-Protocol: {}".format(subprotocol))
        lines.extend("{}: {}".format(name, value) for name, value in extra.items())
        payload = ("\r\n".join(lines) + "\r\n\r\n").encode("latin-1")
        protocol = self.api.Connection(self.api.ConnectionType.SERVER)
        self.handshake_state = "accepting"
        self.protocol = protocol
        try:
            await self.transport.send_all(task, self.client, payload)
        except GeneratorExit:
            raise
        except BaseException as exc:
            self._fail_handshake(exc)
            raise
        else:
            self.subprotocol = subprotocol
            self.accepted = True
            self.handshake_state = "accepted"
            self.last_activity = time.monotonic()
            self._signal(self.coordinator_task, _DECISION_SIGNAL)

    async def reject(self, response: Response) -> None:
        task = self._current_task()
        if self.handshake_state != "pending":
            raise WebSocketStateError("WebSocket handshake is already decided")
        if not isinstance(response, Response):
            raise TypeError("reject() requires a Response")
        if response.status < 300:
            raise ValueError("WebSocket rejection response must have status 300 or greater")
        self.handshake_state = "rejecting"
        try:
            await self._send_http(task, response)
        except GeneratorExit:
            raise
        except BaseException as exc:
            self._fail_handshake(exc)
            raise
        else:
            self.rejected = True
            self.handshake_state = "rejected"
            self._signal(self.coordinator_task, _DECISION_SIGNAL)

    def _fail_handshake(self, error: BaseException) -> None:
        if self.handshake_state == "failed":
            return
        self.accepted = False
        self.rejected = False
        self.protocol = None
        self.handshake_state = "failed"
        self.handshake_error = error
        if isinstance(error, (KeyboardInterrupt, SystemExit)):
            self.fatal_error = error
        self._disconnect(1006)

    def _require_open(self) -> None:
        if not self.accepted:
            raise WebSocketStateError("WebSocket must be accepted first")
        if self.shutdown or self.disconnect is not None:
            raise self.disconnect or WebSocketStateError("WebSocket is closed")

    async def receive(self) -> WebSocketMessage:
        task = self._current_task()
        if not self.accepted:
            raise WebSocketStateError("WebSocket must be accepted first")
        while not self.inbox:
            if self.disconnect is not None:
                raise self.disconnect
            await task.wait_signal(_INBOX_SIGNAL)
        message = self.inbox.popleft()
        self.inbox_bytes -= _message_size(message.data)
        return message

    async def send_message(self, value: str | bytes) -> None:
        self._require_open()
        size = _message_size(value)
        if size > self.config.max_message_bytes:
            raise WebSocketCapacityError("WebSocket message is too large")
        event = (
            self.api.TextMessage(data=value)
            if isinstance(value, str)
            else self.api.BytesMessage(data=value)
        )
        await self._enqueue(event, size, wait=True)

    async def ping(self, payload: bytes) -> None:
        self._require_open()
        if self._pending_ping_generation is not None:
            raise WebSocketStateError("a WebSocket Ping is already awaiting Pong")
        self._ping_generation += 1
        generation = self._ping_generation
        self._pending_ping_generation = generation
        self._pending_ping_payload = payload
        self.pong_deadline = time.monotonic() + self.config.pong_timeout
        try:
            await self._enqueue(
                self.api.Ping(payload=payload), len(payload), wait=True
            )
        except BaseException:
            if self._pending_ping_generation == generation:
                self._clear_pending_ping()
            raise

    async def close(self, code: int, reason: str) -> None:
        self._require_open()
        if not _valid_close_code(code):
            raise ValueError("invalid WebSocket close code")
        if not isinstance(reason, str) or len(reason.encode("utf-8")) > 123:
            raise ValueError("WebSocket close reason must be at most 123 UTF-8 bytes")
        reason_bytes = reason.encode("utf-8")
        self.close_sent = True
        self.close_deadline = time.monotonic() + self.config.close_timeout
        try:
            await self._enqueue(
                self.api.CloseConnection(code=code, reason=reason),
                2 + len(reason_bytes),
                wait=True,
            )
        except BaseException:
            self._disconnect(code, reason)
            raise
        task = self._current_task()
        while not self.peer_closed and time.monotonic() < self.close_deadline:
            await task.sleep(min(self.config.deadline_resolution, self.config.close_timeout))
        if self.disconnect is None:
            self.disconnect = WebSocketDisconnect(code, reason)

    async def _enqueue(self, event: Any, size: int, *, wait: bool) -> None:
        if (
            len(self.outbox) >= self.config.max_outbound_commands
            or self.outbox_bytes + size > self.config.max_outbound_bytes
        ):
            raise WebSocketCapacityError("WebSocket outbound queue is full")
        waiter = self._current_task() if wait else None
        command = _OutboundCommand(event, size, waiter)
        self.outbox.append(command)
        self.outbox_bytes += size
        self._signal(self.writer_task, _OUTBOX_SIGNAL)
        if wait:
            while not command.done:
                await waiter.wait_signal(_ACK_SIGNAL)
            if command.error is not None:
                raise command.error

    def _clear_pending_ping(self) -> None:
        self._pending_ping_generation = None
        self._pending_ping_payload = None
        self.pong_deadline = None

    def _fail_outbound(self, error: BaseException) -> None:
        commands = list(self.outbox)
        self.outbox.clear()
        self.outbox_bytes = 0
        if self.active_command is not None:
            commands.insert(0, self.active_command)
        seen: set[int] = set()
        for command in commands:
            if id(command) in seen:
                continue
            seen.add(id(command))
            command.error = error
            command.done = True
            self._signal(command.waiter, _ACK_SIGNAL)

    def _cancel_task(self, target: Any) -> None:
        if target is None or target is getattr(self.runtime, "cursor", None):
            return
        try:
            self.runtime.cancel_task(target)
        except (KeyboardInterrupt, SystemExit) as exc:
            self.fatal_error = exc
            raise
        except BaseException:
            # ServerHandle retains ownership and retries cancellation in finalization.
            pass

    def _abort_writer(self, error: BaseException) -> None:
        self._fail_outbound(error)
        self._cancel_task(self.writer_task)

    def _cancel_handler(self) -> None:
        self._cancel_task(self.handler_task)

    def _enqueue_control(self, event: Any, size: int = 0) -> bool:
        if (
            len(self.outbox) >= self.config.max_outbound_commands
            or self.outbox_bytes + size > self.config.max_outbound_bytes
        ):
            return False
        self.outbox.append(_OutboundCommand(event, size))
        self.outbox_bytes += size
        self._signal(self.writer_task, _OUTBOX_SIGNAL)
        return True

    def _deliver_message(self, value: str | bytes) -> bool:
        size = _message_size(value)
        if (
            len(self.inbox) >= self.config.max_inbound_messages
            or self.inbox_bytes + size > self.config.max_inbound_bytes
        ):
            return False
        self.inbox.append(WebSocketMessage(value))
        self.inbox_bytes += size
        self._signal(self.handler_task, _INBOX_SIGNAL)
        return True

    def _disconnect(self, code: int, reason: str = "") -> None:
        if self.disconnect is None:
            self.disconnect = WebSocketDisconnect(code, reason)
        self._signal(self.handler_task, _INBOX_SIGNAL)
        self._signal(self.coordinator_task, _DECISION_SIGNAL)

    def request_shutdown(self, code: int = 1001) -> None:
        if self.shutdown:
            return
        self.shutdown = True
        if self.accepted and not self.close_sent and self.protocol is not None:
            self._enqueue_control(
                self.api.CloseConnection(code=code, reason="server shutdown"), 17
            )
            self.close_sent = True
        if self.handshake_state in {"pending", "accepting", "rejecting"}:
            self._fail_handshake(WebSocketDisconnect(code, "server shutdown"))
        else:
            self._disconnect(code, "server shutdown")
        self._cancel_handler()
        self._signal(self.writer_task, _OUTBOX_SIGNAL)


def _token_list(value: str | None) -> tuple[str, ...]:
    if value is None:
        return ()
    return tuple(token.strip() for token in value.split(",") if token.strip())


def _is_http_token(value: str) -> bool:
    return (
        isinstance(value, str)
        and bool(value)
        and all(character in _HTTP_TOKEN_CHARACTERS for character in value)
    )


def _message_size(value: str | bytes) -> int:
    return len(value.encode("utf-8")) if isinstance(value, str) else len(value)


def _valid_close_code(code: int) -> bool:
    return type(code) is int and (
        1000 <= code <= 1014 and code not in {1004, 1005, 1006}
        or 3000 <= code <= 4999
    )


def _valid_websocket_key(value: str | None) -> bool:
    if value is None:
        return False
    try:
        return len(base64.b64decode(value.encode("ascii"), validate=True)) == 16
    except (ValueError, UnicodeEncodeError):
        return False


def _is_upgrade_attempt(request: Request) -> bool:
    headers = request.headers
    return bool(
        headers.get("upgrade")
        or headers.get("sec-websocket-key")
        or headers.get("sec-websocket-version")
        or any(token.lower() == "upgrade" for token in _token_list(headers.get("connection")))
    )


def _validate_upgrade(
    request: Request, route: _WebSocketRoute
) -> Response | None:
    if request.method.upper() != "GET":
        return Response.text("WebSocket upgrade requires GET", status=400)
    if request.version != "HTTP/1.1":
        return Response.text("WebSocket upgrade requires HTTP/1.1", status=400)
    if request.body:
        return Response.text("WebSocket upgrade must not include a body", status=400)
    if request.headers.get("upgrade", "").lower() != "websocket":
        return Response.text("invalid WebSocket Upgrade header", status=400)
    connection_tokens = {
        token.lower() for token in _token_list(request.headers.get("connection"))
    }
    if "upgrade" not in connection_tokens:
        return Response.text("invalid WebSocket Connection header", status=400)
    if request.headers.get("sec-websocket-version") != "13":
        return Response.text(
            "unsupported WebSocket version",
            status=426,
            headers={"Sec-WebSocket-Version": "13"},
        )
    if not _valid_websocket_key(request.headers.get("sec-websocket-key")):
        return Response.text("invalid WebSocket key", status=400)
    offered_subprotocols = _token_list(
        request.headers.get("sec-websocket-protocol")
    )
    offered_header = request.headers.get("sec-websocket-protocol")
    if offered_header is not None and (
        not offered_subprotocols
        or any(not _is_http_token(protocol) for protocol in offered_subprotocols)
    ):
        return Response.text("invalid WebSocket subprotocol", status=400)
    origin = request.headers.get("origin")
    if route.origins is not None and origin not in route.origins:
        return Response.text("WebSocket origin is not allowed", status=403)
    return None


async def run_websocket_connection(
    task: Any, state: _WebSocketState, server_handle: Any
) -> None:
    """Coordinate one accepted HTTP connection through its WebSocket lifetime."""
    state.coordinator_task = task
    server_handle._websocket_states[id(state.client)] = state

    def spawn(routine: Any, name: str) -> Any:
        child = task.spawn(
            routine,
            priority=server_handle._config.connection_priority,
            args=(state,),
            name=name,
        )
        state._children.append(child)
        server_handle._owned_tasks.append(child)
        return child

    try:
        state.handler_task = spawn(_run_handler, "smallserver-websocket-handler")
        state.deadline_task = spawn(
            _run_deadlines, "smallserver-websocket-deadline"
        )
        while state.handshake_state in {"pending", "accepting", "rejecting"}:
            await task.wait_signal(_DECISION_SIGNAL)
        if not state.accepted:
            if state.fatal_error is not None:
                raise state.fatal_error
            return
        state.writer_task = spawn(_run_writer, "smallserver-websocket-writer")
        state.reader_task = spawn(_run_reader, "smallserver-websocket-reader")
        try:
            await task.join(state.handler_task)
        except WebSocketDisconnect:
            pass
        except BaseException as exc:
            state.handler_error = exc

        if isinstance(state.handler_error, (KeyboardInterrupt, SystemExit)):
            raise state.handler_error
        if state.fatal_error is not None:
            raise state.fatal_error

        if state.handler_error is not None and not state.close_sent:
            try:
                state.close_deadline = time.monotonic() + state.config.close_timeout
                await state._enqueue(
                    state.api.CloseConnection(code=1011, reason="handler failed"),
                    16,
                    wait=True,
                )
                state.close_sent = True
            except (KeyboardInterrupt, SystemExit):
                raise
            except BaseException:
                pass
        elif not state.close_sent and state.disconnect is None:
            try:
                state.close_deadline = time.monotonic() + state.config.close_timeout
                await state._enqueue(
                    state.api.CloseConnection(code=1000, reason=""), 2, wait=True
                )
                state.close_sent = True
            except (KeyboardInterrupt, SystemExit):
                raise
            except BaseException:
                pass

        deadline = time.monotonic() + state.config.close_timeout
        while (
            state.outbox or state.writer_busy or not state.peer_closed
        ) and time.monotonic() < deadline:
            await task.sleep(
                min(state.config.deadline_resolution, state.config.close_timeout)
            )
    finally:
        state.request_shutdown()
        for child in list(state._children):
            if child is task:
                continue
            if (
                server_handle._cancel_or_retain_task(child)
                and child in server_handle._owned_tasks
            ):
                server_handle._owned_tasks.remove(child)
        state._children.clear()
        server_handle._websocket_states.pop(id(state.client), None)


async def _run_handler(task: Any, state: _WebSocketState) -> None:
    socket = WebSocket(state)
    try:
        result = state.route.handler(socket)
        if not inspect.isawaitable(result):
            raise TypeError("WebSocket handlers must return an awaitable")
        await result
    except WebSocketDisconnect:
        pass
    except GeneratorExit:
        raise
    except (KeyboardInterrupt, SystemExit) as exc:
        state.handler_error = exc
        if state.handshake_state in {"pending", "accepting", "rejecting"}:
            state._fail_handshake(exc)
        raise
    except BaseException as exc:
        state.handler_error = exc
    finally:
        if state.handshake_state == "pending":
            response = Response.text(
                "internal server error" if state.handler_error is not None else "forbidden",
                status=500 if state.handler_error is not None else 403,
            )
            try:
                await state.reject(response)
            except (KeyboardInterrupt, SystemExit) as exc:
                state.handler_error = exc
                state.fatal_error = exc
                raise
            except BaseException:
                state._disconnect(1006)
        state._signal(state.coordinator_task, _DECISION_SIGNAL)


async def _run_writer(task: Any, state: _WebSocketState) -> None:
    while not state.shutdown or state.outbox:
        while state.outbox:
            command = state.outbox.popleft()
            state.outbox_bytes -= command.size
            state.writer_busy = True
            state.active_command = command
            state.write_deadline = time.monotonic() + state.config.write_timeout
            try:
                payload = state.protocol.send(command.event)
                for offset in range(0, len(payload), state.config.write_chunk_bytes):
                    await state.transport.send_all(
                        task,
                        state.client,
                        payload[offset : offset + state.config.write_chunk_bytes],
                    )
                state.last_activity = time.monotonic()
            except GeneratorExit:
                raise
            except (KeyboardInterrupt, SystemExit) as exc:
                command.error = exc
                state.fatal_error = exc
                state._disconnect(1006)
                state._cancel_handler()
                raise
            except BaseException as exc:
                command.error = exc
                state._disconnect(1006)
                state._cancel_handler()
            finally:
                state.writer_busy = False
                if state.active_command is command:
                    state.active_command = None
                state.write_deadline = None
                command.done = True
                state._signal(command.waiter, _ACK_SIGNAL)
        if not state.shutdown:
            await task.wait_signal(_OUTBOX_SIGNAL)


async def _run_reader(task: Any, state: _WebSocketState) -> None:
    try:
        if state.trailing_data:
            _receive_protocol_data(state, state.trailing_data)
            state.trailing_data = b""
            if _drain_protocol_events(state):
                return
        while not state.shutdown:
            chunk = await state.transport.recv(
                task, state.client, state.config.receive_chunk_bytes
            )
            if not chunk:
                state.protocol.receive_data(None)
                _drain_protocol_events(state)
                state._disconnect(1006)
                state._cancel_handler()
                return
            state.last_activity = time.monotonic()
            _receive_protocol_data(state, chunk)
            if _drain_protocol_events(state):
                return
    except GeneratorExit:
        raise
    except WebSocketCapacityError:
        if state.shutdown:
            return
        state._enqueue_control(
            state.api.CloseConnection(code=1009, reason="message too large"), 19
        )
        state.close_sent = True
        state._disconnect(1009, "message too large")
        state._cancel_handler()
    except (KeyboardInterrupt, SystemExit) as exc:
        state.fatal_error = exc
        state._disconnect(1006)
        state._cancel_handler()
        raise
    except BaseException:
        if state.shutdown:
            return
        state._enqueue_control(
            state.api.CloseConnection(code=1002, reason="protocol error"), 16
        )
        state.close_sent = True
        state._disconnect(1002, "protocol error")
        state._cancel_handler()


def _receive_protocol_data(state: _WebSocketState, data: bytes) -> None:
    for chunk in state._guard.feed(data):
        state.protocol.receive_data(chunk)


def _drain_protocol_events(state: _WebSocketState) -> bool:
    for event in state.protocol.events():
        if isinstance(event, (state.api.TextMessage, state.api.BytesMessage)):
            kind = str if isinstance(event, state.api.TextMessage) else bytes
            if state._message_kind is None:
                state._message_kind = kind
                state._message_buffer.clear()
                state._message_bytes = 0
            if state._message_kind is not kind:
                raise ValueError("WebSocket message type changed during fragmentation")
            state._message_bytes += _message_size(event.data)
            if state._message_bytes > state.config.max_message_bytes:
                raise WebSocketCapacityError("WebSocket message is too large")
            encoded = (
                event.data.encode("utf-8")
                if isinstance(event.data, str)
                else event.data
            )
            state._message_buffer.extend(encoded)
            if event.message_finished:
                value = (
                    bytes(state._message_buffer).decode("utf-8")
                    if kind is str
                    else bytes(state._message_buffer)
                )
                state._message_kind = None
                state._message_buffer.clear()
                state._message_bytes = 0
                if not state._deliver_message(value):
                    raise WebSocketCapacityError("WebSocket inbound queue is full")
        elif isinstance(event, state.api.Ping):
            if not state._enqueue_control(event.response(), len(event.payload)):
                raise WebSocketCapacityError("WebSocket outbound queue is full")
        elif isinstance(event, state.api.Pong):
            _handle_pong(state, event.payload)
        elif isinstance(event, state.api.CloseConnection):
            state.peer_closed = True
            close_code = int(event.code)
            disconnect_reason = event.reason or ""
            if not state.close_sent:
                if close_code == 1002:
                    response = state.api.CloseConnection(
                        code=1002, reason="protocol error"
                    )
                elif close_code == 1007:
                    response = state.api.CloseConnection(
                        code=1007, reason="invalid payload"
                    )
                else:
                    response = event.response()
                if not state._enqueue_control(
                    response, _close_event_size(response)
                ):
                    raise WebSocketCapacityError("WebSocket outbound queue is full")
                state.close_sent = True
            if close_code == 1002:
                disconnect_reason = "protocol error"
            elif close_code == 1007:
                disconnect_reason = "invalid payload"
            state._disconnect(close_code, disconnect_reason)
            return True
    return False


def _handle_pong(state: _WebSocketState, payload: bytes) -> None:
    if (
        state._pending_ping_generation is not None
        and payload == state._pending_ping_payload
    ):
        state._clear_pending_ping()


def _close_event_size(event: Any) -> int:
    if int(event.code) == 1005:
        return 0
    return 2 + len((event.reason or "").encode("utf-8"))


async def _run_deadlines(task: Any, state: _WebSocketState) -> None:
    while not state.shutdown:
        await task.sleep(state.config.deadline_resolution)
        now = time.monotonic()
        if state.handshake_state in {"pending", "accepting", "rejecting"}:
            if now - state.created_at >= state.config.handshake_timeout:
                if state.handshake_state == "pending":
                    try:
                        await state.reject(
                            Response.text("WebSocket handshake timed out", status=408)
                        )
                    except (KeyboardInterrupt, SystemExit) as exc:
                        state.fatal_error = exc
                        raise
                    except BaseException:
                        state._disconnect(1006)
                else:
                    error = WebSocketDisconnect(1006, "handshake timed out")
                    state._fail_handshake(error)
                    state._cancel_handler()
                return
            continue
        if state.pong_deadline is not None and now >= state.pong_deadline:
            _begin_deadline_close(state, 1002, "Pong timeout")
        elif state.accepted and now - state.last_activity >= state.config.idle_timeout:
            _begin_deadline_close(state, 1001, "idle timeout")
        if state.write_deadline is not None and now >= state.write_deadline:
            error = WebSocketDisconnect(1006, "write timed out")
            state._abort_writer(error)
            state._cancel_handler()
            state._disconnect(error.code, error.reason)
            return
        if state.close_deadline is not None and now >= state.close_deadline:
            error = state.disconnect or WebSocketDisconnect(1006, "close timed out")
            state._abort_writer(error)
            state._cancel_handler()
            state._disconnect(error.code, error.reason)
            return


def _begin_deadline_close(
    state: _WebSocketState, code: int, reason: str
) -> None:
    if not state.close_sent and state.protocol is not None:
        reason_bytes = reason.encode("utf-8")
        state._enqueue_control(
            state.api.CloseConnection(code=code, reason=reason),
            2 + len(reason_bytes),
        )
        state.close_sent = True
    if state.close_deadline is None:
        state.close_deadline = time.monotonic() + state.config.close_timeout
    state._disconnect(code, reason)
    state._cancel_handler()
