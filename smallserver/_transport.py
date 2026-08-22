"""Private SmallOS kernel transport boundary for server networking."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol


class WakeupChannelLike(Protocol):
    """Opaque scheduler wakeup channel supplied by the active kernel."""

    @property
    def wait_object(self) -> object: ...

    def notify(self) -> None: ...

    def drain(self) -> None: ...

    def close(self) -> None: ...


class KernelLike(Protocol):
    """SmallOS networking surface consumed by SmallServer."""

    def supports_tcp_server(self) -> bool: ...

    def supports_wakeup_channel(self) -> bool: ...

    def resolve_passive_address(self, host: str, port: int) -> object: ...

    def socket_open(self, address_info: object) -> object: ...

    def socket_setblocking(self, stream: object, flag: bool) -> None: ...

    def socket_set_reuse_address(self, stream: object, enabled: bool) -> None: ...

    def socket_bind(self, stream: object, address: object) -> None: ...

    def socket_listen(self, stream: object, backlog: int) -> None: ...

    def socket_accept(self, listener: object) -> tuple[object, object]: ...

    def socket_local_address(self, stream: object) -> object: ...

    def socket_peer_address(self, stream: object) -> object | None: ...

    def socket_recv(self, stream: object, size: int) -> bytes: ...

    def socket_send(self, stream: object, data: object) -> int: ...

    def socket_close(self, stream: object) -> None: ...

    def socket_retry_mode(self, exc: BaseException, operation: str) -> str | None: ...

    def validate_io_wait_object(self, obj: object) -> tuple[bool, BaseException | None]: ...

    def create_wakeup_channel(self) -> WakeupChannelLike: ...


@dataclass(eq=False)
class TransportHandle:
    """Own one kernel stream and its bounded, retryable close state."""

    raw: object
    closed: bool = False


@dataclass(frozen=True)
class AcceptedConnection:
    """An accepted opaque stream plus kernel-provided peer metadata."""

    stream: TransportHandle
    peer_address: object | None


@dataclass(eq=False)
class WakeupChannel:
    """Validated wake channel with a cached readiness object."""

    raw: WakeupChannelLike
    wait_object: object
    closed: bool = False

    def notify(self) -> None:
        self.raw.notify()

    def drain(self) -> None:
        self.raw.drain()

    def close(self) -> None:
        if self.closed:
            return
        self.raw.close()
        self.closed = True


class KernelTransport:
    """Adapt SmallServer operations to one active SmallOS kernel."""

    _REQUIRED_METHODS = (
        "resolve_passive_address",
        "socket_open",
        "socket_setblocking",
        "socket_set_reuse_address",
        "socket_bind",
        "socket_listen",
        "socket_accept",
        "socket_local_address",
        "socket_peer_address",
        "socket_recv",
        "socket_send",
        "socket_close",
        "socket_retry_mode",
    )

    def __init__(self, kernel: KernelLike) -> None:
        if kernel is None:
            raise RuntimeError("SmallServer requires a runtime with an active kernel")
        if not callable(getattr(kernel, "supports_tcp_server", None)):
            raise TypeError("the active kernel does not implement the TCP server contract")
        if not kernel.supports_tcp_server():
            raise NotImplementedError("the active kernel does not support TCP servers")
        supports_wakeup = getattr(kernel, "supports_wakeup_channel", None)
        wakeup_supported = bool(callable(supports_wakeup) and supports_wakeup())
        missing = [name for name in self._REQUIRED_METHODS if not callable(getattr(kernel, name, None))]
        if wakeup_supported and not callable(getattr(kernel, "create_wakeup_channel", None)):
            missing.append("create_wakeup_channel")
        if missing:
            raise TypeError(
                "the active kernel is missing TCP server operations: {}".format(
                    ", ".join(missing)
                )
            )
        self._kernel = kernel
        self.supports_wakeup_channel = wakeup_supported

    def open_listener(
        self,
        host: str,
        port: int,
        backlog: int,
        reuse_address: bool = True,
    ) -> TransportHandle:
        address_info = self._kernel.resolve_passive_address(host, port)
        listener = TransportHandle(self._kernel.socket_open(address_info))
        try:
            self._kernel.socket_set_reuse_address(listener.raw, reuse_address)
            self._kernel.socket_bind(listener.raw, address_info)
            self._kernel.socket_listen(listener.raw, backlog)
            self._kernel.socket_setblocking(listener.raw, False)
        except BaseException:
            try:
                self.close(listener)
            except BaseException:
                pass
            raise
        return listener

    async def accept(self, task: Any, listener: TransportHandle) -> AcceptedConnection:
        while True:
            try:
                raw_stream, address = self._kernel.socket_accept(listener.raw)
                break
            except BaseException as exc:
                await self._wait_for_retry(task, listener, exc, "accept")
        stream = TransportHandle(raw_stream)
        try:
            self._kernel.socket_setblocking(stream.raw, False)
            peer = self._kernel.socket_peer_address(stream.raw)
        except BaseException:
            try:
                self.close(stream)
            except BaseException:
                pass
            raise
        return AcceptedConnection(stream, peer if peer is not None else address)

    async def recv(self, task: Any, stream: TransportHandle, size: int) -> bytes:
        while True:
            try:
                data = self._kernel.socket_recv(stream.raw, size)
            except BaseException as exc:
                await self._wait_for_retry(task, stream, exc, "recv")
                continue
            if not isinstance(data, bytes):
                raise TypeError("the active kernel returned non-bytes socket data")
            return data

    async def send_all(self, task: Any, stream: TransportHandle, data: bytes) -> None:
        offset = 0
        view = memoryview(data)
        while offset < len(data):
            try:
                sent = self._kernel.socket_send(stream.raw, view[offset:])
            except BaseException as exc:
                await self._wait_for_retry(task, stream, exc, "send")
                continue
            if type(sent) is not int:
                raise TypeError("the active kernel returned an invalid socket send count")
            if sent <= 0:
                raise ConnectionError("the active kernel made no forward progress sending data")
            if sent > len(data) - offset:
                raise RuntimeError("the active kernel returned an oversized socket send count")
            offset += sent

    async def _wait_for_retry(
        self,
        task: Any,
        stream: TransportHandle,
        exc: BaseException,
        operation: str,
    ) -> None:
        mode = self._kernel.socket_retry_mode(exc, operation)
        if mode == "read":
            await task.wait_readable(stream.raw)
            return
        if mode == "write":
            await task.wait_writable(stream.raw)
            return
        raise exc

    def local_address(self, listener: TransportHandle) -> tuple[str, int]:
        address = self._kernel.socket_local_address(listener.raw)
        if not isinstance(address, (tuple, list)) or len(address) < 2:
            raise ValueError("the active kernel returned an invalid local address")
        host, port = address[:2]
        if type(port) is not int or not 0 <= port <= 65535:
            raise ValueError("the active kernel returned an invalid local port")
        return str(host), port

    def close(self, handle: TransportHandle) -> None:
        if handle.closed:
            return
        self._kernel.socket_close(handle.raw)
        handle.closed = True

    def close_safely(self, handle: TransportHandle) -> None:
        try:
            self.close(handle)
        except BaseException:
            pass

    def create_wakeup_channel(self) -> WakeupChannel | None:
        if not self.supports_wakeup_channel:
            return None
        raw_channel = self._kernel.create_wakeup_channel()
        try:
            missing = [
                name
                for name in ("notify", "drain", "close")
                if not callable(getattr(raw_channel, name, None))
            ]
            if missing:
                raise TypeError("the active kernel returned an invalid wakeup channel")
            wait_object = raw_channel.wait_object
            validator = getattr(self._kernel, "validate_io_wait_object", None)
            if callable(validator):
                valid, validation_error = validator(wait_object)
                if not valid:
                    if validation_error is not None:
                        raise validation_error
                    raise ValueError("the active kernel returned an invalid wakeup wait object")
            return WakeupChannel(raw_channel, wait_object)
        except BaseException:
            try:
                close = getattr(raw_channel, "close", None)
                if callable(close):
                    close()
            except BaseException:
                pass
            raise
