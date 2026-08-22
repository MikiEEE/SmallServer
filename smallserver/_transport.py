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

    def socket_send(self, stream: object, data: bytes) -> int: ...

    def socket_close(self, stream: object) -> None: ...

    def socket_needs_read(self, exc: BaseException) -> bool: ...

    def socket_needs_write(self, exc: BaseException) -> bool: ...

    def create_wakeup_channel(self) -> WakeupChannelLike: ...


@dataclass(frozen=True)
class AcceptedConnection:
    """An accepted opaque stream plus kernel-provided peer metadata."""

    stream: object
    peer_address: object | None


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
        "socket_needs_read",
        "socket_needs_write",
        "create_wakeup_channel",
    )

    def __init__(self, kernel: KernelLike) -> None:
        if kernel is None:
            raise RuntimeError("SmallServer requires a runtime with an active kernel")
        if not callable(getattr(kernel, "supports_tcp_server", None)):
            raise TypeError("the active kernel does not implement the TCP server contract")
        if not kernel.supports_tcp_server():
            raise NotImplementedError("the active kernel does not support TCP servers")
        supports_wakeup = getattr(kernel, "supports_wakeup_channel", None)
        if callable(supports_wakeup) and not supports_wakeup():
            raise NotImplementedError("the active kernel does not support wakeup channels")
        missing = [name for name in self._REQUIRED_METHODS if not callable(getattr(kernel, name, None))]
        if missing:
            raise TypeError(
                "the active kernel is missing TCP server operations: {}".format(
                    ", ".join(missing)
                )
            )
        self._kernel = kernel
        # Retain closed objects as well as their identities. This prevents an id
        # from being reused during the transport lifetime and keeps close()
        # idempotent even for unhashable opaque handles.
        self._closed: dict[int, object] = {}

    def open_listener(
        self,
        host: str,
        port: int,
        backlog: int,
        reuse_address: bool = True,
    ) -> object:
        address_info = self._kernel.resolve_passive_address(host, port)
        listener = self._kernel.socket_open(address_info)
        try:
            self._kernel.socket_set_reuse_address(listener, reuse_address)
            self._kernel.socket_bind(listener, address_info)
            self._kernel.socket_listen(listener, backlog)
            self._kernel.socket_setblocking(listener, False)
        except Exception:
            self.close(listener)
            raise
        return listener

    async def accept(self, task: Any, listener: object) -> AcceptedConnection:
        while True:
            try:
                stream, address = self._kernel.socket_accept(listener)
                break
            except Exception as exc:
                if self._kernel.socket_needs_read(exc):
                    await task.wait_readable(listener)
                    continue
                if self._kernel.socket_needs_write(exc):
                    await task.wait_writable(listener)
                    continue
                raise
        try:
            self._kernel.socket_setblocking(stream, False)
            peer = self._kernel.socket_peer_address(stream)
        except Exception:
            self.close(stream)
            raise
        return AcceptedConnection(stream, peer if peer is not None else address)

    async def recv(self, task: Any, stream: object, size: int) -> bytes:
        while True:
            try:
                data = self._kernel.socket_recv(stream, size)
            except Exception as exc:
                if self._kernel.socket_needs_read(exc):
                    await task.wait_readable(stream)
                    continue
                if self._kernel.socket_needs_write(exc):
                    await task.wait_writable(stream)
                    continue
                raise
            if not isinstance(data, bytes):
                raise TypeError("the active kernel returned non-bytes socket data")
            return data

    async def send_all(self, task: Any, stream: object, data: bytes) -> None:
        offset = 0
        while offset < len(data):
            try:
                sent = self._kernel.socket_send(stream, data[offset:])
            except Exception as exc:
                if self._kernel.socket_needs_write(exc):
                    await task.wait_writable(stream)
                    continue
                if self._kernel.socket_needs_read(exc):
                    await task.wait_readable(stream)
                    continue
                raise
            if type(sent) is not int:
                raise TypeError("the active kernel returned an invalid socket send count")
            if sent <= 0:
                return
            if sent > len(data) - offset:
                raise RuntimeError("the active kernel returned an oversized socket send count")
            offset += sent

    def local_address(self, listener: object) -> tuple[str, int]:
        address = self._kernel.socket_local_address(listener)
        if not isinstance(address, (tuple, list)) or len(address) < 2:
            raise ValueError("the active kernel returned an invalid local address")
        host, port = address[:2]
        if type(port) is not int or not 0 <= port <= 65535:
            raise ValueError("the active kernel returned an invalid local port")
        return str(host), port

    def close(self, handle: object) -> None:
        identity = id(handle)
        previous = self._closed.get(identity)
        if previous is handle:
            return
        self._closed[identity] = handle
        self._kernel.socket_close(handle)

    def close_safely(self, handle: object) -> None:
        try:
            self.close(handle)
        except Exception:
            pass

    def create_wakeup_channel(self) -> WakeupChannelLike:
        channel = self._kernel.create_wakeup_channel()
        missing = [
            name for name in ("notify", "drain", "close") if not callable(getattr(channel, name, None))
        ]
        if missing or not hasattr(channel, "wait_object"):
            try:
                close = getattr(channel, "close", None)
                if callable(close):
                    close()
            finally:
                raise TypeError("the active kernel returned an invalid wakeup channel")
        return channel
