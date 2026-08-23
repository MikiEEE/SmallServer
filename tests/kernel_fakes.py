from __future__ import annotations

from dataclasses import dataclass


class NeedsRead(Exception):
    pass


class NeedsWrite(Exception):
    pass


class TLSWantRead(NeedsRead):
    pass


class TLSWantWrite(NeedsWrite):
    pass


class WouldBlock(BlockingIOError):
    pass


@dataclass
class OpaqueHandle:
    """Intentionally unhashable stand-in for a backend-owned resource."""

    name: str


class FakeWakeupChannel:
    def __init__(self) -> None:
        self.wait_object = OpaqueHandle("wakeup-wait")
        self.notify_calls = 0
        self.drain_calls = 0
        self.close_calls = 0
        self.close_failures = 0
        self.notify_failures = 0
        self.drain_error: BaseException | None = None

    def notify(self) -> None:
        self.notify_calls += 1
        if self.notify_failures:
            self.notify_failures -= 1
            raise RuntimeError("notify failed")

    def drain(self) -> None:
        self.drain_calls += 1
        if self.drain_error is not None:
            raise self.drain_error

    def close(self) -> None:
        self.close_calls += 1
        if self.close_failures:
            self.close_failures -= 1
            raise RuntimeError("wakeup close failed")


class FakeKernel:
    def __init__(self, supported: bool = True, wakeup_supported: bool = True) -> None:
        self.supported = supported
        self.wakeup_supported = wakeup_supported
        self.address_info = object()
        self.listener = OpaqueHandle("listener")
        self.wakeup = FakeWakeupChannel()
        self.calls: list[tuple] = []
        self.closed: list[OpaqueHandle] = []
        self.accept_results: list[object] = []
        self.recv_results: dict[int, list[object]] = {}
        self.send_results: dict[int, list[object]] = {}
        self.sent: dict[int, list[bytes]] = {}
        self.peer_addresses: dict[int, object | None] = {}
        self.fail_operation: str | None = None
        self.operation_errors: dict[str, BaseException] = {}
        self.close_failures: dict[int, int] = {}
        self.invalid_wait_objects: set[int] = set()

    def supports_tcp_server(self) -> bool:
        self.calls.append(("supports_tcp_server",))
        return self.supported

    def supports_wakeup_channel(self) -> bool:
        self.calls.append(("supports_wakeup_channel",))
        return self.wakeup_supported

    def resolve_passive_address(self, host: str, port: int) -> object:
        self.calls.append(("resolve_passive_address", host, port))
        return self.address_info

    def socket_open(self, address_info: object) -> object:
        self.calls.append(("socket_open", address_info))
        return self.listener

    def _maybe_fail(self, operation: str) -> None:
        if operation in self.operation_errors:
            raise self.operation_errors[operation]
        if self.fail_operation == operation:
            raise RuntimeError("{} failed".format(operation))

    def socket_setblocking(self, stream: object, flag: bool) -> None:
        self.calls.append(("socket_setblocking", stream, flag))
        self._maybe_fail("setblocking")

    def socket_set_reuse_address(self, stream: object, enabled: bool) -> None:
        self.calls.append(("socket_set_reuse_address", stream, enabled))
        self._maybe_fail("reuse")

    def socket_bind(self, stream: object, address: object) -> None:
        self.calls.append(("socket_bind", stream, address))
        self._maybe_fail("bind")

    def socket_listen(self, stream: object, backlog: int) -> None:
        self.calls.append(("socket_listen", stream, backlog))
        self._maybe_fail("listen")

    def socket_accept(self, listener: object) -> tuple[object, object]:
        self.calls.append(("socket_accept", listener))
        if not self.accept_results:
            raise NeedsRead()
        result = self.accept_results.pop(0)
        if isinstance(result, BaseException):
            raise result
        return result  # type: ignore[return-value]

    def socket_local_address(self, stream: object) -> object:
        self.calls.append(("socket_local_address", stream))
        return ("127.0.0.1", 43210)

    def socket_peer_address(self, stream: object) -> object | None:
        self.calls.append(("socket_peer_address", stream))
        return self.peer_addresses.get(id(stream))

    def socket_recv(self, stream: object, size: int) -> bytes:
        self.calls.append(("socket_recv", stream, size))
        result = self.recv_results[id(stream)].pop(0)
        if isinstance(result, BaseException):
            raise result
        return result  # type: ignore[return-value]

    def socket_send(self, stream: object, data: bytes) -> int:
        self.calls.append(("socket_send", stream, data))
        self.sent.setdefault(id(stream), []).append(data)
        results = self.send_results.get(id(stream))
        result = results.pop(0) if results else len(data)
        if isinstance(result, BaseException):
            raise result
        return result  # type: ignore[return-value]

    def socket_close(self, stream: object) -> None:
        self.calls.append(("socket_close", stream))
        failures = self.close_failures.get(id(stream), 0)
        if failures:
            self.close_failures[id(stream)] = failures - 1
            raise RuntimeError("close failed")
        self.closed.append(stream)  # type: ignore[arg-type]

    def socket_needs_read(self, exc: BaseException) -> bool:
        return isinstance(exc, NeedsRead)

    def socket_needs_write(self, exc: BaseException) -> bool:
        return isinstance(exc, NeedsWrite)

    def socket_retry_mode(self, exc: BaseException, operation: str) -> str | None:
        if isinstance(exc, NeedsRead):
            return "read"
        if isinstance(exc, NeedsWrite):
            return "write"
        if isinstance(exc, WouldBlock):
            return "write" if operation == "send" else "read"
        return None

    def validate_io_wait_object(self, obj: object) -> tuple[bool, BaseException | None]:
        if id(obj) in self.invalid_wait_objects:
            return False, ValueError("invalid wait object")
        return True, None

    def create_wakeup_channel(self) -> FakeWakeupChannel:
        self.calls.append(("create_wakeup_channel",))
        return self.wakeup
