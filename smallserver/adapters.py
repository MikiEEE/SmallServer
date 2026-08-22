"""Explicit lifecycle and HTTP translation helpers for SmallOS adapters."""

from __future__ import annotations

from collections.abc import Callable, Iterator
from types import TracebackType
from typing import Any

from SmallPackage.adapters.errors import (
    AdapterCancelledError,
    AdapterCapacityError,
    AdapterClosedError,
    AdapterError,
    AdapterUnavailableError,
)

from .errors import HTTPError


class AdapterShutdownError(RuntimeError):
    """One or more adapters failed while the registry was shutting down."""

    def __init__(self, failures: tuple[tuple[str, BaseException], ...]) -> None:
        self.failures = failures
        names = ", ".join(name for name, _ in failures)
        super().__init__("adapter shutdown failed: {}".format(names))


class AdapterRegistry:
    """Name and explicitly manage user-created SmallOS execution adapters.

    The registry never creates worker pools or event loops. It delegates calls
    to adapters supplied by the application and closes them only when
    ``shutdown()`` or context-manager exit is explicitly invoked.
    """

    def __init__(self, **adapters: Any) -> None:
        self._adapters: dict[str, Any] = {}
        self._closed = False
        for name, adapter in adapters.items():
            self.register(name, adapter)

    @property
    def closed(self) -> bool:
        return self._closed

    def register(self, name: str, adapter: Any) -> Any:
        if self._closed:
            raise RuntimeError("adapter registry is closed")
        if not isinstance(name, str) or not name or not name.isidentifier():
            raise ValueError("adapter name must be a non-empty identifier")
        if name in self._adapters:
            raise ValueError("adapter is already registered: {}".format(name))
        for registered_name, registered_adapter in self._adapters.items():
            if registered_adapter is adapter:
                raise ValueError(
                    "adapter is already registered as: {}".format(registered_name)
                )
        for method_name in ("call", "shutdown"):
            if not callable(getattr(adapter, method_name, None)):
                raise TypeError("adapter must provide {}()".format(method_name))
        self._adapters[name] = adapter
        return adapter

    def get(self, name: str) -> Any:
        try:
            return self._adapters[name]
        except KeyError as exc:
            raise KeyError("unknown adapter: {}".format(name)) from exc

    def call(
        self,
        name: str,
        callable_obj: Callable[..., Any],
        /,
        *args: Any,
        **kwargs: Any,
    ) -> Any:
        """Return the named adapter's SmallOS-native instruction awaitable."""
        if self._closed:
            raise RuntimeError("adapter registry is closed")
        return self.get(name).call(callable_obj, *args, **kwargs)

    def names(self) -> tuple[str, ...]:
        return tuple(self._adapters)

    def items(self) -> Iterator[tuple[str, Any]]:
        return iter(self._adapters.items())

    def shutdown(self, wait: bool = True, cancel_pending: bool = False) -> None:
        """Shut every adapter down in reverse registration order."""
        if self._closed:
            return
        self._closed = True
        failures: list[tuple[str, BaseException]] = []
        interrupt: BaseException | None = None
        for name, adapter in reversed(tuple(self._adapters.items())):
            try:
                adapter.shutdown(wait=wait, cancel_pending=cancel_pending)
            except BaseException as exc:
                if isinstance(exc, Exception):
                    failures.append((name, exc))
                elif interrupt is None:
                    interrupt = exc
        if interrupt is not None:
            raise interrupt
        if failures:
            raise AdapterShutdownError(tuple(failures))

    def __enter__(self) -> AdapterRegistry:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.shutdown(wait=True, cancel_pending=exc_type is not None)


def http_error_from_adapter(exc: AdapterError) -> HTTPError:
    """Translate an adapter failure without exposing foreign error details."""
    if isinstance(exc, AdapterCapacityError):
        return HTTPError(503, "service is at capacity")
    if isinstance(exc, (AdapterUnavailableError, AdapterClosedError)):
        return HTTPError(503, "service is unavailable")
    if isinstance(exc, AdapterCancelledError):
        return HTTPError(503, "service operation was cancelled")
    return HTTPError(500, "service execution failed")
