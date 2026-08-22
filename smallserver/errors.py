"""Framework-owned HTTP and lifecycle errors."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

try:
    from _thread import allocate_lock
except ImportError:  # pragma: no cover - runtimes without threads need no lock
    allocate_lock = None  # type: ignore[assignment]


class _NoThreadLock:
    def __enter__(self) -> None:
        return None

    def __exit__(self, *args: object) -> None:
        return None


class _CleanupTransaction:
    """Own cleanup actions until each one succeeds."""

    def __init__(self) -> None:
        self._actions: dict[str, Callable[[], None]] = {}
        self._errors: dict[str, BaseException] = {}
        self._lock: Any = allocate_lock() if allocate_lock is not None else _NoThreadLock()

    def add(
        self,
        name: str,
        action: Callable[[], None],
        error: BaseException | None = None,
    ) -> None:
        with self._lock:
            key = name
            suffix = 2
            while key in self._actions:
                key = "{}:{}".format(name, suffix)
                suffix += 1
            self._actions[key] = action
            if error is not None:
                self._errors[key] = error

    def retry(self) -> tuple[BaseException, ...]:
        with self._lock:
            for name, action in tuple(self._actions.items()):
                try:
                    action()
                except BaseException as exc:
                    self._errors[name] = exc
                else:
                    self._actions.pop(name, None)
                    self._errors.pop(name, None)
            return tuple(self._errors.values())

    def transfer(self) -> None:
        """Drop actions after ownership moves to another framework object."""
        with self._lock:
            self._actions.clear()
            self._errors.clear()

    @property
    def errors(self) -> tuple[BaseException, ...]:
        with self._lock:
            return tuple(self._errors.values())

    @property
    def complete(self) -> bool:
        with self._lock:
            return not self._actions


class ServerStartupError(RuntimeError):
    """Startup failed while framework-owned resources still need cleanup.

    The exception retains ownership without exposing kernel handles. Call
    :meth:`retry_cleanup` until it returns ``True``; successful cleanup is
    idempotent.
    """

    def __init__(
        self,
        primary_error: BaseException,
        transaction: _CleanupTransaction,
    ) -> None:
        self.primary_error = primary_error
        self._transaction = transaction
        super().__init__(
            "SmallServer startup failed and resource cleanup is incomplete"
        )

    @property
    def cleanup_errors(self) -> tuple[BaseException, ...]:
        return self._transaction.errors

    @property
    def cleanup_complete(self) -> bool:
        return self._transaction.complete

    def retry_cleanup(self) -> bool:
        """Retry every resource still owned by the failed startup."""
        self._transaction.retry()
        return self._transaction.complete

    def finalize(self) -> bool:
        """Alias for :meth:`retry_cleanup`."""
        return self.retry_cleanup()

    def __del__(self) -> None:
        try:
            if self.cleanup_complete or self.retry_cleanup():
                return
            import warnings

            warnings.warn(
                "abandoned ServerStartupError still owns resources after cleanup retry",
                ResourceWarning,
                stacklevel=2,
            )
        except BaseException:
            # Destructors must never interfere with interpreter shutdown.
            return


class HTTPError(Exception):
    """An expected HTTP response raised by framework or application code."""

    def __init__(self, status: int, detail: str = "") -> None:
        if not 400 <= status <= 599:
            raise ValueError("HTTPError status must be between 400 and 599")
        self.status = status
        self.detail = detail
        super().__init__(detail or "HTTP {}".format(status))
