"""Route registration and handler dispatch for SmallServer."""

from __future__ import annotations

import inspect
from collections.abc import Awaitable, Callable, Iterable
from typing import Any, Literal, NoReturn, Protocol, cast, overload

try:
    from _thread import allocate_lock
except ImportError:  # pragma: no cover - runtimes without threads cannot race
    allocate_lock = None  # type: ignore[assignment]

from ._transport import (
    KernelTransport,
    TransportHandle,
    _TransportAcquisitionFailure,
)
from .errors import (
    HTTPError,
    ServerConfigurationError,
    ServerFinalizationError,
    ServerStartupError,
    _CleanupTransaction,
)
from .http import Request, Response
from .runtime import ManagedRuntimeConfig
from .server import HTTPParseError, HTTPRequestParser, ServerConfig, ServerHandle

Handler = Callable[[Request], Awaitable[Response]]
_METHODS = frozenset({"GET", "POST", "PUT", "PATCH", "DELETE"})


class _NoThreadLock:
    def __enter__(self) -> None:
        return None

    def __exit__(self, *args: object) -> None:
        return None


def _raise_startup_cleanup(
    primary_error: BaseException,
    transaction: _CleanupTransaction,
    on_cleanup_complete: Callable[[], None] | None = None,
) -> NoReturn:
    cleanup_error = ServerStartupError(
        primary_error, transaction, on_cleanup_complete=on_cleanup_complete
    )
    if isinstance(primary_error, (KeyboardInterrupt, SystemExit)):
        raise primary_error from cleanup_error
    raise cleanup_error from primary_error


class _RuntimeLike(Protocol):
    """SmallOS lifecycle surface used by one server invocation."""

    kernel: object

    def fork(self, children: Any) -> Any: ...

    def resume_task(self, task: Any) -> Any: ...

    def cancel_task(self, task: Any) -> Any: ...


class _StartableRuntime(_RuntimeLike, Protocol):
    """Additional lifecycle operation required when SmallServer starts a runtime."""

    def start(self) -> None: ...


def _default_runtime_factory(
    config: ManagedRuntimeConfig | None = None,
) -> _StartableRuntime:
    """Lazily create the supported desktop runtime for managed ``listen``."""
    try:
        from SmallPackage import SmallOS, Unix
    except (ImportError, AttributeError) as exc:
        raise ServerConfigurationError(
            "managed listen() requires SmallOS with the Unix kernel; "
            "install requirements.txt or supply a configured runtime"
        ) from exc
    try:
        runtime_config = None if config is None else config.to_smallos_config()
        return SmallOS(config=runtime_config).setKernel(Unix())
    except Exception as exc:
        raise ServerConfigurationError(
            "managed listen() could not create the default SmallOS Unix runtime; "
            "supply a configured runtime on this platform"
        ) from exc


class _HandleCleanupTransaction(_CleanupTransaction):
    """Retry one handle finalization attempt while exposing all owned errors."""

    def __init__(self, handle: ServerHandle) -> None:
        super().__init__()
        self._handle = handle

        def retry_handle_cleanup() -> None:
            handle.finalize()
            if not handle.finished:
                error = next(
                    iter(handle.cleanup_errors),
                    RuntimeError("server finalization cleanup is incomplete"),
                )
                raise error

        initial_error = next(
            iter(handle.cleanup_errors),
            RuntimeError("server finalization cleanup is incomplete"),
        )
        self.add("server", retry_handle_cleanup, initial_error)

    @property
    def errors(self) -> tuple[BaseException, ...]:
        if not self._handle.finished and self._handle.cleanup_errors:
            return self._handle.cleanup_errors
        return super().errors


class SmallServer:
    """Register static HTTP routes and dispatch requests to async handlers."""

    def __init__(self) -> None:
        self._routes: dict[tuple[str, str], Handler] = {}
        self._active_invocation: object | ServerHandle | None = None
        self._invocation_lock: Any = (
            allocate_lock() if allocate_lock is not None else _NoThreadLock()
        )

    def _reserve_invocation(self) -> object:
        with self._invocation_lock:
            if self._active_invocation is not None:
                raise RuntimeError("this SmallServer already has an active listener")
            marker = object()
            self._active_invocation = marker
            return marker

    def _replace_invocation(
        self, expected: object, replacement: ServerHandle
    ) -> None:
        with self._invocation_lock:
            if self._active_invocation is not expected:
                raise RuntimeError("SmallServer listener ownership changed unexpectedly")
            self._active_invocation = replacement

    def _release_invocation(self, expected: object) -> None:
        with self._invocation_lock:
            if self._active_invocation is expected:
                self._active_invocation = None

    def route(self, path: str, methods: Iterable[str]) -> Callable[[Handler], Handler]:
        if not isinstance(path, str) or not path.startswith("/"):
            raise ValueError("route path must start with '/'")
        normalized = tuple(dict.fromkeys(method.upper() for method in methods))
        if not normalized or any(method not in _METHODS for method in normalized):
            raise ValueError("routes must use one or more supported HTTP methods")

        def register(handler: Handler) -> Handler:
            if not callable(handler):
                raise TypeError("route handler must be callable")
            keys = [(method, path) for method in normalized]
            for method, key_path in keys:
                key = (method, key_path)
                if key in self._routes:
                    raise ValueError("route already registered: {} {}".format(method, path))
            for key in keys:
                self._routes[key] = handler
            return handler

        return register

    def get(self, path: str) -> Callable[[Handler], Handler]:
        return self.route(path, ("GET",))

    def post(self, path: str) -> Callable[[Handler], Handler]:
        return self.route(path, ("POST",))

    def put(self, path: str) -> Callable[[Handler], Handler]:
        return self.route(path, ("PUT",))

    def patch(self, path: str) -> Callable[[Handler], Handler]:
        return self.route(path, ("PATCH",))

    def delete(self, path: str) -> Callable[[Handler], Handler]:
        return self.route(path, ("DELETE",))

    def serve(
        self,
        runtime: _RuntimeLike,
        host: str = "127.0.0.1",
        port: int = 8000,
        config: ServerConfig | None = None,
    ) -> ServerHandle:
        """Bind a TCP listener and schedule SmallOS listener/control tasks.

        The caller owns ``runtime.start()``. On kernels with a wakeup channel,
        ``ServerHandle.close()`` is safe from another thread. Constrained
        kernels use ``await ServerHandle.close_from_task(task)`` on the
        scheduler thread instead.
        """
        config = self._resolve_server_config(config, managed=False)
        self._validate_runtime(runtime, require_start=False)
        return self._bind_and_schedule(runtime, host, port, config)

    @overload
    def listen(
        self,
        host: str = "127.0.0.1",
        port: int = 8000,
        config: ServerConfig | None = None,
        *,
        runtime: None = None,
        start: Literal[True] | None = None,
    ) -> ServerHandle: ...

    @overload
    def listen(
        self,
        host: str = "127.0.0.1",
        port: int = 8000,
        config: ServerConfig | None = None,
        *,
        runtime: _RuntimeLike,
        start: Literal[False] | None = None,
    ) -> ServerHandle: ...

    @overload
    def listen(
        self,
        host: str = "127.0.0.1",
        port: int = 8000,
        config: ServerConfig | None = None,
        *,
        runtime: _StartableRuntime,
        start: bool,
    ) -> ServerHandle: ...

    def listen(
        self,
        host: str = "127.0.0.1",
        port: int = 8000,
        config: ServerConfig | None = None,
        *,
        runtime: _RuntimeLike | None = None,
        start: bool | None = None,
    ) -> ServerHandle:
        """Bind a server and optionally run its SmallOS scheduler.

        With no runtime, this creates and owns a temporary SmallOS/Unix
        runtime, blocks until shutdown, and returns the closed handle. With a
        supplied runtime, scheduling without startup is the default.
        """
        if start is not None and type(start) is not bool:
            raise TypeError("start must be a boolean or None")
        managed = runtime is None
        if managed and start is False:
            raise ValueError("start=False requires a caller-supplied runtime")
        config = self._resolve_server_config(config, managed=managed)
        should_start = managed if start is None else start
        if runtime is None:
            runtime = _default_runtime_factory(config.managed_runtime)
        self._validate_runtime(runtime, require_start=should_start)
        handle = self._bind_and_schedule(runtime, host, port, config)
        if not should_start:
            return handle
        primary_error: BaseException | None = None
        try:
            cast(_StartableRuntime, runtime).start()
        except BaseException as exc:
            primary_error = exc
        finally:
            handle._finish_close(owner_thread=True)
        if primary_error is not None:
            if not handle.finished:
                transaction = self._handle_cleanup_transaction(handle)
                _raise_startup_cleanup(primary_error, transaction)
            if managed and isinstance(primary_error, KeyboardInterrupt):
                return handle
            raise primary_error
        if not handle.finished:
            raise ServerFinalizationError(self._handle_cleanup_transaction(handle))
        return handle

    @staticmethod
    def _handle_cleanup_transaction(handle: ServerHandle) -> _CleanupTransaction:
        return _HandleCleanupTransaction(handle)

    @staticmethod
    def _resolve_server_config(
        config: ServerConfig | None, *, managed: bool
    ) -> ServerConfig:
        if config is not None and not isinstance(config, ServerConfig):
            raise TypeError("config must be a ServerConfig or None")
        resolved = config or ServerConfig()
        runtime_config = resolved.managed_runtime
        if not managed and runtime_config is not None:
            raise ValueError(
                "managed_runtime config applies only when SmallServer creates "
                "the runtime; configure a caller-supplied SmallOS directly"
            )
        if managed:
            priority_levels = (
                ManagedRuntimeConfig().priority_levels
                if runtime_config is None
                else runtime_config.priority_levels
            )
            if max(resolved.listener_priority, resolved.connection_priority) >= priority_levels:
                raise ValueError(
                    "server task priorities must be lower than managed runtime "
                    "priority_levels"
                )
        return resolved

    def _validate_runtime(self, runtime: object, *, require_start: bool) -> None:
        required = ["fork", "resume_task", "cancel_task"]
        if require_start:
            required.append("start")
        missing = [name for name in required if not callable(getattr(runtime, name, None))]
        if missing:
            raise TypeError(
                "runtime is missing required operations: {}".format(", ".join(missing))
            )
        # Constructing the facade is also the pre-bind kernel capability check.
        KernelTransport(getattr(runtime, "kernel", None))

    def _bind_and_schedule(
        self,
        runtime: _RuntimeLike,
        host: str,
        port: int,
        config: ServerConfig,
    ) -> ServerHandle:
        """Shared validated bind-and-schedule core for ``serve`` and ``listen``."""
        from SmallPackage import SmallTask

        if not isinstance(host, str) or not host:
            raise ValueError("host must be a non-empty string")
        if type(port) is not int or not 0 <= port <= 65535:
            raise ValueError("port must be an integer between 0 and 65535")
        marker = self._reserve_invocation()

        def release_marker() -> None:
            self._release_invocation(marker)

        def raise_acquisition_cleanup(
            primary_error: BaseException, transaction: _CleanupTransaction
        ) -> NoReturn:
            _raise_startup_cleanup(
                primary_error,
                transaction,
                on_cleanup_complete=release_marker,
            )

        try:
            transport = KernelTransport(runtime.kernel)
        except BaseException:
            release_marker()
            raise
        try:
            listener = transport.open_listener(host, port, config.max_connections)
        except _TransportAcquisitionFailure as failure:
            raise_acquisition_cleanup(failure.primary_error, failure.transaction)
        except BaseException:
            release_marker()
            raise

        try:
            wakeup = transport.create_wakeup_channel()
        except _TransportAcquisitionFailure as failure:
            try:
                transport.close(listener)
            except BaseException as cleanup_error:
                failure.transaction.add(
                    "listener", lambda: transport.close(listener), cleanup_error
                )
            raise_acquisition_cleanup(failure.primary_error, failure.transaction)
        except BaseException as primary_error:
            try:
                transport.close(listener)
            except BaseException as cleanup_error:
                transaction = _CleanupTransaction()
                transaction.add(
                    "listener", lambda: transport.close(listener), cleanup_error
                )
                raise_acquisition_cleanup(primary_error, transaction)
            release_marker()
            raise

        def release(completed: ServerHandle) -> None:
            self._release_invocation(completed)

        try:
            handle = ServerHandle(
                runtime, transport, listener, wakeup, config, on_finalized=release
            )
        except BaseException as primary_error:
            transaction = _CleanupTransaction()
            if wakeup is not None:
                try:
                    wakeup.close()
                except BaseException as cleanup_error:
                    transaction.add("wakeup", wakeup.close, cleanup_error)
            try:
                transport.close(listener)
            except BaseException as cleanup_error:
                transaction.add(
                    "listener", lambda: transport.close(listener), cleanup_error
                )
            if not transaction.complete:
                raise_acquisition_cleanup(primary_error, transaction)
            release_marker()
            raise
        self._replace_invocation(marker, handle)
        tasks: tuple[Any, ...] = ()
        try:
            listener_task = SmallTask(
                config.listener_priority,
                self._accept_loop,
                args=(handle,),
                name="smallserver-listener",
            )
            tasks = (listener_task,)
            handle._listener_task = listener_task
            handle._owned_tasks.append(listener_task)
            if wakeup is not None:
                close_task = SmallTask(
                    config.listener_priority,
                    self._close_watcher,
                    args=(handle,),
                    name="smallserver-close-watcher",
                )
                tasks = (listener_task, close_task)
                handle._close_task = close_task
                handle._owned_tasks.append(close_task)
            runtime.fork(list(tasks))
        except BaseException as primary_error:
            handle._abort_startup(tasks)
            if not handle.finished:
                transaction = self._handle_cleanup_transaction(handle)
                _raise_startup_cleanup(primary_error, transaction)
            raise
        return handle

    async def dispatch(self, request: Request) -> Response:
        """Run a registered handler or return a deterministic HTTP response."""
        handler = self._routes.get((request.method.upper(), request.path))
        if handler is None:
            allowed = sorted(method for method, path in self._routes if path == request.path)
            if allowed:
                return Response.text("method not allowed", status=405, headers={"Allow": ", ".join(allowed)})
            return Response.text("not found", status=404)
        try:
            result = handler(request)
            if not inspect.isawaitable(result):
                raise TypeError("route handlers must return an awaitable Response")
            response = await result
            if not isinstance(response, Response):
                raise TypeError("route handlers must return Response")
            return response
        except HTTPError as exc:
            return Response.text(exc.detail or "HTTP {}".format(exc.status), status=exc.status)

    async def _accept_loop(self, task: Any, handle: ServerHandle) -> None:
        accepted_in_batch = 0
        while not handle.closed:
            if handle.owned_connection_count >= handle._config.max_connections:
                await handle._wait_for_capacity(task)
                continue
            try:
                accepted = await handle._transport.accept(task, handle._listener)
            except _TransportAcquisitionFailure as failure:
                assert failure.handle is not None
                handle._accepted_setup_failed(
                    failure.primary_error, failure.handle, task
                )
                failure.transaction.transfer()
                raise failure.primary_error
            except BaseException as exc:
                if handle.closed:
                    return
                handle._listener_failed(exc, task)
                raise
            client = accepted.stream
            accepted_in_batch += 1
            if handle.closed:
                if not handle._close_or_retain(client, task):
                    raise client.close_error or RuntimeError(
                        "kernel connection close failed"
                    )
            else:
                from SmallPackage import SmallTask

                connection_task: Any = None
                try:
                    connection_task = SmallTask(
                        handle._config.connection_priority,
                        self._connection_loop,
                        args=(handle, client),
                        name="smallserver-connection",
                    )
                    handle._connections[id(client)] = (client, connection_task)
                    handle._owned_tasks.append(connection_task)
                    runtime = handle._runtime
                    runtime.fork(connection_task)
                except BaseException as registration_error:
                    try:
                        if connection_task is not None:
                            if handle._cancel_or_retain_task(connection_task):
                                if connection_task in handle._owned_tasks:
                                    handle._owned_tasks.remove(connection_task)
                    finally:
                        handle._connections.pop(id(client), None)
                        handle._close_or_retain(
                            client, task, registration_error
                        )
                    handle._listener_failed(registration_error, task)
                    raise
            if accepted_in_batch >= handle._config.accept_batch_size:
                accepted_in_batch = 0
                await task.yield_now()

    async def _close_watcher(self, task: Any, handle: ServerHandle) -> None:
        try:
            assert handle._wakeup is not None
            await task.wait_readable(handle._wakeup.wait_object)
            handle._wakeup.drain()
        finally:
            handle._finish_close(current_task=task)

    async def _connection_loop(
        self, task: Any, handle: ServerHandle, client: TransportHandle
    ) -> None:
        parser = HTTPRequestParser(
            handle._config.max_header_bytes,
            handle._config.max_header_count,
            handle._config.max_body_bytes,
        )
        primary_error: BaseException | None = None
        try:
            while not handle.closed:
                try:
                    chunk = await handle._transport.recv(
                        task, client, handle._config.receive_chunk_bytes
                    )
                except Exception:
                    return
                if not chunk:
                    return
                try:
                    request = parser.feed(chunk)
                except HTTPParseError as exc:
                    await self._send_response(
                        task, handle, client, Response.text(exc.detail, status=exc.status)
                    )
                    return
                if request is None:
                    continue
                try:
                    response = await self.dispatch(request)
                except Exception:
                    response = Response.text("internal server error", status=500)
                await self._send_response(task, handle, client, response)
                return
        except BaseException as exc:
            primary_error = exc
            raise
        finally:
            handle._connection_finished(task, client, primary_error)

    async def _send_response(
        self,
        task: Any,
        handle: ServerHandle,
        client: TransportHandle,
        response: Response,
    ) -> None:
        headers = {name: value for name, value in response.headers.items() if name.lower() != "connection"}
        headers["Connection"] = "close"
        payload = Response(response.status, response.body, headers).to_http1()
        await handle._transport.send_all(task, client, payload)
