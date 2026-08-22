"""Route registration and handler dispatch for SmallServer."""

from __future__ import annotations

import inspect
from collections.abc import Awaitable, Callable, Iterable
from typing import Any

from ._transport import KernelTransport, TransportHandle
from .errors import HTTPError
from .http import Request, Response
from .server import HTTPParseError, HTTPRequestParser, ServerConfig, ServerHandle

Handler = Callable[[Request], Awaitable[Response]]
_METHODS = frozenset({"GET", "POST", "PUT", "PATCH", "DELETE"})


class SmallServer:
    """Register static HTTP routes and dispatch requests to async handlers."""

    def __init__(self) -> None:
        self._routes: dict[tuple[str, str], Handler] = {}

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
        runtime: Any,
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
        from SmallPackage import SmallTask

        if not isinstance(host, str) or not host:
            raise ValueError("host must be a non-empty string")
        if not isinstance(port, int) or not 0 <= port <= 65535:
            raise ValueError("port must be an integer between 0 and 65535")
        config = config or ServerConfig()
        transport = KernelTransport(getattr(runtime, "kernel", None))
        listener = transport.open_listener(host, port, config.max_connections)
        try:
            wakeup = transport.create_wakeup_channel()
        except BaseException:
            transport.close_safely(listener)
            raise
        handle = ServerHandle(runtime, transport, listener, wakeup, config)
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
            if wakeup is not None:
                close_task = SmallTask(
                    config.listener_priority,
                    self._close_watcher,
                    args=(handle,),
                    name="smallserver-close-watcher",
                )
                tasks = (listener_task, close_task)
            runtime.fork(list(tasks))
        except BaseException:
            handle._abort_startup(tasks)
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
            try:
                accepted = await handle._transport.accept(task, handle._listener)
            except Exception as exc:
                if handle.closed:
                    return
                handle._listener_failed(exc, task)
                raise
            client = accepted.stream
            accepted_in_batch += 1
            if handle.closed or len(handle._connections) >= handle._config.max_connections:
                handle._close_or_retain(client)
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
                    runtime = handle._runtime
                    runtime.fork(connection_task)
                except Exception:
                    handle._connections.pop(id(client), None)
                    if connection_task is not None:
                        cancel_task = getattr(handle._runtime, "cancel_task", None)
                        if callable(cancel_task):
                            try:
                                cancel_task(connection_task)
                            except Exception:
                                pass
                    handle._close_or_retain(client)
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
        finally:
            handle._connection_finished(task, client)

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
