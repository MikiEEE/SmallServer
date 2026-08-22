"""Route registration and handler dispatch for SmallServer."""

from __future__ import annotations

import inspect
from collections.abc import Awaitable, Callable, Iterable
from dataclasses import replace
import socket
from typing import Any

from .errors import HTTPError
from .http import Request, Response
from .routing import (
    RegexRouteConfig,
    RouteErrorEvent,
    RouteMatchTimeout,
    RoutePathTooLarge,
    Router,
)
from .server import (
    HTTPParseError,
    HTTPRequestParser,
    RouteObserverChannel,
    ServerConfig,
    ServerHandle,
    run_route_observer,
)

Handler = Callable[[Request], Awaitable[Response]]
RouteErrorObserver = Callable[[RouteErrorEvent], None]


class SmallServer:
    """Register static HTTP routes and dispatch requests to async handlers."""

    def __init__(
        self,
        regex_config: RegexRouteConfig | None = None,
        *,
        route_error_observer: RouteErrorObserver | None = None,
    ) -> None:
        if route_error_observer is not None and not callable(route_error_observer):
            raise TypeError("route_error_observer must be callable or None")
        self._router = Router(regex_config)
        self._route_error_observer = route_error_observer

    def route(self, path: str, methods: Iterable[str]) -> Callable[[Handler], Handler]:
        if not isinstance(path, str) or not path.startswith("/"):
            raise ValueError("route path must start with '/'")
        if "?" in path or "#" in path:
            raise ValueError("route path must not contain a query string or fragment")
        normalized = self._router.normalize_methods(methods)

        def register(handler: Handler) -> Handler:
            if not callable(handler):
                raise TypeError("route handler must be callable")
            self._router.add_static(path, normalized, handler)
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

    def route_regex(self, pattern: str, methods: Iterable[str]) -> Callable[[Handler], Handler]:
        """Register a timeout-bounded full-path regular-expression route."""
        normalized = self._router.normalize_methods(methods)

        def register(handler: Handler) -> Handler:
            if not callable(handler):
                raise TypeError("route handler must be callable")
            self._router.add_regex(pattern, normalized, handler)
            return handler

        return register

    def get_regex(self, pattern: str) -> Callable[[Handler], Handler]:
        return self.route_regex(pattern, ("GET",))

    def post_regex(self, pattern: str) -> Callable[[Handler], Handler]:
        return self.route_regex(pattern, ("POST",))

    def put_regex(self, pattern: str) -> Callable[[Handler], Handler]:
        return self.route_regex(pattern, ("PUT",))

    def patch_regex(self, pattern: str) -> Callable[[Handler], Handler]:
        return self.route_regex(pattern, ("PATCH",))

    def delete_regex(self, pattern: str) -> Callable[[Handler], Handler]:
        return self.route_regex(pattern, ("DELETE",))

    def serve(
        self,
        runtime: Any,
        host: str = "127.0.0.1",
        port: int = 8000,
        config: ServerConfig | None = None,
    ) -> ServerHandle:
        """Bind a TCP listener and schedule SmallOS listener/control tasks.

        The caller owns ``runtime.start()``. ``ServerHandle.close()`` is safe
        from a client or another thread and wakes the scheduler without
        directly mutating SmallOS task state there.
        """
        from SmallPackage import SmallTask

        if not isinstance(host, str) or not host:
            raise ValueError("host must be a non-empty string")
        if not isinstance(port, int) or not 0 <= port <= 65535:
            raise ValueError("port must be an integer between 0 and 65535")
        config = config or ServerConfig()
        listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            listener.bind((host, port))
            listener.listen(config.max_connections)
            listener.setblocking(False)
        except BaseException:
            listener.close()
            raise
        observer_channel = (
            RouteObserverChannel(self._route_error_observer, config.max_route_error_events)
            if self._route_error_observer is not None
            else None
        )
        handle = ServerHandle(runtime, listener, config, observer_channel)
        listener_task = SmallTask(
            config.listener_priority,
            self._accept_loop,
            args=(handle,),
            name="smallserver-listener",
        )
        close_task = SmallTask(
            config.listener_priority,
            self._close_watcher,
            args=(handle,),
            name="smallserver-close-watcher",
        )
        handle._listener_task = listener_task
        tasks: tuple[Any, ...] = (listener_task, close_task)
        if observer_channel is not None:
            observer_task = SmallTask(
                config.listener_priority,
                run_route_observer,
                args=(observer_channel,),
                name="smallserver-route-observer",
            )
            observer_channel.bind(observer_task)
            tasks += (observer_task,)
        try:
            runtime.fork(list(tasks))
        except BaseException:
            handle._abort_startup(tasks)
            raise
        return handle

    async def dispatch(self, request: Request) -> Response:
        """Run a registered handler or return a deterministic HTTP response."""
        handler = self._router.static_handler(request.method, request.path)
        if handler is not None:
            if request.path_params or request.route_pattern is not None:
                request = replace(request, path_params={}, route_pattern=None)
        else:
            try:
                match = self._router.resolve(request.method, request.path)
            except RoutePathTooLarge:
                return Response.text("request target is too large", status=414)
            if match.handler is None:
                if match.allowed_methods:
                    return Response.text("method not allowed", status=405, headers={"Allow": ", ".join(match.allowed_methods)})
                return Response.text("not found", status=404)
            handler = match.handler
            request = replace(request, path_params=match.path_params, route_pattern=match.route_pattern)
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
        while not handle.closed:
            await task.wait_readable(handle._listener)
            if handle.closed:
                return
            while not handle.closed:
                try:
                    client, _ = handle._listener.accept()
                except BlockingIOError:
                    break
                except OSError:
                    return
                client.setblocking(False)
                if len(handle._connections) >= handle._config.max_connections:
                    client.close()
                    continue
                from SmallPackage import SmallTask

                connection_task = SmallTask(
                    handle._config.connection_priority,
                    self._connection_loop,
                    args=(handle, client),
                    name="smallserver-connection",
                )
                handle._connections[client] = connection_task
                runtime = handle._runtime
                runtime.fork(connection_task)

    async def _close_watcher(self, task: Any, handle: ServerHandle) -> None:
        await task.wait_readable(handle._wake_read)
        try:
            while handle._wake_read.recv(1024):
                pass
        except (BlockingIOError, OSError):
            pass
        handle._finish_close()

    async def _connection_loop(self, task: Any, handle: ServerHandle, client: socket.socket) -> None:
        parser = HTTPRequestParser(
            handle._config.max_header_bytes,
            handle._config.max_header_count,
            handle._config.max_body_bytes,
            handle._config.max_request_target_bytes,
        )
        route_error_event: RouteErrorEvent | None = None
        try:
            while not handle.closed:
                try:
                    chunk = client.recv(handle._config.receive_chunk_bytes)
                except BlockingIOError:
                    await task.wait_readable(client)
                    continue
                except OSError:
                    return
                if not chunk:
                    return
                try:
                    request = parser.feed(chunk)
                except HTTPParseError as exc:
                    await self._send_response(task, client, Response.text(exc.detail, status=exc.status))
                    return
                if request is None:
                    continue
                try:
                    response = await self.dispatch(request)
                except RouteMatchTimeout as exc:
                    route_error_event = RouteErrorEvent(
                        route_id=exc.route_id,
                        category="route_match_timeout",
                    )
                    response = Response.text("internal server error", status=500)
                except Exception:
                    response = Response.text("internal server error", status=500)
                await self._send_response(task, client, response)
                return
        finally:
            handle._connections.pop(client, None)
            try:
                client.close()
            except OSError:
                pass
            observer_channel = handle._route_observer_channel
            if route_error_event is not None and observer_channel is not None:
                observer_channel.enqueue(route_error_event, task)

    async def _send_response(self, task: Any, client: socket.socket, response: Response) -> None:
        headers = {name: value for name, value in response.headers.items() if name.lower() != "connection"}
        headers["Connection"] = "close"
        payload = Response(response.status, response.body, headers).to_http1()
        offset = 0
        while offset < len(payload):
            try:
                sent = client.send(payload[offset:])
            except BlockingIOError:
                await task.wait_writable(client)
                continue
            if sent <= 0:
                return
            offset += sent
