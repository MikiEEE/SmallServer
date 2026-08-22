import importlib.util
from dataclasses import FrozenInstanceError
import inspect
import socket
import threading
import time
import unittest

from SmallPackage import SmallOS, Unix
from SmallPackage.adapters.threads import ThreadAdapter

from smallserver import (
    AdapterRegistry,
    RegexRouteConfig,
    Request,
    Response,
    RouteErrorEvent,
    RouteMatchTimeout,
    SmallServer,
)
from smallserver.server import ServerHandle, run_route_observer


HAS_REGEX = importlib.util.find_spec("regex") is not None


class SmallOSServerIntegrationTests(unittest.TestCase):
    def _exchange(self, port: int, payload: bytes) -> bytes:
        with socket.create_connection(("127.0.0.1", port), timeout=3) as connection:
            connection.sendall(payload)
            chunks = []
            while True:
                chunk = connection.recv(4096)
                if not chunk:
                    return b"".join(chunks)
                chunks.append(chunk)

    def _request(self, port: int, path: str) -> bytes:
        return self._exchange(
            port,
            "GET {} HTTP/1.1\r\nHost: localhost\r\n\r\n".format(path).encode("ascii"),
        )

    def test_loopback_server_accepts_fragmented_request_and_shuts_down(self) -> None:
        runtime = SmallOS().setKernel(Unix())
        app = SmallServer()

        @app.get("/health")
        async def health(request):
            return Response.json({"status": "ok"})

        try:
            server = app.serve(runtime, host="127.0.0.1", port=0)
        except PermissionError:
            self.skipTest("the current sandbox does not permit loopback TCP binds")

        received: list[bytes] = []
        errors: list[BaseException] = []

        def client() -> None:
            try:
                with socket.create_connection(("127.0.0.1", server.port), timeout=2) as connection:
                    connection.sendall(b"GET /hea")
                    connection.sendall(b"lth HTTP/1.1\r\nHost: localhost\r\n\r\n")
                    while True:
                        chunk = connection.recv(4096)
                        if not chunk:
                            break
                        received.append(chunk)
            except BaseException as exc:
                errors.append(exc)
            finally:
                server.close()

        worker = threading.Thread(target=client, daemon=True)
        worker.start()
        runtime.start()
        worker.join(timeout=2)

        self.assertFalse(worker.is_alive())
        self.assertEqual(errors, [])
        self.assertEqual(
            b"".join(received),
            b"HTTP/1.1 200 OK\r\nContent-Length: 15\r\nContent-Type: application/json\r\nConnection: close\r\n\r\n{\"status\":\"ok\"}",
        )
        self.assertEqual(runtime.ioReadWaiters, {})
        self.assertEqual(runtime.ioWriteWaiters, {})
        self.assertIsNone(runtime._io_wait_set)
        self.assertTrue(server._listener.closed)
        self.assertIsNone(server.failure)
        self.assertIsNone(server._listener_task.exception)

    def test_blocking_adapter_does_not_block_unrelated_connection(self) -> None:
        runtime = SmallOS().setKernel(Unix())
        app = SmallServer()
        blocking_started = threading.Event()
        release_blocking = threading.Event()
        responses: dict[str, bytes] = {}
        errors: list[BaseException] = []

        def blocking_call() -> str:
            blocking_started.set()
            if not release_blocking.wait(3):
                raise TimeoutError("fast request did not progress")
            return "slow done"

        with AdapterRegistry(
            blocking=ThreadAdapter(max_workers=1, max_pending=2)
        ) as services:

            @app.get("/slow")
            async def slow(request):
                value = await services.call("blocking", blocking_call)
                return Response.text(value)

            @app.get("/fast")
            async def fast(request):
                return Response.text("fast done")

            try:
                server = app.serve(runtime, host="127.0.0.1", port=0)
            except PermissionError:
                self.skipTest("the current sandbox does not permit loopback TCP binds")

            def slow_client() -> None:
                try:
                    responses["slow"] = self._request(server.port, "/slow")
                except BaseException as exc:
                    errors.append(exc)
                finally:
                    server.close()

            def fast_client() -> None:
                try:
                    if not blocking_started.wait(2):
                        raise TimeoutError("blocking adapter did not start")
                    responses["fast"] = self._request(server.port, "/fast")
                except BaseException as exc:
                    errors.append(exc)
                finally:
                    release_blocking.set()

            slow_worker = threading.Thread(target=slow_client, daemon=True)
            fast_worker = threading.Thread(target=fast_client, daemon=True)
            slow_worker.start()
            fast_worker.start()
            runtime.start()
            slow_worker.join(timeout=3)
            fast_worker.join(timeout=3)

            self.assertFalse(slow_worker.is_alive())
            self.assertFalse(fast_worker.is_alive())
            self.assertEqual(errors, [])
            self.assertIn(b"\r\n\r\nfast done", responses["fast"])
            self.assertIn(b"\r\n\r\nslow done", responses["slow"])

    def test_slow_client_does_not_block_a_complete_request(self) -> None:
        runtime = SmallOS().setKernel(Unix())
        app = SmallServer()

        @app.get("/fast")
        async def fast(request):
            return Response.text("fast")

        try:
            server = app.serve(runtime, host="127.0.0.1", port=0)
        except PermissionError:
            self.skipTest("the current sandbox does not permit loopback TCP binds")

        received: list[bytes] = []
        errors: list[BaseException] = []

        def clients() -> None:
            slow = None
            try:
                slow = socket.create_connection(("127.0.0.1", server.port), timeout=2)
                slow.sendall(b"GET /slow HTTP/1.1\r\nHost: local")
                with socket.create_connection(("127.0.0.1", server.port), timeout=2) as fast_client:
                    fast_client.sendall(b"GET /fast HTTP/1.1\r\nHost: localhost\r\n\r\n")
                    while True:
                        chunk = fast_client.recv(4096)
                        if not chunk:
                            break
                        received.append(chunk)
            except BaseException as exc:
                errors.append(exc)
            finally:
                if slow is not None:
                    slow.close()
                server.close()

        worker = threading.Thread(target=clients, daemon=True)
        worker.start()
        runtime.start()
        worker.join(timeout=2)

        self.assertFalse(worker.is_alive())
        self.assertEqual(errors, [])
        self.assertEqual(
            b"".join(received),
            b"HTTP/1.1 200 OK\r\nContent-Length: 4\r\nContent-Type: text/plain; charset=utf-8\r\n"
            b"Connection: close\r\n\r\nfast",
        )
        self.assertEqual(runtime.ioReadWaiters, {})
        self.assertEqual(runtime.ioWriteWaiters, {})

    def test_managed_listen_serves_loopback_and_returns_closed_handle(self) -> None:
        app = SmallServer()

        @app.get("/health")
        async def health(request):
            return Response.json({"status": "ok"})

        returned: list[object] = []
        errors: list[BaseException] = []

        def run_server() -> None:
            try:
                returned.append(app.listen(host="127.0.0.1", port=0))
            except BaseException as exc:
                errors.append(exc)

        worker = threading.Thread(target=run_server, daemon=True)
        worker.start()
        handle = None
        for _ in range(200):
            candidate = app._active_invocation
            if isinstance(candidate, ServerHandle):
                handle = candidate
                break
            if errors:
                break
            time.sleep(0.01)
        if errors and isinstance(errors[0], PermissionError):
            self.skipTest("the current sandbox does not permit loopback TCP binds")
        self.assertEqual(errors, [])
        self.assertIsNotNone(handle)
        assert handle is not None

        response = self._request(handle.port, "/health")
        handle.close()
        worker.join(timeout=3)

        self.assertFalse(worker.is_alive())
        self.assertEqual(errors, [])
        self.assertEqual(returned, [handle])
        self.assertTrue(handle.closed)
        self.assertEqual(handle.port, returned[0].port)
        self.assertIn(b"HTTP/1.1 200 OK", response)

    @unittest.skipUnless(HAS_REGEX, "regex-routes extra is not installed")
    def test_loopback_regex_route_uses_path_without_query(self) -> None:
        runtime = SmallOS().setKernel(Unix())
        app = SmallServer()

        @app.get_regex(r"/files/(?P<name>[^/]+)")
        async def file(request):
            return Response.text(request.path_params["name"] + "?" + request.query_string)

        try:
            server = app.serve(runtime, host="127.0.0.1", port=0)
        except PermissionError:
            self.skipTest("the current sandbox does not permit loopback TCP binds")

        received = []
        errors = []

        def client() -> None:
            try:
                received.append(self._request(server.port, "/files/a%2Fb?download=1"))
            except BaseException as exc:
                errors.append(exc)
            finally:
                server.close()

        worker = threading.Thread(target=client, daemon=True)
        worker.start()
        runtime.start()
        worker.join(timeout=2)
        self.assertFalse(worker.is_alive())
        self.assertEqual(errors, [])
        self.assertIn(b"\r\n\r\na%2Fb?download=1", b"".join(received))

    @unittest.skipUnless(HAS_REGEX, "regex-routes extra is not installed")
    def test_regex_timeout_is_observed_once_and_does_not_stop_server(self) -> None:
        runtime = SmallOS().setKernel(Unix())
        observed: list[RouteErrorEvent] = []
        observer_graph = []
        observer_finished = threading.Event()
        observer_threads = []

        def observe(event: RouteErrorEvent) -> None:
            observed.append(event)
            observer_threads.append(threading.current_thread())
            caller_locals = []
            frame = inspect.currentframe()
            while frame is not None:
                caller_locals.append(dict(frame.f_locals))
                if frame.f_code is run_route_observer.__code__:
                    break
                frame = frame.f_back
            observer_graph.extend(_reachable_container_values(caller_locals))
            observer_finished.set()
            raise RuntimeError("intentional observer failure")

        app = SmallServer(
            RegexRouteConfig(match_timeout=0.001, total_match_timeout=0.005),
            route_error_observer=observe,
        )

        pattern_secret = "sensitive-pattern-marker"

        @app.post_regex(r"/(a+)+$(?#sensitive-pattern-marker)")
        async def expensive(request):
            return Response()

        @app.get("/health")
        async def health(request):
            return Response.text("healthy")

        try:
            server = app.serve(runtime, host="127.0.0.1", port=0)
        except PermissionError:
            self.skipTest("the current sandbox does not permit loopback TCP binds")

        hostile_path = "/" + "a" * 5000 + "!"
        authorization_secret = "Bearer sensitive-authorization-marker"
        body_secret = b"sensitive-body-marker"
        runtime_thread = threading.Thread(
            target=runtime.start,
            name="smallos-runtime-test",
            daemon=True,
        )
        runtime_thread.start()
        request = (
            "POST {} HTTP/1.1\r\n"
            "Host: localhost\r\n"
            "Authorization: {}\r\n"
            "Content-Length: {}\r\n\r\n"
        ).format(hostile_path, authorization_secret, len(body_secret)).encode("ascii")
        received = [self._exchange(server.port, request + body_secret)]
        received.append(self._request(server.port, "/health"))
        self.assertTrue(observer_finished.wait(2), "route observer did not run")
        server.close()
        runtime_thread.join(timeout=3)
        self.assertFalse(runtime_thread.is_alive())
        self.assertEqual(len(observed), 1)
        event = observed[0]
        self.assertEqual(event.route_id, "regex-route-1")
        self.assertEqual(event.category, "route_match_timeout")
        with self.assertRaises(FrozenInstanceError):
            event.route_id = "changed"  # type: ignore[misc]
        self.assertFalse(hasattr(event, "__traceback__"))
        self.assertFalse(hasattr(event, "__cause__"))
        self.assertFalse(hasattr(event, "__context__"))

        reachable = _reachable_objects(event)
        reachable_strings = {value for value in reachable if isinstance(value, str)}
        self.assertEqual(
            reachable_strings,
            {"route_id", "category", "regex-route-1", "route_match_timeout"},
        )
        self.assertFalse(any(isinstance(value, Request) for value in reachable))
        for secret in (hostile_path, authorization_secret, body_secret.decode("ascii"), pattern_secret):
            self.assertNotIn(secret, reachable_strings)

        caller_strings = {value for value in observer_graph if isinstance(value, str)}
        self.assertFalse(any(isinstance(value, Request) for value in observer_graph))
        self.assertFalse(any(isinstance(value, RouteMatchTimeout) for value in observer_graph))
        for secret in (hostile_path, authorization_secret, body_secret.decode("ascii"), pattern_secret):
            self.assertNotIn(secret, caller_strings)
        self.assertNotIn(body_secret, observer_graph)
        self.assertEqual(server.route_observer_failures, 1)
        self.assertEqual(server.dropped_route_error_events, 0)
        self.assertEqual(observer_threads, [runtime_thread])
        self.assertNotIn(
            "smallserver-route-observer",
            {thread.name for thread in threading.enumerate()},
        )
        channel = server._route_observer_channel
        self.assertIsNotNone(channel)
        assert channel is not None
        self.assertFalse(channel.accepting)
        self.assertEqual(list(channel.events), [])
        self.assertIsNone(channel.task)
        self.assertTrue(received[0].startswith(b"HTTP/1.1 500 Internal Server Error\r\n"))
        self.assertNotIn(hostile_path.encode("ascii"), received[0])
        self.assertTrue(received[1].startswith(b"HTTP/1.1 200 OK\r\n"))
        self.assertTrue(received[1].endswith(b"healthy"))

    @unittest.skipUnless(HAS_REGEX, "regex-routes extra is not installed")
    def test_regex_path_limit_returns_414_before_matching(self) -> None:
        runtime = SmallOS().setKernel(Unix())
        app = SmallServer(RegexRouteConfig(max_path_bytes=8))

        @app.get_regex(r"/.*")
        async def route(request):
            return Response.text("must not run")

        try:
            server = app.serve(runtime, host="127.0.0.1", port=0)
        except PermissionError:
            self.skipTest("the current sandbox does not permit loopback TCP binds")

        received = []
        errors = []

        def client() -> None:
            try:
                received.append(self._request(server.port, "/12345678"))
            except BaseException as exc:
                errors.append(exc)
            finally:
                server.close()

        worker = threading.Thread(target=client, daemon=True)
        worker.start()
        runtime.start()
        worker.join(timeout=2)
        self.assertFalse(worker.is_alive())
        self.assertEqual(errors, [])
        self.assertTrue(received[0].startswith(b"HTTP/1.1 414 URI Too Long\r\n"))
        self.assertNotIn(b"must not run", received[0])


def _reachable_objects(root):
    pending = [root]
    seen = set()
    result = []
    while pending:
        value = pending.pop()
        identity = id(value)
        if identity in seen:
            continue
        seen.add(identity)
        result.append(value)
        if isinstance(value, dict):
            pending.extend(value.keys())
            pending.extend(value.values())
        elif isinstance(value, (tuple, list, set, frozenset)):
            pending.extend(value)
        elif hasattr(value, "__dict__"):
            pending.append(vars(value))
    return result


def _reachable_container_values(root):
    """Walk frame-local containers without traversing scheduler object graphs."""
    pending = [root]
    seen = set()
    result = []
    while pending:
        value = pending.pop()
        identity = id(value)
        if identity in seen:
            continue
        seen.add(identity)
        result.append(value)
        if isinstance(value, dict):
            pending.extend(value.keys())
            pending.extend(value.values())
        elif isinstance(value, (tuple, list, set, frozenset)):
            pending.extend(value)
    return result
