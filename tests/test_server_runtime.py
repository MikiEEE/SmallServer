import importlib.util
import socket
import threading
import unittest

from SmallPackage import SmallOS, Unix
from SmallPackage.adapters.threads import ThreadAdapter

from smallserver import AdapterRegistry, RegexRouteConfig, Response, RouteMatchTimeout, SmallServer


HAS_REGEX = importlib.util.find_spec("regex") is not None


class SmallOSServerIntegrationTests(unittest.TestCase):
    def _request(self, port: int, path: str) -> bytes:
        with socket.create_connection(("127.0.0.1", port), timeout=3) as connection:
            connection.sendall(
                "GET {} HTTP/1.1\r\nHost: localhost\r\n\r\n".format(path).encode("ascii")
            )
            chunks = []
            while True:
                chunk = connection.recv(4096)
                if not chunk:
                    return b"".join(chunks)
                chunks.append(chunk)

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
        observed: list[RouteMatchTimeout] = []
        app = SmallServer(
            RegexRouteConfig(match_timeout=0.001, total_match_timeout=0.005),
            route_error_observer=observed.append,
        )

        @app.get_regex(r"/(a+)+$")
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
        received = []
        errors = []

        def client() -> None:
            try:
                received.append(self._request(server.port, hostile_path))
                received.append(self._request(server.port, "/health"))
            except BaseException as exc:
                errors.append(exc)
            finally:
                server.close()

        worker = threading.Thread(target=client, daemon=True)
        worker.start()
        runtime.start()
        worker.join(timeout=3)
        self.assertFalse(worker.is_alive())
        self.assertEqual(errors, [])
        self.assertEqual(len(observed), 1)
        self.assertEqual(observed[0].route_id, "regex-route-1")
        self.assertNotIn(hostile_path, str(observed[0]))
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
