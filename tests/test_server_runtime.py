import socket
import threading
import unittest

from SmallPackage import SmallOS, Unix
from SmallPackage.adapters.threads import ThreadAdapter

from smallserver import AdapterRegistry, Response, SmallServer


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
