import socket
import threading
import unittest

from SmallPackage import SmallOS, Unix

from smallserver import Response, SmallServer


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
