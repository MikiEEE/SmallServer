import ast
from pathlib import Path
import unittest

from smallserver import Response, SmallServer
from smallserver._transport import KernelTransport
from smallserver.server import ServerConfig, ServerHandle

from tests.kernel_fakes import FakeKernel, NeedsRead, NeedsWrite, OpaqueHandle


def run_immediate(coroutine):
    try:
        while True:
            coroutine.send(None)
    except StopIteration as exc:
        return exc.value


class FakeTask:
    def __init__(self) -> None:
        self.waits: list[tuple[str, object]] = []

    async def wait_readable(self, handle: object) -> None:
        self.waits.append(("read", handle))

    async def wait_writable(self, handle: object) -> None:
        self.waits.append(("write", handle))


class KernelTransportTests(unittest.TestCase):
    def test_capability_failure_happens_before_address_resolution(self) -> None:
        kernel = FakeKernel(supported=False)
        with self.assertRaisesRegex(NotImplementedError, "does not support"):
            KernelTransport(kernel)
        self.assertEqual(kernel.calls, [("supports_tcp_server",)])

    def test_wakeup_capability_failure_happens_before_address_resolution(self) -> None:
        kernel = FakeKernel(wakeup_supported=False)
        with self.assertRaisesRegex(NotImplementedError, "wakeup channels"):
            KernelTransport(kernel)
        self.assertEqual(
            kernel.calls,
            [("supports_tcp_server",), ("supports_wakeup_channel",)],
        )

    def test_incomplete_contract_fails_before_address_resolution(self) -> None:
        kernel = FakeKernel()
        kernel.socket_bind = None  # type: ignore[assignment]
        with self.assertRaisesRegex(TypeError, "socket_bind"):
            KernelTransport(kernel)
        self.assertEqual(
            kernel.calls,
            [("supports_tcp_server",), ("supports_wakeup_channel",)],
        )

    def test_listener_uses_one_opaque_address_record_and_rolls_back_failure(self) -> None:
        kernel = FakeKernel()
        kernel.fail_operation = "listen"
        transport = KernelTransport(kernel)
        with self.assertRaisesRegex(RuntimeError, "listen failed"):
            transport.open_listener("0.0.0.0", 0, 7)
        open_call = next(call for call in kernel.calls if call[0] == "socket_open")
        bind_call = next(call for call in kernel.calls if call[0] == "socket_bind")
        self.assertIs(open_call[1], kernel.address_info)
        self.assertIs(bind_call[2], kernel.address_info)
        self.assertEqual(kernel.closed, [kernel.listener])

    def test_accept_and_stream_operations_honor_both_retry_directions(self) -> None:
        kernel = FakeKernel()
        transport = KernelTransport(kernel)
        client = OpaqueHandle("client")
        fallback_peer = ("192.0.2.4", 80)
        kernel.accept_results = [NeedsRead(), NeedsWrite(), (client, fallback_peer)]
        kernel.recv_results[id(client)] = [NeedsWrite(), NeedsRead(), b"request"]
        kernel.send_results[id(client)] = [NeedsRead(), NeedsWrite(), 2, 3]
        task = FakeTask()

        accepted = run_immediate(transport.accept(task, kernel.listener))
        received = run_immediate(transport.recv(task, client, 16))
        run_immediate(transport.send_all(task, client, b"reply"))

        self.assertIs(accepted.stream, client)
        self.assertEqual(accepted.peer_address, fallback_peer)
        self.assertEqual(received, b"request")
        self.assertEqual(
            [mode for mode, _ in task.waits],
            ["read", "write", "write", "read", "read", "write"],
        )
        self.assertEqual(kernel.sent[id(client)][-1], b"ply")

    def test_accept_configuration_failure_closes_the_new_stream_once(self) -> None:
        kernel = FakeKernel()
        transport = KernelTransport(kernel)
        client = OpaqueHandle("client")
        kernel.accept_results = [(client, ("127.0.0.1", 1))]
        kernel.fail_operation = "setblocking"
        with self.assertRaisesRegex(RuntimeError, "setblocking failed"):
            run_immediate(transport.accept(FakeTask(), kernel.listener))
        transport.close_safely(client)
        self.assertEqual(kernel.closed, [client])

    def test_connection_registration_failure_cancels_task_and_closes_stream(self) -> None:
        class Runtime:
            def __init__(self) -> None:
                self.cancelled = []

            def fork(self, task) -> None:
                raise RuntimeError("capacity")

            def cancel_task(self, task) -> None:
                self.cancelled.append(task)
                task.cancel()

        kernel = FakeKernel()
        transport = KernelTransport(kernel)
        listener = transport.open_listener("127.0.0.1", 0, 3)
        handle = ServerHandle(
            Runtime(), transport, listener, transport.create_wakeup_channel(), ServerConfig()
        )
        client = OpaqueHandle("client")
        kernel.accept_results = [(client, ("127.0.0.1", 1)), RuntimeError("listener failed")]

        run_immediate(SmallServer()._accept_loop(FakeTask(), handle))

        self.assertEqual(len(handle._runtime.cancelled), 1)
        self.assertEqual(kernel.closed, [client])
        self.assertEqual(handle._connections, {})

    def test_server_handle_signals_and_releases_each_resource_once(self) -> None:
        class Runtime:
            def __init__(self) -> None:
                self.resumed = []

            def resume_task(self, task) -> None:
                self.resumed.append(task)
                raise RuntimeError("resume failed")

        kernel = FakeKernel()
        transport = KernelTransport(kernel)
        listener = transport.open_listener("127.0.0.1", 0, 3)
        wakeup = transport.create_wakeup_channel()
        runtime = Runtime()
        handle = ServerHandle(runtime, transport, listener, wakeup, ServerConfig())
        connection = OpaqueHandle("connection")
        connection_task = object()
        listener_task = object()
        handle._connections[id(connection)] = (connection, connection_task)
        handle._listener_task = listener_task

        handle.close()
        handle.close()
        handle._finish_close()
        handle._finish_close()
        transport.close_safely(connection)

        self.assertEqual(wakeup.notify_calls, 1)
        self.assertEqual(wakeup.close_calls, 1)
        self.assertEqual(runtime.resumed, [connection_task, listener_task])
        self.assertEqual(kernel.closed, [connection, listener])

    def test_fake_kernel_connection_preserves_http_response_bytes(self) -> None:
        class Runtime:
            def resume_task(self, task) -> None:
                pass

        kernel = FakeKernel()
        transport = KernelTransport(kernel)
        listener = transport.open_listener("127.0.0.1", 0, 3)
        handle = ServerHandle(
            Runtime(), transport, listener, transport.create_wakeup_channel(), ServerConfig()
        )
        client = OpaqueHandle("client")
        kernel.recv_results[id(client)] = [b"GET /health HTTP/1.1\r\nHost: localhost\r\n\r\n"]
        app = SmallServer()

        @app.get("/health")
        async def health(request):
            return Response.json({"status": "ok"})

        run_immediate(app._connection_loop(FakeTask(), handle, client))

        self.assertEqual(
            kernel.sent[id(client)][0],
            b"HTTP/1.1 200 OK\r\nContent-Length: 15\r\nContent-Type: application/json\r\n"
            b"Connection: close\r\n\r\n{\"status\":\"ok\"}",
        )
        self.assertEqual(kernel.closed, [client])

    def test_production_modules_do_not_import_platform_networking(self) -> None:
        forbidden = {"socket", "select", "selectors", "ssl"}
        package = Path(__file__).parents[1] / "smallserver"
        found = []
        for path in package.glob("*.py"):
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
            for node in ast.walk(tree):
                if isinstance(node, ast.Import):
                    found.extend((path.name, alias.name) for alias in node.names if alias.name in forbidden)
                elif isinstance(node, ast.ImportFrom) and node.module in forbidden:
                    found.append((path.name, node.module))
        self.assertEqual(found, [])
