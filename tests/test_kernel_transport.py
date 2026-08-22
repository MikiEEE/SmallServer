import ast
from pathlib import Path
import unittest

from smallserver import Response, SmallServer
from smallserver._transport import KernelTransport, TransportHandle
from smallserver.server import ServerConfig, ServerHandle

from tests.kernel_fakes import (
    FakeKernel,
    NeedsRead,
    NeedsWrite,
    OpaqueHandle,
    TLSWantRead,
    TLSWantWrite,
    WouldBlock,
)


def run_immediate(coroutine):
    try:
        while True:
            coroutine.send(None)
    except StopIteration as exc:
        return exc.value


class FakeTask:
    def __init__(self) -> None:
        self.waits: list[tuple[str, object]] = []
        self.yields = 0

    async def wait_readable(self, handle: object) -> None:
        self.waits.append(("read", handle))

    async def wait_writable(self, handle: object) -> None:
        self.waits.append(("write", handle))

    async def yield_now(self) -> None:
        self.yields += 1

    async def wait_signal(self, signal: int) -> None:
        self.waits.append(("signal", signal))


class KernelTransportTests(unittest.TestCase):
    def test_capability_failure_happens_before_address_resolution(self) -> None:
        kernel = FakeKernel(supported=False)
        with self.assertRaisesRegex(NotImplementedError, "does not support"):
            KernelTransport(kernel)
        self.assertEqual(kernel.calls, [("supports_tcp_server",)])

    def test_kernel_without_wakeup_support_retains_scheduler_close_path(self) -> None:
        kernel = FakeKernel(wakeup_supported=False)
        transport = KernelTransport(kernel)
        self.assertFalse(transport.supports_wakeup_channel)
        self.assertIsNone(transport.create_wakeup_channel())

    def test_incomplete_contract_fails_before_address_resolution(self) -> None:
        kernel = FakeKernel()
        kernel.socket_bind = None  # type: ignore[assignment]
        with self.assertRaisesRegex(TypeError, "socket_bind"):
            KernelTransport(kernel)
        self.assertEqual(
            kernel.calls,
            [("supports_tcp_server",), ("supports_wakeup_channel",)],
        )

    def test_wakeup_validator_is_mandatory_before_address_resolution(self) -> None:
        for validator in (None, 42):
            with self.subTest(validator=validator):
                kernel = FakeKernel()
                kernel.validate_io_wait_object = validator  # type: ignore[assignment]
                with self.assertRaisesRegex(TypeError, "validate_io_wait_object"):
                    KernelTransport(kernel)
                self.assertEqual(
                    kernel.calls,
                    [("supports_tcp_server",), ("supports_wakeup_channel",)],
                )

    def test_wakeup_validator_rejects_invalid_object(self) -> None:
        kernel = FakeKernel()
        kernel.invalid_wait_objects.add(id(kernel.wakeup.wait_object))
        transport = KernelTransport(kernel)
        with self.assertRaisesRegex(ValueError, "invalid wait object"):
            transport.create_wakeup_channel()
        self.assertEqual(kernel.wakeup.close_calls, 1)

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

    def test_listener_rollback_catches_base_exception_and_preserves_primary_error(self) -> None:
        class FatalSetup(BaseException):
            pass

        kernel = FakeKernel()
        primary = FatalSetup("setup interrupted")
        kernel.operation_errors["listen"] = primary
        transport = KernelTransport(kernel)
        with self.assertRaises(FatalSetup) as raised:
            transport.open_listener("127.0.0.1", 0, 1)
        self.assertIs(raised.exception, primary)
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

        listener = TransportHandle(kernel.listener)
        accepted = run_immediate(transport.accept(task, listener))
        received = run_immediate(transport.recv(task, accepted.stream, 16))
        run_immediate(transport.send_all(task, accepted.stream, b"reply"))

        self.assertIs(accepted.stream.raw, client)
        self.assertEqual(accepted.peer_address, fallback_peer)
        self.assertEqual(received, b"request")
        self.assertEqual(
            [mode for mode, _ in task.waits],
            ["read", "write", "write", "read", "read", "write"],
        )
        self.assertEqual(kernel.sent[id(client)][-1], b"ply")
        self.assertTrue(all(isinstance(part, memoryview) for part in kernel.sent[id(client)]))

    def test_operation_aware_retry_maps_eagain_and_tls_opposite_directions(self) -> None:
        kernel = FakeKernel()
        transport = KernelTransport(kernel)
        listener = TransportHandle(kernel.listener)
        client = OpaqueHandle("client")
        kernel.accept_results = [WouldBlock(11, "EAGAIN"), (client, None)]
        kernel.recv_results[id(client)] = [WouldBlock(11, "EAGAIN"), TLSWantWrite(), b"ok"]
        kernel.send_results[id(client)] = [WouldBlock(11, "EAGAIN"), TLSWantRead(), 2]
        task = FakeTask()

        accepted = run_immediate(transport.accept(task, listener))
        run_immediate(transport.recv(task, accepted.stream, 2))
        run_immediate(transport.send_all(task, accepted.stream, b"ok"))

        self.assertEqual(
            [mode for mode, _ in task.waits],
            ["read", "read", "write", "write", "read"],
        )

    def test_accept_configuration_failure_closes_the_new_stream_once(self) -> None:
        kernel = FakeKernel()
        transport = KernelTransport(kernel)
        client = OpaqueHandle("client")
        kernel.accept_results = [(client, ("127.0.0.1", 1))]
        kernel.fail_operation = "setblocking"
        with self.assertRaisesRegex(RuntimeError, "setblocking failed"):
            run_immediate(transport.accept(FakeTask(), TransportHandle(kernel.listener)))
        self.assertEqual(kernel.closed, [client])

    def test_send_requires_forward_progress(self) -> None:
        for result in (0, -1):
            with self.subTest(result=result):
                kernel = FakeKernel()
                transport = KernelTransport(kernel)
                client = OpaqueHandle("client")
                kernel.send_results[id(client)] = [result]
                with self.assertRaisesRegex(ConnectionError, "forward progress"):
                    run_immediate(
                        transport.send_all(FakeTask(), TransportHandle(client), b"payload")
                    )

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
        clients = [OpaqueHandle("client-{}".format(index)) for index in range(4)]
        kernel.accept_results = [
            *((client, ("127.0.0.1", 1)) for client in clients),
            RuntimeError("listener failed"),
        ]
        task = FakeTask()
        handle._config = ServerConfig(accept_batch_size=2)

        with self.assertRaisesRegex(RuntimeError, "capacity"):
            run_immediate(SmallServer()._accept_loop(task, handle))

        self.assertEqual(len(handle._runtime.cancelled), 1)
        self.assertEqual(kernel.closed, clients[:1])
        self.assertEqual(handle._connections, {})
        self.assertEqual(task.yields, 0)
        self.assertEqual(str(handle.failure), "capacity")
        run_immediate(SmallServer()._close_watcher(FakeTask(), handle))
        self.assertEqual(kernel.closed, clients[:1] + [listener.raw])

    def test_registration_retains_failed_task_and_stream_cleanup(self) -> None:
        class CancelFailure(BaseException):
            pass

        class Runtime:
            def __init__(self) -> None:
                self.cancel_attempts = 0

            def fork(self, task) -> None:
                raise RuntimeError("fork primary")

            def cancel_task(self, task) -> None:
                self.cancel_attempts += 1
                if self.cancel_attempts <= 2:
                    raise CancelFailure("cancel cleanup")
                task.cancel()

            def resume_task(self, task) -> None:
                pass

        kernel = FakeKernel()
        transport = KernelTransport(kernel)
        listener = transport.open_listener("127.0.0.1", 0, 2)
        runtime = Runtime()
        handle = ServerHandle(
            runtime,
            transport,
            listener,
            transport.create_wakeup_channel(),
            ServerConfig(max_connections=2),
        )
        clients = [OpaqueHandle("first"), OpaqueHandle("must-not-accept")]
        kernel.accept_results = [*((client, None) for client in clients)]
        kernel.close_failures[id(clients[0])] = 2

        with self.assertRaisesRegex(RuntimeError, "fork primary") as raised:
            run_immediate(SmallServer()._accept_loop(FakeTask(), handle))

        self.assertIs(handle.failure, raised.exception)
        self.assertTrue(handle.closed)
        self.assertEqual(handle.owned_connection_count, 1)
        self.assertEqual(len(handle._pending_task_cancellations), 1)
        self.assertEqual(len(handle.cleanup_errors), 2)
        self.assertEqual(
            len([call for call in kernel.calls if call[0] == "socket_accept"]), 1
        )

        run_immediate(SmallServer()._close_watcher(FakeTask(), handle))
        self.assertFalse(handle.finished)
        self.assertEqual(len(handle.cleanup_errors), 2)
        handle._finish_close()
        self.assertTrue(handle.finished)
        self.assertEqual(handle.cleanup_errors, ())
        self.assertEqual(runtime.cancel_attempts, 3)
        self.assertCountEqual(kernel.closed, [clients[0], listener.raw])

    def test_accept_base_exception_is_fatal_and_re_raised_identically(self) -> None:
        class FatalAccept(BaseException):
            pass

        class Runtime:
            def resume_task(self, task) -> None:
                pass

        kernel = FakeKernel()
        transport = KernelTransport(kernel)
        listener = transport.open_listener("127.0.0.1", 0, 1)
        handle = ServerHandle(
            Runtime(), transport, listener, transport.create_wakeup_channel(), ServerConfig()
        )
        primary = FatalAccept("fatal accept")
        kernel.accept_results = [primary]

        with self.assertRaises(FatalAccept) as raised:
            run_immediate(SmallServer()._accept_loop(FakeTask(), handle))

        self.assertIs(raised.exception, primary)
        self.assertIs(handle.failure, primary)
        self.assertTrue(handle.closed)
        self.assertEqual(kernel.wakeup.notify_calls, 1)
        run_immediate(SmallServer()._close_watcher(FakeTask(), handle))
        self.assertTrue(handle.finished)

    def test_accept_exception_after_close_is_normal_listener_exit(self) -> None:
        class FatalAccept(BaseException):
            pass

        class Runtime:
            def resume_task(self, task) -> None:
                pass

        kernel = FakeKernel()
        transport = KernelTransport(kernel)
        listener = transport.open_listener("127.0.0.1", 0, 1)
        handle = ServerHandle(
            Runtime(), transport, listener, transport.create_wakeup_channel(), ServerConfig()
        )
        shutdown_exception = FatalAccept("accept invalidated by shutdown")

        def close_then_fail(raw_listener):
            handle.close()
            raise shutdown_exception

        kernel.socket_accept = close_then_fail  # type: ignore[method-assign]

        self.assertIsNone(run_immediate(SmallServer()._accept_loop(FakeTask(), handle)))
        self.assertTrue(handle.closed)
        self.assertIsNone(handle.failure)
        self.assertEqual(kernel.wakeup.notify_calls, 1)
        run_immediate(SmallServer()._close_watcher(FakeTask(), handle))
        self.assertTrue(handle.finished)

    def test_full_capacity_blocks_on_scheduler_signal_without_accepting(self) -> None:
        class Runtime:
            def resume_task(self, task) -> None:
                pass

        kernel = FakeKernel()
        transport = KernelTransport(kernel)
        listener = transport.open_listener("127.0.0.1", 0, 1)
        handle = ServerHandle(
            Runtime(),
            transport,
            listener,
            transport.create_wakeup_channel(),
            ServerConfig(max_connections=1, accept_batch_size=2),
        )
        occupied = TransportHandle(OpaqueHandle("occupied"))
        handle._connections[id(occupied)] = (occupied, object())
        clients = [OpaqueHandle("overflow-{}".format(index)) for index in range(4)]
        kernel.accept_results = [*((client, None) for client in clients)]

        class CapacityTask(FakeTask):
            async def wait_signal(self, signal: int) -> None:
                await super().wait_signal(signal)
                raise RuntimeError("stop capacity probe")

        task = CapacityTask()

        with self.assertRaisesRegex(RuntimeError, "stop capacity probe"):
            run_immediate(SmallServer()._accept_loop(task, handle))

        self.assertEqual(task.waits, [("signal", ServerHandle._CAPACITY_SIGNAL)])
        self.assertFalse(any(call[0] == "socket_accept" for call in kernel.calls))
        self.assertEqual(handle.owned_connection_count, 1)
        self.assertEqual(kernel.closed, [])
        run_immediate(SmallServer()._close_watcher(FakeTask(), handle))
        self.assertEqual(kernel.closed, [occupied.raw, listener.raw])

    def test_capacity_release_signals_blocked_listener(self) -> None:
        class Runtime:
            def resume_task(self, task) -> None:
                pass

        class ListenerTask:
            def __init__(self) -> None:
                self.signals = []

            def acceptSignal(self, signal: int) -> int:
                self.signals.append(signal)
                return 0

        kernel = FakeKernel()
        transport = KernelTransport(kernel)
        handle = ServerHandle(
            Runtime(),
            transport,
            transport.open_listener("127.0.0.1", 0, 1),
            transport.create_wakeup_channel(),
            ServerConfig(max_connections=1),
        )
        listener_task = ListenerTask()
        handle._listener_task = listener_task
        handle._capacity_waiting = True
        client = TransportHandle(OpaqueHandle("capacity-holder"))
        connection_task = FakeTask()
        handle._connections[id(client)] = (client, connection_task)

        handle._connection_finished(connection_task, client)

        self.assertEqual(handle.owned_connection_count, 0)
        self.assertEqual(listener_task.signals, [ServerHandle._CAPACITY_SIGNAL])
        self.assertEqual(kernel.closed, [client.raw])

    def test_persistent_rejected_close_failure_is_fatal_and_bounded(self) -> None:
        class Runtime:
            def fork(self, task) -> None:
                raise RuntimeError("task capacity")

            def cancel_task(self, task) -> None:
                task.cancel()

            def resume_task(self, task) -> None:
                pass

        kernel = FakeKernel()
        transport = KernelTransport(kernel)
        listener = transport.open_listener("127.0.0.1", 0, 2)
        handle = ServerHandle(
            Runtime(),
            transport,
            listener,
            transport.create_wakeup_channel(),
            ServerConfig(max_connections=2, accept_batch_size=1),
        )
        clients = [OpaqueHandle("attacker-{}".format(index)) for index in range(20)]
        kernel.accept_results = [*((client, None) for client in clients)]
        kernel.close_failures[id(clients[0])] = 100
        task = FakeTask()

        with self.assertRaisesRegex(RuntimeError, "task capacity"):
            run_immediate(SmallServer()._accept_loop(task, handle))

        accepts = [call for call in kernel.calls if call[0] == "socket_accept"]
        self.assertEqual(len(accepts), 1)
        self.assertTrue(handle.closed)
        self.assertEqual(str(handle.failure), "task capacity")
        self.assertEqual(str(handle.cleanup_errors[0]), "close failed")
        self.assertEqual(handle.owned_connection_count, 1)
        self.assertLessEqual(
            handle.owned_connection_count, handle._config.max_connections
        )

    def test_accepted_configuration_close_failure_transfers_to_server(self) -> None:
        class Runtime:
            def resume_task(self, task) -> None:
                pass

        kernel = FakeKernel()
        transport = KernelTransport(kernel)
        listener = transport.open_listener("127.0.0.1", 0, 1)
        handle = ServerHandle(
            Runtime(), transport, listener, transport.create_wakeup_channel(), ServerConfig()
        )
        client = OpaqueHandle("unconfigured")
        kernel.accept_results = [(client, None)]
        kernel.operation_errors["setblocking"] = RuntimeError("configure failed")
        kernel.close_failures[id(client)] = 2

        with self.assertRaisesRegex(RuntimeError, "configure failed"):
            run_immediate(SmallServer()._accept_loop(FakeTask(), handle))

        self.assertTrue(handle.closed)
        self.assertEqual(handle.owned_connection_count, 1)
        self.assertEqual(str(handle.failure), "configure failed")
        run_immediate(SmallServer()._close_watcher(FakeTask(), handle))
        self.assertFalse(handle.finished)
        handle._finish_close()
        self.assertTrue(handle.finished)
        self.assertCountEqual(kernel.closed, [client, listener.raw])

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
        owned_connection = TransportHandle(connection)
        handle._connections[id(owned_connection)] = (owned_connection, connection_task)
        handle._listener_task = listener_task

        handle.close()
        handle.close()
        handle._finish_close()
        handle._finish_close()
        transport.close_safely(owned_connection)

        self.assertEqual(kernel.wakeup.notify_calls, 1)
        self.assertEqual(kernel.wakeup.close_calls, 1)
        self.assertEqual(runtime.resumed, [connection_task, listener_task])
        self.assertEqual(kernel.closed, [connection, listener.raw])

    def test_notification_failure_can_be_retried_until_close_watcher_finishes(self) -> None:
        class Runtime:
            def resume_task(self, task) -> None:
                pass

        kernel = FakeKernel()
        transport = KernelTransport(kernel)
        handle = ServerHandle(
            Runtime(),
            transport,
            transport.open_listener("127.0.0.1", 0, 1),
            transport.create_wakeup_channel(),
            ServerConfig(),
        )
        kernel.wakeup.notify_failures = 1
        with self.assertRaisesRegex(RuntimeError, "notify failed"):
            handle.close()
        self.assertTrue(handle.closed)
        handle.close()
        self.assertEqual(kernel.wakeup.notify_calls, 2)
        kernel.wakeup.drain_error = RuntimeError("drain failed")
        with self.assertRaisesRegex(RuntimeError, "drain failed"):
            run_immediate(SmallServer()._close_watcher(FakeTask(), handle))
        self.assertEqual(kernel.closed, [kernel.listener])
        self.assertEqual(kernel.wakeup.close_calls, 1)

    def test_failed_connection_finally_retains_ownership_until_shutdown_retry(self) -> None:
        class Runtime:
            def resume_task(self, task) -> None:
                pass

        kernel = FakeKernel()
        transport = KernelTransport(kernel)
        listener = transport.open_listener("127.0.0.1", 0, 1)
        handle = ServerHandle(
            Runtime(), transport, listener, transport.create_wakeup_channel(), ServerConfig()
        )
        client = OpaqueHandle("client")
        owned_client = TransportHandle(client)
        task = FakeTask()
        handle._connections[id(owned_client)] = (owned_client, task)
        kernel.recv_results[id(client)] = [b""]
        kernel.close_failures[id(client)] = 1

        run_immediate(SmallServer()._connection_loop(task, handle, owned_client))

        self.assertFalse(owned_client.closed)
        self.assertIn(id(owned_client), handle._closing_connections)
        self.assertEqual(len(handle.cleanup_errors), 1)
        self.assertTrue(handle.closed)
        self.assertIs(handle.failure, owned_client.close_error)
        handle.close()
        run_immediate(SmallServer()._close_watcher(FakeTask(), handle))
        self.assertTrue(owned_client.closed)
        self.assertTrue(handle.finished)
        self.assertEqual(handle.cleanup_errors, ())
        self.assertEqual(kernel.closed, [client, listener.raw])

    def test_connection_cleanup_failure_does_not_replace_primary_error(self) -> None:
        class Runtime:
            def resume_task(self, task) -> None:
                pass

        kernel = FakeKernel()
        transport = KernelTransport(kernel)
        listener = transport.open_listener("127.0.0.1", 0, 1)
        handle = ServerHandle(
            Runtime(), transport, listener, transport.create_wakeup_channel(), ServerConfig()
        )
        client = OpaqueHandle("client")
        owned_client = TransportHandle(client)
        task = FakeTask()
        primary = RuntimeError("send failed")
        kernel.recv_results[id(client)] = [b"GET /missing HTTP/1.1\r\nHost: localhost\r\n\r\n"]
        kernel.send_results[id(client)] = [primary]
        kernel.close_failures[id(client)] = 1

        with self.assertRaises(RuntimeError) as raised:
            run_immediate(SmallServer()._connection_loop(task, handle, owned_client))

        self.assertIs(raised.exception, primary)
        self.assertIn(id(owned_client), handle._closing_connections)
        self.assertIs(handle.cleanup_errors[0], owned_client.close_error)
        self.assertIs(handle.failure, primary)

    def test_finalization_retries_listener_and_wakeup_close_failures(self) -> None:
        class Runtime:
            def __init__(self) -> None:
                self.cursor = object()

            def resume_task(self, task) -> None:
                pass

        kernel = FakeKernel()
        transport = KernelTransport(kernel)
        listener = transport.open_listener("127.0.0.1", 0, 1)
        wakeup = transport.create_wakeup_channel()
        assert wakeup is not None
        runtime = Runtime()
        handle = ServerHandle(runtime, transport, listener, wakeup, ServerConfig())
        kernel.close_failures[id(listener.raw)] = 1
        kernel.wakeup.close_failures = 1

        handle.close()
        handle._finish_close()

        self.assertFalse(handle.finished)
        self.assertFalse(listener.closed)
        self.assertFalse(wakeup.closed)
        self.assertEqual(len(handle.cleanup_errors), 2)
        with self.assertRaisesRegex(RuntimeError, "cleanup is incomplete"):
            handle.close()
        run_immediate(handle.close_from_task(runtime.cursor))
        handle._finish_close()
        self.assertTrue(handle.finished)
        self.assertTrue(listener.closed)
        self.assertTrue(wakeup.closed)
        self.assertEqual(handle.cleanup_errors, ())
        self.assertEqual(kernel.wakeup.close_calls, 2)
        listener_close_calls = [
            call for call in kernel.calls if call[:2] == ("socket_close", listener.raw)
        ]
        self.assertEqual(len(listener_close_calls), 2)

    def test_invalid_or_failing_wakeup_wait_object_closes_acquired_channel(self) -> None:
        class FailingWaitChannel:
            close_calls = 0

            @property
            def wait_object(self):
                raise RuntimeError("wait object failed")

            def notify(self):
                pass

            def drain(self):
                pass

            def close(self):
                self.close_calls += 1

        for property_failure in (True, False):
            with self.subTest(property_failure=property_failure):
                kernel = FakeKernel()
                channel = FailingWaitChannel() if property_failure else kernel.wakeup
                kernel.create_wakeup_channel = lambda: channel  # type: ignore[method-assign]
                if not property_failure:
                    kernel.invalid_wait_objects.add(id(channel.wait_object))
                transport = KernelTransport(kernel)
                with self.assertRaisesRegex((RuntimeError, ValueError), "wait object|invalid"):
                    transport.create_wakeup_channel()
                self.assertEqual(channel.close_calls, 1)

    def test_no_wakeup_kernel_requires_explicit_scheduler_close(self) -> None:
        class Runtime:
            def __init__(self) -> None:
                self.kernel = FakeKernel(wakeup_supported=False)
                self.cursor = object()
                self.resumed = []
                self.forked = []

            def fork(self, tasks) -> None:
                self.forked.extend(tasks if isinstance(tasks, list) else [tasks])

            def resume_task(self, task) -> None:
                self.resumed.append(task)

            def cancel_task(self, task) -> None:
                pass

        runtime = Runtime()
        handle = SmallServer().serve(runtime, host="0.0.0.0", port=8080)
        self.assertEqual(len(runtime.forked), 1)
        self.assertIsNone(handle._wakeup)

        with self.assertRaisesRegex(RuntimeError, "outside its scheduler"):
            handle.close()
        self.assertFalse(handle.closed)
        with self.assertRaisesRegex(RuntimeError, "currently running"):
            run_immediate(handle.close_from_task(object()))
        run_immediate(handle.close_from_task(runtime.cursor))
        self.assertTrue(handle.closed)
        self.assertEqual(runtime.kernel.closed, [runtime.kernel.listener])

    def test_micropython_like_kernel_serves_and_closes_from_task(self) -> None:
        class MicroRuntime:
            def __init__(self) -> None:
                self.kernel = FakeKernel(wakeup_supported=False)
                self.cursor = FakeTask()
                self.forked = []
                self.resumed = []

            def fork(self, tasks) -> None:
                self.forked.extend(tasks if isinstance(tasks, list) else [tasks])

            def resume_task(self, task) -> None:
                self.resumed.append(task)

        runtime = MicroRuntime()
        app = SmallServer()

        @app.get("/micro")
        async def micro(request):
            return Response.text("opaque-ok")

        handle = app.serve(runtime, host="0.0.0.0", port=8080)
        self.assertEqual(len(runtime.forked), 1)
        self.assertIsNone(handle._wakeup)

        raw_client = OpaqueHandle("micro-client")
        client = TransportHandle(raw_client)
        connection_task = FakeTask()
        handle._connections[id(client)] = (client, connection_task)
        runtime.kernel.recv_results[id(raw_client)] = [
            b"GET /micro HTTP/1.1\r\nHost: device\r\n\r\n"
        ]

        run_immediate(app._connection_loop(connection_task, handle, client))
        self.assertIn(
            b"\r\n\r\nopaque-ok", bytes(runtime.kernel.sent[id(raw_client)][0])
        )
        run_immediate(handle.close_from_task(runtime.cursor))

        self.assertTrue(handle.finished)
        self.assertEqual(
            runtime.kernel.closed, [raw_client, runtime.kernel.listener]
        )

    def test_close_state_is_per_handle_and_failed_close_can_be_retried(self) -> None:
        kernel = FakeKernel()
        transport = KernelTransport(kernel)
        raw = OpaqueHandle("connection")
        handle = TransportHandle(raw)
        kernel.close_failures[id(raw)] = 1
        self.assertFalse(transport.close_safely(handle))
        self.assertFalse(handle.closed)
        self.assertIsInstance(handle.close_error, RuntimeError)
        self.assertTrue(transport.close_safely(handle))
        self.assertTrue(transport.close_safely(handle))
        self.assertTrue(handle.closed)
        self.assertIsNone(handle.close_error)
        close_calls = [call for call in kernel.calls if call[:2] == ("socket_close", raw)]
        self.assertEqual(len(close_calls), 2)
        self.assertFalse(hasattr(transport, "_closed"))

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

        owned_client = TransportHandle(client)
        run_immediate(app._connection_loop(FakeTask(), handle, owned_client))

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
