from __future__ import annotations

import ast
from contextlib import nullcontext
import gc
import os
from pathlib import Path
import subprocess
import sys
import threading
import unittest
import warnings
from unittest.mock import patch

from smallserver import (
    ManagedRuntimeConfig,
    ServerConfig,
    ServerConfigurationError,
    ServerFinalizationError,
    ServerStartupError,
    SmallServer,
)
from smallserver._transport import TransportHandle
from smallserver.server import ServerHandle

from tests.kernel_fakes import FakeKernel, OpaqueHandle


class FakeRuntime:
    def __init__(self, kernel: FakeKernel | None = None) -> None:
        self.kernel = kernel or FakeKernel()
        self.forked: list[object] = []
        self.cancelled: list[object] = []
        self.started = 0
        self.start_error: BaseException | None = None
        self.unrelated_task = object()

    def fork(self, children) -> object:
        if isinstance(children, list):
            self.forked.extend(children)
        else:
            self.forked.append(children)
        return 0

    def start(self) -> None:
        self.started += 1
        if self.start_error is not None:
            raise self.start_error

    def resume_task(self, task) -> int:
        return 0

    def cancel_task(self, task) -> int:
        self.cancelled.append(task)
        cancel = getattr(task, "cancel", None)
        if callable(cancel):
            cancel()
        return 0


class ServerLifecycleTests(unittest.TestCase):
    def test_managed_runtime_settings_are_passed_to_the_factory(self) -> None:
        runtime = FakeRuntime()
        runtime_config = ManagedRuntimeConfig(
            task_capacity=256,
            priority_levels=5,
            io_buffer_length=64,
            eternal_watchers=True,
            client_defaults={"http": {"max_response_size": 2048}},
        )
        server_config = ServerConfig(managed_runtime=runtime_config)

        with patch(
            "smallserver.app._default_runtime_factory", return_value=runtime
        ) as factory:
            handle = SmallServer().listen(config=server_config, port=0)

        factory.assert_called_once_with(runtime_config)
        self.assertTrue(handle.finished)

    def test_managed_runtime_defaults_are_explicitly_passed_to_smallos(self) -> None:
        runtime = FakeRuntime()
        with patch(
            "smallserver.app._default_runtime_factory", return_value=runtime
        ) as factory:
            handle = SmallServer().listen(port=0)

        factory.assert_called_once_with(ManagedRuntimeConfig())
        self.assertTrue(handle.finished)

    def test_default_factory_applies_settings_to_real_smallos_config(self) -> None:
        from smallserver.app import _default_runtime_factory

        config = ManagedRuntimeConfig(
            task_capacity=33,
            priority_levels=6,
            io_buffer_length=17,
            eternal_watchers=True,
            client_defaults={"http": {"max_response_size": 8192}},
        )
        runtime = _default_runtime_factory(config)

        self.assertEqual(runtime.config.task_capacity, 33)
        self.assertEqual(runtime.config.priority_levels, 6)
        self.assertEqual(runtime.config.io_buffer_length, 17)
        self.assertTrue(runtime.config.eternal_watchers)
        self.assertEqual(
            runtime.config.client_defaults_for("http")["max_response_size"], 8192
        )

    def test_caller_owned_runtime_rejects_managed_runtime_settings_pre_bind(self) -> None:
        runtime = FakeRuntime()
        server_config = ServerConfig(managed_runtime=ManagedRuntimeConfig())

        with self.assertRaisesRegex(ValueError, "caller-supplied SmallOS"):
            SmallServer().listen(runtime=runtime, config=server_config, port=0)
        with self.assertRaisesRegex(ValueError, "caller-supplied SmallOS"):
            SmallServer().serve(runtime, config=server_config, port=0)

        self.assertEqual(runtime.kernel.calls, [])
        self.assertEqual(runtime.forked, [])

    def test_managed_priorities_are_validated_before_runtime_creation(self) -> None:
        config = ServerConfig(
            connection_priority=3,
            managed_runtime=ManagedRuntimeConfig(priority_levels=3),
        )
        with patch("smallserver.app._default_runtime_factory") as factory:
            with self.assertRaisesRegex(ValueError, "priority_levels"):
                SmallServer().listen(config=config, port=0)
        factory.assert_not_called()

        insufficient = ServerConfig(
            max_connections=32,
            managed_runtime=ManagedRuntimeConfig(task_capacity=33),
        )
        with patch("smallserver.app._default_runtime_factory") as factory:
            with self.assertRaisesRegex(ValueError, r"max_connections \+ 2"):
                SmallServer().listen(config=insufficient, port=0)
        factory.assert_not_called()

        implicit_defaults = ServerConfig(max_connections=1023)
        with patch("smallserver.app._default_runtime_factory") as factory:
            with self.assertRaisesRegex(ValueError, r"max_connections \+ 2"):
                SmallServer().listen(config=implicit_defaults, port=0)
        factory.assert_not_called()

    def test_primary_demo_hides_runtime_and_registers_all_http_methods(self) -> None:
        root = Path(__file__).parents[1]
        demo_path = root / "demo.py"
        tree = ast.parse(demo_path.read_text(encoding="utf-8"), filename=str(demo_path))
        forbidden: list[str] = []
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                forbidden.extend(
                    alias.name
                    for alias in node.names
                    if alias.name in {"socket", "SmallPackage"}
                )
            elif isinstance(node, ast.ImportFrom) and node.module in {
                "socket",
                "SmallPackage",
            }:
                forbidden.append(node.module)
        self.assertEqual(forbidden, [])
        demo_source = demo_path.read_text(encoding="utf-8")
        self.assertIn('print("Starting SmallServer', demo_source)
        self.assertNotIn('print("SmallServer listening', demo_source)

        import demo

        self.assertEqual(
            {method for method, path in demo.app._routes if path == "/tasks"},
            {"GET", "POST", "PUT", "PATCH", "DELETE"},
        )

    def test_supplied_runtime_defaults_to_schedule_without_starting(self) -> None:
        runtime = FakeRuntime()
        handle = SmallServer().listen(runtime=runtime, port=0)

        self.assertEqual(runtime.started, 0)
        self.assertEqual(len(runtime.forked), 2)
        self.assertFalse(handle.closed)
        handle.finalize()

    def test_no_wakeup_runtime_can_be_owner_finalized_and_reused(self) -> None:
        runtime = FakeRuntime(FakeKernel(wakeup_supported=False))
        app = SmallServer()
        handle = app.listen(runtime=runtime, port=0)

        self.assertEqual(len(runtime.forked), 1)
        self.assertIsNone(handle._wakeup)
        with self.assertRaisesRegex(RuntimeError, "outside its scheduler"):
            handle.close()

        handle.finalize()
        self.assertTrue(handle.closed)
        self.assertEqual(runtime.cancelled, runtime.forked)
        self.assertEqual(runtime.kernel.closed, [runtime.kernel.listener])

        next_runtime = FakeRuntime(FakeKernel(wakeup_supported=False))
        next_handle = app.listen(runtime=next_runtime, port=0)
        next_handle.finalize()

    def test_supplied_runtime_start_true_starts_once_and_returns_closed_handle(self) -> None:
        runtime = FakeRuntime()
        handle = SmallServer().listen(runtime=runtime, start=True, port=0)

        self.assertEqual(runtime.started, 1)
        self.assertTrue(handle.closed)
        self.assertEqual(handle.address, ("127.0.0.1", 43210))
        self.assertEqual(runtime.cancelled, runtime.forked)
        self.assertNotIn(runtime.unrelated_task, runtime.cancelled)
        self.assertEqual(runtime.kernel.wakeup.close_calls, 1)

    def test_runtime_failure_finalizes_but_does_not_hide_primary_error(self) -> None:
        runtime = FakeRuntime()
        runtime.start_error = RuntimeError("scheduler failed")
        app = SmallServer()

        with self.assertRaisesRegex(RuntimeError, "scheduler failed"):
            app.listen(runtime=runtime, start=True, port=0)

        self.assertEqual(runtime.kernel.wakeup.close_calls, 1)
        self.assertEqual([item.name for item in runtime.kernel.closed], ["listener"])
        # Complete cleanup releases the application for a later invocation.
        next_handle = app.listen(runtime=FakeRuntime(), port=0)
        next_handle.finalize()

    def test_runtime_failure_retains_invocation_until_cleanup_retry(self) -> None:
        runtime = FakeRuntime()
        primary = RuntimeError("scheduler failed")
        runtime.start_error = primary
        runtime.kernel.close_failures[id(runtime.kernel.listener)] = 1
        app = SmallServer()

        with self.assertRaises(ServerStartupError) as raised:
            app.listen(runtime=runtime, start=True, port=0)

        cleanup = raised.exception
        self.assertIs(cleanup.primary_error, primary)
        with self.assertRaisesRegex(RuntimeError, "active listener"):
            app.serve(FakeRuntime(), port=0)
        self.assertTrue(cleanup.retry_cleanup())
        next_handle = app.serve(FakeRuntime(), port=0)
        next_handle.finalize()

    def test_managed_normal_return_exposes_cleanup_owner_for_retry(self) -> None:
        runtime = FakeRuntime()
        runtime.kernel.close_failures[id(runtime.kernel.listener)] = 1
        app = SmallServer()

        with patch("smallserver.app._default_runtime_factory", return_value=runtime):
            with self.assertRaises(ServerFinalizationError) as raised:
                app.listen(port=0)

        cleanup = raised.exception
        self.assertFalse(cleanup.cleanup_complete)
        with self.assertRaisesRegex(RuntimeError, "active listener"):
            app.serve(FakeRuntime(), port=0)
        self.assertTrue(cleanup.retry_cleanup())
        next_handle = app.serve(FakeRuntime(), port=0)
        next_handle.finalize()

    def test_abandoned_finalization_error_warns_and_retains_cleanup(self) -> None:
        runtime = FakeRuntime()
        runtime.kernel.close_failures[id(runtime.kernel.listener)] = 3
        app = SmallServer()

        with patch("smallserver.app._default_runtime_factory", return_value=runtime):
            with self.assertRaises(ServerFinalizationError) as raised:
                app.listen(port=0)

        transaction = raised.exception._transaction
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always", ResourceWarning)
            raised.exception = None
            gc.collect()

        self.assertEqual(len(caught), 1)
        self.assertIs(caught[0].category, ResourceWarning)
        self.assertIn("ServerFinalizationError", str(caught[0].message))
        self.assertEqual(len(transaction.retry()), 1)
        self.assertEqual(len(transaction.retry()), 0)
        next_handle = app.serve(FakeRuntime(), port=0)
        next_handle.finalize()

    def test_managed_keyboard_interrupt_is_swallowed_after_cleanup(self) -> None:
        runtime = FakeRuntime()
        runtime.start_error = KeyboardInterrupt()

        with patch("smallserver.app._default_runtime_factory", return_value=runtime):
            handle = SmallServer().listen(port=0)

        self.assertTrue(handle.closed)
        self.assertEqual(runtime.started, 1)
        self.assertEqual(runtime.kernel.wakeup.close_calls, 1)

    def test_supplied_runtime_keyboard_interrupt_propagates_after_cleanup(self) -> None:
        runtime = FakeRuntime()
        runtime.start_error = KeyboardInterrupt()

        with self.assertRaises(KeyboardInterrupt):
            SmallServer().listen(runtime=runtime, start=True, port=0)

        self.assertEqual(runtime.kernel.wakeup.close_calls, 1)
        self.assertEqual([item.name for item in runtime.kernel.closed], ["listener"])

    def test_system_exit_identity_survives_lifecycle_cleanup_failure(self) -> None:
        for managed in (False, True):
            with self.subTest(managed=managed):
                runtime = FakeRuntime()
                primary = SystemExit(17)
                runtime.start_error = primary
                runtime.kernel.close_failures[id(runtime.kernel.listener)] = 1
                app = SmallServer()

                context = (
                    patch("smallserver.app._default_runtime_factory", return_value=runtime)
                    if managed
                    else nullcontext()
                )
                with context:
                    with self.assertRaises(SystemExit) as raised:
                        if managed:
                            app.listen(port=0)
                        else:
                            app.listen(runtime=runtime, start=True, port=0)

                self.assertIs(raised.exception, primary)
                cleanup = raised.exception.__cause__
                self.assertIsInstance(cleanup, ServerStartupError)
                assert isinstance(cleanup, ServerStartupError)
                with self.assertRaisesRegex(RuntimeError, "active listener"):
                    app.serve(FakeRuntime(), port=0)
                self.assertTrue(cleanup.retry_cleanup())

    def test_invalid_ownership_and_runtime_fail_before_binding(self) -> None:
        with patch("smallserver.app._default_runtime_factory") as factory:
            with self.assertRaisesRegex(ValueError, "caller-supplied"):
                SmallServer().listen(start=False)
        factory.assert_not_called()

        kernel = FakeKernel()

        class InvalidRuntime:
            def __init__(self) -> None:
                self.kernel = kernel

        with self.assertRaisesRegex(TypeError, "fork"):
            SmallServer().listen(runtime=InvalidRuntime())
        self.assertEqual(kernel.calls, [])

        with self.assertRaisesRegex(TypeError, "boolean"):
            SmallServer().listen(runtime=FakeRuntime(), start=1)  # type: ignore[arg-type]

    def test_runtime_without_cancel_is_rejected_without_registry_growth(self) -> None:
        from SmallPackage import SmallOS

        class RuntimeWithoutCancellation:
            def __init__(self) -> None:
                self.inner = SmallOS()
                self.kernel = FakeKernel()

            def fork(self, children) -> object:
                return self.inner.fork(children)

            def resume_task(self, task) -> object:
                return self.inner.resume_task(task)

        runtime = RuntimeWithoutCancellation()
        before = len(runtime.inner.tasks)

        with self.assertRaisesRegex(TypeError, "cancel_task"):
            SmallServer().serve(runtime, port=0)  # type: ignore[arg-type]

        self.assertEqual(len(runtime.inner.tasks), before)
        self.assertEqual(runtime.kernel.calls, [])

    def test_simultaneous_invocations_reserve_once_and_bind_once(self) -> None:
        gate = threading.Barrier(2)
        bind_calls: list[object] = []
        bind_lock = threading.Lock()

        class RacingKernel(FakeKernel):
            def __init__(self) -> None:
                super().__init__()
                self._first_capability_check = True

            def supports_tcp_server(self) -> bool:
                result = super().supports_tcp_server()
                if self._first_capability_check:
                    self._first_capability_check = False
                    gate.wait(timeout=2)
                return result

            def socket_bind(self, stream: object, address: object) -> None:
                with bind_lock:
                    bind_calls.append(stream)
                super().socket_bind(stream, address)

        app = SmallServer()
        handles: list[ServerHandle] = []
        errors: list[BaseException] = []

        def invoke() -> None:
            try:
                handles.append(app.serve(FakeRuntime(RacingKernel()), port=0))
            except BaseException as exc:
                errors.append(exc)

        workers = [threading.Thread(target=invoke) for _ in range(2)]
        for worker in workers:
            worker.start()
        for worker in workers:
            worker.join(timeout=3)

        self.assertTrue(all(not worker.is_alive() for worker in workers))
        self.assertEqual(len(handles), 1)
        self.assertEqual(len(bind_calls), 1)
        self.assertEqual(len(errors), 1)
        self.assertRegex(str(errors[0]), "active listener")
        handles[0].finalize()

    def test_acquisition_cleanup_retains_invocation_until_retry(self) -> None:
        runtime = FakeRuntime()
        primary = RuntimeError("listen setup failed")
        runtime.kernel.operation_errors["listen"] = primary
        runtime.kernel.close_failures[id(runtime.kernel.listener)] = 1
        app = SmallServer()

        with self.assertRaises(ServerStartupError) as raised:
            app.serve(runtime, port=0)

        cleanup = raised.exception
        self.assertIs(cleanup.primary_error, primary)
        with self.assertRaisesRegex(RuntimeError, "active listener"):
            app.serve(FakeRuntime(), port=0)
        self.assertTrue(cleanup.retry_cleanup())
        next_handle = app.serve(FakeRuntime(), port=0)
        next_handle.finalize()

    def test_concurrent_invocation_is_rejected_and_sequential_reuse_succeeds(self) -> None:
        app = SmallServer()
        first = app.serve(FakeRuntime(), port=0)

        with self.assertRaisesRegex(RuntimeError, "active listener"):
            app.serve(FakeRuntime(), port=0)

        first.finalize()
        second = app.serve(FakeRuntime(), port=0)
        second.finalize()
        self.assertTrue(second.closed)

    def test_cleanup_failure_retains_invocation_until_retry_finishes(self) -> None:
        runtime = FakeRuntime()
        app = SmallServer()
        handle = app.serve(runtime, port=0)
        runtime.kernel.close_failures[id(runtime.kernel.listener)] = 1

        handle.finalize()

        self.assertFalse(handle.finished)
        self.assertEqual(len(handle.cleanup_errors), 1)
        self.assertEqual(runtime.cancelled, runtime.forked)
        with self.assertRaisesRegex(RuntimeError, "active listener"):
            app.serve(FakeRuntime(), port=0)

        handle.finalize()

        self.assertTrue(handle.finished)
        self.assertEqual(handle.cleanup_errors, ())
        # Cleanup retries do not cancel owned or unrelated tasks a second time.
        self.assertEqual(runtime.cancelled, runtime.forked)
        self.assertNotIn(runtime.unrelated_task, runtime.cancelled)
        next_handle = app.serve(FakeRuntime(), port=0)
        next_handle.finalize()

    def test_startup_failure_exposes_retained_handle_for_cleanup_retry(self) -> None:
        class FailingForkRuntime(FakeRuntime):
            def fork(self, children) -> object:
                super().fork(children)
                raise RuntimeError("fork failed")

        runtime = FailingForkRuntime()
        runtime.kernel.close_failures[id(runtime.kernel.listener)] = 1
        app = SmallServer()

        with self.assertRaises(ServerStartupError) as raised:
            app.serve(runtime, port=0)

        cleanup = raised.exception
        self.assertEqual(str(cleanup.primary_error), "fork failed")
        self.assertEqual(len(cleanup.cleanup_errors), 1)
        with self.assertRaisesRegex(RuntimeError, "active listener"):
            app.serve(FakeRuntime(), port=0)
        self.assertTrue(cleanup.retry_cleanup())
        next_handle = app.serve(FakeRuntime(), port=0)
        next_handle.finalize()

    def test_managed_interrupt_propagates_when_cleanup_is_incomplete(self) -> None:
        runtime = FakeRuntime()
        runtime.start_error = KeyboardInterrupt()
        runtime.kernel.close_failures[id(runtime.kernel.listener)] = 1
        app = SmallServer()

        with patch("smallserver.app._default_runtime_factory", return_value=runtime):
            with self.assertRaises(KeyboardInterrupt) as raised:
                app.listen(port=0)

        cleanup = raised.exception.__cause__
        self.assertIsInstance(cleanup, ServerStartupError)
        assert isinstance(cleanup, ServerStartupError)
        self.assertIs(cleanup.primary_error, raised.exception)
        with self.assertRaisesRegex(RuntimeError, "active listener"):
            app.serve(FakeRuntime(), port=0)
        self.assertTrue(cleanup.retry_cleanup())

    def test_finalization_is_idempotent_with_live_connections(self) -> None:
        runtime = FakeRuntime()
        handle = SmallServer().serve(runtime, port=0)
        client = TransportHandle(OpaqueHandle("live-client"))
        client_task = object()
        handle._connections[id(client)] = (client, client_task)
        handle._owned_tasks.append(client_task)

        handle.finalize()
        handle.finalize()
        handle.close()

        self.assertEqual(runtime.kernel.wakeup.close_calls, 1)
        self.assertEqual(
            [item.name for item in runtime.kernel.closed],
            ["live-client", "listener"],
        )
        self.assertEqual(runtime.cancelled.count(client_task), 1)

    def test_connection_task_cancellation_is_attempted_once_per_finalize(self) -> None:
        class RetryCancellationRuntime(FakeRuntime):
            def __init__(self) -> None:
                super().__init__()
                self.attempts: dict[int, int] = {}

            def cancel_task(self, task) -> int:
                identity = id(task)
                self.attempts[identity] = self.attempts.get(identity, 0) + 1
                if (
                    getattr(task, "fail_first_cancel", False)
                    and self.attempts[identity] == 1
                ):
                    raise RuntimeError("cancel failed")
                return super().cancel_task(task)

        class ConnectionTask:
            fail_first_cancel = True

        runtime = RetryCancellationRuntime()
        handle = SmallServer().serve(runtime, port=0)
        client = TransportHandle(OpaqueHandle("retry-client"))
        connection_task = ConnectionTask()
        handle._connections[id(client)] = (client, connection_task)
        handle._owned_tasks.append(connection_task)

        handle.finalize()

        self.assertEqual(runtime.attempts[id(connection_task)], 1)
        self.assertFalse(handle.finished)
        handle.finalize()
        self.assertEqual(runtime.attempts[id(connection_task)], 2)
        self.assertTrue(handle.finished)

    def test_default_runtime_configuration_failure_is_framework_owned(self) -> None:
        with patch.dict(sys.modules, {"SmallPackage": None}):
            with self.assertRaises(ServerConfigurationError):
                from smallserver.app import _default_runtime_factory

                _default_runtime_factory()

    def test_importing_smallserver_does_not_eagerly_import_smallos(self) -> None:
        root = Path(__file__).parents[1]
        environment = os.environ.copy()
        environment.pop("PYTHONPATH", None)
        result = subprocess.run(
            [
                sys.executable,
                "-c",
                "import sys, smallserver; print('SmallPackage' in sys.modules)",
            ],
            cwd=root,
            env=environment,
            check=True,
            capture_output=True,
            text=True,
        )
        self.assertEqual(result.stdout.strip(), "False")


if __name__ == "__main__":
    unittest.main()
