from __future__ import annotations

import ast
import os
from pathlib import Path
import subprocess
import sys
import unittest
from unittest.mock import patch

from smallserver import ServerConfigurationError, ServerStartupError, SmallServer
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
