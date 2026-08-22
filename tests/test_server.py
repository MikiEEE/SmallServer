import unittest
from unittest.mock import patch

from smallserver import ServerStartupError, SmallServer
from smallserver.server import HTTPParseError, HTTPRequestParser, ServerConfig

from tests.kernel_fakes import FakeKernel


class HTTPRequestParserTests(unittest.TestCase):
    def parser(self) -> HTTPRequestParser:
        return HTTPRequestParser(max_header_bytes=128, max_header_count=2, max_body_bytes=32)

    def test_fragmented_content_length_request(self) -> None:
        parser = self.parser()
        self.assertIsNone(parser.feed(b"POST /items HTTP/1.1\r\nHost: localhost\r\nContent-Length: 4\r\n\r"))
        request = parser.feed(b"\ntest")
        self.assertIsNotNone(request)
        assert request is not None
        self.assertEqual((request.method, request.path, request.body), ("POST", "/items", b"test"))

    def test_rejects_unsupported_or_ambiguous_framing(self) -> None:
        with self.assertRaisesRegex(HTTPParseError, "transfer-encoding"):
            self.parser().feed(b"POST / HTTP/1.1\r\nTransfer-Encoding: chunked\r\n\r\n")
        with self.assertRaisesRegex(HTTPParseError, "multiple content-length"):
            self.parser().feed(b"POST / HTTP/1.1\r\nContent-Length: 1\r\nContent-Length: 1\r\n\r\nx")

    def test_enforces_finite_header_and_body_limits(self) -> None:
        with self.assertRaisesRegex(HTTPParseError, "headers are too large"):
            self.parser().feed(b"G" * 129)
        with self.assertRaisesRegex(HTTPParseError, "body is too large"):
            self.parser().feed(b"POST / HTTP/1.1\r\nContent-Length: 33\r\n\r\n")

    def test_requires_host_and_rejects_invalid_origin_form(self) -> None:
        with self.assertRaisesRegex(HTTPParseError, "Host"):
            self.parser().feed(b"GET / HTTP/1.1\r\n\r\n")
        with self.assertRaisesRegex(HTTPParseError, "origin-form"):
            self.parser().feed(b"GET /items#fragment HTTP/1.1\r\nHost: localhost\r\n\r\n")

    def test_config_rejects_unbounded_limits(self) -> None:
        with self.assertRaisesRegex(ValueError, "max_connections"):
            ServerConfig(max_connections=0)
        with self.assertRaisesRegex(ValueError, "max_connections"):
            ServerConfig(max_connections=True)

    def test_serve_closes_kernel_resources_when_runtime_fork_fails(self) -> None:
        class Runtime:
            def __init__(self) -> None:
                self.kernel = FakeKernel()
                self.cancelled = 0

            def fork(self, tasks) -> None:
                raise RuntimeError("no task capacity")

            def cancel_task(self, task) -> None:
                self.cancelled += 1
                task.cancel()

            def resume_task(self, task) -> None:
                pass

        runtime = Runtime()
        with self.assertRaisesRegex(RuntimeError, "capacity"):
            SmallServer().serve(runtime)
        self.assertEqual(runtime.cancelled, 2)
        self.assertEqual([handle.name for handle in runtime.kernel.closed], ["listener"])
        self.assertEqual(runtime.kernel.wakeup.close_calls, 1)

    def test_serve_closes_kernel_resources_when_task_construction_fails(self) -> None:
        from SmallPackage import SmallTask as RealSmallTask

        class Runtime:
            def __init__(self) -> None:
                self.kernel = FakeKernel()
                self.cancelled = []

            def resume_task(self, task) -> None:
                pass

            def cancel_task(self, task) -> None:
                self.cancelled.append(task)
                task.cancel()

        calls = 0

        def construct_task(*args, **kwargs):
            nonlocal calls
            calls += 1
            if calls == 2:
                raise RuntimeError("task failed")
            return RealSmallTask(*args, **kwargs)

        runtime = Runtime()
        with patch("SmallPackage.SmallTask", side_effect=construct_task):
            with self.assertRaisesRegex(RuntimeError, "task failed"):
                SmallServer().serve(runtime)
        self.assertEqual(len(runtime.cancelled), 1)
        self.assertEqual([handle.name for handle in runtime.kernel.closed], ["listener"])
        self.assertEqual(runtime.kernel.wakeup.close_calls, 1)

    def test_listener_setup_failure_retains_owner_until_retry_succeeds(self) -> None:
        class Runtime:
            def __init__(self) -> None:
                self.kernel = FakeKernel()

        runtime = Runtime()
        primary = RuntimeError("listen setup failed")
        runtime.kernel.operation_errors["listen"] = primary
        runtime.kernel.close_failures[id(runtime.kernel.listener)] = 2

        with self.assertRaises(ServerStartupError) as raised:
            SmallServer().serve(runtime)

        error = raised.exception
        self.assertIs(error.primary_error, primary)
        self.assertEqual(len(error.cleanup_errors), 1)
        self.assertFalse(error.retry_cleanup())
        self.assertTrue(error.retry_cleanup())
        self.assertTrue(error.retry_cleanup())
        self.assertTrue(error.cleanup_complete)
        self.assertEqual(runtime.kernel.closed, [runtime.kernel.listener])

    def test_wakeup_failure_retains_owner_until_retry_succeeds(self) -> None:
        class Runtime:
            def __init__(self) -> None:
                self.kernel = FakeKernel()

        runtime = Runtime()
        runtime.kernel.invalid_wait_objects.add(id(runtime.kernel.wakeup.wait_object))
        runtime.kernel.wakeup.close_failures = 2

        with self.assertRaises(ServerStartupError) as raised:
            SmallServer().serve(runtime)

        error = raised.exception
        self.assertIsInstance(error.primary_error, ValueError)
        self.assertEqual(runtime.kernel.closed, [runtime.kernel.listener])
        self.assertFalse(error.finalize())
        self.assertTrue(error.finalize())
        self.assertEqual(runtime.kernel.wakeup.close_calls, 3)

    def test_task_registration_failure_retains_all_server_resources(self) -> None:
        class Runtime:
            def __init__(self) -> None:
                self.kernel = FakeKernel()
                self.cancelled = []

            def fork(self, tasks) -> None:
                raise RuntimeError("no task capacity")

            def cancel_task(self, task) -> None:
                self.cancelled.append(task)
                task.cancel()

            def resume_task(self, task) -> None:
                pass

        runtime = Runtime()
        runtime.kernel.close_failures[id(runtime.kernel.listener)] = 2
        runtime.kernel.wakeup.close_failures = 2

        with self.assertRaises(ServerStartupError) as raised:
            SmallServer().serve(runtime)

        error = raised.exception
        self.assertEqual(str(error.primary_error), "no task capacity")
        self.assertEqual(len(error.cleanup_errors), 2)
        self.assertFalse(error.retry_cleanup())
        self.assertTrue(error.retry_cleanup())
        self.assertEqual(runtime.kernel.closed, [runtime.kernel.listener])
        self.assertEqual(runtime.kernel.wakeup.close_calls, 3)

    def test_task_cancellation_failure_is_owned_until_retry(self) -> None:
        class Runtime:
            def __init__(self) -> None:
                self.kernel = FakeKernel()
                self.cancel_attempts: dict[int, int] = {}

            def fork(self, tasks) -> None:
                raise RuntimeError("registration failed")

            def cancel_task(self, task) -> None:
                attempts = self.cancel_attempts.get(id(task), 0) + 1
                self.cancel_attempts[id(task)] = attempts
                if attempts <= 2:
                    raise RuntimeError("cancel failed")
                task.cancel()

            def resume_task(self, task) -> None:
                pass

        runtime = Runtime()
        with self.assertRaises(ServerStartupError) as raised:
            SmallServer().serve(runtime)

        error = raised.exception
        self.assertEqual(str(error.primary_error), "registration failed")
        self.assertEqual(len(error.cleanup_errors), 2)
        self.assertFalse(error.retry_cleanup())
        self.assertTrue(error.retry_cleanup())
        self.assertTrue(error.cleanup_complete)
        self.assertEqual(runtime.kernel.closed, [runtime.kernel.listener])
        self.assertEqual(runtime.kernel.wakeup.close_calls, 1)
