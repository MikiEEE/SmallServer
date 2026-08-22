import unittest
from unittest.mock import patch

from smallserver import RouteErrorEvent, SmallServer
from smallserver.server import HTTPParseError, HTTPRequestParser, RouteObserverChannel, ServerConfig


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

    def test_splits_query_without_decoding_or_normalizing_path(self) -> None:
        request = self.parser().feed(
            b"GET /items/a%2Fb?tag=x%20y HTTP/1.1\r\nHost: localhost\r\n\r\n"
        )
        self.assertIsNotNone(request)
        assert request is not None
        self.assertEqual(request.raw_target, "/items/a%2Fb?tag=x%20y")
        self.assertEqual(request.path, "/items/a%2Fb")
        self.assertEqual(request.query_string, "tag=x%20y")

    def test_enforces_request_target_limit_independently(self) -> None:
        parser = HTTPRequestParser(256, 2, 32, max_request_target_bytes=8)
        with self.assertRaisesRegex(HTTPParseError, "request target") as raised:
            parser.feed(b"GET /12345678 HTTP/1.1\r\nHost: x\r\n\r\n")
        self.assertEqual(raised.exception.status, 414)

    def test_config_rejects_unbounded_limits(self) -> None:
        with self.assertRaisesRegex(ValueError, "max_connections"):
            ServerConfig(max_connections=0)
        with self.assertRaisesRegex(ValueError, "max_connections"):
            ServerConfig(max_connections=True)
        with self.assertRaisesRegex(ValueError, "max_request_target_bytes"):
            ServerConfig(max_request_target_bytes=0)
        with self.assertRaisesRegex(ValueError, "max_route_error_events"):
            ServerConfig(max_route_error_events=0)

    def test_config_preserves_legacy_positional_field_mapping(self) -> None:
        config = ServerConfig(1, 2, 3, 4, 5, 6, 7)
        self.assertEqual(config.max_connections, 1)
        self.assertEqual(config.max_header_bytes, 2)
        self.assertEqual(config.max_header_count, 3)
        self.assertEqual(config.max_body_bytes, 4)
        self.assertEqual(config.receive_chunk_bytes, 5)
        self.assertEqual(config.listener_priority, 6)
        self.assertEqual(config.connection_priority, 7)
        self.assertEqual(config.max_request_target_bytes, 8 * 1024)
        self.assertEqual(config.max_route_error_events, 16)

    def test_route_observer_channel_has_deterministic_capacity_and_stop(self) -> None:
        class ObserverTask:
            @staticmethod
            def getID() -> int:
                return 9

        class SourceTask:
            signals = []

            def sendSignal(self, pid, signal) -> int:
                self.signals.append((pid, signal))
                return 0

        channel = RouteObserverChannel(lambda event: None, max_events=1)
        channel.bind(ObserverTask())
        source = SourceTask()
        first = RouteErrorEvent("regex-route-1", "route_match_timeout")
        second = RouteErrorEvent("regex-route-2", "route_match_timeout")
        self.assertTrue(channel.enqueue(first, source))
        self.assertFalse(channel.enqueue(second, source))
        self.assertEqual(list(channel.events), [first])
        self.assertEqual(channel.dropped, 1)
        self.assertEqual(source.signals, [(9, 31)])
        channel.stop()
        self.assertFalse(channel.accepting)
        self.assertEqual(list(channel.events), [])
        self.assertEqual(channel.dropped, 2)

        failing_channel = RouteObserverChannel(lambda event: None, max_events=1)
        failing_channel.bind(ObserverTask())

        class FailingSourceTask:
            def sendSignal(self, pid, signal) -> int:
                raise RuntimeError("signal failed")

        self.assertFalse(failing_channel.enqueue(first, FailingSourceTask()))
        self.assertEqual(list(failing_channel.events), [])
        self.assertEqual(failing_channel.dropped, 1)

    def test_serve_closes_bound_socket_when_runtime_fork_fails(self) -> None:
        class Listener:
            closed = False

            def setsockopt(self, *args) -> None:
                pass

            def bind(self, address) -> None:
                pass

            def listen(self, backlog) -> None:
                pass

            def setblocking(self, blocking) -> None:
                pass

            def close(self) -> None:
                self.closed = True

        class Runtime:
            cancelled = 0

            def fork(self, tasks) -> None:
                raise RuntimeError("no task capacity")

            def cancel_task(self, task) -> None:
                self.cancelled += 1

        listener = Listener()
        runtime = Runtime()
        with patch("smallserver.app.socket.socket", return_value=listener):
            with self.assertRaisesRegex(RuntimeError, "capacity"):
                SmallServer().serve(runtime)
        self.assertTrue(listener.closed)
        self.assertEqual(runtime.cancelled, 2)

    def test_observer_task_is_included_in_startup_rollback(self) -> None:
        class Listener:
            closed = False

            def setsockopt(self, *args) -> None:
                pass

            def bind(self, address) -> None:
                pass

            def listen(self, backlog) -> None:
                pass

            def setblocking(self, blocking) -> None:
                pass

            def close(self) -> None:
                self.closed = True

        class Runtime:
            tasks = []
            cancelled = []

            def fork(self, tasks) -> None:
                self.tasks = list(tasks)
                raise RuntimeError("no task capacity")

            def cancel_task(self, task) -> None:
                self.cancelled.append(task)

        listener = Listener()
        runtime = Runtime()
        app = SmallServer(route_error_observer=lambda event: None)
        with patch("smallserver.app.socket.socket", return_value=listener):
            with self.assertRaisesRegex(RuntimeError, "capacity"):
                app.serve(runtime)
        self.assertTrue(listener.closed)
        self.assertEqual(len(runtime.tasks), 3)
        self.assertEqual(runtime.cancelled, runtime.tasks)
        self.assertEqual(
            [task.name for task in runtime.tasks],
            [
                "smallserver-listener",
                "smallserver-close-watcher",
                "smallserver-route-observer",
            ],
        )
