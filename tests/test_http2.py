import builtins
import asyncio
import importlib.util
import socket
import threading
import unittest
from unittest.mock import patch

try:
    from h2.config import H2Configuration
    from h2.connection import H2Connection
    from h2.events import (
        ConnectionTerminated,
        DataReceived,
        ResponseReceived,
        StreamEnded,
        StreamReset,
    )
except ImportError:
    H2_AVAILABLE = False
else:
    H2_AVAILABLE = True

from SmallPackage import SmallOS, Unix

from smallserver import (
    HTTP2Config,
    Headers,
    RegexRouteConfig,
    Request,
    Response,
    RouteErrorEvent,
    RouteMatchTimeout,
    SmallServer,
)
from smallserver.app import _H2ConnectionState
from smallserver._transport import KernelTransport, TransportHandle
from smallserver.errors import ServerConfigurationError
from smallserver.http2 import H2Protocol, _FrameBudget
from smallserver.server import (
    RouteObserverChannel,
    ServerConfig,
    ServerHandle,
    run_route_observer,
)
from tests.kernel_fakes import FakeKernel, OpaqueHandle


class HTTP2OptionalDependencyTests(unittest.TestCase):
    def test_dependency_is_lazy_and_missing_extra_is_actionable(self):
        original = builtins.__import__

        def reject_h2(name, *args, **kwargs):
            if name == "h2" or name.startswith("h2."):
                raise ImportError("missing")
            return original(name, *args, **kwargs)

        with patch("builtins.__import__", side_effect=reject_h2):
            with self.assertRaisesRegex(ServerConfigurationError, "smallserver\\[http2\\]"):
                H2Protocol()

    def test_incomplete_extra_is_rejected_during_preflight(self):
        original = builtins.__import__

        def reject_events(name, *args, **kwargs):
            if name == "h2.events":
                raise ImportError("broken events module")
            return original(name, *args, **kwargs)

        with patch("builtins.__import__", side_effect=reject_events):
            with self.assertRaisesRegex(ServerConfigurationError, "complete hyper-h2"):
                H2Protocol()

    def test_timeout_configuration_is_finite_and_positive(self):
        for values in (
            {"handshake_timeout": 0},
            {"idle_timeout": -1},
            {"idle_timeout": True},
            {"handshake_timeout": float("nan")},
            {"handshake_timeout": float("inf")},
            {"idle_timeout": float("-inf")},
        ):
            with self.subTest(values=values):
                with self.assertRaises(ValueError):
                    HTTP2Config(**values)

    def test_dependency_preflight_happens_before_address_resolution(self):
        class Runtime:
            def __init__(self):
                self.kernel = FakeKernel()

            def fork(self, tasks):
                return None

            def resume_task(self, task):
                return None

            def cancel_task(self, task):
                return None

        runtime = Runtime()
        with patch(
            "smallserver.app.require_http2",
            side_effect=ServerConfigurationError("broken HTTP/2 dependency"),
        ):
            with self.assertRaisesRegex(ServerConfigurationError, "broken"):
                SmallServer().serve(runtime, protocol="http2")
        self.assertFalse(
            any(call[0] == "resolve_passive_address" for call in runtime.kernel.calls)
        )

    def test_h2_force_close_failure_retains_one_owner_until_retry(self):
        class Runtime:
            def resume_task(self, task):
                return None

            def cancel_task(self, task):
                return None

        kernel = FakeKernel()
        transport = KernelTransport(kernel)
        listener = transport.open_listener("127.0.0.1", 0, 2)
        wakeup = transport.create_wakeup_channel()
        handle = ServerHandle(Runtime(), transport, listener, wakeup, ServerConfig())
        raw_client = OpaqueHandle("h2-client")
        client = TransportHandle(raw_client)
        reader_task = object()
        writer_error = RuntimeError("writer failed")
        handle._connections[id(client)] = (client, reader_task)
        handle._graceful_connections.add(id(client))
        handle._graceful_closers[id(client)] = lambda: None
        kernel.close_failures[id(raw_client)] = 2

        self.assertFalse(
            handle._force_connection_close(client, object(), writer_error)
        )
        self.assertEqual(handle.owned_connection_count, 1)
        self.assertEqual(len(handle.cleanup_errors), 1)
        self.assertIs(handle.failure, writer_error)

        handle._finish_close()
        self.assertFalse(handle.finished)
        self.assertEqual(handle.owned_connection_count, 1)
        handle._finish_close()
        self.assertTrue(handle.finished)
        self.assertEqual(handle.owned_connection_count, 0)


@unittest.skipUnless(H2_AVAILABLE, "install the smallserver[test] HTTP/2 extra")
class HTTP2ProtocolTests(unittest.TestCase):
    def _pair(self, config=None):
        client = H2Connection(
            config=H2Configuration(client_side=True, header_encoding="utf-8")
        )
        server = H2Protocol(config)
        client.initiate_connection()
        server_bytes = server.initiate()
        server.receive_data(client.data_to_send())
        client.receive_data(server_bytes + server.flush())
        return client, server

    def test_prior_knowledge_request_uses_shared_values_and_response(self):
        client, server = self._pair()
        client.send_headers(
            1,
            [
                (":method", "POST"),
                (":scheme", "http"),
                (":authority", "localhost"),
                (":path", "/echo"),
                ("content-type", "text/plain"),
            ],
        )
        client.send_data(1, b"hello", end_stream=True)
        ready = server.receive_data(client.data_to_send())
        self.assertEqual(len(ready), 1)
        self.assertEqual(ready[0].request.version, "HTTP/2")
        self.assertEqual(ready[0].request.body, b"hello")
        self.assertEqual(ready[0].request.headers["host"], "localhost")

        self.assertTrue(server.queue_response(1, Response.text("world")))
        events = client.receive_data(server.flush())
        self.assertTrue(any(isinstance(event, ResponseReceived) for event in events))
        self.assertEqual(
            b"".join(event.data for event in events if isinstance(event, DataReceived)),
            b"world",
        )
        self.assertTrue(any(isinstance(event, StreamEnded) for event in events))

    def test_multiplexed_streams_can_finish_out_of_order(self):
        client, server = self._pair()
        for stream_id, path in ((1, "/slow"), (3, "/fast")):
            client.send_headers(
                stream_id,
                [
                    (":method", "GET"),
                    (":scheme", "http"),
                    (":authority", "localhost"),
                    (":path", path),
                ],
                end_stream=True,
            )
        ready = server.receive_data(client.data_to_send())
        self.assertEqual([item.stream_id for item in ready], [1, 3])
        server.queue_response(3, Response.text("fast"))
        first = client.receive_data(server.flush())
        self.assertTrue(any(isinstance(event, StreamEnded) and event.stream_id == 3 for event in first))
        server.queue_response(1, Response.text("slow"))
        second = client.receive_data(server.flush())
        self.assertTrue(any(isinstance(event, StreamEnded) and event.stream_id == 1 for event in second))

    def test_request_and_response_limits_reset_streams_without_unbounded_buffers(self):
        config = HTTP2Config(
            max_body_bytes=4,
            max_connection_buffer_bytes=8,
            max_response_body_bytes=4,
            max_pending_output_bytes=128,
            max_control_output_bytes=64,
        )
        client, server = self._pair(config)
        client.send_headers(
            1,
            [
                (":method", "POST"),
                (":scheme", "http"),
                (":authority", "localhost"),
                (":path", "/"),
            ],
        )
        client.send_data(1, b"12345", end_stream=True)
        self.assertEqual(server.receive_data(client.data_to_send()), ())
        client.receive_data(server.flush())
        self.assertEqual(server.active_stream_count, 0)
        self.assertEqual(server.pending_output_bytes, 0)

    def test_malformed_preface_is_a_connection_error(self):
        server = H2Protocol()
        server.initiate()
        with self.assertRaisesRegex(ValueError, "client preface"):
            server.receive_data(b"NOT HTTP/2")

    def test_peer_reset_is_reported_once_for_handler_cancellation(self):
        client, server = self._pair()
        client.send_headers(
            1,
            [
                (":method", "POST"),
                (":scheme", "http"),
                (":authority", "localhost"),
                (":path", "/work"),
            ],
        )
        server.receive_data(client.data_to_send())
        client.reset_stream(1)
        server.receive_data(client.data_to_send())
        self.assertEqual(server.take_cancelled_streams(), (1,))
        self.assertEqual(server.take_cancelled_streams(), ())

    def test_same_batch_end_then_reset_drops_ready_request_but_keeps_sibling(self):
        client, server = self._pair(HTTP2Config(reader_frame_batch_size=1))
        client.send_headers(
            1,
            [
                (":method", "GET"),
                (":scheme", "http"),
                (":authority", "localhost"),
                (":path", "/cancelled"),
            ],
            end_stream=True,
        )
        client.reset_stream(1)
        client.send_headers(
            3,
            [
                (":method", "GET"),
                (":scheme", "http"),
                (":authority", "localhost"),
                (":path", "/healthy"),
            ],
            end_stream=True,
        )
        ready = server.receive_data(client.data_to_send())
        self.assertEqual(ready, ())
        while server.has_pending_input:
            ready = server.receive_data(b"")
        self.assertEqual([item.stream_id for item in ready], [3])
        self.assertEqual(server.take_cancelled_streams(), (1,))
        self.assertFalse(server.is_stream_active(1))
        self.assertTrue(server.is_stream_active(3))

    def test_completed_slow_handler_body_remains_in_connection_budget(self):
        config = HTTP2Config(
            max_body_bytes=4,
            max_connection_buffer_bytes=6,
        )
        client, server = self._pair(config)
        for stream_id, body in ((1, b"1234"), (3, b"5678")):
            client.send_headers(
                stream_id,
                [
                    (":method", "POST"),
                    (":scheme", "http"),
                    (":authority", "localhost"),
                    (":path", "/slow"),
                    ("content-length", "4"),
                ],
            )
            client.send_data(stream_id, body, end_stream=True)
            ready = server.receive_data(client.data_to_send())
            if stream_id == 1:
                self.assertEqual([item.stream_id for item in ready], [1])
                self.assertEqual(server.buffered_request_bytes, 4)
            else:
                self.assertEqual(ready, ())
        events = client.receive_data(server.flush())
        self.assertTrue(
            any(isinstance(event, StreamReset) and event.stream_id == 3 for event in events)
        )
        self.assertEqual(server.buffered_request_bytes, 4)
        server.queue_response(1, Response.text("done"))
        self.assertEqual(server.buffered_request_bytes, 0)

    def test_completed_body_has_one_retained_payload_object(self):
        client, server = self._pair()
        client.send_headers(
            1,
            [
                (":method", "POST"),
                (":scheme", "http"),
                (":authority", "localhost"),
                (":path", "/slow"),
            ],
        )
        client.send_data(1, b"retained", end_stream=True)
        ready = server.receive_data(client.data_to_send())
        retained = server._inbound[1].body
        self.assertIsInstance(retained, bytes)
        self.assertIs(retained, ready[0].request.body)

    def test_bad_stream_metadata_resets_only_that_stream(self):
        client, server = self._pair()
        client.send_headers(
            1,
            [
                (":method", "POST"),
                (":scheme", "http"),
                (":authority", "localhost"),
                (":path", "/bad"),
                ("content-length", "2"),
            ],
        )
        client.send_data(1, b"x", end_stream=True)
        client.send_headers(
            3,
            [
                (":method", "GET"),
                (":scheme", "http"),
                (":authority", "localhost"),
                (":path", "/good"),
            ],
            end_stream=True,
        )
        ready = server.receive_data(client.data_to_send())
        self.assertEqual([item.stream_id for item in ready], [3])
        events = client.receive_data(server.flush())
        self.assertTrue(
            any(isinstance(event, StreamReset) and event.stream_id == 1 for event in events)
        )

    def test_invalid_method_and_origin_form_are_stream_errors(self):
        client, server = self._pair()
        client.config.validate_outbound_headers = False
        for stream_id, method, path in (
            (1, "BAD METHOD", "/bad"),
            (3, "GET", "/bad#fragment"),
        ):
            client.send_headers(
                stream_id,
                [
                    (":method", method),
                    (":scheme", "http"),
                    (":authority", "localhost"),
                    (":path", path),
                ],
                end_stream=True,
            )
        client.send_headers(
            5,
            [
                (":method", "GET"),
                (":scheme", "http"),
                (":authority", "localhost"),
                (":path", "/good"),
            ],
            end_stream=True,
        )
        ready = server.receive_data(client.data_to_send())
        self.assertEqual([item.stream_id for item in ready], [5])
        events = client.receive_data(server.flush())
        self.assertEqual(
            {event.stream_id for event in events if isinstance(event, StreamReset)},
            {1, 3},
        )

    def test_invalid_content_length_is_a_stream_error(self):
        client, server = self._pair()
        client.send_headers(
            1,
            [
                (":method", "POST"),
                (":scheme", "http"),
                (":authority", "localhost"),
                (":path", "/bad"),
                ("content-length", "-1"),
            ],
            end_stream=True,
        )
        client.send_headers(
            3,
            [
                (":method", "GET"),
                (":scheme", "http"),
                (":authority", "localhost"),
                (":path", "/good"),
            ],
            end_stream=True,
        )
        ready = server.receive_data(client.data_to_send())
        self.assertEqual([item.stream_id for item in ready], [3])
        events = client.receive_data(server.flush())
        self.assertTrue(
            any(isinstance(event, StreamReset) and event.stream_id == 1 for event in events)
        )

    def test_control_output_is_bounded_and_frames_are_processed_in_batches(self):
        config = HTTP2Config(
            reader_frame_batch_size=2,
            max_control_output_bytes=64,
        )
        client, server = self._pair(config)
        for value in range(6):
            client.ping(value.to_bytes(8, "big"))
        server.receive_data(client.data_to_send())
        self.assertTrue(server.has_pending_input)
        batches = 1
        while server.has_pending_input:
            client.receive_data(server.flush())
            server.receive_data(b"")
            batches += 1
        client.receive_data(server.flush())
        self.assertGreaterEqual(batches, 3)
        self.assertEqual(server.pending_output_bytes, 0)

        limited_client, limited_server = self._pair(
            HTTP2Config(max_control_output_bytes=52)
        )
        for value in range(4):
            limited_client.ping(value.to_bytes(8, "big"))
        with self.assertRaisesRegex(ValueError, "control output"):
            limited_server.receive_data(limited_client.data_to_send())

    def test_compressed_header_budget_rejects_declared_size_before_payload(self):
        budget = _FrameBudget(HTTP2Config(max_compressed_header_bytes=4))
        budget.feed(b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n")
        header = b"\x00\x00\x05" + b"\x01\x04" + b"\x00\x00\x00\x01"
        with self.assertRaisesRegex(ValueError, "compressed header"):
            budget.feed(header)
        self.assertEqual(len(budget._payload), 0)

    def test_response_headers_and_command_resets_obey_output_budget(self):
        header_client, header_server = self._pair(
            HTTP2Config(
                max_pending_output_bytes=64,
                max_control_output_bytes=64,
                max_response_body_bytes=1,
            )
        )
        header_client.send_headers(
            1,
            [
                (":method", "GET"),
                (":scheme", "http"),
                (":authority", "localhost"),
                (":path", "/"),
            ],
            end_stream=True,
        )
        header_server.receive_data(header_client.data_to_send())
        header_server.flush()
        header_server.queue_response(
            1,
            Response(headers={"x-large": "abcdefghijklmnopqrstuvwxyz" * 8}),
        )
        with self.assertRaisesRegex(ValueError, "control output"):
            header_server.flush()

        reset_client, reset_server = self._pair(
            HTTP2Config(
                max_pending_output_bytes=52,
                max_control_output_bytes=52,
                max_response_body_bytes=1,
            )
        )
        reset_client.send_headers(
            1,
            [
                (":method", "GET"),
                (":scheme", "http"),
                (":authority", "localhost"),
                (":path", "/"),
            ],
            end_stream=True,
        )
        for value in range(3):
            reset_client.ping(value.to_bytes(8, "big"))
        reset_server.receive_data(reset_client.data_to_send())
        reset_server.queue_response(1, Response(body=b"xx"))
        with self.assertRaisesRegex(ValueError, "control output"):
            reset_server.flush()

    @unittest.skipUnless(
        importlib.util.find_spec("regex") is not None,
        "install the smallserver[test] regex extra",
    )
    def test_regex_timeout_observer_is_opaque_once_and_sibling_survives(self):
        secret = "/private-target-should-not-escape"
        app = SmallServer()

        @app.get_regex(r"/private-target-(?P<value>.*)")
        async def timed_route(request):
            return Response.text("must not run")

        @app.get("/healthy")
        async def healthy(request):
            return Response.text("healthy")

        class ObserverTask:
            done = False

            @staticmethod
            def getID():
                return 17

            @staticmethod
            def acceptSignal(signal):
                return 0

        class HandlerTask:
            def sendSignal(self, task_id, signal):
                return 0

        class Protocol:
            def __init__(self):
                self.responses = {}

            def queue_response(self, stream_id, response):
                self.responses[stream_id] = response

            def drop_stream(self, stream_id):
                raise AssertionError("completed streams must not be dropped")

        observed = []

        def observe(event):
            observed.append(event)
            channel.stop()

        channel = RouteObserverChannel(observe, max_events=4)
        channel.bind(ObserverTask())
        task = HandlerTask()
        handle = type(
            "Handle",
            (),
            {"_route_observer_channel": channel, "_owned_tasks": [task]},
        )()
        protocol = Protocol()
        state = _H2ConnectionState(protocol)
        state.handlers = {1: task, 3: task}
        hostile = Request("GET", secret, Headers(), version="HTTP/2")
        sibling = Request("GET", "/healthy", Headers(), version="HTTP/2")

        with patch.object(
            app._router,
            "_match",
            side_effect=RouteMatchTimeout("regex-route-1"),
        ):
            asyncio.run(app._http2_handler(task, handle, state, 1, hostile))
        asyncio.run(app._http2_handler(task, handle, state, 3, sibling))
        asyncio.run(run_route_observer(ObserverTask(), channel))

        self.assertEqual(protocol.responses[1].status, 500)
        self.assertEqual(protocol.responses[3].body, b"healthy")
        self.assertEqual(
            observed,
            [RouteErrorEvent("regex-route-1", "route_match_timeout")],
        )
        event_graph = repr(observed[0])
        self.assertNotIn(secret, event_graph)
        self.assertFalse(hasattr(observed[0], "__traceback__"))


@unittest.skipUnless(H2_AVAILABLE, "install the smallserver[test] HTTP/2 extra")
class HTTP2ServerIntegrationTests(unittest.TestCase):
    _pair = HTTP2ProtocolTests._pair

    @unittest.skipUnless(
        importlib.util.find_spec("regex") is not None,
        "install the smallserver[test] regex extra",
    )
    def test_regex_timeout_is_observed_once_without_harming_other_streams(self):
        runtime = SmallOS().setKernel(Unix())
        observed = []
        observer_finished = threading.Event()

        def observe(event):
            observed.append(event)
            observer_finished.set()
            raise RuntimeError("intentional observer failure")

        app = SmallServer(
            RegexRouteConfig(match_timeout=0.001, total_match_timeout=0.005),
            route_error_observer=observe,
        )

        @app.post_regex(r"/(a+)+$")
        async def expensive(request):
            return Response.text("must not run")

        @app.get("/healthy")
        async def healthy(request):
            return Response.text("healthy")

        try:
            server = app.serve(
                runtime, host="127.0.0.1", port=0, protocol="http2"
            )
        except PermissionError:
            self.skipTest("the current sandbox does not permit loopback TCP binds")

        hostile_path = "/" + "a" * 5000 + "!"
        authorization_secret = "Bearer h2-private-authorization"
        body_secret = b"h2-private-body"
        statuses = {}
        bodies = {1: bytearray(), 3: bytearray(), 5: bytearray()}
        errors = []

        def client_work():
            try:
                client = H2Connection(
                    config=H2Configuration(
                        client_side=True, header_encoding="utf-8"
                    )
                )
                client.initiate_connection()
                with socket.create_connection(
                    ("127.0.0.1", server.port), timeout=3
                ) as connection:
                    connection.sendall(client.data_to_send())
                    client.send_headers(
                        1,
                        [
                            (":method", "POST"),
                            (":scheme", "http"),
                            (":authority", "localhost"),
                            (":path", hostile_path),
                            ("authorization", authorization_secret),
                            ("content-length", str(len(body_secret))),
                        ],
                    )
                    client.send_data(1, body_secret, end_stream=True)
                    client.send_headers(
                        3,
                        [
                            (":method", "GET"),
                            (":scheme", "http"),
                            (":authority", "localhost"),
                            (":path", "/healthy"),
                        ],
                        end_stream=True,
                    )
                    connection.sendall(client.data_to_send())
                    ended = set()
                    while not {1, 3}.issubset(ended):
                        data = connection.recv(65535)
                        if not data:
                            raise RuntimeError("HTTP/2 connection ended before sibling response")
                        for event in client.receive_data(data):
                            if isinstance(event, ResponseReceived):
                                statuses[event.stream_id] = dict(event.headers)[":status"]
                            elif isinstance(event, DataReceived):
                                bodies[event.stream_id].extend(event.data)
                                client.acknowledge_received_data(
                                    event.flow_controlled_length, event.stream_id
                                )
                            elif isinstance(event, StreamEnded):
                                ended.add(event.stream_id)
                        pending = client.data_to_send()
                        if pending:
                            connection.sendall(pending)

                    client.send_headers(
                        5,
                        [
                            (":method", "GET"),
                            (":scheme", "http"),
                            (":authority", "localhost"),
                            (":path", "/healthy"),
                        ],
                        end_stream=True,
                    )
                    connection.sendall(client.data_to_send())
                    while 5 not in ended:
                        data = connection.recv(65535)
                        if not data:
                            raise RuntimeError("HTTP/2 connection ended before later response")
                        for event in client.receive_data(data):
                            if isinstance(event, ResponseReceived):
                                statuses[event.stream_id] = dict(event.headers)[":status"]
                            elif isinstance(event, DataReceived):
                                bodies[event.stream_id].extend(event.data)
                                client.acknowledge_received_data(
                                    event.flow_controlled_length, event.stream_id
                                )
                            elif isinstance(event, StreamEnded):
                                ended.add(event.stream_id)
                        pending = client.data_to_send()
                        if pending:
                            connection.sendall(pending)

                    if not observer_finished.wait(2):
                        raise TimeoutError("route observer did not run")
                    server.close()
                    while connection.recv(65535):
                        pass
            except BaseException as exc:
                errors.append(exc)
                try:
                    server.close()
                except BaseException:
                    pass

        worker = threading.Thread(target=client_work, daemon=True)
        worker.start()
        runtime.start()
        worker.join(timeout=4)

        self.assertFalse(worker.is_alive())
        self.assertEqual(errors, [])
        self.assertEqual(statuses, {1: "500", 3: "200", 5: "200"})
        self.assertEqual(bytes(bodies[3]), b"healthy")
        self.assertEqual(bytes(bodies[5]), b"healthy")
        self.assertNotIn(hostile_path.encode("ascii"), bytes(bodies[1]))
        self.assertEqual(
            observed,
            [RouteErrorEvent("regex-route-1", "route_match_timeout")],
        )
        self.assertEqual(
            vars(observed[0]),
            {"route_id": "regex-route-1", "category": "route_match_timeout"},
        )
        for secret in (hostile_path, authorization_secret, body_secret.decode("ascii")):
            self.assertNotIn(secret, repr(observed[0]))
        self.assertFalse(hasattr(observed[0], "__traceback__"))
        self.assertEqual(server.route_observer_failures, 1)
        self.assertEqual(server.dropped_route_error_events, 0)
        self.assertTrue(server.finished)
        self.assertIsNone(server.failure)
        self.assertEqual(server.owned_connection_count, 0)
        self.assertEqual(runtime.ioReadWaiters, {})
        self.assertEqual(runtime.ioWriteWaiters, {})
        channel = server._route_observer_channel
        self.assertIsNotNone(channel)
        assert channel is not None
        self.assertIsNone(channel.task)
        self.assertEqual(list(channel.events), [])

    def test_prior_knowledge_multiplexing_and_graceful_goaway(self):
        runtime = SmallOS().setKernel(Unix())
        app = SmallServer()

        @app.get("/one")
        async def one(request):
            return Response.text("one")

        @app.get("/two")
        async def two(request):
            return Response.text("two")

        try:
            server = app.serve(
                runtime, host="127.0.0.1", port=0, protocol="http2"
            )
        except PermissionError:
            self.skipTest("the current sandbox does not permit loopback TCP binds")

        bodies = {1: bytearray(), 3: bytearray()}
        ended = set()
        terminated = []
        errors = []

        def client_work():
            try:
                client = H2Connection(
                    config=H2Configuration(
                        client_side=True, header_encoding="utf-8"
                    )
                )
                client.initiate_connection()
                with socket.create_connection(
                    ("127.0.0.1", server.port), timeout=3
                ) as connection:
                    connection.sendall(client.data_to_send())
                    for stream_id, path in ((1, "/one"), (3, "/two")):
                        client.send_headers(
                            stream_id,
                            [
                                (":method", "GET"),
                                (":scheme", "http"),
                                (":authority", "localhost"),
                                (":path", path),
                            ],
                            end_stream=True,
                        )
                    connection.sendall(client.data_to_send())
                    while len(ended) < 2:
                        data = connection.recv(65535)
                        if not data:
                            raise RuntimeError("HTTP/2 connection ended early")
                        for event in client.receive_data(data):
                            if isinstance(event, DataReceived):
                                bodies[event.stream_id].extend(event.data)
                                client.acknowledge_received_data(
                                    event.flow_controlled_length, event.stream_id
                                )
                            elif isinstance(event, StreamEnded):
                                ended.add(event.stream_id)
                        pending = client.data_to_send()
                        if pending:
                            connection.sendall(pending)
                    server.close()
                    while True:
                        data = connection.recv(65535)
                        if not data:
                            break
                        terminated.extend(
                            event
                            for event in client.receive_data(data)
                            if isinstance(event, ConnectionTerminated)
                        )
            except BaseException as exc:
                errors.append(exc)
                try:
                    server.close()
                except BaseException:
                    pass

        worker = threading.Thread(target=client_work, daemon=True)
        worker.start()
        runtime.start()
        worker.join(timeout=3)
        self.assertFalse(worker.is_alive())
        self.assertEqual(errors, [])
        self.assertEqual(bytes(bodies[1]), b"one")
        self.assertEqual(bytes(bodies[3]), b"two")
        self.assertTrue(terminated)
        self.assertTrue(server.finished)
        self.assertIsNone(server.failure)

    def test_same_batch_reset_never_spawns_cancelled_handler(self):
        runtime = SmallOS().setKernel(Unix())
        app = SmallServer()
        called = []

        @app.get("/cancelled")
        async def cancelled(request):
            called.append("cancelled")
            return Response.text("wrong")

        @app.get("/healthy")
        async def healthy(request):
            called.append("healthy")
            return Response.text("ok")

        try:
            server = app.serve(
                runtime, host="127.0.0.1", port=0, protocol="http2"
            )
        except PermissionError:
            self.skipTest("the current sandbox does not permit loopback TCP binds")
        errors = []
        healthy_body = bytearray()

        def client_work():
            try:
                client = H2Connection(config=H2Configuration(client_side=True))
                client.initiate_connection()
                with socket.create_connection(
                    ("127.0.0.1", server.port), timeout=3
                ) as connection:
                    connection.sendall(client.data_to_send())
                    client.send_headers(
                        1,
                        [
                            (":method", "GET"),
                            (":scheme", "http"),
                            (":authority", "localhost"),
                            (":path", "/cancelled"),
                        ],
                        end_stream=True,
                    )
                    client.reset_stream(1)
                    client.send_headers(
                        3,
                        [
                            (":method", "GET"),
                            (":scheme", "http"),
                            (":authority", "localhost"),
                            (":path", "/healthy"),
                        ],
                        end_stream=True,
                    )
                    connection.sendall(client.data_to_send())
                    ended = False
                    while not ended:
                        for event in client.receive_data(connection.recv(65535)):
                            if isinstance(event, DataReceived) and event.stream_id == 3:
                                healthy_body.extend(event.data)
                            elif isinstance(event, StreamEnded) and event.stream_id == 3:
                                ended = True
                    server.close()
                    while connection.recv(65535):
                        pass
            except BaseException as exc:
                errors.append(exc)
                try:
                    server.close()
                except BaseException:
                    pass

        worker = threading.Thread(target=client_work, daemon=True)
        worker.start()
        runtime.start()
        worker.join(timeout=3)
        self.assertFalse(worker.is_alive())
        self.assertEqual(errors, [])
        self.assertEqual(called, ["healthy"])
        self.assertEqual(bytes(healthy_body), b"ok")
        self.assertEqual(server.owned_connection_count, 0)

    def test_large_response_respects_flow_control(self):
        body = b"x" * 100_000
        config = HTTP2Config(
            max_response_body_bytes=len(body),
            max_pending_output_bytes=len(body) + 64 * 1024,
        )
        client, server = self._pair(config)
        client.send_headers(
            1,
            [
                (":method", "GET"),
                (":scheme", "http"),
                (":authority", "localhost"),
                (":path", "/"),
            ],
            end_stream=True,
        )
        server.receive_data(client.data_to_send())
        server.queue_response(1, Response(body=body))
        received = bytearray()
        ended = False
        for _ in range(20):
            events = client.receive_data(server.flush())
            for event in events:
                if isinstance(event, DataReceived):
                    received.extend(event.data)
                    client.acknowledge_received_data(
                        event.flow_controlled_length, event.stream_id
                    )
                elif isinstance(event, StreamEnded):
                    ended = True
            updates = client.data_to_send()
            if updates:
                server.receive_data(updates)
            if ended:
                break
        self.assertTrue(ended)
        self.assertEqual(bytes(received), body)
        self.assertEqual(server.pending_output_bytes, 0)

    def test_writer_send_failure_closes_only_client_and_listener_stays_healthy(self):
        runtime = SmallOS().setKernel(Unix())
        app = SmallServer()

        @app.get("/fail")
        async def fail(request):
            return Response.text("response")

        try:
            server = app.serve(
                runtime, host="127.0.0.1", port=0, protocol="http2"
            )
        except PermissionError:
            self.skipTest("the current sandbox does not permit loopback TCP binds")
        original_transport = server._transport
        failure_injected = False

        class FailingWriterTransport:
            def __getattr__(self, name):
                return getattr(original_transport, name)

            async def send_all(self, task, stream, data):
                nonlocal failure_injected
                if (
                    getattr(task, "name", "") == "smallserver-http2-writer"
                    and not failure_injected
                ):
                    failure_injected = True
                    raise RuntimeError("injected HTTP/2 writer failure")
                await original_transport.send_all(task, stream, data)

        server._transport = FailingWriterTransport()
        errors = []

        def client_work():
            try:
                responses = []
                for _attempt in range(2):
                    client = H2Connection(config=H2Configuration(client_side=True))
                    client.initiate_connection()
                    body = bytearray()
                    with socket.create_connection(
                        ("127.0.0.1", server.port), timeout=3
                    ) as connection:
                        connection.sendall(client.data_to_send())
                        client.send_headers(
                            1,
                            [
                                (":method", "GET"),
                                (":scheme", "http"),
                                (":authority", "localhost"),
                                (":path", "/fail"),
                            ],
                            end_stream=True,
                        )
                        connection.sendall(client.data_to_send())
                        ended = False
                        while not ended:
                            data = connection.recv(65535)
                            if not data:
                                break
                            for event in client.receive_data(data):
                                if isinstance(event, DataReceived):
                                    body.extend(event.data)
                                elif isinstance(event, StreamEnded):
                                    ended = True
                    responses.append(bytes(body))
                self.assertEqual(responses, [b"", b"response"])
                server.close()
            except BaseException as exc:
                errors.append(exc)
                try:
                    server.close()
                except BaseException:
                    pass

        worker = threading.Thread(target=client_work, daemon=True)
        worker.start()
        runtime.start()
        worker.join(timeout=3)
        self.assertFalse(worker.is_alive())
        self.assertEqual(errors, [])
        self.assertTrue(failure_injected)
        self.assertIsNone(server.failure)
        self.assertEqual(server.owned_connection_count, 0)
        self.assertTrue(server.finished)

    def test_shutdown_force_closes_a_blocked_writer(self):
        runtime = SmallOS().setKernel(Unix())
        app = SmallServer()

        @app.get("/blocked")
        async def blocked(request):
            return Response.text("response")

        try:
            server = app.serve(
                runtime, host="127.0.0.1", port=0, protocol="http2"
            )
        except PermissionError:
            self.skipTest("the current sandbox does not permit loopback TCP binds")
        original_transport = server._transport
        writer_blocked = threading.Event()

        class BlockingWriterTransport:
            def __getattr__(self, name):
                return getattr(original_transport, name)

            async def send_all(self, task, stream, data):
                if getattr(task, "name", "") == "smallserver-http2-writer":
                    writer_blocked.set()
                    await task.wait_signal(28)
                    return
                await original_transport.send_all(task, stream, data)

        server._transport = BlockingWriterTransport()
        errors = []

        def client_work():
            try:
                client = H2Connection(config=H2Configuration(client_side=True))
                client.initiate_connection()
                with socket.create_connection(
                    ("127.0.0.1", server.port), timeout=3
                ) as connection:
                    connection.sendall(client.data_to_send())
                    client.send_headers(
                        1,
                        [
                            (":method", "GET"),
                            (":scheme", "http"),
                            (":authority", "localhost"),
                            (":path", "/blocked"),
                        ],
                        end_stream=True,
                    )
                    connection.sendall(client.data_to_send())
                    if not writer_blocked.wait(2):
                        raise TimeoutError("writer did not enter its blocked wait")
                    server.close()
                    while connection.recv(65535):
                        pass
            except BaseException as exc:
                errors.append(exc)
                try:
                    server.close()
                except BaseException:
                    pass

        worker = threading.Thread(target=client_work, daemon=True)
        worker.start()
        runtime.start()
        worker.join(timeout=3)
        self.assertFalse(worker.is_alive())
        self.assertEqual(errors, [])
        self.assertTrue(server.finished)
        self.assertEqual(server.owned_connection_count, 0)

    def test_protocol_construction_failure_releases_accepted_connection(self):
        runtime = SmallOS().setKernel(Unix())
        app = SmallServer()
        try:
            with patch(
                "smallserver.app.H2Protocol",
                side_effect=RuntimeError("injected constructor failure"),
            ):
                server = app.serve(
                    runtime, host="127.0.0.1", port=0, protocol="http2"
                )

                def client_work():
                    with socket.create_connection(
                        ("127.0.0.1", server.port), timeout=3
                    ) as connection:
                        while connection.recv(1024):
                            pass
                    server.close()

                worker = threading.Thread(target=client_work, daemon=True)
                worker.start()
                runtime.start()
                worker.join(timeout=3)
        except PermissionError:
            self.skipTest("the current sandbox does not permit loopback TCP binds")
        self.assertFalse(worker.is_alive())
        self.assertTrue(server.finished)
        self.assertEqual(server.owned_connection_count, 0)

    def test_handshake_timeout_closes_silent_client_and_releases_capacity(self):
        runtime = SmallOS().setKernel(Unix())
        app = SmallServer()
        try:
            server = app.serve(
                runtime,
                host="127.0.0.1",
                port=0,
                protocol="http2",
                http2_config=HTTP2Config(handshake_timeout=0.01),
            )
        except PermissionError:
            self.skipTest("the current sandbox does not permit loopback TCP binds")
        errors = []

        def client_work():
            try:
                with socket.create_connection(
                    ("127.0.0.1", server.port), timeout=3
                ) as connection:
                    while connection.recv(1024):
                        pass
                server.close()
            except BaseException as exc:
                errors.append(exc)
                try:
                    server.close()
                except BaseException:
                    pass

        worker = threading.Thread(target=client_work, daemon=True)
        worker.start()
        runtime.start()
        worker.join(timeout=3)
        self.assertFalse(worker.is_alive())
        self.assertEqual(errors, [])
        self.assertEqual(server.owned_connection_count, 0)
        self.assertTrue(server.finished)

    def test_idle_timeout_closes_prefaced_client_and_releases_capacity(self):
        runtime = SmallOS().setKernel(Unix())
        app = SmallServer()
        try:
            server = app.serve(
                runtime,
                host="127.0.0.1",
                port=0,
                protocol="http2",
                http2_config=HTTP2Config(
                    handshake_timeout=1,
                    idle_timeout=0.01,
                ),
            )
        except PermissionError:
            self.skipTest("the current sandbox does not permit loopback TCP binds")
        errors = []

        def client_work():
            try:
                client = H2Connection(config=H2Configuration(client_side=True))
                client.initiate_connection()
                with socket.create_connection(
                    ("127.0.0.1", server.port), timeout=3
                ) as connection:
                    connection.sendall(client.data_to_send())
                    while connection.recv(1024):
                        pass
                server.close()
            except BaseException as exc:
                errors.append(exc)
                try:
                    server.close()
                except BaseException:
                    pass

        worker = threading.Thread(target=client_work, daemon=True)
        worker.start()
        runtime.start()
        worker.join(timeout=3)
        self.assertFalse(worker.is_alive())
        self.assertEqual(errors, [])
        self.assertEqual(server.owned_connection_count, 0)
        self.assertTrue(server.finished)


if __name__ == "__main__":
    unittest.main()
