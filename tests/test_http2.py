import builtins
import socket
import threading
import unittest
from unittest.mock import patch

from h2.config import H2Configuration
from h2.connection import H2Connection
from h2.events import ConnectionTerminated, DataReceived, ResponseReceived, StreamEnded

from SmallPackage import SmallOS, Unix

from smallserver import HTTP2Config, Response, SmallServer
from smallserver.errors import ServerConfigurationError
from smallserver.http2 import H2Protocol


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

    def test_dependency_is_lazy_and_missing_extra_is_actionable(self):
        original = builtins.__import__

        def reject_h2(name, *args, **kwargs):
            if name == "h2" or name.startswith("h2."):
                raise ImportError("missing")
            return original(name, *args, **kwargs)

        with patch("builtins.__import__", side_effect=reject_h2):
            with self.assertRaisesRegex(ServerConfigurationError, "smallserver\\[http2\\]"):
                H2Protocol()

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
            max_pending_output_bytes=8,
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


class HTTP2ServerIntegrationTests(unittest.TestCase):
    _pair = HTTP2ProtocolTests._pair

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

    def test_large_response_respects_flow_control(self):
        body = b"x" * 100_000
        config = HTTP2Config(
            max_response_body_bytes=len(body),
            max_pending_output_bytes=len(body),
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


if __name__ == "__main__":
    unittest.main()
