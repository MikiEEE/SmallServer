from __future__ import annotations

import base64
import importlib.util
import socket
import threading
import time
import unittest
from unittest.mock import patch

from SmallPackage import SmallOS, SmallTask, SmallWebSocketClient, Unix

from smallserver import (
    AdapterRegistry,
    ThreadAdapter,
    Headers,
    Request,
    Response,
    SmallServer,
    WebSocket,
    WebSocketCapacityError,
    WebSocketConfig,
    WebSocketDisconnect,
    WebSocketUnavailable,
)
from smallserver.websocket import (
    _FrameGuard,
    _WebSocketState,
    _WebSocketRoute,
    _drain_protocol_events,
    _handle_pong,
    _load_wsproto,
    _receive_protocol_data,
    _run_deadlines,
    _run_handler,
    _run_writer,
    _validate_upgrade,
)


HAS_WSPROTO = importlib.util.find_spec("wsproto") is not None


def upgrade_request(**headers: str) -> Request:
    values = {
        "Host": "localhost",
        "Upgrade": "websocket",
        "Connection": "keep-alive, Upgrade",
        "Sec-WebSocket-Version": "13",
        "Sec-WebSocket-Key": "dGhlIHNhbXBsZSBub25jZQ==",
    }
    values.update(headers)
    return Request("GET", "/ws", Headers(values))


async def unused_handler(socket: WebSocket) -> None:
    await socket.reject(Response(status=403))


class _RecordingTransport:
    def __init__(self) -> None:
        self.send_calls = 0
        self.payloads: list[bytes] = []

    async def send_all(self, task, client, payload: bytes) -> None:
        self.send_calls += 1
        self.payloads.append(payload)


class _BlockingTransport(_RecordingTransport):
    async def send_all(self, task, client, payload: bytes) -> None:
        self.send_calls += 1
        self.payloads.append(payload)
        await task.wait_signal(28)


def _make_state(
    runtime,
    transport,
    handler,
    *,
    config: WebSocketConfig | None = None,
) -> _WebSocketState:
    return _WebSocketState(
        runtime,
        transport,
        object(),
        upgrade_request(),
        _WebSocketRoute(handler, None, ()),
        config or WebSocketConfig(),
        b"",
    )


def _make_accepted_state(
    runtime,
    transport,
    handler,
    *,
    config: WebSocketConfig | None = None,
) -> _WebSocketState:
    state = _make_state(runtime, transport, handler, config=config)
    state.protocol = state.api.Connection(state.api.ConnectionType.SERVER)
    state.accepted = True
    state.handshake_state = "accepted"
    return state


class WebSocketProtocolTests(unittest.TestCase):
    def test_config_rejects_unbounded_or_inconsistent_limits(self) -> None:
        with self.assertRaisesRegex(ValueError, "max_message_bytes"):
            WebSocketConfig(max_message_bytes=0)
        with self.assertRaisesRegex(ValueError, "must not exceed"):
            WebSocketConfig(max_frame_payload_bytes=2, max_message_bytes=1)
        with self.assertRaisesRegex(ValueError, "idle_timeout"):
            WebSocketConfig(idle_timeout=float("inf"))
        with self.assertRaisesRegex(ValueError, "write_timeout"):
            WebSocketConfig(write_timeout=0)

    def test_registration_is_lazy_and_coexists_with_get(self) -> None:
        app = SmallServer()

        @app.get("/ws")
        async def ordinary(request):
            return Response.text("http")

        with patch("importlib.import_module") as importer:

            @app.websocket("/ws")
            async def websocket(socket):
                await socket.accept()

        importer.assert_not_called()
        self.assertIsNotNone(app._router.static_handler("GET", "/ws"))
        self.assertIn("/ws", app._websocket_routes)

    def test_missing_optional_engine_has_clear_error(self) -> None:
        real_import = __import__("importlib").import_module

        def missing(name: str):
            if name.startswith("wsproto"):
                raise ImportError("missing")
            return real_import(name)

        with patch("importlib.import_module", side_effect=missing):
            with self.assertRaisesRegex(WebSocketUnavailable, "websocket.*extra"):
                _load_wsproto()

    def test_upgrade_validation_and_rfc_example_accept_inputs(self) -> None:
        route = _WebSocketRoute(unused_handler, None, ("chat.v1",))
        self.assertIsNone(_validate_upgrade(upgrade_request(), route))
        response = _validate_upgrade(
            upgrade_request(**{"Sec-WebSocket-Version": "12"}), route
        )
        self.assertIsNotNone(response)
        assert response is not None
        self.assertEqual(response.status, 426)
        self.assertEqual(response.headers["sec-websocket-version"], "13")
        for name, value in (
            ("Upgrade", "not-websocket"),
            ("Connection", "keep-alive"),
            ("Sec-WebSocket-Key", base64.b64encode(b"short").decode("ascii")),
        ):
            with self.subTest(name=name):
                self.assertEqual(
                    _validate_upgrade(upgrade_request(**{name: value}), route).status,
                    400,
                )

    def test_origin_policy_is_explicit(self) -> None:
        route = _WebSocketRoute(
            unused_handler, frozenset({"https://allowed.example"}), ()
        )
        denied = _validate_upgrade(
            upgrade_request(Origin="https://denied.example"), route
        )
        self.assertIsNotNone(denied)
        assert denied is not None
        self.assertEqual(denied.status, 403)
        self.assertIsNone(
            _validate_upgrade(
                upgrade_request(Origin="https://allowed.example"), route
            )
        )

    def test_subprotocol_tokens_are_ascii(self) -> None:
        app = SmallServer()
        for invalid in ("chat:v1", "café", "chat v1", ""):
            with self.subTest(invalid=invalid):
                with self.assertRaisesRegex(ValueError, "HTTP tokens"):
                    app.websocket(
                        "/invalid-{}".format(len(app._websocket_routes)),
                        subprotocols=(invalid,),
                    )

        route = _WebSocketRoute(unused_handler, None, ("chat.v1",))
        for offered in ("chat:v1", "café", ","):
            with self.subTest(offered=offered):
                invalid = _validate_upgrade(
                    upgrade_request(**{"Sec-WebSocket-Protocol": offered}), route
                )
                self.assertIsNotNone(invalid)
                assert invalid is not None
                self.assertEqual(invalid.status, 400)

    @unittest.skipUnless(HAS_WSPROTO, "websocket extra is not installed")
    def test_extensions_cannot_be_selected_in_accept_response(self) -> None:
        runtime = SmallOS().setKernel(Unix())
        transport = _RecordingTransport()
        state = _make_state(runtime, transport, unused_handler)
        outcome: list[BaseException] = []

        async def accept_with_extension(task) -> None:
            try:
                await WebSocket(state).accept(
                    headers={"Sec-WebSocket-Extensions": "permessage-deflate"}
                )
            except BaseException as exc:
                outcome.append(exc)

        attempt = SmallTask(2, accept_with_extension, name="extension-rejection")
        runtime.fork(attempt)
        runtime.start()
        self.assertIsInstance(outcome[0], ValueError)
        self.assertEqual(state.handshake_state, "pending")
        self.assertEqual(transport.send_calls, 0)

    @unittest.skipUnless(HAS_WSPROTO, "websocket extra is not installed")
    def test_atomic_handshake_timeout_is_terminal_while_send_is_blocked(self) -> None:
        runtime = SmallOS().setKernel(Unix())
        transport = _BlockingTransport()
        config = WebSocketConfig(
            handshake_timeout=0.02,
            idle_timeout=1,
            close_timeout=0.02,
            deadline_resolution=0.005,
        )
        state = _make_state(runtime, transport, unused_handler, config=config)
        outcomes: list[BaseException] = []

        async def accept_job(task) -> None:
            state.handler_task = task
            try:
                await WebSocket(state).accept()
            except BaseException as exc:
                outcomes.append(exc)

        accept_task = SmallTask(2, accept_job, name="blocked-handshake")
        deadline_task = SmallTask(
            2, _run_deadlines, args=(state,), name="handshake-deadline"
        )
        state.deadline_task = deadline_task
        runtime.fork([accept_task, deadline_task])
        runtime.start()

        self.assertEqual(transport.send_calls, 1)
        self.assertEqual(state.handshake_state, "failed")
        self.assertFalse(state.accepted)
        self.assertFalse(state.rejected)
        self.assertIsNotNone(state.handshake_error)

        async def retry_job(task) -> None:
            try:
                await WebSocket(state).accept()
            except BaseException as exc:
                outcomes.append(exc)

        retry_task = SmallTask(2, retry_job, name="handshake-retry")
        runtime.fork(retry_task)
        runtime.start()
        self.assertIsInstance(outcomes[-1], Exception)
        self.assertIn("already decided", str(outcomes[-1]))
        self.assertEqual(transport.send_calls, 1)

        pending = _make_state(
            SmallOS().setKernel(Unix()), _RecordingTransport(), unused_handler
        )
        pending.request_shutdown()
        self.assertEqual(pending.handshake_state, "failed")
        self.assertIsInstance(pending.handshake_error, WebSocketDisconnect)

    @unittest.skipUnless(HAS_WSPROTO, "websocket extra is not installed")
    def test_ping_is_armed_before_send_and_only_matching_pong_clears(self) -> None:
        runtime = SmallOS().setKernel(Unix())
        state = _make_accepted_state(runtime, _RecordingTransport(), unused_handler)
        observations: list[tuple[object, ...]] = []
        _handle_pong(state, b"unsolicited")
        self.assertIsNone(state._pending_ping_generation)

        async def immediate_pong(event, size, *, wait):
            observations.append(
                (
                    state._pending_ping_generation,
                    state._pending_ping_payload,
                    state.pong_deadline is not None,
                )
            )
            _handle_pong(state, b"wrong")
            observations.append((state._pending_ping_generation,))
            _handle_pong(state, b"probe")

        state._enqueue = immediate_pong

        async def ping_job(task) -> None:
            await WebSocket(state).ping(b"probe")

        ping_task = SmallTask(2, ping_job, name="fast-pong")
        runtime.fork(ping_task)
        runtime.start()
        self.assertIsNone(ping_task.exception)
        self.assertEqual(observations[0][1:], (b"probe", True))
        self.assertIsNotNone(observations[1][0])
        self.assertIsNone(state._pending_ping_generation)
        self.assertIsNone(state.pong_deadline)

    @unittest.skipUnless(HAS_WSPROTO, "websocket extra is not installed")
    def test_fragment_metadata_is_coalesced_and_close_reasons_are_sanitized(self) -> None:
        api = _load_wsproto()
        runtime = SmallOS().setKernel(Unix())
        state = _make_accepted_state(runtime, _RecordingTransport(), unused_handler)
        client = api.Connection(api.ConnectionType.CLIENT)

        _receive_protocol_data(
            state,
            client.send(
                api.TextMessage(data="a", message_finished=False)
            ),
        )
        _drain_protocol_events(state)
        for _ in range(2048):
            _receive_protocol_data(
                state,
                client.send(api.TextMessage(data="", message_finished=False)),
            )
            _drain_protocol_events(state)
            self.assertEqual(len(state._message_buffer), 1)
        _receive_protocol_data(
            state,
            client.send(api.TextMessage(data="b", message_finished=True)),
        )
        _drain_protocol_events(state)
        self.assertEqual(state.inbox.popleft().text, "ab")

        peer_close_state = _make_accepted_state(
            runtime, _RecordingTransport(), unused_handler
        )
        peer = api.Connection(api.ConnectionType.CLIENT)
        _receive_protocol_data(
            peer_close_state,
            peer.send(api.CloseConnection(code=1000, reason="peer detail")),
        )
        self.assertTrue(_drain_protocol_events(peer_close_state))
        command = peer_close_state.outbox.popleft()
        self.assertEqual(command.size, 2 + len(b"peer detail"))
        self.assertEqual(command.event.reason, "peer detail")

        protocol_error_state = _make_accepted_state(
            runtime, _RecordingTransport(), unused_handler
        )
        _receive_protocol_data(protocol_error_state, b"\x83\x80mask")
        self.assertTrue(_drain_protocol_events(protocol_error_state))
        generated = protocol_error_state.outbox.popleft()
        self.assertEqual(int(generated.event.code), 1002)
        self.assertEqual(generated.event.reason, "protocol error")
        self.assertEqual(protocol_error_state.disconnect.reason, "protocol error")

    @unittest.skipUnless(HAS_WSPROTO, "websocket extra is not installed")
    def test_slow_writer_is_interrupted_by_bounded_deadlines(self) -> None:
        runtime = SmallOS().setKernel(Unix())
        config = WebSocketConfig(
            idle_timeout=1,
            write_timeout=0.02,
            close_timeout=0.02,
            pong_timeout=1,
            deadline_resolution=0.005,
        )
        state = _make_accepted_state(
            runtime, _BlockingTransport(), unused_handler, config=config
        )
        outcomes: list[str] = []

        async def sender(task) -> None:
            state.handler_task = task
            try:
                await WebSocket(state).send_text("blocked")
            finally:
                outcomes.append("sender-finished")

        sender_task = SmallTask(2, sender, name="blocked-sender")
        writer_task = SmallTask(2, _run_writer, args=(state,), name="blocked-writer")
        deadline_task = SmallTask(2, _run_deadlines, args=(state,), name="write-deadline")
        state.writer_task = writer_task
        state.deadline_task = deadline_task
        started = time.monotonic()
        runtime.fork([sender_task, writer_task, deadline_task])
        runtime.start()

        self.assertLess(time.monotonic() - started, 0.5)
        self.assertEqual(outcomes, ["sender-finished"])
        self.assertIsNotNone(state.disconnect)
        self.assertEqual(state.disconnect.code, 1006)
        self.assertEqual(state.disconnect.reason, "write timed out")
        self.assertEqual(state.outbox_bytes, 0)
        self.assertEqual(len(state.outbox), 0)

    @unittest.skipUnless(HAS_WSPROTO, "websocket extra is not installed")
    def test_critical_handler_failures_keep_identity_and_ordinary_errors_translate(self) -> None:
        for critical in (KeyboardInterrupt("stop"), SystemExit(7)):
            with self.subTest(critical=type(critical).__name__):
                runtime = SmallOS().setKernel(Unix())

                async def critical_handler(socket, error=critical) -> None:
                    raise error

                state = _make_state(
                    runtime, _RecordingTransport(), critical_handler
                )
                handler_task = SmallTask(
                    2, _run_handler, args=(state,), name="critical-handler"
                )
                state.handler_task = handler_task
                runtime.fork(handler_task)
                try:
                    runtime.start()
                except (KeyboardInterrupt, SystemExit) as caught:
                    self.assertIs(caught, critical)
                else:
                    self.fail("critical handler exception did not escape unchanged")
                self.assertIs(state.fatal_error, critical)
                self.assertEqual(state.handshake_state, "failed")

        runtime = SmallOS().setKernel(Unix())

        async def ordinary_handler(socket) -> None:
            raise RuntimeError("private detail")

        transport = _RecordingTransport()
        state = _make_state(runtime, transport, ordinary_handler)
        handler_task = SmallTask(
            2, _run_handler, args=(state,), name="ordinary-handler"
        )
        state.handler_task = handler_task
        runtime.fork(handler_task)
        runtime.start()
        self.assertIsNone(handler_task.exception)
        self.assertTrue(state.rejected)
        self.assertIn(b"HTTP/1.1 500 Internal Server Error", transport.payloads[0])
        self.assertNotIn(b"private detail", transport.payloads[0])

    @unittest.skipUnless(HAS_WSPROTO, "websocket extra is not installed")
    def test_frame_guard_bounds_declared_length_before_payload(self) -> None:
        guard = _FrameGuard(max_payload_bytes=1024)
        declared = b"\x82\xff" + (65537).to_bytes(8, "big") + b"mask"
        with self.assertRaises(WebSocketCapacityError):
            for byte in declared:
                guard.feed(bytes([byte]))
        self.assertLessEqual(len(guard._header), 14)

    @unittest.skipUnless(HAS_WSPROTO, "websocket extra is not installed")
    def test_partial_masked_fragmented_input_and_protocol_errors(self) -> None:
        api = _load_wsproto()
        client = api.Connection(api.ConnectionType.CLIENT)
        server = api.Connection(api.ConnectionType.SERVER)
        guard = _FrameGuard(1024)
        payload = client.send(
            api.TextMessage(data="hel", frame_finished=True, message_finished=False)
        ) + client.send(
            api.TextMessage(data="lo", frame_finished=True, message_finished=True)
        )
        for byte in payload:
            for chunk in guard.feed(bytes([byte])):
                server.receive_data(chunk)
        events = list(server.events())
        self.assertEqual("".join(event.data for event in events), "hello")
        self.assertTrue(events[-1].message_finished)
        with self.assertRaisesRegex(ValueError, "masked"):
            _FrameGuard(1024).feed(b"\x81\x01x")

    @unittest.skipUnless(HAS_WSPROTO, "websocket extra is not installed")
    def test_inbound_and_outbound_mailboxes_are_bounded(self) -> None:
        config = WebSocketConfig(
            max_frame_payload_bytes=8,
            max_message_bytes=8,
            max_inbound_messages=1,
            max_inbound_bytes=3,
            max_outbound_commands=1,
            max_outbound_bytes=3,
        )
        state = _WebSocketState(
            object(),
            object(),
            object(),
            upgrade_request(),
            _WebSocketRoute(unused_handler, None, ()),
            config,
            b"",
        )
        self.assertTrue(state._deliver_message("abc"))
        self.assertFalse(state._deliver_message("x"))
        self.assertTrue(state._enqueue_control(state.api.Ping(payload=b"abc"), 3))
        self.assertFalse(state._enqueue_control(state.api.Ping(payload=b"x"), 1))


@unittest.skipUnless(HAS_WSPROTO, "websocket extra is not installed")
class WebSocketLoopbackTests(unittest.TestCase):
    def _http_exchange(self, port: int, payload: bytes) -> bytes:
        with socket.create_connection(("127.0.0.1", port), timeout=3) as stream:
            stream.sendall(payload)
            chunks = []
            while True:
                chunk = stream.recv(4096)
                if not chunk:
                    return b"".join(chunks)
                chunks.append(chunk)

    def test_wsproto_client_interoperability_and_http_coexistence(self) -> None:
        api = _load_wsproto()
        runtime = SmallOS().setKernel(Unix())
        app = SmallServer(
            websocket_config=WebSocketConfig(
                max_frame_payload_bytes=4096,
                max_message_bytes=4096,
                idle_timeout=5,
                handshake_timeout=2,
                close_timeout=1,
            )
        )

        @app.get("/ws")
        async def normal_get(request):
            return Response.text("ordinary-http")

        @app.websocket(
            "/ws",
            origins={"https://allowed.example"},
            subprotocols=("chat.v1",),
        )
        async def echo(websocket: WebSocket) -> None:
            await websocket.accept(subprotocol="chat.v1")
            async for message in websocket:
                if message.is_text:
                    await websocket.send_text(message.text)
                else:
                    await websocket.send_bytes(message.bytes)

        try:
            server = app.serve(runtime, host="127.0.0.1", port=0)
        except PermissionError:
            self.skipTest("the current sandbox does not permit loopback TCP binds")

        outcomes: list[object] = []
        errors: list[BaseException] = []

        def client_work() -> None:
            try:
                ordinary = self._http_exchange(
                    server.port,
                    b"GET /ws HTTP/1.1\r\nHost: localhost\r\n\r\n",
                )
                outcomes.append(ordinary)
                with socket.create_connection(
                    ("127.0.0.1", server.port), timeout=3
                ) as stream:
                    stream.sendall(
                        b"GET /ws?room=1 HTTP/1.1\r\n"
                        b"Host: localhost\r\n"
                        b"Upgrade: websocket\r\n"
                        b"Connection: keep-alive, Upgrade\r\n"
                        b"Sec-WebSocket-Version: 13\r\n"
                        b"Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n"
                        b"Sec-WebSocket-Protocol: chat.v1\r\n"
                        b"Origin: https://allowed.example\r\n\r\n"
                    )
                    response = b""
                    while b"\r\n\r\n" not in response:
                        response += stream.recv(4096)
                    outcomes.append(response)
                    client = api.Connection(api.ConnectionType.CLIENT)
                    stream.sendall(
                        client.send(
                            api.TextMessage(
                                data="hel",
                                frame_finished=True,
                                message_finished=False,
                            )
                        )
                        + client.send(
                            api.TextMessage(
                                data="lo",
                                frame_finished=True,
                                message_finished=True,
                            )
                        )
                    )
                    outcomes.extend(_receive_events(stream, client, api.TextMessage))
                    stream.sendall(client.send(api.BytesMessage(data=b"binary")))
                    outcomes.extend(_receive_events(stream, client, api.BytesMessage))
                    stream.sendall(client.send(api.Ping(payload=b"probe")))
                    outcomes.extend(_receive_events(stream, client, api.Pong))
                    stream.sendall(
                        client.send(api.CloseConnection(code=1000, reason="done"))
                    )
                    outcomes.extend(_receive_events(stream, client, api.CloseConnection))
            except BaseException as exc:
                errors.append(exc)
            finally:
                server.close()

        worker = threading.Thread(target=client_work, daemon=True)
        worker.start()
        runtime.start()
        worker.join(timeout=5)

        self.assertFalse(worker.is_alive())
        self.assertEqual(errors, [])
        self.assertIn(b"ordinary-http", outcomes[0])
        self.assertIn(b"HTTP/1.1 101 Switching Protocols", outcomes[1])
        self.assertIn(b"Sec-WebSocket-Accept: s3pPLMBiTxaQ9kYGzzhZRbK+xOo=", outcomes[1])
        self.assertIn(b"Sec-WebSocket-Protocol: chat.v1", outcomes[1])
        self.assertTrue(any(getattr(event, "data", None) == "hello" for event in outcomes))
        self.assertTrue(any(getattr(event, "data", None) == b"binary" for event in outcomes))
        self.assertTrue(any(getattr(event, "payload", None) == b"probe" for event in outcomes))
        self.assertTrue(
            any(getattr(event, "code", None) == 1000 for event in outcomes),
            outcomes,
        )
        self.assertEqual(server._websocket_states, {})
        self.assertEqual(runtime.ioReadWaiters, {})
        self.assertEqual(runtime.ioWriteWaiters, {})

    def test_smallos_websocket_client_interoperability(self) -> None:
        runtime = SmallOS().setKernel(Unix())
        app = SmallServer(
            websocket_config=WebSocketConfig(
                max_frame_payload_bytes=4096,
                max_message_bytes=4096,
                idle_timeout=5,
                close_timeout=1,
            )
        )

        @app.websocket("/native", subprotocols=("smallos.v1",))
        async def echo(websocket: WebSocket) -> None:
            await websocket.accept(subprotocol="smallos.v1")
            message = await websocket.receive()
            await websocket.send_text(message.text)

        try:
            server = app.serve(runtime, host="127.0.0.1", port=0)
        except PermissionError:
            self.skipTest("the current sandbox does not permit loopback TCP binds")

        outcome: dict[str, object] = {}

        async def client_job(task) -> None:
            client = SmallWebSocketClient(
                task,
                host="127.0.0.1",
                port=server.port,
                client_key="dGhlIHNhbXBsZSBub25jZQ==",
            )
            try:
                await client.connect("/native", subprotocols=("smallos.v1",))
                outcome["subprotocol"] = client.negotiated_subprotocol
                await client.send_text("native-client")
                outcome["message"] = await client.receive()
            finally:
                await client.disconnect()
                server.close()

        client_task = SmallTask(2, client_job, name="smallserver-ws-client")
        runtime.fork(client_task)
        runtime.start()

        self.assertIsNone(client_task.exception)
        self.assertEqual(outcome["subprotocol"], "smallos.v1")
        self.assertEqual(
            outcome["message"], {"type": "text", "data": "native-client"}
        )
        self.assertTrue(server.finished)
        self.assertEqual(runtime.ioReadWaiters, {})
        self.assertEqual(runtime.ioWriteWaiters, {})

    def test_waiting_websocket_does_not_delay_unrelated_http(self) -> None:
        runtime = SmallOS().setKernel(Unix())
        app = SmallServer(
            websocket_config=WebSocketConfig(
                max_frame_payload_bytes=4096,
                max_message_bytes=4096,
                idle_timeout=5,
                close_timeout=1,
            )
        )

        @app.get("/fast")
        async def fast(request):
            return Response.text("fast")

        @app.websocket("/waiting")
        async def waiting(websocket: WebSocket) -> None:
            await websocket.accept()
            await websocket.receive()

        try:
            server = app.serve(runtime, host="127.0.0.1", port=0)
        except PermissionError:
            self.skipTest("the current sandbox does not permit loopback TCP binds")

        api = _load_wsproto()
        outcomes: list[bytes] = []
        errors: list[BaseException] = []

        def client_work() -> None:
            try:
                with socket.create_connection(
                    ("127.0.0.1", server.port), timeout=3
                ) as websocket_stream:
                    websocket_stream.sendall(
                        b"GET /waiting HTTP/1.1\r\nHost: localhost\r\n"
                        b"Upgrade: websocket\r\nConnection: Upgrade\r\n"
                        b"Sec-WebSocket-Version: 13\r\n"
                        b"Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\r\n"
                    )
                    response = b""
                    while b"\r\n\r\n" not in response:
                        response += websocket_stream.recv(4096)
                    outcomes.append(response)
                    outcomes.append(
                        self._http_exchange(
                            server.port,
                            b"GET /fast HTTP/1.1\r\nHost: localhost\r\n\r\n",
                        )
                    )
                    client = api.Connection(api.ConnectionType.CLIENT)
                    websocket_stream.sendall(
                        client.send(api.CloseConnection(code=1000, reason="done"))
                    )
                    _receive_events(
                        websocket_stream, client, api.CloseConnection
                    )
            except BaseException as exc:
                errors.append(exc)
            finally:
                server.close()

        worker = threading.Thread(target=client_work, daemon=True)
        worker.start()
        runtime.start()
        worker.join(timeout=5)

        self.assertFalse(worker.is_alive())
        self.assertEqual(errors, [])
        self.assertIn(b"HTTP/1.1 101 Switching Protocols", outcomes[0])
        self.assertIn(b"fast", outcomes[1])
        self.assertTrue(server.finished)

    def test_malformed_and_oversized_frames_close_with_safe_codes(self) -> None:
        runtime = SmallOS().setKernel(Unix())
        app = SmallServer(
            websocket_config=WebSocketConfig(
                max_frame_payload_bytes=8,
                max_message_bytes=8,
                idle_timeout=5,
                close_timeout=1,
            )
        )

        @app.websocket("/bounded")
        async def bounded(websocket: WebSocket) -> None:
            await websocket.accept()
            await websocket.receive()

        try:
            server = app.serve(runtime, host="127.0.0.1", port=0)
        except PermissionError:
            self.skipTest("the current sandbox does not permit loopback TCP binds")

        api = _load_wsproto()
        close_codes: list[int | None] = []
        errors: list[BaseException] = []

        def send_bad_frame(frame: bytes) -> None:
            with socket.create_connection(
                ("127.0.0.1", server.port), timeout=3
            ) as stream:
                stream.sendall(
                    b"GET /bounded HTTP/1.1\r\nHost: localhost\r\n"
                    b"Upgrade: websocket\r\nConnection: Upgrade\r\n"
                    b"Sec-WebSocket-Version: 13\r\n"
                    b"Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\r\n"
                )
                response = b""
                while b"\r\n\r\n" not in response:
                    response += stream.recv(4096)
                stream.sendall(frame)
                client = api.Connection(api.ConnectionType.CLIENT)
                events = _receive_events(stream, client, api.CloseConnection)
                close = next(
                    event for event in events if isinstance(event, api.CloseConnection)
                )
                close_codes.append(close.code)
                stream.sendall(client.send(close.response()))

        def client_work() -> None:
            try:
                send_bad_frame(b"\x81\x01x")
                send_bad_frame(b"\x82\xfe\x00\x7emask")
            except BaseException as exc:
                errors.append(exc)
            finally:
                server.close()

        worker = threading.Thread(target=client_work, daemon=True)
        worker.start()
        runtime.start()
        worker.join(timeout=5)

        self.assertFalse(worker.is_alive())
        self.assertEqual(errors, [])
        self.assertEqual(close_codes, [1002, 1009])
        self.assertTrue(server.finished)

    def test_handshake_idle_and_pong_deadlines_are_bounded(self) -> None:
        runtime = SmallOS().setKernel(Unix())
        app = SmallServer(
            websocket_config=WebSocketConfig(
                max_frame_payload_bytes=1024,
                max_message_bytes=1024,
                handshake_timeout=0.1,
                idle_timeout=0.2,
                pong_timeout=0.05,
                close_timeout=0.2,
                deadline_resolution=0.01,
            )
        )

        @app.websocket("/handshake-timeout")
        async def handshake_timeout(websocket: WebSocket) -> None:
            await runtime.cursor.sleep(1)

        @app.websocket("/idle-timeout")
        async def idle_timeout(websocket: WebSocket) -> None:
            await websocket.accept()
            await runtime.cursor.sleep(5)

        @app.websocket("/pong-timeout")
        async def pong_timeout(websocket: WebSocket) -> None:
            await websocket.accept()
            await websocket.ping(b"deadline")
            await websocket.receive()

        try:
            server = app.serve(runtime, host="127.0.0.1", port=0)
        except PermissionError:
            self.skipTest("the current sandbox does not permit loopback TCP binds")

        api = _load_wsproto()
        outcomes: dict[str, object] = {}
        errors: list[BaseException] = []

        def connect(path: str):
            stream = socket.create_connection(("127.0.0.1", server.port), timeout=3)
            stream.sendall(
                "GET {} HTTP/1.1\r\nHost: localhost\r\n"
                "Upgrade: websocket\r\nConnection: Upgrade\r\n"
                "Sec-WebSocket-Version: 13\r\n"
                "Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\r\n".format(
                    path
                ).encode("ascii")
            )
            response = b""
            while b"\r\n\r\n" not in response:
                response += stream.recv(4096)
            headers, separator, websocket_data = response.partition(b"\r\n\r\n")
            return stream, headers + separator, websocket_data

        def client_work() -> None:
            try:
                stream, response, _ = connect("/handshake-timeout")
                outcomes["handshake"] = response
                stream.close()

                stream, response, websocket_data = connect("/idle-timeout")
                outcomes["idle_handshake"] = response
                idle_client = api.Connection(api.ConnectionType.CLIENT)
                idle_events = _receive_events(
                    stream,
                    idle_client,
                    api.CloseConnection,
                    initial_data=websocket_data,
                )
                idle_close = next(
                    event
                    for event in idle_events
                    if isinstance(event, api.CloseConnection)
                )
                outcomes["idle_code"] = idle_close.code
                stream.sendall(idle_client.send(idle_close.response()))
                stream.close()

                stream, response, websocket_data = connect("/pong-timeout")
                outcomes["pong_handshake"] = response
                pong_client = api.Connection(api.ConnectionType.CLIENT)
                ping_events = _receive_events(
                    stream,
                    pong_client,
                    api.Ping,
                    initial_data=websocket_data,
                )
                outcomes["ping_payload"] = next(
                    event.payload
                    for event in ping_events
                    if isinstance(event, api.Ping)
                )
                pong_close = next(
                    (
                        event
                        for event in ping_events
                        if isinstance(event, api.CloseConnection)
                    ),
                    None,
                )
                if pong_close is None:
                    close_events = _receive_events(
                        stream, pong_client, api.CloseConnection
                    )
                    pong_close = next(
                        event
                        for event in close_events
                        if isinstance(event, api.CloseConnection)
                    )
                outcomes["pong_code"] = pong_close.code
                stream.sendall(pong_client.send(pong_close.response()))
                stream.close()
            except BaseException as exc:
                errors.append(exc)
            finally:
                server.close()

        worker = threading.Thread(target=client_work, daemon=True)
        worker.start()
        runtime.start()
        worker.join(timeout=5)

        self.assertFalse(worker.is_alive())
        self.assertEqual(errors, [])
        self.assertIn(b"HTTP/1.1 408 Request Timeout", outcomes["handshake"])
        self.assertIn(
            b"HTTP/1.1 101 Switching Protocols", outcomes["idle_handshake"]
        )
        self.assertEqual(outcomes["idle_code"], 1001)
        self.assertEqual(outcomes["ping_payload"], b"deadline")
        self.assertEqual(outcomes["pong_code"], 1002)
        self.assertTrue(server.finished)

    def test_idle_deadline_cancels_adapter_waiting_handler(self) -> None:
        runtime = SmallOS().setKernel(Unix())
        release = threading.Event()
        entered = threading.Event()
        app = SmallServer(
            websocket_config=WebSocketConfig(
                idle_timeout=0.05,
                close_timeout=0.1,
                deadline_resolution=0.01,
            )
        )
        errors: list[BaseException] = []

        def blocking_work() -> None:
            entered.set()
            if not release.wait(2):
                raise TimeoutError("adapter worker was not released")

        with AdapterRegistry(
            blocking=ThreadAdapter(max_workers=1, max_pending=1)
        ) as services:

            @app.websocket("/adapter-idle")
            async def adapter_idle(websocket: WebSocket) -> None:
                await websocket.accept()
                await services.call("blocking", blocking_work)

            try:
                server = app.serve(runtime, host="127.0.0.1", port=0)
            except PermissionError:
                self.skipTest("the current sandbox does not permit loopback TCP binds")

            close_codes: list[int | None] = []

            def client_work() -> None:
                try:
                    with socket.create_connection(
                        ("127.0.0.1", server.port), timeout=3
                    ) as stream:
                        stream.sendall(
                            b"GET /adapter-idle HTTP/1.1\r\nHost: localhost\r\n"
                            b"Upgrade: websocket\r\nConnection: Upgrade\r\n"
                            b"Sec-WebSocket-Version: 13\r\n"
                            b"Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\r\n"
                        )
                        response = b""
                        while b"\r\n\r\n" not in response:
                            response += stream.recv(4096)
                        if not entered.wait(1):
                            raise TimeoutError("adapter handler did not start")
                        api = _load_wsproto()
                        client = api.Connection(api.ConnectionType.CLIENT)
                        events = _receive_events(
                            stream, client, api.CloseConnection
                        )
                        close = next(
                            event
                            for event in events
                            if isinstance(event, api.CloseConnection)
                        )
                        close_codes.append(close.code)
                        stream.sendall(client.send(close.response()))
                except BaseException as exc:
                    errors.append(exc)
                finally:
                    release.set()
                    server.close()

            worker = threading.Thread(target=client_work, daemon=True)
            worker.start()
            runtime.start()
            worker.join(timeout=5)

            self.assertFalse(worker.is_alive())
            self.assertEqual(errors, [])
            self.assertEqual(close_codes, [1001])
            self.assertTrue(server.finished)

    def test_server_shutdown_attempts_close_and_releases_children(self) -> None:
        api = _load_wsproto()
        runtime = SmallOS().setKernel(Unix())
        app = SmallServer(
            websocket_config=WebSocketConfig(
                max_frame_payload_bytes=4096,
                max_message_bytes=4096,
                close_timeout=1,
                idle_timeout=5,
            )
        )

        @app.websocket("/live")
        async def live(websocket: WebSocket) -> None:
            await websocket.accept()
            await websocket.receive()

        try:
            server = app.serve(runtime, host="127.0.0.1", port=0)
        except PermissionError:
            self.skipTest("the current sandbox does not permit loopback TCP binds")

        close_events: list[object] = []
        states: list[object] = []
        errors: list[BaseException] = []

        def client_work() -> None:
            try:
                with socket.create_connection(
                    ("127.0.0.1", server.port), timeout=3
                ) as stream:
                    stream.sendall(
                        b"GET /live HTTP/1.1\r\nHost: localhost\r\n"
                        b"Upgrade: websocket\r\nConnection: Upgrade\r\n"
                        b"Sec-WebSocket-Version: 13\r\n"
                        b"Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\r\n"
                    )
                    response = b""
                    while b"\r\n\r\n" not in response:
                        response += stream.recv(4096)
                    client = api.Connection(api.ConnectionType.CLIENT)
                    states.extend(server._websocket_states.values())
                    server.close()
                    events = _receive_events(stream, client, api.CloseConnection)
                    close_events.extend(events)
                    close_event = next(
                        event
                        for event in events
                        if isinstance(event, api.CloseConnection)
                    )
                    stream.sendall(client.send(close_event.response()))
            except BaseException as exc:
                errors.append(exc)

        worker = threading.Thread(target=client_work, daemon=True)
        worker.start()
        runtime.start()
        worker.join(timeout=5)

        self.assertFalse(worker.is_alive())
        self.assertEqual(errors, [])
        self.assertTrue(
            any(getattr(event, "code", None) == 1001 for event in close_events),
            (
                close_events,
                states[0].disconnect if states else None,
                states[0].handler_error if states else None,
            ),
        )
        self.assertTrue(server.finished)
        self.assertEqual(server._websocket_states, {})
        self.assertEqual(runtime.ioReadWaiters, {})
        self.assertEqual(runtime.ioWriteWaiters, {})

    def test_handler_failure_sends_sanitized_1011_close(self) -> None:
        api = _load_wsproto()
        runtime = SmallOS().setKernel(Unix())
        app = SmallServer(
            websocket_config=WebSocketConfig(
                max_frame_payload_bytes=4096,
                max_message_bytes=4096,
                close_timeout=1,
                idle_timeout=5,
            )
        )

        @app.websocket("/fail")
        async def fail(websocket: WebSocket) -> None:
            await websocket.accept()
            raise RuntimeError("sensitive-handler-detail")

        try:
            server = app.serve(runtime, host="127.0.0.1", port=0)
        except PermissionError:
            self.skipTest("the current sandbox does not permit loopback TCP binds")

        close_events: list[object] = []
        errors: list[BaseException] = []

        def client_work() -> None:
            try:
                with socket.create_connection(
                    ("127.0.0.1", server.port), timeout=3
                ) as stream:
                    stream.sendall(
                        b"GET /fail HTTP/1.1\r\nHost: localhost\r\n"
                        b"Upgrade: websocket\r\nConnection: Upgrade\r\n"
                        b"Sec-WebSocket-Version: 13\r\n"
                        b"Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\r\n"
                    )
                    response = b""
                    while b"\r\n\r\n" not in response:
                        response += stream.recv(4096)
                    _, _, websocket_data = response.partition(b"\r\n\r\n")
                    client = api.Connection(api.ConnectionType.CLIENT)
                    events = _receive_events(
                        stream,
                        client,
                        api.CloseConnection,
                        initial_data=websocket_data,
                    )
                    close_events.extend(events)
                    close_event = next(
                        event
                        for event in events
                        if isinstance(event, api.CloseConnection)
                    )
                    stream.sendall(client.send(close_event.response()))
            except BaseException as exc:
                errors.append(exc)
            finally:
                server.close()

        worker = threading.Thread(target=client_work, daemon=True)
        worker.start()
        runtime.start()
        worker.join(timeout=5)

        self.assertFalse(worker.is_alive())
        self.assertEqual(errors, [])
        failure = next(
            event for event in close_events if isinstance(event, api.CloseConnection)
        )
        self.assertEqual(failure.code, 1011)
        self.assertNotIn("sensitive", failure.reason)
        self.assertTrue(server.finished)


def _receive_events(stream, connection, event_type, *, initial_data=b""):
    deadline = time.monotonic() + 3
    received = []
    pending = initial_data
    while time.monotonic() < deadline:
        data = pending or stream.recv(4096)
        pending = b""
        if not data:
            return received
        connection.receive_data(data)
        events = list(connection.events())
        received.extend(events)
        if any(isinstance(event, event_type) for event in events):
            return received
    raise TimeoutError("expected WebSocket event was not received")


if __name__ == "__main__":
    unittest.main()
