"""Lazy, bounded HTTP/2 protocol state built on optional hyper-h2."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .errors import ServerConfigurationError
from .http import Headers, Request, Response


HTTP2_CLIENT_PREFACE = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n"


@dataclass(frozen=True)
class HTTP2Config:
    """Finite protocol and buffering limits for HTTP/2 connections."""

    max_concurrent_streams: int = 100
    max_header_count: int = 100
    max_header_bytes: int = 16 * 1024
    max_compressed_header_bytes: int = 16 * 1024
    max_body_bytes: int = 1024 * 1024
    max_connection_buffer_bytes: int = 4 * 1024 * 1024
    max_pending_output_bytes: int = 4 * 1024 * 1024
    max_response_body_bytes: int = 2 * 1024 * 1024
    max_frame_size: int = 16 * 1024

    def __post_init__(self) -> None:
        for name, value in self.__dict__.items():
            if type(value) is not int or value <= 0:
                raise ValueError("{} must be a positive integer".format(name))
        if not 16_384 <= self.max_frame_size <= 16_777_215:
            raise ValueError("max_frame_size must be between 16384 and 16777215")
        if self.max_body_bytes > self.max_connection_buffer_bytes:
            raise ValueError(
                "max_body_bytes cannot exceed max_connection_buffer_bytes"
            )
        if self.max_response_body_bytes > self.max_pending_output_bytes:
            raise ValueError(
                "max_response_body_bytes cannot exceed max_pending_output_bytes"
            )


@dataclass(frozen=True)
class H2ReadyRequest:
    """A complete request stream ready for application dispatch."""

    stream_id: int
    request: Request


@dataclass
class _InboundStream:
    method: str
    path: str
    headers: Headers
    body: bytearray


@dataclass
class _OutboundStream:
    body: bytes
    offset: int = 0


class _FrameBudget:
    """Account compressed header blocks without implementing frame semantics."""

    def __init__(self, config: HTTP2Config) -> None:
        self._config = config
        self._preface = bytearray()
        self._header = bytearray()
        self._remaining = 0
        self._frame_type = 0
        self._frame_stream = 0
        self._frame_flags = 0
        self._header_stream: int | None = None
        self._header_bytes = 0

    def feed(self, data: bytes) -> None:
        view = memoryview(data)
        offset = 0
        if len(self._preface) < len(HTTP2_CLIENT_PREFACE):
            needed = len(HTTP2_CLIENT_PREFACE) - len(self._preface)
            take = min(needed, len(view))
            self._preface.extend(view[:take])
            offset += take
            expected = HTTP2_CLIENT_PREFACE[: len(self._preface)]
            if bytes(self._preface) != expected:
                raise ValueError("invalid HTTP/2 client preface")
            if offset == len(view):
                return

        while offset < len(view):
            if self._remaining == 0:
                needed = 9 - len(self._header)
                take = min(needed, len(view) - offset)
                self._header.extend(view[offset : offset + take])
                offset += take
                if len(self._header) < 9:
                    return
                length = int.from_bytes(self._header[:3], "big")
                if length > self._config.max_frame_size:
                    raise ValueError("HTTP/2 frame exceeds configured maximum")
                self._frame_type = self._header[3]
                self._frame_flags = self._header[4]
                self._frame_stream = int.from_bytes(self._header[5:9], "big") & 0x7FFFFFFF
                self._header.clear()
                self._remaining = length
                if length == 0:
                    self._finish_frame()
                    continue

            take = min(self._remaining, len(view) - offset)
            if self._frame_type in (0x1, 0x9):
                self._account_header_bytes(take)
            self._remaining -= take
            offset += take
            if self._remaining == 0:
                self._finish_frame()

    def _account_header_bytes(self, count: int) -> None:
        if self._frame_type == 0x1 and self._header_stream is None:
            self._header_stream = self._frame_stream
            self._header_bytes = 0
        self._header_bytes += count
        if self._header_bytes > self._config.max_compressed_header_bytes:
            raise ValueError("HTTP/2 compressed header block is too large")

    def _finish_frame(self) -> None:
        if self._frame_type in (0x1, 0x9) and self._frame_flags & 0x4:
            self._header_stream = None
            self._header_bytes = 0


def require_http2() -> None:
    """Fail clearly without importing hyper-h2 on HTTP/1.1 paths."""
    try:
        import h2  # type: ignore[import-not-found]
    except ImportError as exc:
        raise ServerConfigurationError(
            "HTTP/2 requires the optional dependency; install smallserver[http2]"
        ) from exc
    version = getattr(h2, "__version__", "")
    if not isinstance(version, str) or not version.startswith("4."):
        raise ServerConfigurationError("HTTP/2 requires hyper-h2 version 4.x")


class H2Protocol:
    """One connection's sans-I/O HTTP/2 and bounded stream state."""

    _FORBIDDEN_HEADERS = {
        "connection",
        "keep-alive",
        "proxy-connection",
        "transfer-encoding",
        "upgrade",
    }

    def __init__(self, config: HTTP2Config | None = None) -> None:
        require_http2()
        from h2.config import H2Configuration  # type: ignore[import-not-found]
        from h2.connection import H2Connection  # type: ignore[import-not-found]
        from h2.errors import ErrorCodes  # type: ignore[import-not-found]
        from h2.events import (  # type: ignore[import-not-found]
            ConnectionTerminated,
            DataReceived,
            RemoteSettingsChanged,
            RequestReceived,
            StreamEnded,
            StreamReset,
            TrailersReceived,
            WindowUpdated,
        )
        from h2.settings import SettingCodes  # type: ignore[import-not-found]

        self.config = config or HTTP2Config()
        h2_config = H2Configuration(
            client_side=False,
            header_encoding="utf-8",
            validate_inbound_headers=True,
            normalize_inbound_headers=False,
        )
        self.connection = H2Connection(config=h2_config)
        self.connection.local_settings[SettingCodes.MAX_CONCURRENT_STREAMS] = (
            self.config.max_concurrent_streams
        )
        self.connection.local_settings[SettingCodes.MAX_HEADER_LIST_SIZE] = (
            self.config.max_header_bytes
        )
        self.connection.local_settings[SettingCodes.MAX_FRAME_SIZE] = (
            self.config.max_frame_size
        )
        self._events = {
            "request": RequestReceived,
            "data": DataReceived,
            "ended": StreamEnded,
            "reset": StreamReset,
            "trailers": TrailersReceived,
            "window": WindowUpdated,
            "settings": RemoteSettingsChanged,
            "terminated": ConnectionTerminated,
        }
        self._error_codes = ErrorCodes
        self._frames = _FrameBudget(self.config)
        self._inbound: dict[int, _InboundStream] = {}
        self._active_streams: set[int] = set()
        self._outbound: dict[int, _OutboundStream] = {}
        self._commands: list[tuple[str, int, Response | None]] = []
        self._buffered_request_bytes = 0
        self._pending_output_bytes = 0
        self._cancelled_streams: list[int] = []
        self.last_processed_stream_id = 0
        self.remote_closed = False
        self.local_closed = False

    @property
    def active_stream_count(self) -> int:
        return len(self._active_streams)

    @property
    def pending_output_bytes(self) -> int:
        return self._pending_output_bytes

    def initiate(self) -> bytes:
        self.connection.initiate_connection()
        return self.connection.data_to_send()

    def receive_data(self, data: bytes) -> tuple[H2ReadyRequest, ...]:
        self._frames.feed(data)
        events = self.connection.receive_data(data)
        ready: list[H2ReadyRequest] = []
        for event in events:
            if isinstance(event, self._events["request"]):
                self._request_received(event.stream_id, event.headers)
            elif isinstance(event, self._events["data"]):
                self._data_received(
                    event.stream_id, event.data, event.flow_controlled_length
                )
            elif isinstance(event, self._events["ended"]):
                completed = self._stream_ended(event.stream_id)
                if completed is not None:
                    ready.append(completed)
            elif isinstance(event, self._events["reset"]):
                self._cancelled_streams.append(event.stream_id)
                self.drop_stream(event.stream_id)
            elif isinstance(event, self._events["trailers"]):
                self._reset_stream(event.stream_id, self._error_codes.PROTOCOL_ERROR)
            elif isinstance(event, self._events["terminated"]):
                self.remote_closed = True
            elif isinstance(event, (self._events["window"], self._events["settings"])):
                pass
        return tuple(ready)

    def take_cancelled_streams(self) -> tuple[int, ...]:
        """Return peer-reset stream ids exactly once."""
        cancelled, self._cancelled_streams = self._cancelled_streams, []
        return tuple(cancelled)

    def _request_received(self, stream_id: int, raw_headers: Any) -> None:
        if len(self._active_streams) >= self.config.max_concurrent_streams:
            self._reset_stream(stream_id, self._error_codes.REFUSED_STREAM)
            return
        try:
            method, path, headers = self._decode_request_headers(raw_headers)
        except (TypeError, ValueError):
            self._reset_stream(stream_id, self._error_codes.PROTOCOL_ERROR)
            return
        self._active_streams.add(stream_id)
        self._inbound[stream_id] = _InboundStream(method, path, headers, bytearray())

    def _decode_request_headers(self, raw_headers: Any) -> tuple[str, str, Headers]:
        if len(raw_headers) > self.config.max_header_count:
            raise ValueError("too many HTTP/2 request headers")
        decoded_size = 0
        pseudo: dict[str, str] = {}
        regular: list[tuple[str, str]] = []
        cookies: list[str] = []
        seen_regular = False
        for name, value in raw_headers:
            if not isinstance(name, str) or not isinstance(value, str):
                raise TypeError("HTTP/2 headers must decode to text")
            decoded_size += len(name.encode("utf-8")) + len(value.encode("utf-8")) + 32
            if decoded_size > self.config.max_header_bytes:
                raise ValueError("HTTP/2 decoded headers are too large")
            if name.startswith(":"):
                if seen_regular or name in pseudo:
                    raise ValueError("invalid HTTP/2 pseudo-header ordering")
                pseudo[name] = value
                continue
            seen_regular = True
            lowered = name.lower()
            if name != lowered or lowered in self._FORBIDDEN_HEADERS:
                raise ValueError("forbidden HTTP/2 request header")
            if lowered == "te" and value.lower() != "trailers":
                raise ValueError("invalid HTTP/2 TE header")
            if lowered == "cookie":
                cookies.append(value)
            else:
                regular.append((name, value))
        allowed = {":method", ":scheme", ":authority", ":path"}
        if set(pseudo) - allowed:
            raise ValueError("unknown HTTP/2 pseudo-header")
        if pseudo.get(":method") == "CONNECT":
            raise ValueError("HTTP/2 CONNECT is not supported")
        if not all(pseudo.get(name) for name in (":method", ":scheme", ":path")):
            raise ValueError("missing required HTTP/2 pseudo-header")
        authority = pseudo.get(":authority")
        existing_host = any(name == "host" for name, _value in regular)
        if authority and existing_host:
            raise ValueError("HTTP/2 authority and host must not both be supplied")
        if not authority and not existing_host:
            raise ValueError("HTTP/2 requests require :authority or host")
        if authority:
            regular.append(("host", authority))
        if cookies:
            regular.append(("cookie", "; ".join(cookies)))
        return pseudo[":method"], pseudo[":path"], Headers(regular)

    def _data_received(self, stream_id: int, data: bytes, flow_length: int) -> None:
        self.connection.acknowledge_received_data(flow_length, stream_id)
        stream = self._inbound.get(stream_id)
        if stream is None:
            return
        next_stream_size = len(stream.body) + len(data)
        next_connection_size = self._buffered_request_bytes + len(data)
        if (
            next_stream_size > self.config.max_body_bytes
            or next_connection_size > self.config.max_connection_buffer_bytes
        ):
            self._reset_stream(stream_id, self._error_codes.ENHANCE_YOUR_CALM)
            return
        stream.body.extend(data)
        self._buffered_request_bytes = next_connection_size

    def _stream_ended(self, stream_id: int) -> H2ReadyRequest | None:
        stream = self._inbound.pop(stream_id, None)
        if stream is None:
            return None
        self._buffered_request_bytes -= len(stream.body)
        self.last_processed_stream_id = max(self.last_processed_stream_id, stream_id)
        request = Request(
            stream.method,
            stream.path,
            stream.headers,
            bytes(stream.body),
            "HTTP/2",
        )
        return H2ReadyRequest(stream_id, request)

    def queue_response(self, stream_id: int, response: Response) -> bool:
        if stream_id not in self._active_streams:
            return False
        body_size = len(response.body)
        if (
            body_size > self.config.max_response_body_bytes
            or self._pending_output_bytes + body_size
            > self.config.max_pending_output_bytes
        ):
            if not any(command[1] == stream_id for command in self._commands):
                self._commands.append(("reset", stream_id, None))
            return False
        if len(self._commands) >= self.config.max_concurrent_streams * 2:
            self._reset_stream(stream_id, self._error_codes.ENHANCE_YOUR_CALM)
            return False
        self._pending_output_bytes += body_size
        self._commands.append(("response", stream_id, response))
        return True

    def flush(self) -> bytes:
        commands, self._commands = self._commands, []
        for operation, stream_id, response in commands:
            if operation == "reset":
                self._reset_stream(stream_id, self._error_codes.ENHANCE_YOUR_CALM)
                continue
            assert response is not None
            self._start_response(stream_id, response)

        for stream_id, outbound in tuple(self._outbound.items()):
            remaining = len(outbound.body) - outbound.offset
            if remaining <= 0:
                self._outbound.pop(stream_id, None)
                self._active_streams.discard(stream_id)
                continue
            try:
                window = self.connection.local_flow_control_window(stream_id)
            except Exception:
                self.drop_stream(stream_id)
                continue
            chunk_size = min(
                remaining,
                max(0, window),
                self.connection.max_outbound_frame_size,
            )
            if chunk_size <= 0:
                continue
            end_stream = chunk_size == remaining
            chunk = memoryview(outbound.body)[
                outbound.offset : outbound.offset + chunk_size
            ]
            try:
                self.connection.send_data(stream_id, chunk, end_stream=end_stream)
            except Exception:
                self.drop_stream(stream_id)
                continue
            outbound.offset += chunk_size
            self._pending_output_bytes -= chunk_size
            if end_stream:
                self._outbound.pop(stream_id, None)
                self._active_streams.discard(stream_id)
        return self.connection.data_to_send()

    def _start_response(self, stream_id: int, response: Response) -> None:
        headers: list[tuple[str, str]] = [(":status", str(response.status))]
        forbidden = self._FORBIDDEN_HEADERS | {"te"}
        for name, value in response.headers.items():
            lowered = name.lower()
            if lowered in forbidden:
                continue
            headers.append((lowered, value))
        if response.headers.get("content-length") is None:
            headers.append(("content-length", str(len(response.body))))
        header_size = sum(
            len(name.encode("utf-8")) + len(value.encode("utf-8")) + 32
            for name, value in headers
        )
        if header_size > self.config.max_header_bytes:
            self._pending_output_bytes -= len(response.body)
            self._reset_stream(stream_id, self._error_codes.INTERNAL_ERROR)
            return
        try:
            self.connection.send_headers(
                stream_id, headers, end_stream=not response.body
            )
        except Exception:
            self._pending_output_bytes -= len(response.body)
            self.drop_stream(stream_id)
            return
        if response.body:
            self._outbound[stream_id] = _OutboundStream(response.body)
        else:
            self._active_streams.discard(stream_id)

    def drop_stream(self, stream_id: int) -> None:
        inbound = self._inbound.pop(stream_id, None)
        if inbound is not None:
            self._buffered_request_bytes -= len(inbound.body)
        outbound = self._outbound.pop(stream_id, None)
        if outbound is not None:
            self._pending_output_bytes -= len(outbound.body) - outbound.offset
        kept: list[tuple[str, int, Response | None]] = []
        for command in self._commands:
            if command[1] == stream_id and command[2] is not None:
                self._pending_output_bytes -= len(command[2].body)
            else:
                kept.append(command)
        self._commands = kept
        self._active_streams.discard(stream_id)

    def _reset_stream(self, stream_id: int, error_code: Any) -> None:
        try:
            self.connection.reset_stream(stream_id, error_code=error_code)
        except Exception:
            pass
        self.drop_stream(stream_id)

    def close(self, error_code: int = 0) -> bytes:
        if not self.local_closed:
            self.local_closed = True
            try:
                self.connection.close_connection(
                    error_code=error_code,
                    last_stream_id=self.last_processed_stream_id,
                )
            except Exception:
                pass
        self._inbound.clear()
        self._outbound.clear()
        self._commands.clear()
        self._active_streams.clear()
        self._buffered_request_bytes = 0
        self._pending_output_bytes = 0
        return self.connection.data_to_send()
