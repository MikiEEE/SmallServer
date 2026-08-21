"""Deterministic HTTP request and response value objects."""

from __future__ import annotations

from collections.abc import Iterable, Iterator, Mapping
from dataclasses import dataclass
import json
import re
from types import MappingProxyType
from typing import Any

_TOKEN = re.compile(r"^[!#$%&'*+.^_`|~0-9A-Za-z-]+$")
_REASONS = {200: "OK", 201: "Created", 204: "No Content", 400: "Bad Request", 404: "Not Found", 405: "Method Not Allowed", 413: "Payload Too Large", 500: "Internal Server Error", 503: "Service Unavailable"}


class Headers(Mapping[str, str]):
    """Immutable, case-insensitive HTTP headers with validated values."""

    def __init__(self, values: Mapping[str, str] | Iterable[tuple[str, str]] | None = None) -> None:
        items = values.items() if isinstance(values, Mapping) else (values or ())
        normalized: dict[str, tuple[str, str]] = {}
        for name, value in items:
            if not isinstance(name, str) or not _TOKEN.fullmatch(name):
                raise ValueError("invalid HTTP header name")
            if not isinstance(value, str) or any(
                character != "\t"
                and (ord(character) < 0x20 or ord(character) == 0x7F or ord(character) > 0xFF)
                for character in value
            ):
                raise ValueError("invalid HTTP header value")
            key = name.lower()
            if key in normalized:
                raise ValueError("duplicate HTTP header: {}".format(name))
            normalized[key] = (name, value)
        self._values = MappingProxyType(normalized)

    def __getitem__(self, name: str) -> str:
        return self._values[name.lower()][1]

    def __iter__(self) -> Iterator[str]:
        return (entry[0] for entry in self._values.values())

    def __len__(self) -> int:
        return len(self._values)

    def items(self) -> Iterator[tuple[str, str]]:  # type: ignore[override]
        return iter(self._values.values())

    def get(self, name: str, default: str | None = None) -> str | None:
        entry = self._values.get(name.lower())
        return default if entry is None else entry[1]


@dataclass(frozen=True)
class Request:
    """An immutable request passed to a route handler."""

    method: str
    path: str
    headers: Headers
    body: bytes = b""
    version: str = "HTTP/1.1"

    def __post_init__(self) -> None:
        if not _TOKEN.fullmatch(self.method):
            raise ValueError("invalid HTTP method")
        if not self.path.startswith("/"):
            raise ValueError("request path must start with '/'")
        if not isinstance(self.headers, Headers):
            object.__setattr__(self, "headers", Headers(self.headers))
        if not isinstance(self.body, bytes):
            raise TypeError("request body must be bytes")


@dataclass(frozen=True)
class Response:
    """An immutable HTTP response with deterministic HTTP/1.1 serialization."""

    status: int = 200
    body: bytes = b""
    headers: Headers = Headers()

    def __post_init__(self) -> None:
        if not isinstance(self.status, int) or not 100 <= self.status <= 599:
            raise ValueError("response status must be between 100 and 599")
        if not isinstance(self.body, bytes):
            raise TypeError("response body must be bytes")
        if not isinstance(self.headers, Headers):
            object.__setattr__(self, "headers", Headers(self.headers))
        content_length = self.headers.get("content-length")
        if content_length is not None and content_length != str(len(self.body)):
            raise ValueError("content-length does not match response body")

    @classmethod
    def text(cls, value: str, status: int = 200, headers: Mapping[str, str] | None = None) -> Response:
        combined = dict(headers or {})
        combined.setdefault("Content-Type", "text/plain; charset=utf-8")
        return cls(status=status, body=value.encode("utf-8"), headers=Headers(combined))

    @classmethod
    def json(cls, value: Any, status: int = 200, headers: Mapping[str, str] | None = None) -> Response:
        combined = dict(headers or {})
        combined.setdefault("Content-Type", "application/json")
        body = json.dumps(value, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
        return cls(status=status, body=body, headers=Headers(combined))

    def to_http1(self) -> bytes:
        reason = _REASONS.get(self.status, "")
        lines = ["HTTP/1.1 {} {}".format(self.status, reason).rstrip()]
        if self.headers.get("content-length") is None:
            lines.append("Content-Length: {}".format(len(self.body)))
        lines.extend("{}: {}".format(name, value) for name, value in self.headers.items())
        return ("\r\n".join(lines) + "\r\n\r\n").encode("latin-1") + self.body
