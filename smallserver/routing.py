"""Deterministic static and timeout-bounded regular-expression routing."""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Iterable, Mapping
from dataclasses import dataclass
import importlib
import math
import time
from types import MappingProxyType
from typing import Any

from .http import Request, Response

Handler = Callable[[Request], Awaitable[Response]]
SUPPORTED_METHODS = frozenset({"GET", "POST", "PUT", "PATCH", "DELETE"})


class RegexRoutesUnavailable(RuntimeError):
    """Raised when regex routes are used without their optional dependency."""


class RouteMatchTimeout(RuntimeError):
    """Raised when a bounded regex route match exceeds its deadline."""

    def __init__(self, route_id: str) -> None:
        self.route_id = route_id
        super().__init__("regular-expression route matching timed out ({})".format(route_id))


@dataclass(frozen=True)
class RegexRouteConfig:
    """Finite limits applied to regex registration and hostile request paths."""

    max_path_bytes: int = 8 * 1024
    max_pattern_length: int = 1024
    max_routes: int = 100
    match_timeout: float = 0.01
    total_match_timeout: float = 0.05
    max_named_captures: int = 20

    def __post_init__(self) -> None:
        for name in ("max_path_bytes", "max_pattern_length", "max_routes", "max_named_captures"):
            value = getattr(self, name)
            if type(value) is not int or value <= 0:
                raise ValueError("{} must be a positive integer".format(name))
        for name in ("match_timeout", "total_match_timeout"):
            value = getattr(self, name)
            if type(value) not in (int, float) or not math.isfinite(value) or value <= 0:
                raise ValueError("{} must be a finite positive number".format(name))


@dataclass(frozen=True)
class RouteMatch:
    handler: Handler | None
    path_params: Mapping[str, str]
    route_pattern: str | None
    allowed_methods: tuple[str, ...] = ()


@dataclass(frozen=True)
class _RegexRoute:
    route_id: str
    pattern: str
    compiled: Any
    handlers: Mapping[str, Handler]
    literal_prefix: str


class Router:
    """Resolve static and ordered regex routes without slowing static lookup."""

    def __init__(self, regex_config: RegexRouteConfig | None = None) -> None:
        self._static: dict[tuple[str, str], Handler] = {}
        self._regex: list[_RegexRoute] = []
        self._regex_by_pattern: dict[str, int] = {}
        self.regex_config = regex_config or RegexRouteConfig()

    @staticmethod
    def normalize_methods(methods: Iterable[str]) -> tuple[str, ...]:
        try:
            normalized = tuple(dict.fromkeys(method.upper() for method in methods))
        except AttributeError as exc:
            raise ValueError("routes must use one or more supported HTTP methods") from exc
        if not normalized or any(method not in SUPPORTED_METHODS for method in normalized):
            raise ValueError("routes must use one or more supported HTTP methods")
        return normalized

    def add_static(self, path: str, methods: tuple[str, ...], handler: Handler) -> None:
        keys = [(method, path) for method in methods]
        for key in keys:
            if key in self._static:
                raise ValueError("route already registered: {} {}".format(key[0], path))
        for key in keys:
            self._static[key] = handler

    def add_regex(self, pattern: str, methods: tuple[str, ...], handler: Handler) -> None:
        if not isinstance(pattern, str):
            raise TypeError("regex route pattern must be a string")
        existing_index = self._regex_by_pattern.get(pattern)
        if existing_index is not None:
            existing = self._regex[existing_index]
            duplicate = next((method for method in methods if method in existing.handlers), None)
            if duplicate is not None:
                raise ValueError("regex route already registered: {}".format(duplicate))
            handlers = dict(existing.handlers)
            handlers.update((method, handler) for method in methods)
            self._regex[existing_index] = _RegexRoute(
                existing.route_id,
                existing.pattern,
                existing.compiled,
                MappingProxyType(handlers),
                existing.literal_prefix,
            )
            return

        if len(self._regex) >= self.regex_config.max_routes:
            raise ValueError("maximum registered regex routes exceeded")
        compiled = self._compile(pattern)
        route = _RegexRoute(
            "regex-route-{}".format(len(self._regex) + 1),
            pattern,
            compiled,
            MappingProxyType({method: handler for method in methods}),
            _literal_prefix(pattern),
        )
        self._regex_by_pattern[pattern] = len(self._regex)
        self._regex.append(route)

    def resolve(self, method: str, path: str) -> RouteMatch:
        method = method.upper()
        static = self._static.get((method, path))
        if static is not None:
            return RouteMatch(static, MappingProxyType({}), path)

        if not self._regex:
            allowed = tuple(sorted(method for method, registered_path in self._static if registered_path == path))
            return RouteMatch(None, MappingProxyType({}), None, allowed)
        self._validate_path(path)
        deadline = time.monotonic() + self.regex_config.total_match_timeout
        cached: dict[int, Any] = {}
        for index, route in enumerate(self._regex):
            if method not in route.handlers:
                continue
            match = self._match(route, path, deadline)
            cached[index] = match
            if match is not None:
                return RouteMatch(
                    route.handlers[method],
                    MappingProxyType(_captures(match)),
                    route.pattern,
                )

        allowed = {registered_method for registered_method, registered_path in self._static if registered_path == path}
        for index, route in enumerate(self._regex):
            match = cached.get(index)
            if index not in cached:
                match = self._match(route, path, deadline)
            if match is not None:
                allowed.update(route.handlers)
        return RouteMatch(None, MappingProxyType({}), None, tuple(sorted(allowed)))

    def _compile(self, pattern: str) -> Any:
        if not isinstance(pattern, str):
            raise TypeError("regex route pattern must be a string")
        if not pattern.startswith("/"):
            raise ValueError("regex route pattern must start with a literal '/'")
        if len(pattern) > self.regex_config.max_pattern_length:
            raise ValueError("regex route pattern is too long")
        names = _named_group_names(pattern)
        if len(names) != len(set(names)):
            raise ValueError("regex route pattern contains duplicate named groups")
        if len(names) > self.regex_config.max_named_captures:
            raise ValueError("regex route pattern has too many named captures")
        try:
            engine = importlib.import_module("regex")
        except ImportError as exc:
            raise RegexRoutesUnavailable(
                "regular-expression routes require 'smallserver[regex-routes]'"
            ) from exc
        try:
            compiled = engine.compile(pattern)
        except Exception:
            raise ValueError("invalid regex route pattern") from None
        try:
            empty_match = compiled.fullmatch("", timeout=self.regex_config.match_timeout)
        except TimeoutError as exc:
            raise ValueError("regex route pattern validation timed out") from exc
        if empty_match is not None:
            raise ValueError("regex route pattern must not match an empty path")
        return compiled

    def _validate_path(self, path: str) -> None:
        try:
            size = len(path.encode("ascii"))
        except UnicodeEncodeError as exc:
            raise ValueError("request path must contain ASCII characters only") from exc
        if size > self.regex_config.max_path_bytes:
            raise ValueError("request path is too large for routing")

    def _match(self, route: _RegexRoute, path: str, deadline: float) -> Any:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise RouteMatchTimeout(route.route_id)
        if len(route.literal_prefix) > 1 and not path.startswith(route.literal_prefix):
            return None
        timeout = min(float(self.regex_config.match_timeout), remaining)
        try:
            return route.compiled.fullmatch(path, timeout=timeout)
        except TimeoutError as exc:
            raise RouteMatchTimeout(route.route_id) from exc


def _captures(match: Any) -> dict[str, str]:
    return {name: value for name, value in match.groupdict().items() if value is not None}


def _literal_prefix(pattern: str) -> str:
    """Return only the leading literals that are safe to use as a rejection index."""
    special = frozenset(".[](){}*+?|^$\\")
    end = 0
    while end < len(pattern) and pattern[end] not in special:
        end += 1
    return pattern[:end]


def _named_group_names(pattern: str) -> list[str]:
    """Find named-group declarations while ignoring escapes and character classes."""
    names: list[str] = []
    escaped = False
    in_class = False
    index = 0
    while index < len(pattern):
        character = pattern[index]
        if escaped:
            escaped = False
            index += 1
            continue
        if character == "\\":
            escaped = True
            index += 1
            continue
        if character == "[":
            in_class = True
            index += 1
            continue
        if character == "]" and in_class:
            in_class = False
            index += 1
            continue
        marker_length = 0
        if not in_class and pattern.startswith("(?P<", index):
            marker_length = 4
        elif not in_class and pattern.startswith("(?<", index):
            next_character = pattern[index + 3 : index + 4]
            if next_character not in ("=", "!"):
                marker_length = 3
        if marker_length:
            end = pattern.find(">", index + marker_length)
            if end >= 0:
                names.append(pattern[index + marker_length : end])
                index = end + 1
                continue
        index += 1
    return names
