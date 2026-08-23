"""SmallOS-native HTTP framework primitives."""

from typing import TYPE_CHECKING, Any

from .app import SmallServer
from .errors import (
    HTTPError,
    ServerConfigurationError,
    ServerFinalizationError,
    ServerStartupError,
)
from .http import Headers, Request, Response
from .http2 import HTTP2Config
from .routing import (
    RegexRouteConfig,
    RegexRoutesUnavailable,
    RouteErrorEvent,
    RouteMatchTimeout,
    RoutePathTooLarge,
)
from .runtime import ManagedRuntimeConfig
from .server import ServerConfig, ServerHandle
from .websocket import (
    WebSocket,
    WebSocketCapacityError,
    WebSocketConfig,
    WebSocketDisconnect,
    WebSocketMessage,
    WebSocketStateError,
    WebSocketUnavailable,
)

if TYPE_CHECKING:
    from .adapters import (
        AdapterCancelledError,
        AdapterCapacityError,
        AdapterClosedError,
        AdapterError,
        AdapterExecutionError,
        AdapterProtocolError,
        AdapterRegistry,
        AdapterShutdownError,
        AdapterUnavailableError,
        AsyncioAdapter,
        ThreadAdapter,
        http_error_from_adapter,
    )


_ADAPTER_EXPORTS = {
    "AdapterCancelledError",
    "AdapterCapacityError",
    "AdapterClosedError",
    "AdapterError",
    "AdapterExecutionError",
    "AdapterProtocolError",
    "AdapterRegistry",
    "AdapterShutdownError",
    "AdapterUnavailableError",
    "AsyncioAdapter",
    "ThreadAdapter",
    "http_error_from_adapter",
}


def __getattr__(name: str) -> Any:
    """Load optional SmallOS adapter integration only when it is requested."""
    if name in _ADAPTER_EXPORTS:
        from . import adapters

        value = getattr(adapters, name)
        globals()[name] = value
        return value
    raise AttributeError("module {!r} has no attribute {!r}".format(__name__, name))

__all__ = [
    "AdapterCancelledError",
    "AdapterCapacityError",
    "AdapterClosedError",
    "AdapterError",
    "AdapterExecutionError",
    "AdapterProtocolError",
    "AdapterRegistry",
    "AdapterShutdownError",
    "AdapterUnavailableError",
    "AsyncioAdapter",
    "Headers",
    "HTTPError",
    "HTTP2Config",
    "ManagedRuntimeConfig",
    "RegexRouteConfig",
    "RegexRoutesUnavailable",
    "Request",
    "Response",
    "RouteErrorEvent",
    "RouteMatchTimeout",
    "RoutePathTooLarge",
    "ServerConfig",
    "ServerConfigurationError",
    "ServerFinalizationError",
    "ServerHandle",
    "ServerStartupError",
    "SmallServer",
    "ThreadAdapter",
    "WebSocket",
    "WebSocketCapacityError",
    "WebSocketConfig",
    "WebSocketDisconnect",
    "WebSocketMessage",
    "WebSocketStateError",
    "WebSocketUnavailable",
    "http_error_from_adapter",
]
