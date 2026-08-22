"""SmallOS-native HTTP framework primitives."""

from .app import SmallServer
from .adapters import AdapterRegistry, AdapterShutdownError, http_error_from_adapter
from .errors import HTTPError
from .http import Headers, Request, Response
from .routing import RegexRouteConfig, RegexRoutesUnavailable, RouteMatchTimeout, RoutePathTooLarge
from .server import ServerConfig, ServerHandle

__all__ = [
    "AdapterRegistry",
    "AdapterShutdownError",
    "Headers",
    "HTTPError",
    "RegexRouteConfig",
    "RegexRoutesUnavailable",
    "Request",
    "Response",
    "RouteMatchTimeout",
    "RoutePathTooLarge",
    "ServerConfig",
    "ServerHandle",
    "SmallServer",
    "http_error_from_adapter",
]
