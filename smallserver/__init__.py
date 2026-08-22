"""SmallOS-native HTTP framework primitives."""

from typing import TYPE_CHECKING, Any

from .app import SmallServer
from .errors import HTTPError, ServerConfigurationError, ServerStartupError
from .http import Headers, Request, Response
from .server import ServerConfig, ServerHandle

if TYPE_CHECKING:
    from .adapters import AdapterRegistry, AdapterShutdownError, http_error_from_adapter


def __getattr__(name: str) -> Any:
    """Load optional SmallOS adapter integration only when it is requested."""
    if name in {"AdapterRegistry", "AdapterShutdownError", "http_error_from_adapter"}:
        from . import adapters

        value = getattr(adapters, name)
        globals()[name] = value
        return value
    raise AttributeError("module {!r} has no attribute {!r}".format(__name__, name))

__all__ = [
    "AdapterRegistry",
    "AdapterShutdownError",
    "Headers",
    "HTTPError",
    "Request",
    "Response",
    "ServerConfig",
    "ServerConfigurationError",
    "ServerHandle",
    "ServerStartupError",
    "SmallServer",
    "http_error_from_adapter",
]
