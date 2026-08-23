# Protocol and feature roadmap

The current branch provides bounded HTTP/1.1, static and regex routes, shared
HTTP values, RFC 6455 Upgrade, explicit SmallOS lifecycle control, and
application-owned execution adapters.

## Routing extensions

Timeout-bounded regex routes and immutable named captures are implemented as
the optional `regex-routes` extra. Exact static routes retain precedence.

## WebSocket server

Optional RFC 6455 server support over HTTP/1.1 Upgrade is implemented through
the `websocket` extra with SmallOS-native transport ownership and bounded
protocol state. TLS, compression, custom extensions, and RFC 8441 WebSockets
over HTTP/2 remain separate concerns.

## HTTP/2 server

The HTTP/2 feature is planned as an optional cleartext prior-knowledge server
using the hyper-h2 4.x sans-I/O stack. Its branch is responsible for documenting
dependency installation, stream concurrency, flow control, protocol limits,
GOAWAY, and graceful shutdown. SmallServer does not yet accept HTTP/2
connections or export an HTTP/2 configuration type.

## Existing HTTP/1.1 limits

Keep-alive, pipelining, TLS termination, automatic protocol detection, h2c
upgrade, middleware/ASGI compatibility, and automatic request-data decoding are
not implemented here. Treat this page as direction, not a compatibility promise.
