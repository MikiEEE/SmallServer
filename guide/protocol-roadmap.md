# Protocol and feature roadmap

This guide documents bounded HTTP/1.1, optional cleartext prior-knowledge
HTTP/2, exact static routes, shared HTTP values, explicit SmallOS lifecycle
control, and application-owned execution adapters.

Feature branches extend this foundation independently. Until such a branch is
merged into the branch you install, its API is not available.

## Routing extensions

The regex-routing feature introduces an explicit timeout-bounded regex route
form and captured path parameters while preserving exact static-route
precedence. It is not part of this base. Base applications should continue to
register literal paths and should not assume automatic query parsing.

## WebSocket server

The WebSocket feature is planned as optional RFC 6455 server support over an
HTTP/1.1 Upgrade, using SmallOS-native transport ownership and bounded
protocol state. TLS, compression, and RFC 8441 WebSockets over HTTP/2 remain
separate concerns. No WebSocket API is exported by this base.

## HTTP/2 server

HTTP/2 is available as an optional cleartext prior-knowledge server using the
hyper-h2 4.x sans-I/O stack. See [Cleartext HTTP/2](http2.md) for dependency
installation, stream concurrency, flow control, protocol limits, GOAWAY, and
graceful shutdown. TLS/ALPN remains deferred until SmallOS exposes a
server-side TLS kernel capability; h2c upgrade and protocol autodetection are
not supported.

## Existing HTTP/1.1 limits

Keep-alive, pipelining, TLS termination, automatic protocol detection, h2c
upgrade, middleware/ASGI compatibility, and automatic request-data decoding are
not implemented here. Treat future items as direction, not a compatibility promise.
