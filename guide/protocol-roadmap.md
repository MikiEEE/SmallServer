# Protocol and feature roadmap

This guide documents bounded HTTP/1.1, optional cleartext prior-knowledge
HTTP/2, RFC 6455 WebSockets, exact and optional regex routes, shared HTTP
values, explicit SmallOS lifecycle control, and application-owned execution
adapters.

Feature branches extend this foundation independently. Until such a branch is
merged into the branch you install, its API is not available.

## Routing extensions

The optional regex-routing extra provides timeout-bounded full-path matching
and named captured path parameters while preserving exact static-route
precedence. It does not provide automatic path templates or decoding.

## WebSocket server

Optional RFC 6455 server support is available over HTTP/1.1 Upgrade using
SmallOS-native transport ownership and bounded protocol state. See
[WebSockets](websockets.md). TLS, compression, and RFC 8441 WebSockets over
HTTP/2 remain separate concerns.

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
