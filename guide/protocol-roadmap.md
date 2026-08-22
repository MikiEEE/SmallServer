# Protocol and feature roadmap

This guide base documents the exact API at the server-lifecycle milestone:
bounded HTTP/1.1, exact static routes, shared HTTP values, explicit SmallOS
lifecycle control, and application-owned execution adapters.

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

The HTTP/2 feature is planned as an optional cleartext prior-knowledge server
using the hyper-h2 4.x sans-I/O stack. Its branch is responsible for documenting
dependency installation, stream concurrency, flow control, protocol limits,
GOAWAY, and graceful shutdown. This base does not accept HTTP/2 connections and
does not export an HTTP/2 configuration type.

## Existing HTTP/1.1 limits

Keep-alive, pipelining, TLS termination, automatic protocol detection, h2c
upgrade, middleware/ASGI compatibility, and automatic request-data decoding are
not implemented here. Treat this page as direction, not a compatibility promise.
