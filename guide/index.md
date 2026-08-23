# SmallServer guide

This guide documents the integrated lifecycle, regex routing, WebSockets, and
HTTP/2 feature set. Start with the managed server path, then open the focused
page for the part you are changing.

## Learn SmallServer

1. [Getting started](getting-started.md) — install, create an app, and run it.
2. [Routing](routing.md) — exact and optional regex paths, captures, methods, 404, and 405 behavior.
3. [WebSockets](websockets.md) — HTTP/1.1 Upgrade, messages, and deadlines.
4. [Requests and responses](requests-and-responses.md) — immutable HTTP values.
5. [Runtime and lifecycle](runtime-lifecycle.md) — managed and caller-owned modes.
6. [Configuration](configuration.md) — finite parser and connection limits.
7. [Cleartext HTTP/2](http2.md) — optional prior-knowledge multiplexing and limits.

## Integrate and operate

- [Third-party adapters](adapters.md)
- [Errors and observability](errors-observability.md)
- [Platforms and kernels](platforms-kernels.md)
- [API reference](api-reference.md)

## Project direction

- [Protocol roadmap](protocol-roadmap.md)
- [Development](development.md)

This branch supports bounded HTTP/1.1, optional timeout-bounded regex routing,
RFC 6455 WebSocket Upgrade, and optional cleartext prior-knowledge HTTP/2.
TLS/ALPN, h2c upgrade, and RFC 8441 remain outside the current boundary.
