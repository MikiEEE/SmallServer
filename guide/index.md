# SmallServer guide

This guide documents the current SmallServer API: bounded HTTP/1.1, static and
regex routing, WebSockets, managed or caller-owned SmallOS lifecycle, and
execution adapters.

## Learn SmallServer

1. [Getting started](getting-started.md) — install, create an app, and run it.
2. [Routing](routing.md) — exact and bounded regex routes.
3. [WebSockets](websockets.md) — HTTP/1.1 Upgrade, messages, and deadlines.
4. [Requests and responses](requests-and-responses.md) — immutable HTTP values.
5. [Runtime and lifecycle](runtime-lifecycle.md) — managed and caller-owned modes.
6. [Configuration](configuration.md) — finite parser and protocol limits.

## Integrate and operate

- [Third-party adapters](adapters.md)
- [Errors and observability](errors-observability.md)
- [Platforms and kernels](platforms-kernels.md)
- [API reference](api-reference.md)

## Project direction

- [Protocol roadmap](protocol-roadmap.md)
- [Development](development.md)

SmallServer currently supports HTTP/1.1 and RFC 6455 Upgrade only. See the
roadmap for deferred HTTP/2, TLS, compression, and keep-alive work.
