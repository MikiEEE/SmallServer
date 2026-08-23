# API reference

This page summarizes the public names exported by `smallserver`. Signatures
omit overload detail where prose is clearer.

## Application

### `SmallServer(regex_config=None, *, route_error_observer=None, websocket_config=None)`

- `get(path)`, `post(path)`, `put(path)`, `patch(path)`, `delete(path)` — route
  decorators for one supported method.
- `route(path, methods)` — atomic multi-method route decorator.
- `get_regex`, `post_regex`, `put_regex`, `patch_regex`, `delete_regex`, and
  `route_regex` — optional timeout-bounded full-path route decorators.
- `websocket(path, *, origins=None, subprotocols=())` — static WebSocket route.
- `async dispatch(request)` — dispatch an existing `Request`.
- `listen(host="127.0.0.1", port=8000, config=None, *, runtime=None, start=None)`
  — managed blocking lifecycle or caller-owned scheduling/startup.
- `serve(runtime, host="127.0.0.1", port=8000, config=None)` — schedule against
  a caller-owned runtime and return immediately.

## HTTP values

### `Headers(values=None)`

Immutable, case-insensitive mapping with `items()` and `get()`.

### `Request(method, path, headers, body=b"", version="HTTP/1.1", ...)`

Frozen request value with validated method, routed path, headers, byte body,
raw target, query string, immutable path parameters, and route pattern.

### `Response(status=200, body=b"", headers=Headers())`

Frozen response value. `Response.text()`, `Response.json()`, and `to_http1()`
provide common construction and serialization paths.

## Server lifecycle

### `ServerConfig(...)`

Frozen finite-limit configuration. See [Configuration](configuration.md).

### `ServerHandle`

Read-only properties: `address`, `port`, `closed`, `failure`, `finished`,
`cleanup_errors`, and `owned_connection_count`.

Operations: `close()`, `async close_from_task(task)`, and `finalize()`.

## Regex routing

- `RegexRouteConfig` — finite route, pattern, capture, path, and timeout limits.
- `RegexRoutesUnavailable` — the optional matching engine is missing.
- `RouteMatchTimeout` and `RoutePathTooLarge` — bounded matching failures.
- `RouteErrorEvent` — sanitized event sent to the optional observer.

## WebSockets

- `WebSocketConfig` — finite frame, message, mailbox, connection, and deadline
  limits.
- `WebSocket` — `accept`, `reject`, receive/send methods, `ping`, `close`,
  iteration, `request`, and negotiated `subprotocol`.
- `WebSocketMessage` — complete typed text or binary message.
- `WebSocketDisconnect`, `WebSocketStateError`, and `WebSocketCapacityError` —
  application-visible lifecycle and capacity outcomes.
- `WebSocketUnavailable` — the optional `wsproto` engine is missing.

See [WebSockets](websockets.md) for handshake and completion semantics.

## Adapters

### `AdapterRegistry(**adapters)`

Methods: `register`, `get`, `call`, `names`, `items`, and `shutdown`. It also
implements a context manager and exposes `closed`.

### `http_error_from_adapter(exc)`

Convert a SmallOS `AdapterError` to a sanitized `HTTPError`.

### `AdapterShutdownError`

Raised after registry shutdown attempts every adapter but one or more fail.
Its `failures` tuple contains `(name, exception)` entries.

## Errors

- `HTTPError(status, detail="")`
- `ServerConfigurationError`
- `ServerStartupError`
- `ServerFinalizationError`

Cleanup errors expose `cleanup_errors`, `cleanup_complete`, `retry_cleanup()`,
and `finalize()`. `ServerStartupError` additionally exposes `primary_error`.

Public typing information is shipped through `smallserver/py.typed`.
