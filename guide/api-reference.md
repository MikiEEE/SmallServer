# API reference

This page summarizes the public names exported by `smallserver`. Signatures
omit overload detail where prose is clearer.

## Application

### `SmallServer(regex_config=None, *, route_error_observer=None)`

- `get(path)`, `post(path)`, `put(path)`, `patch(path)`, `delete(path)` — route
  decorators for one supported method.
- `route(path, methods)` — atomic multi-method route decorator.
- `get_regex`, `post_regex`, `put_regex`, `patch_regex`, `delete_regex`, and
  `route_regex(pattern, methods)` — optional timeout-bounded regex decorators.
- `async dispatch(request)` — dispatch an existing `Request`.
- `listen(host="127.0.0.1", port=8000, config=None, *, protocol="http1",`
  `http2_config=None, runtime=None, start=None)`
  — managed blocking lifecycle or caller-owned scheduling/startup.
- `serve(runtime, host="127.0.0.1", port=8000, config=None, *,`
  `protocol="http1", http2_config=None)` — schedule against a caller-owned
  runtime and return immediately.

## HTTP values

### `Headers(values=None)`

Immutable, case-insensitive mapping with `items()` and `get()`.

### `Request(method, path, headers, body=b"", version="HTTP/1.1")`

Frozen request value with validated method, path, headers, and byte body, plus
`raw_target`, `query_string`, immutable `path_params`, and `route_pattern`.

### `Response(status=200, body=b"", headers=Headers())`

Frozen response value. `Response.text()`, `Response.json()`, and `to_http1()`
provide common construction and serialization paths.

## Server lifecycle

### `ServerConfig(...)`

Frozen finite-limit configuration. See [Configuration](configuration.md).

### `HTTP2Config(...)`

Optional cleartext HTTP/2 stream, buffer, frame-batch, and timeout limits. See
[Cleartext HTTP/2](http2.md). Constructing a listener with `protocol="http2"`
requires the `smallserver[http2]` extra.

### `ServerHandle`

Read-only properties: `address`, `port`, `closed`, `failure`, `finished`,
`cleanup_errors`, `owned_connection_count`, `dropped_route_error_events`, and
`route_observer_failures`.

Operations: `close()`, `async close_from_task(task)`, and `finalize()`.

### `RegexRouteConfig(...)`

Finite optional-regex limits. `RouteErrorEvent`, `RouteMatchTimeout`,
`RoutePathTooLarge`, and `RegexRoutesUnavailable` describe its bounded error
surface. Runtime regex matching requires `smallserver[regex-routes]`.

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
