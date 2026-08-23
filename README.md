# SmallServer

SmallServer is a small, SmallOS-native HTTP framework for Python 3.10+. It
serves bounded HTTP/1.1 requests and optional cleartext prior-knowledge HTTP/2,
supports exact and timeout-bounded regex routes, and provides explicit
lifecycle and third-party execution controls.

```python
from smallserver import Response, SmallServer

app = SmallServer()


@app.get("/health")
async def health(request):
    return Response.json({"status": "ok"})


if __name__ == "__main__":
    app.listen(host="127.0.0.1", port=8000)
```

Install the canonical SmallOS master dependency, the package, and test tools:

```console
python3 -m pip install -r requirements.txt
python3 -m pip install -e '.[test]'
python3 demo.py
```

Application code can use blocking `app.listen()` without importing SmallOS.
Advanced applications can supply their own runtime, schedule the server without
starting it, and own execution adapters for blocking or asyncio-native
libraries.

## Optional protocol and routing extras

HTTP/2 uses the bounded hyper-h2 4.x integration:

```console
python3 -m pip install -e '.[http2]'
```

Select `protocol="http2"` on `listen()` or `serve()`. The implementation
supports cleartext prior knowledge, multiplexed stream handlers, bounded flow
control, and GOAWAY. HTTP/2 TLS/ALPN remains deferred until SmallOS exposes a
server-side TLS kernel capability.

Timeout-bounded regex routes require the optional matching engine:

```console
python3 -m pip install -e '.[regex-routes]'
```

Static lookup remains dependency-free and takes precedence over regex routes.
Regex routes use full-path matching, run in registration order, and expose only
named captures through immutable `request.path_params`:

```python
@app.get_regex(r"/users/(?P<user_id>[0-9]+)")
async def get_user(request):
    return Response.json({"user_id": request.path_params["user_id"]})
```

Pattern, path, capture, and matching-time limits are configurable with
`RegexRouteConfig`. On HTTP/1.1 and HTTP/2 listeners, a match timeout becomes a
sanitized 500 response; direct `dispatch()` raises `RouteMatchTimeout`. An
optional network-listener `route_error_observer` receives only an immutable `RouteErrorEvent`
with an opaque route ID and category; it never receives the request target,
headers, body, traceback, or exception graph.

### Configure the managed SmallOS runtime

When `listen()` creates the runtime, `ServerConfig.managed_runtime` passes the
relevant scheduler and client defaults into SmallOS before the listener binds:

```python
from smallserver import ManagedRuntimeConfig, ServerConfig

config = ServerConfig(
    max_connections=200,
    managed_runtime=ManagedRuntimeConfig(
        task_capacity=512,
        priority_levels=8,
        io_buffer_length=2048,
        eternal_watchers=False,
        client_defaults={
            "http": {"max_response_size": 8 * 1024 * 1024},
        },
    ),
)

app.listen(host="127.0.0.1", port=8000, config=config)
```

This bridge is only for SmallServer-owned runtimes. If you supply `runtime=`,
configure it directly with `SmallOS(config=...)`; SmallServer rejects
`managed_runtime` rather than mutating caller-owned scheduler state.
For HTTP/1.1, `task_capacity` must reserve at least `max_connections + 2` task
slots for the listener and shutdown-control tasks, and both server task
priorities must be below `priority_levels`. Configuring a regex route-error
observer adds one dedicated SmallOS task, so that mode requires at least
`max_connections + 3` slots. HTTP/2 needs additional headroom for its bounded
connection-control and stream-handler tasks.

## Current boundaries

HTTP/1.1 serves one request per connection. Keep-alive, pipelining, TLS,
automatic path templates, WebSockets, HTTP/1.1 h2c upgrade, and automatic
protocol detection are not implemented. Regex routes are an explicit optional
route form, not automatic path templates.

## Documentation

- [Guide index](guide/index.md)
- [Getting started](guide/getting-started.md)
- [Routing](guide/routing.md)
- [Requests and responses](guide/requests-and-responses.md)
- [Runtime and lifecycle](guide/runtime-lifecycle.md)
- [Configuration](guide/configuration.md)
- [Cleartext HTTP/2](guide/http2.md)
- [Third-party adapters](guide/adapters.md)
- [Errors and observability](guide/errors-observability.md)
- [Platforms and kernels](guide/platforms-kernels.md)
- [API reference](guide/api-reference.md)
- [Protocol roadmap](guide/protocol-roadmap.md)
- [Development](guide/development.md)

See [`demo.py`](demo.py) for all five supported HTTP methods,
[`examples/http2_prior_knowledge.py`](examples/http2_prior_knowledge.py) for
HTTP/2, [`examples/manual_runtime.py`](examples/manual_runtime.py) for
caller-owned SmallOS startup, and
[`examples/adapters_demo.py`](examples/adapters_demo.py) for blocking and
asyncio escape hatches.

## Releases

Release pull requests merge from `develop` into `main` with a new
`project.version`. Successful CI on that exact `main` commit creates a tagged
GitHub release containing checked wheel and source archives. See
[`RELEASING.md`](RELEASING.md) for the complete process and the current reason
PyPI publication remains disabled.

SmallServer is early-stage software. Review the documented limits and lifecycle
contract before deploying it outside controlled environments.
