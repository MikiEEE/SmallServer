# SmallServer

SmallServer is a SmallOS-native web framework for Python 3.10+. It serves
bounded HTTP/1.1 requests, exact and timeout-bounded regex routes, optional
RFC 6455 WebSockets, explicit runtime lifecycle control, and third-party
execution adapters.

```python
from smallserver import Response, SmallServer

app = SmallServer()


@app.get("/health")
async def health(request):
    return Response.json({"status": "ok"})


if __name__ == "__main__":
    app.listen(host="127.0.0.1", port=8000)
```

Install the canonical SmallOS master dependency and this package, then run the
demo:

```console
python3 -m pip install -r requirements.txt
python3 -m pip install -e .
python3 demo.py
```

Application code can use blocking `app.listen()` without importing SmallOS.
Advanced applications can supply their own runtime, schedule without starting
it, and own adapters for blocking or asyncio-native libraries.

## Optional features

Static routing and HTTP-only imports need neither optional protocol package.
Install only the feature an application serves:

```console
python3 -m pip install -e '.[regex-routes]'
python3 -m pip install -e '.[websocket]'
```

Regex routes use bounded full-path matching after exact static lookup.
WebSocket routes use a separate static route table, so an ordinary `GET` and a
WebSocket Upgrade may coexist at one path.

```python
from smallserver import WebSocket


@app.websocket("/echo", origins={"https://app.example.com"})
async def echo(socket: WebSocket) -> None:
    await socket.accept()
    async for message in socket:
        if message.is_text:
            await socket.send_text(message.text)
        else:
            await socket.send_bytes(message.bytes)
```

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
`task_capacity` must reserve at least `max_connections + 2` task slots for the
listener and shutdown-control tasks, and both server task priorities must be
below `priority_levels`.
Configuring a regex route-error observer adds one dedicated SmallOS task, so
that mode requires at least `max_connections + 3` slots.

Managed `listen()` blocks and catches Ctrl-C after closing its listener, wakeup
channel, connections, and server tasks. It returns the closed `ServerHandle`,
whose cached `address` and `port` remain available for diagnostics. Each
current connection accepts one request and sends a `Connection: close`
response.

## Documentation

- [Guide index](guide/index.md)
- [Getting started](guide/getting-started.md)
- [Routing](guide/routing.md)
- [WebSockets](guide/websockets.md)
- [Requests and responses](guide/requests-and-responses.md)
- [Runtime and lifecycle](guide/runtime-lifecycle.md)
- [Configuration](guide/configuration.md)
- [Third-party adapters](guide/adapters.md)
- [Errors and observability](guide/errors-observability.md)
- [Platforms and kernels](guide/platforms-kernels.md)
- [API reference](guide/api-reference.md)
- [Protocol roadmap](guide/protocol-roadmap.md)
- [Development](guide/development.md)

See [`demo.py`](demo.py) for all five HTTP methods and a WebSocket route,
[`examples/websocket_echo.py`](examples/websocket_echo.py) for a bounded echo
server, [`examples/manual_runtime.py`](examples/manual_runtime.py) for
caller-owned SmallOS startup, and
[`examples/adapters_demo.py`](examples/adapters_demo.py) for blocking and
asyncio escape hatches.

```python
from SmallPackage import SmallOS, Unix
from smallserver import Response, SmallServer

runtime = SmallOS().setKernel(Unix())
app = SmallServer()

@app.get("/health")
async def health(request):
    return Response.json({"status": "ok"})

server = app.listen(runtime=runtime, start=False)
try:
    runtime.start()
finally:
    server.finalize()
```

On a kernel with `supports_wakeup_channel() == True`, call `server.close()`
from another thread or client-control path to request scheduler-safe shutdown.
`Unix` provides this cross-thread wakeup capability.

Constrained kernels may support TCP servers without supporting a thread-safe
wakeup channel. On those kernels, `server.close()` raises instead of mutating
runtime state from an unsafe context. A currently running SmallOS task can use
`await server.close_from_task(task)` to close on the scheduler thread. The
handle's `finished` property becomes true only after the listener, every
connection, and the wakeup channel have closed successfully; `cleanup_errors`
reports close failures that remain available for a later scheduler-side retry.

If startup fails and the kernel also fails to release an acquired listener or
wakeup resource, `serve()` raises `ServerStartupError`. Its `primary_error`
preserves the startup failure and `cleanup_errors` reports the outstanding
cleanup attempts without exposing kernel handles. Keep the exception and call
`retry_cleanup()` (or `finalize()`) until it returns `True`; later calls remain
safe and return `True`. `KeyboardInterrupt` and `SystemExit` are always
re-raised as the identical exception; when rollback is incomplete, their
`__cause__` is the `ServerStartupError` cleanup owner. Abandoning an incomplete
startup error performs one best-effort cleanup retry and emits a
`ResourceWarning` if resources remain owned.

`max_connections` bounds every connection stream still owned by the server,
including streams retained after a failed close. At capacity the listener
blocks on a SmallOS scheduler signal without polling or accepting another
connection; releasing capacity signals the listener. Any connection close
failure is fatal and stops further acceptance while retaining the stream for
an explicit shutdown-cleanup retry.

Each current connection accepts one request and sends a `Connection: close`
response.

`app.serve(runtime, ...)` remains the equivalent schedule-and-return
compatibility API. `listen(runtime=runtime, start=True)` starts the supplied
runtime exactly once and finalizes only server-owned resources when it exits;
the runtime itself still belongs to the caller.

While the scheduler is running on a kernel with a wakeup channel,
`server.close()` is the thread-safe shutdown signal. Kernels without that
capability must call `await server.close_from_task(task)` from their currently
running SmallOS task. After a manually started scheduler has already exited or
failed, `server.finalize()` is the idempotent owner-thread cleanup operation on
either kind of kernel.

Execution adapters are likewise application-owned. Construct and close them
around the runtime lifecycle rather than expecting managed `listen()` to
create or stop adapter threads or asyncio loops. See
[`examples/manual_runtime.py`](examples/manual_runtime.py) for the complete
manual shape.

Only one listener invocation can be active on an application at a time. Once
its handle reports `finished`, all retained cleanup has completed and the same
application can listen again. A failed cleanup attempt keeps the invocation
reserved until a later successful retry.

## Define routes

Use one decorator for each supported method. Handlers receive an immutable
`Request` and must return a `Response`.

```python
from smallserver import Request, Response, SmallServer

app = SmallServer()

@app.get("/health")
async def health(request: Request) -> Response:
    return Response.json({"status": "ok"})

@app.post("/widgets")
async def create_widget(request: Request) -> Response:
    # request.body is always bytes.
    return Response.json({"created": True}, status=201)

@app.put("/widgets")
async def replace_widgets(request: Request) -> Response:
    return Response.text("replaced")

@app.patch("/widgets")
async def patch_widgets(request: Request) -> Response:
    return Response.text("updated")

@app.delete("/widgets")
async def delete_widgets(request: Request) -> Response:
    return Response(status=204)
```

Exact static routes are dependency-free. Optional regex routes expose bounded
named captures; automatic path-template syntax is not implemented.

## Dispatch a request

The listener creates requests and calls `dispatch()`. The same boundary is
useful in application tests:

```python
async def test_health() -> None:
    request = Request(
        method="GET",
        path="/health",
        headers={"Accept": "application/json"},
    )

    response = await app.dispatch(request)
    assert response.status == 200
    assert response.body == b'{"status":"ok"}'
    assert response.headers["content-type"] == "application/json"
```

For a path that is registered but does not accept the request method,
`dispatch()` returns a 405 response and an `Allow` header. An unknown path
returns 404.

## Build responses

`Response.text()` encodes UTF-8 text and supplies a text content type.
`Response.json()` emits compact UTF-8 JSON. The response serializer adds an
accurate `Content-Length` header when one was not supplied.

```python
response = Response.text("hello", headers={"X-Request-ID": "abc123"})
wire_bytes = response.to_http1()

# b"HTTP/1.1 200 OK\\r\\nContent-Length: 5..."
```

Header names and values are validated: duplicate names (case-insensitively),
forbidden control characters, and values outside the HTTP/1.1 Latin-1 wire
range are rejected.

## Expected application errors

Raise `HTTPError` inside a handler when an expected client-facing response is
clearer than constructing it inline:

```python
from smallserver import HTTPError

@app.delete("/widgets")
async def delete_widget(request: Request) -> Response:
    raise HTTPError(413, "request is too large")
```

`dispatch()` turns this into a text response with status 413. Direct dispatch
leaves unexpected exceptions visible; network listeners return a sanitized
500 response.

## Third-party blocking and asyncio libraries

SmallServer delegates foreign execution to SmallOS's bounded adapters. The
application creates those adapters explicitly and can group them in an
`AdapterRegistry` for naming and deterministic shutdown:

```python
from SmallPackage.adapters.asyncio_loop import AsyncioAdapter
from SmallPackage.adapters.threads import ThreadAdapter
from smallserver import AdapterRegistry, Response

async def fetch_records(rows):
    # Construct loop-affine clients inside the adapter-owned event loop.
    async with make_async_client() as client:
        return await client.fetch(rows)

with AdapterRegistry(
    database=ThreadAdapter(max_workers=1, max_pending=8),
    async_sdk=AsyncioAdapter(max_pending=32),
) as services:

    @app.get("/records")
    async def records(request):
        rows = await services.call("database", repository.list_records)
        result = await services.call("async_sdk", fetch_records, rows)
        return Response.json(result)

    server = app.serve(runtime, host="127.0.0.1", port=8000)
    runtime.start()
```

Use one thread worker for thread-affine resources such as a single SQLite
connection. `AsyncioAdapter` owns one persistent event loop and must receive an
async callable, not a task or future created on another loop.

Adapter errors remain visible to handlers. `http_error_from_adapter()` is an
opt-in, detail-sanitizing translation: capacity/unavailable failures become
503 and protocol/execution failures become 500.

Run the standard-library adapter example with:

```bash
python3 examples/adapters_demo.py
```

## Development relationship

SmallOS's canonical upstream is [MikiEEE/SmallOS](https://github.com/MikiEEE/SmallOS). During initial development, install the canonical `master` branch:

```bash
python3 -m pip install -r requirements.txt
```

SmallOS's normalized distribution name is currently unavailable for public package installation; SmallServer must not claim a PyPI dependency until that is resolved.

## Releases

Release pull requests merge from `develop` into `main` with a new
`project.version`. Successful CI on that exact `main` commit creates a tagged
GitHub release containing checked wheel and source archives. See
[`RELEASING.md`](RELEASING.md) for the complete process and the current reason
PyPI publication remains disabled.
