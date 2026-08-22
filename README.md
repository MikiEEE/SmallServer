# SmallServer

SmallServer is a SmallOS-native web framework in early development. It provides
a bounded HTTP/1.1 server and async routing for GET, POST, PUT, PATCH, and
DELETE. Static routes are built in, timeout-bounded regular-expression routes
are available through an optional dependency, and explicit escape hatches
support blocking and asyncio-native libraries.

## Current scope

The current package provides an HTTP/1.1 baseline over a SmallOS runtime. It can:

- register static or optional regular-expression async routes for GET, POST,
  PUT, PATCH, and DELETE;
- dispatch an already-created `Request` to a handler;
- return deterministic `Response` values, including HTTP/1.1 bytes;
- return 404 for an unknown path and 405 with `Allow` for a known path using
  the wrong method.
- bind a non-blocking TCP listener, accept bounded concurrent connections, and
  wait for read/write readiness through SmallOS;
- parse one `Content-Length` HTTP/1.1 request per connection and close after
  its response.

Keep-alive/pipelining, TLS, automatic path templates, and HTTP/2 are not
implemented yet.

## Install for development

```bash
python3 -m pip install -r requirements.txt
python3 -m pip install -e .
python3 -m unittest discover -s tests -v
```

Install the optional matching engine when an application uses raw regex routes:

```bash
python3 -m pip install -e '.[regex-routes]'
```

Static routing neither imports nor requires that dependency.

SmallOS is installed from the canonical `master` branch in `requirements.txt`.
It owns scheduling, socket readiness, and foreign execution adapters.

## Run the demo

The included demo binds an ephemeral loopback TCP port, starts the SmallOS
runtime, and uses a separate loopback client to exercise the real listener.

```bash
python3 -m pip install -r requirements.txt
python3 demo.py
```

It exercises POST, GET, PATCH, PUT, DELETE, and a 404 response. The static
`/tasks` path is intentional: path parameters arrive with a later milestone.

## Bind a server

Create the application, bind it to a SmallOS runtime, then start that runtime.
`port=0` asks the operating system for an available port, which is useful in
tests and local tooling.

```python
from SmallPackage import SmallOS, Unix
from smallserver import Response, SmallServer

runtime = SmallOS().setKernel(Unix())
app = SmallServer()

@app.get("/health")
async def health(request):
    return Response.json({"status": "ok"})

server = app.serve(runtime, host="127.0.0.1", port=8000)
runtime.start()
```

Call `server.close()` from another thread or client-control path to request a
scheduler-safe shutdown. Each current connection accepts one request and sends
a `Connection: close` response.

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

Static route lookup is dictionary-based and always takes precedence over a
regex route for the same method and path. Richer lifecycle hooks are deferred;
the current `ServerHandle` provides explicit shutdown.

## Define regular-expression routes

Regex routes use full-path matching and run in registration order after static
lookup. Only named captures become immutable `request.path_params`; an optional
group that did not participate is omitted.

```python
@app.get_regex(r"/users/(?P<user_id>[0-9]+)")
async def get_user(request: Request) -> Response:
    return Response.json({"user_id": request.path_params["user_id"]})

@app.route_regex(
    r"/articles/(?P<slug>[a-z0-9]+(?:-[a-z0-9]+)*)",
    methods=("GET", "PATCH"),
)
async def article(request: Request) -> Response:
    return Response.json({"slug": request.path_params["slug"]})
```

Patterns must begin with a literal `/` and do not need `^` or `$`. SmallServer
wraps the complete expression in a slash guard, so every top-level alternative
is constrained to an origin-form path. They are trusted application
configuration, but paths are hostile input: SmallServer
bounds pattern length, route count, named captures, path bytes, each match, and
the total matching time. Prefer unambiguous repetition and narrow character
classes even with these deadlines. A timeout raises `RouteMatchTimeout` with an
opaque route ID and becomes a sanitized 500 response on the network path.

Applications can observe that failure without receiving the hostile target:

```python
from smallserver import RouteMatchTimeout

def observe_route_error(error: RouteMatchTimeout) -> None:
    logger.error("route matching failed: %s", error.route_id)

app = SmallServer(route_error_observer=observe_route_error)
```

The synchronous observer runs once on the connection task and should return
quickly; observer failures are isolated from the response path. A path above
the configured regex-routing byte limit returns 414 before matching begins.

Requests retain the exact ASCII origin-form target in `request.raw_target`.
Routing uses `request.path`, which excludes the query string;
`request.query_string` contains the raw text after `?`. Neither paths nor named
captures are percent-decoded, so `/files/a%2Fb` remains distinct from
`/files/a/b`. `request.route_pattern` identifies the selected static path or
regex pattern.

Run `python benchmarks/route_benchmark.py` for a same-process comparison of
the pre-router dictionary dispatch model and current router dispatch. Its JSON
also records configured versus observed hostile-pattern timeout when the extra
is installed; rates are machine-specific and should be compared on the same
host.

Release validation can run `python tests/installed_regex_smoke.py` from an
environment where the built `smallserver[regex-routes]` wheel is installed.
The project requires Python 3.10 or newer.

## Dispatch a request

The listener creates requests and calls `dispatch()`. The same boundary is
useful in application tests:

```python
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

`dispatch()` turns this into a text response with status 413. Unexpected
exceptions are intentionally left visible for the future SmallOS server's
runtime error handling.

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
