# SmallServer

SmallServer is a small, SmallOS-native HTTP framework for Python 3.10+. The
current base serves bounded HTTP/1.1 requests, routes exact paths to async
handlers, and provides explicit lifecycle and third-party execution controls.

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
example:

```console
python3 -m pip install -r requirements.txt
python3 -m pip install -e .
python3 demo.py
```

Application code can use blocking `app.listen()` without importing SmallOS.
Advanced applications can supply their own runtime, schedule the server without
starting it, and own execution adapters for blocking or asyncio-native
libraries.

Current boundaries are intentional: one request is served per HTTP/1.1
connection; routes are exact static paths; keep-alive, pipelining, TLS, path
parameters, WebSockets, and HTTP/2 are not part of this base.

## Documentation

- [Guide index](guide/index.md)
- [Getting started](guide/getting-started.md)
- [Routing](guide/routing.md)
- [Requests and responses](guide/requests-and-responses.md)
- [Runtime and lifecycle](guide/runtime-lifecycle.md)
- [Configuration](guide/configuration.md)
- [Third-party adapters](guide/adapters.md)
- [Errors and observability](guide/errors-observability.md)
- [Platforms and kernels](guide/platforms-kernels.md)
- [API reference](guide/api-reference.md)
- [Protocol roadmap](guide/protocol-roadmap.md)
- [Development](guide/development.md)

See [`demo.py`](demo.py) for all five supported HTTP methods,
[`examples/manual_runtime.py`](examples/manual_runtime.py) for caller-owned
SmallOS startup, and [`examples/adapters_demo.py`](examples/adapters_demo.py)
for blocking and asyncio escape hatches.

SmallServer is early-stage software. Review the documented limits and lifecycle
contract before deploying it outside controlled environments.
