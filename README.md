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

Current boundaries are intentional: each ordinary HTTP/1.1 connection serves
one request; keep-alive, pipelining, TLS, automatic path templates, WebSocket
compression, RFC 8441, and HTTP/2 are not implemented.

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

SmallServer is early-stage software. Review the documented limits and lifecycle
contract before deploying it outside controlled environments.
