# Third-party adapters

SmallServer handlers run on the SmallOS scheduler and must not call blocking
functions or drive a second event loop directly. SmallServer exposes the
SmallOS-backed escape hatches through its own public API, so application code
does not need to import adapter modules from `SmallPackage`:

- `ThreadAdapter` for blocking or thread-affine callables;
- `AsyncioAdapter` for coroutine-based libraries on a persistent asyncio loop.

The application creates, bounds, and shuts down these adapters. SmallServer
does not create adapter workers as a side effect of `listen()`.

```python
from smallserver import (
    AdapterError,
    AdapterRegistry,
    Response,
    ThreadAdapter,
    http_error_from_adapter,
)

services = AdapterRegistry(blocking=ThreadAdapter(max_workers=2, max_pending=8))


async def handler(request):
    try:
        result = await services.call("blocking", str.upper, "smallserver")
    except AdapterError as exc:
        raise http_error_from_adapter(exc)
    return Response.text(result)


services.shutdown()
```

In a real application, keep the registry alive around the complete runtime
lifecycle; do not shut it down immediately after defining a handler. The
context manager calls `shutdown()` automatically and cancels pending adapter
work when its body exits with an exception.

`AdapterRegistry` accepts named, user-created adapters that provide `call()`
and `shutdown()`. It rejects duplicate names and duplicate adapter objects,
delegates calls, exposes stable registration order through `names()` and
`items()`, and shuts adapters down in reverse registration order.

Import `ThreadAdapter` and `AsyncioAdapter` from `smallserver`. Their work is
still scheduled and completed through SmallOS, but the backend module layout is
not part of application code. SmallServer also exports `AdapterError` and its
capacity, unavailable, closed, cancelled, protocol, and execution subclasses
for explicit handling.

`http_error_from_adapter()` intentionally sanitizes adapter failures:
capacity, unavailable, closed, and cancelled conditions become generic 503
responses; other adapter failures become a generic 500. Log internal causes in
application-controlled telemetry if needed, but do not expose them to clients.

See [`examples/adapters_demo.py`](../examples/adapters_demo.py) for a runnable
SQLite and asyncio example and [`examples/manual_runtime.py`](../examples/manual_runtime.py)
for the surrounding runtime lifecycle.
