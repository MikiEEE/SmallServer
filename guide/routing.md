# Routing

SmallServer currently matches a request method and path exactly. Register
routes with `get`, `post`, `put`, `patch`, `delete`, or the multi-method
`route` decorator.

```python
from smallserver import Response, SmallServer

app = SmallServer()


@app.route("/status", methods=("GET", "POST"))
async def status(request):
    return Response.text(request.method)
```

Methods passed to `route` are normalized to uppercase and duplicates are
removed. Registration rejects an empty method set, unsupported methods,
non-callable handlers, duplicate method/path pairs, and paths that do not start
with `/`. A failed multi-method registration does not partially add a route.

## Dispatch behavior

- An exact method/path match runs its async handler.
- A known path with the wrong method returns 405 and a sorted `Allow` header.
- An unknown path returns 404.
- A handler must return an awaitable whose result is a `Response`.
- Raising `HTTPError` produces the requested 4xx or 5xx response.

An ordinary handler exception becomes a generic 500 when the network server
invokes it. A direct call to `await app.dispatch(request)` preserves ordinary
exceptions for tests and embedding code.

## Static-path boundary

This base does not parse path parameters or split query strings. The request
target is matched as received, so `/items` and `/items?limit=10` are different
route keys. Register stable static paths and parse only data whose format your
application explicitly controls.

Timeout-bounded regex routes and captured parameters are being developed as an
optional route form; see the [protocol and feature roadmap](protocol-roadmap.md#routing-extensions).
Do not write base-compatible examples that assume `/items/{id}` or automatic
query parsing.
