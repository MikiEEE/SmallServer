# Routing

SmallServer gives exact static routes precedence, then evaluates optional
timeout-bounded regex routes in registration order. Register static routes
with `get`, `post`, `put`, `patch`, `delete`, or `route`.

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

## Request targets and regex routes

Routing uses `request.path`; the undecoded query remains in
`request.query_string`, and `request.raw_target` preserves both. Install
`smallserver[regex-routes]` to register full-path expressions:

```python
@app.get_regex(r"/items/(?P<item_id>[0-9]+)")
async def item(request):
    return Response.json({"id": request.path_params["item_id"]})
```

Only named captures are exposed, as an immutable mapping. Patterns, paths,
route counts, captures, individual matches, and total matching time are
bounded by `RegexRouteConfig`. This is an explicit regex API, not automatic
`/items/{id}` template parsing or percent decoding.
