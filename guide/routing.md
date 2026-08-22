# Routing

SmallServer checks exact static routes first, then optional timeout-bounded
regular-expression routes. Register static routes with `get`, `post`, `put`,
`patch`, `delete`, or the multi-method `route` decorator.

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

## Request targets

The parser preserves the exact ASCII origin-form target as
`request.raw_target`. Routing uses `request.path`, excluding the raw query
string stored in `request.query_string`. Neither field nor a regex capture is
percent-decoded, so `/files/a%2Fb` remains distinct from `/files/a/b`.

## Regex routes

Install the bounded matching engine only when needed:

```console
python3 -m pip install -e '.[regex-routes]'
```

```python
@app.get_regex(r"/users/(?P<user_id>[0-9]+)")
async def user(request):
    return Response.json({"user_id": request.path_params["user_id"]})
```

`route_regex(pattern, methods)` and the five method-specific regex decorators
use full-path matching in registration order after static lookup. Only named
captures are exposed through immutable `request.path_params`; an unmatched
optional group is omitted. `request.route_pattern` identifies the selected
pattern.

Patterns must begin with a literal `/`. Registration and dispatch bound route
count, pattern length, capture count, path bytes, each match, and total matching
time. A timeout becomes a sanitized 500 on the network path and may be observed
through the bounded `route_error_observer` channel without disclosing the
hostile path. Oversized paths return 414 before matching.
