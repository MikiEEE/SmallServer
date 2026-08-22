# Requests and responses

`Request`, `Response`, and `Headers` are immutable value objects shared by the
router and server.

## Request

A handler receives:

- `method`: a valid HTTP token;
- `path`: the request target, beginning with `/`;
- `headers`: a case-insensitive `Headers` mapping;
- `body`: complete request bytes;
- `version`: `HTTP/1.1` for the current network server.

The base parser accepts one origin-form HTTP/1.1 request framed by zero or one
`Content-Length` header. It rejects transfer encoding, multiple content lengths,
missing `Host`, invalid targets, oversized input, and pipelined bytes. It does
not decode JSON, forms, query parameters, or text for you.

```python
import json

from smallserver import HTTPError, Request, Response


async def create(request: Request) -> Response:
    try:
        value = json.loads(request.body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise HTTPError(400, "body must be valid JSON") from exc
    return Response.json({"received": value}, status=201)
```

## Headers

Header lookup is case-insensitive while iteration preserves the originally
provided spelling. Names must be HTTP tokens; values cannot contain control
characters other than horizontal tab or characters outside Latin-1. Duplicate
names are rejected after case folding.

```python
from smallserver import Headers

headers = Headers({"Content-Type": "application/json"})
assert headers["content-type"] == "application/json"
```

## Response

Construct `Response(status, body, headers)`, or use `Response.text()` and
`Response.json()`. Bodies must already be `bytes`. An explicit `Content-Length`
must exactly match the body; otherwise construction fails. The HTTP/1.1 server
adds a length when absent and sends `Connection: close`.

```python
from smallserver import Response

plain = Response.text("ready")
created = Response.json({"id": "1"}, status=201)
empty = Response(status=204)
```

`Response.to_http1()` is available for deterministic serialization and tests.
Applications normally return the value and let SmallServer write it.
