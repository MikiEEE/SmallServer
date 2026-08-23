# Getting started

## Requirements

SmallServer requires Python 3.10 or newer. `requirements.txt` installs the
canonical SmallOS GitHub release tagged `v1.2.0`; SmallServer itself declares
no package-index runtime dependency yet.

```console
python3 -m pip install -r requirements.txt
python3 -m pip install -e .
```

The first command needs Git and network access. The release tag makes the
SmallOS source revision reproducible; deployments should still lock all of
their transitive build dependencies.

## Create an application

```python
from smallserver import Request, Response, SmallServer

app = SmallServer()


@app.get("/health")
async def health(request: Request) -> Response:
    return Response.json({"status": "ok"})


if __name__ == "__main__":
    app.listen(host="127.0.0.1", port=8000)
```

Run the file and request the exact path:

```console
curl -i http://127.0.0.1:8000/health
```

`listen()` creates a SmallOS runtime with its Unix kernel, blocks while that
runtime runs, and handles Ctrl-C by cleaning up server-owned resources. Normal
application code does not need to import SmallOS.

Use `port=0` when a test or tool needs the kernel to choose an available port.
Because managed `listen()` blocks, inspect the returned handle only after the
runtime has stopped. For access to the bound port while the server is running,
use [caller-owned runtime mode](runtime-lifecycle.md#caller-owned-runtime).

## Try the task demo

[`demo.py`](../demo.py) implements GET, POST, PUT, PATCH, and DELETE on the
static `/tasks` route:

```console
python3 demo.py
curl -i http://127.0.0.1:8000/tasks
curl -i -X POST -H 'Content-Type: application/json' \
  --data '{"title":"read the guide"}' http://127.0.0.1:8000/tasks
```

Every current HTTP/1.1 connection serves one request and closes after the
response. See [Routing](routing.md) and [Configuration](configuration.md) before
building a larger application.
