# Runtime and lifecycle

SmallOS always owns scheduling and I/O readiness. SmallServer offers one
managed mode for normal applications and explicit modes for applications that
coordinate other SmallOS tasks.

Only one listener invocation may be active on a `SmallServer` instance. The
instance can be reused after its handle is fully finished.

## Managed runtime

```python
from smallserver import Response, SmallServer

app = SmallServer()


@app.get("/")
async def index(request):
    return Response.text("hello")


app.listen(host="127.0.0.1", port=8000)
```

With no `runtime`, `listen()` lazily creates `SmallOS().setKernel(Unix())`,
starts it, blocks until shutdown, and finalizes server-owned resources. In this
managed mode, Ctrl-C is consumed after successful cleanup and the closed
`ServerHandle` is returned.

## Caller-owned runtime

Supply a configured runtime to schedule the listener without starting it:

```python
from SmallPackage import SmallOS, Unix
from smallserver import Response, SmallServer

runtime = SmallOS().setKernel(Unix())
app = SmallServer()


@app.get("/health")
async def health(request):
    return Response.json({"status": "ok"})


handle = app.listen(runtime=runtime, start=False, port=0)
print(handle.address)
try:
    runtime.start()
finally:
    handle.finalize()
```

With a supplied runtime, `start=False` is the default. `app.serve(runtime, ...)`
is the equivalent schedule-and-return compatibility API. Passing `start=True`
starts the supplied runtime once; the caller still owns that runtime.

## Shutdown operations

- `handle.close()` requests shutdown from outside the scheduler when the kernel
  provides a wakeup channel. Unix supports this path.
- `await handle.close_from_task(task)` shuts down from the currently running
  SmallOS task and is required on kernels without a wakeup channel.
- `handle.finalize()` performs idempotent owner-thread cleanup after a manually
  started scheduler has exited or failed.

`closed` means shutdown was requested. `finished` is stronger: the listener,
wakeup channel, connections, and retained cleanup work have all completed.
Failed closes remain owned and appear in `cleanup_errors`; call the appropriate
cleanup operation again from a safe context.

`address` and `port` are cached and remain readable after close. `failure`
reports the first fatal listener or connection-cleanup failure.

See [Errors and observability](errors-observability.md) for incomplete startup
and finalization transactions.
