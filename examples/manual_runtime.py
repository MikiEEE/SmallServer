"""Run SmallServer with caller-owned SmallOS lifecycle control."""

from SmallPackage import SmallOS, Unix

from smallserver import Response, SmallServer


runtime = SmallOS().setKernel(Unix())
server = SmallServer()


@server.get("/health")
async def health(request):
    return Response.json({"status": "ok"})


if __name__ == "__main__":
    # Create any execution adapters beside the runtime and close them in the
    # application's own finally block. SmallServer never owns those adapters.
    handle = server.listen(
        host="127.0.0.1",
        port=8000,
        runtime=runtime,
        start=False,
    )
    print("SmallServer listening on http://{}:{}".format(*handle.address))
    try:
        runtime.start()
    finally:
        handle.finalize()
