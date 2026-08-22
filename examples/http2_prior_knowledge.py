"""Run SmallServer's cleartext prior-knowledge HTTP/2 demo."""

from smallserver import HTTP2Config, Response, SmallServer


app = SmallServer()


@app.get("/health")
async def health(request):
    return Response.json({"status": "ok", "protocol": request.version})


@app.post("/echo")
async def echo(request):
    return Response(body=request.body, headers={"Content-Type": "application/octet-stream"})


if __name__ == "__main__":
    print("HTTP/2 prior-knowledge server: http://127.0.0.1:8000")
    print("Try: curl --http2-prior-knowledge http://127.0.0.1:8000/health")
    app.listen(
        host="127.0.0.1",
        port=8000,
        protocol="http2",
        http2_config=HTTP2Config(max_concurrent_streams=32),
    )
