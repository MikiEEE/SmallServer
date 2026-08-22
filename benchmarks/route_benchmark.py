"""Small routing microbenchmark; run with ``python benchmarks/route_benchmark.py``."""

from __future__ import annotations

import asyncio
import importlib.util
from pathlib import Path
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from smallserver import Headers, RegexRouteConfig, Request, Response, RouteMatchTimeout, SmallServer


async def benchmark() -> None:
    app = SmallServer()

    @app.get("/health")
    async def health(request):
        return Response()

    request = Request("GET", "/health", Headers())
    iterations = 25_000
    started = time.perf_counter()
    for _ in range(iterations):
        await app.dispatch(request)
    static_elapsed = time.perf_counter() - started
    print("static: {:.0f} dispatches/second".format(iterations / static_elapsed))

    if importlib.util.find_spec("regex") is None:
        print("regex: skipped (install smallserver[regex-routes])")
        return

    bounded = SmallServer(RegexRouteConfig(match_timeout=0.002, total_match_timeout=0.005))

    @bounded.get_regex(r"/(a+)+$")
    async def hostile(request):
        return Response()

    started = time.perf_counter()
    try:
        await bounded.dispatch(Request("GET", "/" + "a" * 5000 + "!", Headers()))
    except RouteMatchTimeout:
        pass
    print("worst-case regex timeout: {:.4f}s".format(time.perf_counter() - started))


if __name__ == "__main__":
    asyncio.run(benchmark())
