"""Repeatable routing comparison; run with ``python benchmarks/route_benchmark.py``."""

from __future__ import annotations

import argparse
import asyncio
import importlib.util
import inspect
import json
from pathlib import Path
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from smallserver import Headers, RegexRouteConfig, Request, Response, RouteMatchTimeout, SmallServer


async def _legacy_dispatch(routes, request):
    """Model the pre-router static dictionary dispatch for same-run comparison."""
    handler = routes[(request.method.upper(), request.path)]
    result = handler(request)
    if not inspect.isawaitable(result):
        raise TypeError("benchmark handler must be awaitable")
    response = await result
    if not isinstance(response, Response):
        raise TypeError("benchmark handler must return Response")
    return response


async def benchmark(iterations: int) -> dict[str, float | int | str]:
    app = SmallServer()

    @app.get("/health")
    async def health(request):
        return Response()

    request = Request("GET", "/health", Headers())
    legacy_routes = {("GET", "/health"): health}

    started = time.perf_counter()
    for _ in range(iterations):
        await _legacy_dispatch(legacy_routes, request)
    legacy_elapsed = time.perf_counter() - started

    started = time.perf_counter()
    for _ in range(iterations):
        await app.dispatch(request)
    router_elapsed = time.perf_counter() - started

    legacy_rate = iterations / legacy_elapsed
    router_rate = iterations / router_elapsed
    result: dict[str, float | int | str] = {
        "iterations": iterations,
        "legacy_static_dispatches_per_second": round(legacy_rate, 2),
        "router_static_dispatches_per_second": round(router_rate, 2),
        "router_to_legacy_ratio": round(router_rate / legacy_rate, 4),
    }

    if importlib.util.find_spec("regex") is None:
        result["regex"] = "skipped; install smallserver[regex-routes]"
        return result

    bounded = SmallServer(RegexRouteConfig(match_timeout=0.002, total_match_timeout=0.005))

    @bounded.get_regex(r"/(a+)+$")
    async def hostile(request):
        return Response()

    started = time.perf_counter()
    try:
        await bounded.dispatch(Request("GET", "/" + "a" * 5000 + "!", Headers()))
    except RouteMatchTimeout:
        pass
    result["configured_regex_match_timeout_seconds"] = 0.002
    result["observed_worst_case_regex_seconds"] = round(time.perf_counter() - started, 6)
    return result


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--iterations", type=int, default=25_000)
    arguments = parser.parse_args()
    if arguments.iterations <= 0:
        parser.error("--iterations must be positive")
    print(json.dumps(asyncio.run(benchmark(arguments.iterations)), indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
