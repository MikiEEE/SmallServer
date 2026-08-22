"""Repeatable routing comparison; run with ``python benchmarks/route_benchmark.py``."""

from __future__ import annotations

import argparse
import asyncio
import importlib.util
import inspect
import json
from pathlib import Path
import statistics
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from smallserver import Headers, RegexRouteConfig, Request, Response, RouteMatchTimeout, SmallServer

STATIC_DISPATCH_RATIO_FLOOR = 0.80


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


async def _measure(operation, iterations: int) -> float:
    started = time.perf_counter()
    for _ in range(iterations):
        await operation()
    return iterations / (time.perf_counter() - started)


async def benchmark(iterations: int, rounds: int) -> dict[str, float | int | str | bool]:
    app = SmallServer()

    @app.get("/health")
    async def health(request):
        return Response()

    request = Request("GET", "/health", Headers())
    legacy_routes = {("GET", "/health"): health}

    async def legacy_operation():
        return await _legacy_dispatch(legacy_routes, request)

    async def router_operation():
        return await app.dispatch(request)

    await _measure(legacy_operation, min(iterations, 1_000))
    await _measure(router_operation, min(iterations, 1_000))
    legacy_rates = []
    router_rates = []
    ratios = []
    for round_number in range(rounds):
        if round_number % 2:
            router_rate = await _measure(router_operation, iterations)
            legacy_rate = await _measure(legacy_operation, iterations)
        else:
            legacy_rate = await _measure(legacy_operation, iterations)
            router_rate = await _measure(router_operation, iterations)
        legacy_rates.append(legacy_rate)
        router_rates.append(router_rate)
        ratios.append(router_rate / legacy_rate)

    median_ratio = statistics.median(ratios)
    result: dict[str, float | int | str | bool] = {
        "iterations": iterations,
        "rounds": rounds,
        "legacy_static_dispatches_per_second": round(statistics.median(legacy_rates), 2),
        "router_static_dispatches_per_second": round(statistics.median(router_rates), 2),
        "router_to_legacy_ratio": round(median_ratio, 4),
        "static_dispatch_ratio_floor": STATIC_DISPATCH_RATIO_FLOOR,
        "static_dispatch_floor_passed": median_ratio >= STATIC_DISPATCH_RATIO_FLOOR,
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
    parser.add_argument("--rounds", type=int, default=5)
    parser.add_argument("--release", action="store_true")
    arguments = parser.parse_args()
    if arguments.iterations <= 0:
        parser.error("--iterations must be positive")
    if arguments.rounds <= 0:
        parser.error("--rounds must be positive")
    result = asyncio.run(benchmark(arguments.iterations, arguments.rounds))
    print(json.dumps(result, indent=2, sort_keys=True))
    if arguments.release and not result["static_dispatch_floor_passed"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
