"""Public typing fixture for mypy/pyright and compile-only release checks."""

from smallserver import Request, Response, RouteErrorEvent, SmallServer


def observe(event: RouteErrorEvent) -> None:
    route_id: str = event.route_id
    assert route_id


def application() -> SmallServer:
    app = SmallServer(route_error_observer=observe)

    @app.get_regex(r"/users/(?P<user_id>[0-9]+)")
    async def user(request: Request) -> Response:
        user_id: str = request.path_params["user_id"]
        pattern: str | None = request.route_pattern
        return Response.json({"user_id": user_id, "pattern": pattern})

    return app
