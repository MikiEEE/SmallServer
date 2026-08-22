"""Smoke an installed ``smallserver[regex-routes]`` package outside the source path."""

import asyncio

from smallserver import Headers, Request, Response, RouteErrorEvent, SmallServer


async def main() -> None:
    event = RouteErrorEvent("regex-route-smoke", "route_match_timeout")
    assert (event.route_id, event.category) == ("regex-route-smoke", "route_match_timeout")
    app = SmallServer()

    @app.get_regex(r"/users/(?P<user_id>[0-9]+)")
    async def user(request: Request) -> Response:
        return Response.text(request.path_params["user_id"])

    response = await app.dispatch(Request("GET", "/users/42?source=smoke", Headers()))
    assert response.status == 200
    assert response.body == b"42"


if __name__ == "__main__":
    asyncio.run(main())
