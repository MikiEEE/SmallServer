import unittest

from smallserver import Headers, HTTPError, Request, Response, SmallServer


class RoutingTests(unittest.IsolatedAsyncioTestCase):
    async def test_all_supported_method_decorators_dispatch(self) -> None:
        app = SmallServer()
        methods = ("get", "post", "put", "patch", "delete")
        for method in methods:
            decorator = getattr(app, method)

            @decorator("/" + method)
            async def handler(request, expected=method):
                return Response.text(expected)

        for method in methods:
            response = await app.dispatch(Request(method.upper(), "/" + method, Headers()))
            self.assertEqual(response.body, method.encode())

    async def test_not_found_and_method_not_allowed_are_distinct(self) -> None:
        app = SmallServer()

        @app.get("/items")
        async def items(request):
            return Response()

        self.assertEqual((await app.dispatch(Request("GET", "/missing", Headers()))).status, 404)
        response = await app.dispatch(Request("POST", "/items", Headers()))
        self.assertEqual(response.status, 405)
        self.assertEqual(response.headers["allow"], "GET")

    async def test_http_error_becomes_response(self) -> None:
        app = SmallServer()

        @app.delete("/items")
        async def remove(request):
            raise HTTPError(413, "too large")

        response = await app.dispatch(Request("DELETE", "/items", Headers()))
        self.assertEqual((response.status, response.body), (413, b"too large"))

    async def test_handler_contract_is_strict(self) -> None:
        app = SmallServer()

        @app.put("/items")
        async def bad(request):
            return "nope"

        with self.assertRaisesRegex(TypeError, "must return Response"):
            await app.dispatch(Request("PUT", "/items", Headers()))

    def test_rejects_duplicate_and_unsupported_routes(self) -> None:
        app = SmallServer()

        @app.get("/one")
        async def one(request):
            return Response()

        with self.assertRaisesRegex(ValueError, "already registered"):
            app.get("/one")(one)
        with self.assertRaisesRegex(ValueError, "supported HTTP methods"):
            app.route("/trace", ("TRACE",))

    async def test_failed_multi_method_registration_is_atomic(self) -> None:
        app = SmallServer()

        @app.get("/one")
        async def one(request):
            return Response()

        with self.assertRaisesRegex(ValueError, "already registered"):
            app.route("/one", ("POST", "GET"))(one)
        response = await app.dispatch(Request("POST", "/one", Headers()))
        self.assertEqual(response.status, 405)
