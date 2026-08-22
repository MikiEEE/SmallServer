import importlib.util
import time
import unittest
from unittest.mock import patch

from smallserver import (
    Headers,
    RegexRouteConfig,
    RegexRoutesUnavailable,
    Request,
    Response,
    RouteMatchTimeout,
    SmallServer,
)


HAS_REGEX = importlib.util.find_spec("regex") is not None


class OptionalRegexDependencyTests(unittest.TestCase):
    def test_static_routes_do_not_import_optional_engine(self) -> None:
        app = SmallServer()

        async def handler(request):
            return Response()

        with patch("smallserver.routing.importlib.import_module", side_effect=AssertionError("imported")):
            app.get("/health")(handler)

    def test_registration_explains_missing_extra(self) -> None:
        app = SmallServer()

        async def handler(request):
            return Response()

        with patch("smallserver.routing.importlib.import_module", side_effect=ImportError):
            with self.assertRaisesRegex(RegexRoutesUnavailable, r"smallserver\[regex-routes\]"):
                app.get_regex(r"/users/[0-9]+")(handler)


@unittest.skipUnless(HAS_REGEX, "regex-routes extra is not installed")
class RegexRoutingTests(unittest.IsolatedAsyncioTestCase):
    async def test_named_captures_are_immutable_and_optional_groups_are_omitted(self) -> None:
        app = SmallServer()
        seen = []

        @app.get_regex(r"/users/(?P<user_id>[0-9]+)(?:/(?P<section>[a-z]+))?")
        async def user(request):
            seen.append(request)
            return Response.json(dict(request.path_params))

        response = await app.dispatch(Request("GET", "/users/42?debug=1", Headers()))
        self.assertEqual(response.body, b'{"user_id":"42"}')
        self.assertEqual(seen[0].raw_target, "/users/42?debug=1")
        self.assertEqual(seen[0].query_string, "debug=1")
        self.assertEqual(seen[0].route_pattern, r"/users/(?P<user_id>[0-9]+)(?:/(?P<section>[a-z]+))?")
        with self.assertRaises(TypeError):
            seen[0].path_params["user_id"] = "1"  # type: ignore[index]
        await app.dispatch(Request("GET", "/users/43/profile", Headers()))
        self.assertEqual(dict(seen[0].path_params), {"user_id": "42"})
        self.assertEqual(dict(seen[1].path_params), {"user_id": "43", "section": "profile"})

    async def test_captures_preserve_percent_encoded_octets(self) -> None:
        app = SmallServer()

        @app.get_regex(r"/files/(?P<name>[^/]+)")
        async def file(request):
            return Response.text(request.path_params["name"])

        response = await app.dispatch(Request("GET", "/files/a%2Fb", Headers()))
        self.assertEqual(response.body, b"a%2Fb")

    async def test_static_precedence_and_regex_registration_order(self) -> None:
        app = SmallServer()

        @app.get_regex(r"/items/(?P<value>.+)")
        async def broad(request):
            return Response.text("broad")

        @app.get_regex(r"/items/(?P<value>[0-9]+)")
        async def narrow(request):
            return Response.text("narrow")

        @app.get("/items/7")
        async def exact(request):
            return Response.text("static")

        self.assertEqual((await app.dispatch(Request("GET", "/items/7", Headers()))).body, b"static")
        self.assertEqual((await app.dispatch(Request("GET", "/items/8", Headers()))).body, b"broad")

    async def test_method_first_matching_and_sorted_allow(self) -> None:
        app = SmallServer()

        @app.post("/records/1")
        async def static_post(request):
            return Response.text("post")

        @app.delete_regex(r"/records/(?P<record_id>[0-9]+)")
        async def regex_delete(request):
            return Response.text("delete")

        @app.get_regex(r"/records/(?P<record_id>.+)")
        async def regex_get(request):
            return Response.text("get")

        self.assertEqual((await app.dispatch(Request("GET", "/records/1", Headers()))).body, b"get")
        response = await app.dispatch(Request("PATCH", "/records/1", Headers()))
        self.assertEqual(response.status, 405)
        self.assertEqual(response.headers["allow"], "DELETE, GET, POST")

    async def test_all_regex_method_decorators_dispatch(self) -> None:
        app = SmallServer()
        for method in ("get", "post", "put", "patch", "delete"):
            decorator = getattr(app, method + "_regex")

            @decorator("/" + method + r"/(?P<value>[0-9]+)")
            async def handler(request, expected=method):
                return Response.text(expected + request.path_params["value"])

        for method in ("get", "post", "put", "patch", "delete"):
            response = await app.dispatch(Request(method.upper(), "/" + method + "/2", Headers()))
            self.assertEqual(response.body, (method + "2").encode())

    async def test_same_pattern_disjoint_methods_merge_and_duplicates_are_atomic(self) -> None:
        app = SmallServer()

        async def first(request):
            return Response.text("first")

        async def second(request):
            return Response.text("second")

        app.get_regex(r"/merged/(?P<id>[0-9]+)")(first)
        app.post_regex(r"/merged/(?P<id>[0-9]+)")(second)
        with self.assertRaisesRegex(ValueError, "already registered"):
            app.route_regex(r"/merged/(?P<id>[0-9]+)", ("PATCH", "GET"))(second)
        self.assertEqual((await app.dispatch(Request("PATCH", "/merged/1", Headers()))).status, 405)
        self.assertEqual((await app.dispatch(Request("POST", "/merged/1", Headers()))).body, b"second")

    def test_registration_limits_and_invalid_patterns(self) -> None:
        async def handler(request):
            return Response()

        cases = (
            (r"items/[0-9]+", "literal '/'"),
            (r"/[", "invalid"),
            (r"/(?P<id>x)(?P<id>y)", "duplicate"),
        )
        for pattern, message in cases:
            with self.subTest(pattern=pattern):
                with self.assertRaisesRegex((ValueError, TypeError), message):
                    SmallServer().get_regex(pattern)(handler)

        with self.assertRaisesRegex(ValueError, "too long"):
            SmallServer(RegexRouteConfig(max_pattern_length=4)).get_regex("/long")(handler)
        with self.assertRaisesRegex(ValueError, "too many"):
            SmallServer(RegexRouteConfig(max_named_captures=1)).get_regex(
                r"/(?P<one>x)(?P<two>y)"
            )(handler)
        limited = SmallServer(RegexRouteConfig(max_routes=1))
        limited.get_regex(r"/one")(handler)
        with self.assertRaisesRegex(ValueError, "maximum"):
            limited.get_regex(r"/two")(handler)

    async def test_catastrophic_backtracking_is_bounded_and_path_is_not_disclosed(self) -> None:
        app = SmallServer(RegexRouteConfig(match_timeout=0.001, total_match_timeout=0.005))

        @app.get_regex(r"/(a+)+$")
        async def handler(request):
            return Response()

        hostile_path = "/" + "a" * 5000 + "!"
        started = time.monotonic()
        with self.assertRaises(RouteMatchTimeout) as raised:
            await app.dispatch(Request("GET", hostile_path, Headers()))
        elapsed = time.monotonic() - started
        self.assertLess(elapsed, 0.25)
        self.assertNotIn(hostile_path, str(raised.exception))
        self.assertEqual(raised.exception.route_id, "regex-route-1")

    async def test_total_budget_bounds_many_individually_fast_misses(self) -> None:
        class SlowPattern:
            def fullmatch(self, value, timeout):
                if not value:
                    return None
                time.sleep(min(timeout / 2, 0.001))
                return None

        class Engine:
            @staticmethod
            def compile(pattern):
                return SlowPattern()

        app = SmallServer(
            RegexRouteConfig(max_routes=20, match_timeout=0.01, total_match_timeout=0.003)
        )

        async def handler(request):
            return Response()

        with patch("smallserver.routing.importlib.import_module", return_value=Engine()):
            for index in range(10):
                app.get_regex("/(?:route-{}).*".format(index))(handler)
        started = time.monotonic()
        with self.assertRaises(RouteMatchTimeout):
            await app.dispatch(Request("GET", "/route", Headers()))
        self.assertLess(time.monotonic() - started, 0.05)

    async def test_manual_dispatch_path_limit_is_enforced_before_matching(self) -> None:
        app = SmallServer(RegexRouteConfig(max_path_bytes=8))

        @app.get_regex(r"/.*")
        async def handler(request):
            return Response()

        with self.assertRaisesRegex(ValueError, "too large"):
            await app.dispatch(Request("GET", "/12345678", Headers()))


class RegexRouteConfigTests(unittest.TestCase):
    def test_rejects_nonfinite_or_nonpositive_limits(self) -> None:
        for kwargs in (
            {"max_routes": 0},
            {"max_routes": True},
            {"match_timeout": 0},
            {"match_timeout": float("inf")},
            {"total_match_timeout": float("nan")},
        ):
            with self.subTest(kwargs=kwargs):
                with self.assertRaises(ValueError):
                    RegexRouteConfig(**kwargs)
