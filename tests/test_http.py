import unittest

from smallserver import Headers, Request, Response


class HTTPValueTests(unittest.TestCase):
    def test_headers_are_case_insensitive_and_immutable(self) -> None:
        headers = Headers({"Content-Type": "text/plain"})
        self.assertEqual(headers["content-type"], "text/plain")
        self.assertEqual(list(headers.items()), [("Content-Type", "text/plain")])

    def test_response_serialization_is_deterministic(self) -> None:
        response = Response.text("ok", headers={"X-Test": "yes"})
        self.assertEqual(response.to_http1(), b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nX-Test: yes\r\nContent-Type: text/plain; charset=utf-8\r\n\r\nok")

    def test_json_helper_and_content_length_guard(self) -> None:
        self.assertEqual(Response.json({"a": 1}).body, b'{"a":1}')
        with self.assertRaisesRegex(ValueError, "content-length"):
            Response(body=b"ok", headers=Headers({"Content-Length": "3"}))

    def test_rejects_header_injection_and_invalid_request(self) -> None:
        with self.assertRaisesRegex(ValueError, "header value"):
            Headers({"X-Test": "ok\r\nInjected: true"})
        with self.assertRaisesRegex(ValueError, "path"):
            Request("GET", "items", Headers())

    def test_header_values_match_the_wire_encoding(self) -> None:
        response = Response(headers=Headers({"X-Label": "caf\N{LATIN SMALL LETTER E WITH ACUTE}"}))
        self.assertIn(b"X-Label: caf\xe9\r\n", response.to_http1())
        for invalid in ("nul\x00", "delete\x7f", "emoji \N{GRINNING FACE}"):
            with self.subTest(invalid=invalid):
                with self.assertRaisesRegex(ValueError, "header value"):
                    Headers({"X-Test": invalid})
