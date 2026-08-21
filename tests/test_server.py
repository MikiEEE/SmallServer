import unittest

from smallserver.server import HTTPParseError, HTTPRequestParser, ServerConfig


class HTTPRequestParserTests(unittest.TestCase):
    def parser(self) -> HTTPRequestParser:
        return HTTPRequestParser(max_header_bytes=128, max_header_count=2, max_body_bytes=32)

    def test_fragmented_content_length_request(self) -> None:
        parser = self.parser()
        self.assertIsNone(parser.feed(b"POST /items HTTP/1.1\r\nHost: localhost\r\nContent-Length: 4\r\n\r"))
        request = parser.feed(b"\ntest")
        self.assertIsNotNone(request)
        assert request is not None
        self.assertEqual((request.method, request.path, request.body), ("POST", "/items", b"test"))

    def test_rejects_unsupported_or_ambiguous_framing(self) -> None:
        with self.assertRaisesRegex(HTTPParseError, "transfer-encoding"):
            self.parser().feed(b"POST / HTTP/1.1\r\nTransfer-Encoding: chunked\r\n\r\n")
        with self.assertRaisesRegex(HTTPParseError, "multiple content-length"):
            self.parser().feed(b"POST / HTTP/1.1\r\nContent-Length: 1\r\nContent-Length: 1\r\n\r\nx")

    def test_enforces_finite_header_and_body_limits(self) -> None:
        with self.assertRaisesRegex(HTTPParseError, "headers are too large"):
            self.parser().feed(b"G" * 129)
        with self.assertRaisesRegex(HTTPParseError, "body is too large"):
            self.parser().feed(b"POST / HTTP/1.1\r\nContent-Length: 33\r\n\r\n")

    def test_config_rejects_unbounded_limits(self) -> None:
        with self.assertRaisesRegex(ValueError, "max_connections"):
            ServerConfig(max_connections=0)
