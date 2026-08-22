import contextlib
import io
import sys
import unittest
from unittest.mock import patch

from benchmarks import route_benchmark


class RouteBenchmarkCLITests(unittest.TestCase):
    def test_release_mode_rejects_undersized_samples(self) -> None:
        cases = (
            ("9999", "5", "10000 iterations"),
            ("10000", "4", "5 rounds"),
        )
        for iterations, rounds, message in cases:
            with self.subTest(iterations=iterations, rounds=rounds):
                stderr = io.StringIO()
                with patch.object(
                    sys,
                    "argv",
                    [
                        "route_benchmark.py",
                        "--release",
                        "--iterations",
                        iterations,
                        "--rounds",
                        rounds,
                    ],
                ):
                    with contextlib.redirect_stderr(stderr):
                        with self.assertRaises(SystemExit) as raised:
                            route_benchmark.main()
                self.assertEqual(raised.exception.code, 2)
                self.assertIn(message, stderr.getvalue())
