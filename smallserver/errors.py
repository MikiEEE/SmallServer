"""Framework-owned HTTP errors."""

from __future__ import annotations


class HTTPError(Exception):
    """An expected HTTP response raised by framework or application code."""

    def __init__(self, status: int, detail: str = "") -> None:
        if not 400 <= status <= 599:
            raise ValueError("HTTPError status must be between 400 and 599")
        self.status = status
        self.detail = detail
        super().__init__(detail or "HTTP {}".format(status))
