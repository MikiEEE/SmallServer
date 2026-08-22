"""Use blocking SQLite and asyncio work from SmallServer route handlers."""

from __future__ import annotations

import asyncio
import json
import sqlite3
import threading

from SmallPackage import SmallOS, SmallTask, Unix
from SmallPackage.adapters.asyncio_loop import AsyncioAdapter
from SmallPackage.adapters.errors import AdapterError
from SmallPackage.adapters.threads import ThreadAdapter

from smallserver import (
    AdapterRegistry,
    Headers,
    Request,
    Response,
    SmallServer,
    http_error_from_adapter,
)


class SQLiteStore:
    """Own one SQLite connection on a single adapter worker thread."""

    def __init__(self) -> None:
        self.connection: sqlite3.Connection | None = None
        self.owner_thread: int | None = None

    def open(self) -> None:
        self.owner_thread = threading.get_ident()
        self.connection = sqlite3.connect(":memory:")
        self.connection.execute("CREATE TABLE messages (body TEXT NOT NULL)")

    def add_and_list(self, body: str) -> list[str]:
        if self.connection is None or threading.get_ident() != self.owner_thread:
            raise RuntimeError("SQLiteStore used outside its adapter worker")
        self.connection.execute("INSERT INTO messages (body) VALUES (?)", (body,))
        self.connection.commit()
        return [str(row[0]) for row in self.connection.execute("SELECT body FROM messages")]

    def close(self) -> None:
        if self.connection is not None:
            self.connection.close()
        self.connection = None
        self.owner_thread = None


async def uppercase(value: str) -> tuple[str, int]:
    """Stand in for an asyncio-native SDK call on the persistent adapter loop."""
    await asyncio.sleep(0)
    return value.upper(), id(asyncio.get_running_loop())


def main() -> None:
    runtime = SmallOS().setKernel(Unix())
    app = SmallServer()
    store = SQLiteStore()

    with AdapterRegistry(
        database=ThreadAdapter(max_workers=1, max_pending=8),
        async_sdk=AsyncioAdapter(max_pending=8),
    ) as services:

        @app.post("/messages")
        async def add_message(request):
            try:
                rows = await services.call("database", store.add_and_list, request.body.decode())
                return Response.json({"messages": rows})
            except AdapterError as exc:
                raise http_error_from_adapter(exc)

        @app.get("/uppercase")
        async def async_library(request):
            try:
                first, first_loop = await services.call("async_sdk", uppercase, "smallserver")
                second, second_loop = await services.call("async_sdk", uppercase, "smallos")
                return Response.json(
                    {"values": [first, second], "persistent_loop": first_loop == second_loop}
                )
            except AdapterError as exc:
                raise http_error_from_adapter(exc)

        async def scenario(task):
            await services.call("database", store.open)
            try:
                database_response = await app.dispatch(
                    Request("POST", "/messages", Headers(), b"cooperative blocking work")
                )
                asyncio_response = await app.dispatch(Request("GET", "/uppercase", Headers()))
                return database_response, asyncio_response
            finally:
                await services.call("database", store.close)

        target = SmallTask(2, scenario, name="smallserver-adapters-demo")
        runtime.fork(target)
        runtime.start()

        if target.exception is not None:
            raise target.exception
        assert target.result is not None
        for response in target.result:
            print(json.dumps(json.loads(response.body.decode()), indent=2))


if __name__ == "__main__":
    main()
