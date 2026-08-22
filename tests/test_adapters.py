import asyncio
import json
import threading
import unittest

from SmallPackage import SmallOS, SmallTask, Unix
from SmallPackage.adapters.asyncio_loop import AsyncioAdapter
from SmallPackage.adapters.errors import AdapterCapacityError, AdapterProtocolError
from SmallPackage.adapters.threads import ThreadAdapter

from smallserver import (
    AdapterRegistry,
    AdapterShutdownError,
    Headers,
    Request,
    Response,
    SmallServer,
    http_error_from_adapter,
)


class FakeAdapter:
    def __init__(self, events: list[str], name: str) -> None:
        self.events = events
        self.name = name
        self.closed = False
        self.shutdown_calls = 0

    def call(self, callable_obj, /, *args, **kwargs):
        return callable_obj(*args, **kwargs)

    def shutdown(self, wait=True, cancel_pending=False) -> None:
        self.closed = True
        self.shutdown_calls += 1
        self.events.append(self.name)


class AdapterRegistryTests(unittest.TestCase):
    def test_registry_delegates_and_shuts_down_in_reverse_order(self) -> None:
        events: list[str] = []
        first = FakeAdapter(events, "first")
        second = FakeAdapter(events, "second")
        registry = AdapterRegistry(first=first, second=second)

        self.assertEqual(registry.call("first", lambda value: value + 1, 2), 3)
        self.assertEqual(registry.names(), ("first", "second"))
        registry.shutdown()

        self.assertEqual(events, ["second", "first"])
        self.assertTrue(registry.closed)
        with self.assertRaisesRegex(RuntimeError, "closed"):
            registry.call("first", lambda: None)

    def test_adapter_error_translation_is_explicit_and_sanitized(self) -> None:
        overloaded = http_error_from_adapter(AdapterCapacityError("secret queue detail"))
        failed = http_error_from_adapter(AdapterProtocolError("secret protocol detail"))
        self.assertEqual((overloaded.status, overloaded.detail), (503, "service is at capacity"))
        self.assertEqual((failed.status, failed.detail), (500, "service execution failed"))

    def test_registry_rejects_aliases_and_preserves_interrupts(self) -> None:
        events: list[str] = []
        shared = FakeAdapter(events, "shared")
        registry = AdapterRegistry(primary=shared)
        with self.assertRaisesRegex(ValueError, "registered as"):
            registry.register("alias", shared)

        class InterruptingAdapter(FakeAdapter):
            def shutdown(self, wait=True, cancel_pending=False) -> None:
                super().shutdown(wait=wait, cancel_pending=cancel_pending)
                raise KeyboardInterrupt("stop")

        trailing = FakeAdapter(events, "trailing")
        interrupting = InterruptingAdapter(events, "interrupting")
        registry = AdapterRegistry(trailing=trailing, interrupting=interrupting)
        with self.assertRaisesRegex(KeyboardInterrupt, "stop"):
            registry.shutdown()
        self.assertEqual(events[-2:], ["interrupting", "trailing"])
        self.assertEqual(trailing.shutdown_calls, 1)

    def test_registry_aggregates_ordinary_shutdown_failures(self) -> None:
        events: list[str] = []

        class BrokenAdapter(FakeAdapter):
            def shutdown(self, wait=True, cancel_pending=False) -> None:
                super().shutdown(wait=wait, cancel_pending=cancel_pending)
                raise RuntimeError(self.name)

        registry = AdapterRegistry(
            first=BrokenAdapter(events, "first"),
            second=BrokenAdapter(events, "second"),
        )
        with self.assertRaises(AdapterShutdownError) as raised:
            registry.shutdown()
        self.assertEqual(events, ["second", "first"])
        self.assertEqual([name for name, _ in raised.exception.failures], ["second", "first"])

    def test_route_handler_uses_thread_and_asyncio_adapters(self) -> None:
        runtime = SmallOS().setKernel(Unix())
        app = SmallServer()
        scheduler_thread = threading.get_ident()

        with AdapterRegistry(
            blocking=ThreadAdapter(max_workers=1, max_pending=4),
            foreign_async=AsyncioAdapter(max_pending=4),
        ) as services:

            async def foreign_thread_id() -> int:
                await asyncio.sleep(0)
                return threading.get_ident()

            @app.get("/adapter-threads")
            async def adapter_threads(request):
                worker_thread = await services.call("blocking", threading.get_ident)
                loop_thread = await services.call("foreign_async", foreign_thread_id)
                return Response.json(
                    {
                        "worker_is_foreign": worker_thread != scheduler_thread,
                        "loop_is_foreign": loop_thread != scheduler_thread,
                        "workers_are_distinct": worker_thread != loop_thread,
                    }
                )

            async def dispatch(task):
                return await app.dispatch(Request("GET", "/adapter-threads", Headers()))

            target = SmallTask(2, dispatch, name="smallserver-adapter-test")
            runtime.fork(target)
            runtime.start()

            if target.exception is not None:
                raise target.exception
            assert target.result is not None
            payload = json.loads(target.result.body.decode("utf-8"))
            self.assertEqual(
                payload,
                {
                    "worker_is_foreign": True,
                    "loop_is_foreign": True,
                    "workers_are_distinct": True,
                },
            )

        self.assertTrue(services.closed)
